"""
Orchestrator worker — background threads.
- Assignment thread: assigns queries to free scrapers
- Poller thread: polls active jobs for completion
- DLX: failed queries go to dead_letter_queue with retry count, retried up to 3 times
"""
import json
import time
import threading
import httpx
import redis as redis_lib
from config import REDIS_URL

_running = False
_assign_thread = None
_poll_thread = None

MAX_RETRIES = 3
JOB_TIMEOUT = 480  # 8 min max per job


def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def start(pat_getter):
    global _running, _assign_thread, _poll_thread
    if _running:
        return
    _running = True
    _assign_thread = threading.Thread(target=_assign_loop, daemon=True)
    _poll_thread = threading.Thread(target=_poll_loop, daemon=True)
    _assign_thread.start()
    _poll_thread.start()


def stop():
    global _running
    _running = False


def _get_scrapers(r) -> list[dict]:
    raw = r.smembers("active_scrapers")
    scrapers = []
    for item in raw:
        try:
            scrapers.append(json.loads(item))
        except:
            pass
    scrapers.sort(key=lambda s: s.get("run_id", 0))
    return scrapers


def _get_free_scrapers(scrapers: list[dict], r) -> list[dict]:
    free = []
    for s in scrapers:
        tunnel_url = s.get("tunnel_url")
        if tunnel_url and not r.exists(f"scraper_busy:{tunnel_url}"):
            free.append(s)
    return free


def _push_to_dlx(r, query: str, reason: str):
    """Push failed query to DLX with retry tracking."""
    key = f"dlx:{query}"
    retries = int(r.get(key) or 0)
    retries += 1

    if retries <= MAX_RETRIES:
        # Retry — push back to main queue
        r.set(key, str(retries), ex=3600)  # track retries for 1 hour
        r.rpush("query_queue", query)
        r.incr("stats:dlx_retries")
    else:
        # Exhausted retries — move to dead letter queue
        r.rpush("dead_letter_queue", json.dumps({
            "query": query,
            "reason": reason,
            "retries": retries,
            "failed_at": time.time()
        }))
        r.delete(key)
        r.incr("stats:dlx_dead")


# ─── Assignment Thread ──────────────────────────────────────────────────────

def _assign_loop():
    global _running
    r = get_redis()

    while _running:
        try:
            queue_len = r.llen("query_queue")
            if not queue_len:
                time.sleep(1)
                continue

            scrapers = _get_scrapers(r)
            free_scrapers = _get_free_scrapers(scrapers, r)

            if not free_scrapers:
                time.sleep(1)
                continue

            # Assign to all free scrapers
            for scraper in free_scrapers:
                query = r.lpop("query_queue")
                if not query:
                    break

                tunnel_url = scraper["tunnel_url"]
                r.set(f"scraper_busy:{tunnel_url}", "1", ex=JOB_TIMEOUT)

                try:
                    resp = httpx.post(
                        f"{tunnel_url}/api/v1/scrape",
                        json={"query": query},
                        timeout=10
                    )
                    data = resp.json()
                    if data.get("status") == "started":
                        job_info = json.dumps({
                            "query": query,
                            "job_id": data["job_id"],
                            "tunnel_url": tunnel_url,
                            "started_at": time.time()
                        })
                        r.rpush("active_jobs", job_info)
                    elif data.get("status") == "busy":
                        r.rpush("query_queue", query)
                    else:
                        _push_to_dlx(r, query, f"scraper_error: {data.get('message','unknown')}")
                        r.delete(f"scraper_busy:{tunnel_url}")
                except httpx.ConnectError:
                    # Scraper dead — remove from active set and DLX the query
                    _remove_dead_scraper(r, scraper)
                    _push_to_dlx(r, query, "connect_error")
                except httpx.TimeoutException:
                    _push_to_dlx(r, query, "request_timeout")
                    r.delete(f"scraper_busy:{tunnel_url}")
                except Exception as e:
                    _push_to_dlx(r, query, str(e)[:100])
                    r.delete(f"scraper_busy:{tunnel_url}")

            time.sleep(0.5)  # Fast cycle

        except Exception:
            time.sleep(2)


def _remove_dead_scraper(r, scraper: dict):
    """Remove a dead scraper from the active set."""
    tunnel_url = scraper.get("tunnel_url", "")
    r.delete(f"scraper_busy:{tunnel_url}")
    # Remove from set
    for member in r.smembers("active_scrapers"):
        try:
            data = json.loads(member)
            if data.get("tunnel_url") == tunnel_url:
                r.srem("active_scrapers", member)
        except:
            pass


# ─── Poll Thread ────────────────────────────────────────────────────────────

def _poll_loop():
    global _running
    r = get_redis()

    while _running:
        try:
            length = r.llen("active_jobs")
            if not length:
                time.sleep(1)
                continue

            requeue = []
            for _ in range(length):
                job_raw = r.lpop("active_jobs")
                if not job_raw:
                    break

                job = json.loads(job_raw)
                tunnel_url = job["tunnel_url"]
                job_id = job["job_id"]
                query = job["query"]
                started = job.get("started_at", 0)

                # Check timeout
                if time.time() - started > JOB_TIMEOUT:
                    r.delete(f"scraper_busy:{tunnel_url}")
                    _push_to_dlx(r, query, "job_timeout")
                    continue

                try:
                    resp = httpx.get(f"{tunnel_url}/api/v1/status/{job_id}", timeout=8)
                    data = resp.json()

                    if data.get("status") == "done":
                        results = data.get("results", [])
                        if results:
                            r.rpush("result_queue", json.dumps({
                                "query": query,
                                "results": results,
                                "total": data.get("total", 0)
                            }))
                        r.delete(f"scraper_busy:{tunnel_url}")
                        r.incr("stats:completed_jobs")
                    elif data.get("status") == "error":
                        r.delete(f"scraper_busy:{tunnel_url}")
                        _push_to_dlx(r, query, f"job_error: {data.get('error','')[:80]}")
                    else:
                        # Still running
                        requeue.append(job_raw)
                except httpx.ConnectError:
                    _remove_dead_scraper(r, {"tunnel_url": tunnel_url})
                    _push_to_dlx(r, query, "poll_connect_error")
                except Exception:
                    if time.time() - started < JOB_TIMEOUT:
                        requeue.append(job_raw)
                    else:
                        r.delete(f"scraper_busy:{tunnel_url}")
                        _push_to_dlx(r, query, "poll_timeout")

            for item in requeue:
                r.rpush("active_jobs", item)

            time.sleep(1)  # Poll every second

        except Exception:
            time.sleep(2)
