"""
Orchestrator worker — background thread.
Assigns queries to ALL available scrapers in parallel (round-robin).
Polls active jobs for completion.
"""
import json
import time
import threading
import httpx
import redis as redis_lib
from config import REDIS_URL

_running = False
_thread = None
_rr_index = 0


def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def start(pat_getter):
    global _running, _thread
    if _running:
        return
    _running = True
    _thread = threading.Thread(target=_loop, args=(pat_getter,), daemon=True)
    _thread.start()


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
    """Return all scrapers that are NOT currently busy."""
    free = []
    for s in scrapers:
        tunnel_url = s.get("tunnel_url")
        if tunnel_url and not r.exists(f"scraper_busy:{tunnel_url}"):
            free.append(s)
    return free


def _loop(pat_getter):
    global _running
    r = get_redis()

    while _running:
        try:
            # Poll active jobs first
            _poll_active_jobs(r)

            # Get queue length
            queue_len = r.llen("query_queue")
            if not queue_len:
                time.sleep(2)
                continue

            # Get all free scrapers
            scrapers = _get_scrapers(r)
            free_scrapers = _get_free_scrapers(scrapers, r)

            if not free_scrapers:
                time.sleep(3)
                continue

            # Assign one query to each free scraper
            assigned = 0
            for scraper in free_scrapers:
                query = r.lpop("query_queue")
                if not query:
                    break  # queue empty

                tunnel_url = scraper["tunnel_url"]

                # Lock scraper
                r.set(f"scraper_busy:{tunnel_url}", "1", ex=600)

                # Send scrape request (non-blocking, fire and move on)
                try:
                    resp = httpx.post(
                        f"{tunnel_url}/api/v1/scrape",
                        json={"query": query},
                        timeout=15
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
                        assigned += 1
                    elif data.get("status") == "busy":
                        # Scraper actually busy, push query back
                        r.rpush("query_queue", query)
                        # Don't release lock — it's genuinely busy
                    else:
                        # Error, push query back, release lock
                        r.rpush("query_queue", query)
                        r.delete(f"scraper_busy:{tunnel_url}")
                except Exception:
                    # Connection failed, push query back, release lock
                    r.rpush("query_queue", query)
                    r.delete(f"scraper_busy:{tunnel_url}")

            # Small sleep between assignment cycles
            time.sleep(2)

        except Exception:
            time.sleep(3)


def _poll_active_jobs(r):
    """Check active jobs for completion, push results to result_queue."""
    length = r.llen("active_jobs")
    if not length:
        return

    requeue = []
    for _ in range(length):
        job_raw = r.lpop("active_jobs")
        if not job_raw:
            break

        job = json.loads(job_raw)
        tunnel_url = job["tunnel_url"]
        job_id = job["job_id"]
        query = job["query"]

        try:
            resp = httpx.get(f"{tunnel_url}/api/v1/status/{job_id}", timeout=10)
            data = resp.json()

            if data.get("status") == "done":
                result = {
                    "query": query,
                    "results": data.get("results", []),
                    "total": data.get("total", 0)
                }
                r.rpush("result_queue", json.dumps(result))
                r.delete(f"scraper_busy:{tunnel_url}")
            elif data.get("status") == "error":
                r.delete(f"scraper_busy:{tunnel_url}")
            else:
                # Still running — keep it
                requeue.append(job_raw)
        except Exception:
            # Connection failed
            if time.time() - job.get("started_at", 0) < 600:
                requeue.append(job_raw)
            else:
                # Timed out, release
                r.delete(f"scraper_busy:{tunnel_url}")

    # Put still-running jobs back
    for item in requeue:
        r.rpush("active_jobs", item)
