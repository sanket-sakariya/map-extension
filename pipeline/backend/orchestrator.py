"""
Orchestrator worker — background thread.
Uses round-robin to distribute queries across available workflow scrapers.
"""
import json
import time
import threading
import httpx
import redis as redis_lib
from config import REDIS_URL

_running = False
_thread = None
_rr_index = 0  # round-robin counter


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
    """Get ordered list of scrapers from Redis set."""
    raw = r.smembers("active_scrapers")
    scrapers = []
    for item in raw:
        try:
            scrapers.append(json.loads(item))
        except:
            pass
    # Sort by run_id for stable ordering
    scrapers.sort(key=lambda s: s.get("run_id", 0))
    return scrapers


def _pick_scraper_round_robin(scrapers: list[dict], r) -> dict | None:
    """Round-robin selection, skipping busy scrapers."""
    global _rr_index
    if not scrapers:
        return None

    n = len(scrapers)
    for _ in range(n):
        idx = _rr_index % n
        _rr_index += 1
        candidate = scrapers[idx]
        tunnel_url = candidate.get("tunnel_url")
        if not tunnel_url:
            continue
        # Skip if busy
        if r.exists(f"scraper_busy:{tunnel_url}"):
            continue
        return candidate

    return None  # all busy


def _loop(pat_getter):
    global _running
    r = get_redis()

    while _running:
        try:
            # Poll active jobs first
            _poll_active_jobs(r)

            # Check query queue
            query = r.lpop("query_queue")
            if not query:
                time.sleep(3)
                continue

            scrapers = _get_scrapers(r)
            scraper = _pick_scraper_round_robin(scrapers, r)

            if not scraper:
                # No free scraper, push back and wait
                r.rpush("query_queue", query)
                time.sleep(5)
                continue

            tunnel_url = scraper["tunnel_url"]

            # Lock this scraper
            r.set(f"scraper_busy:{tunnel_url}", "1", ex=600)

            # Send scrape request
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
                elif data.get("status") == "busy":
                    # Already busy (shouldn't happen with lock, but handle)
                    r.rpush("query_queue", query)
                else:
                    # Error — push query back, release lock
                    r.rpush("query_queue", query)
                    r.delete(f"scraper_busy:{tunnel_url}")
            except Exception:
                r.rpush("query_queue", query)
                r.delete(f"scraper_busy:{tunnel_url}")

        except Exception:
            time.sleep(5)


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
                # Still running
                requeue.append(job_raw)
        except Exception:
            # Connection failed — check timeout
            if time.time() - job.get("started_at", 0) < 600:
                requeue.append(job_raw)
            else:
                r.delete(f"scraper_busy:{tunnel_url}")

    # Put still-running jobs back
    for item in requeue:
        r.rpush("active_jobs", item)
