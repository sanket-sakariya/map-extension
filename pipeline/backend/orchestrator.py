"""
Orchestrator worker — runs as a background thread.
1. Watches query_queue in Redis
2. Finds available workflow scrapers (with tunnel URLs)
3. Sends scrape request to a free scraper
4. Polls job status
5. Pushes completed results to result_queue
"""
import json
import time
import threading
import httpx
import redis as redis_lib
from config import REDIS_URL

_running = False
_thread = None


def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def start(pat_getter):
    """Start orchestrator in background. pat_getter() returns current PAT."""
    global _running, _thread
    if _running:
        return
    _running = True
    _thread = threading.Thread(target=_loop, args=(pat_getter,), daemon=True)
    _thread.start()


def stop():
    global _running
    _running = False


def _loop(pat_getter):
    global _running
    r = get_redis()

    while _running:
        try:
            # Check if there are queries to process
            query = r.lpop("query_queue")
            if not query:
                time.sleep(5)
                continue

            # Get available scrapers from active_scrapers set
            scrapers_raw = r.smembers("active_scrapers")
            if not scrapers_raw:
                # No scrapers available, push query back
                r.rpush("query_queue", query)
                time.sleep(10)
                continue

            # Find a free scraper (not currently busy)
            assigned = False
            for scraper_json in scrapers_raw:
                scraper = json.loads(scraper_json)
                tunnel_url = scraper.get("tunnel_url")
                if not tunnel_url:
                    continue

                # Check if scraper is free
                lock_key = f"scraper_busy:{tunnel_url}"
                if r.exists(lock_key):
                    continue

                # Lock this scraper
                r.set(lock_key, "1", ex=600)  # 10 min max

                # Send scrape request
                try:
                    resp = httpx.post(
                        f"{tunnel_url}/api/v1/scrape",
                        json={"query": query},
                        timeout=15
                    )
                    data = resp.json()
                    if data.get("status") == "started":
                        job_id = data["job_id"]
                        # Track this job
                        job_info = json.dumps({
                            "query": query,
                            "job_id": job_id,
                            "tunnel_url": tunnel_url,
                            "started_at": time.time()
                        })
                        r.rpush("active_jobs", job_info)
                        assigned = True
                        break
                    elif data.get("status") == "busy":
                        # Scraper busy, try next
                        r.delete(lock_key)
                        continue
                    else:
                        r.delete(lock_key)
                        continue
                except Exception:
                    r.delete(lock_key)
                    continue

            if not assigned:
                # Push query back if no scraper available
                r.rpush("query_queue", query)
                time.sleep(5)

        except Exception as e:
            time.sleep(5)

    # Also run job poller in same loop
    _poll_active_jobs(r)


def _poll_active_jobs(r):
    """Check active jobs for completion."""
    jobs_to_process = []
    length = r.llen("active_jobs")

    for _ in range(length):
        job_raw = r.lpop("active_jobs")
        if not job_raw:
            break
        jobs_to_process.append(json.loads(job_raw))

    for job in jobs_to_process:
        tunnel_url = job["tunnel_url"]
        job_id = job["job_id"]
        query = job["query"]

        try:
            resp = httpx.get(f"{tunnel_url}/api/v1/status/{job_id}", timeout=10)
            data = resp.json()

            if data.get("status") == "done":
                # Push results to result_queue
                result = {
                    "query": query,
                    "results": data.get("results", []),
                    "total": data.get("total", 0)
                }
                r.rpush("result_queue", json.dumps(result))
                # Unlock scraper
                r.delete(f"scraper_busy:{tunnel_url}")
            elif data.get("status") == "error":
                r.delete(f"scraper_busy:{tunnel_url}")
            else:
                # Still running, put back
                r.rpush("active_jobs", json.dumps(job))
        except Exception:
            # Connection error — put back for retry
            if time.time() - job.get("started_at", 0) < 600:
                r.rpush("active_jobs", json.dumps(job))
            else:
                # Timed out, release
                r.delete(f"scraper_busy:{tunnel_url}")


def poll_once():
    """Single poll cycle — called from main loop."""
    r = get_redis()
    _poll_active_jobs(r)
