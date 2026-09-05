"""
Domain pinger worker — the long-running process behind the `domain-pinger`
Deployment.

Runs the same image as the API (only the command differs), so there is no second
image to build or version. Any number of replicas can run: batches are claimed
with FOR UPDATE SKIP LOCKED, so replicas never collide and scaling is linear.

  loop:
    claim N due domains (leased, so a crash just releases them)
    check them concurrently (DNS always, RDAP where it earns its cost)
    write the batch back in one statement
"""
import asyncio
import os
import signal
import sys
import time

import redis as redis_lib
from sqlalchemy import text as sql_text

from config import REDIS_URL
from database import engine
import checker
import domains as dom

BATCH_SIZE = int(os.getenv("PING_BATCH_SIZE", "500"))
IDLE_SLEEP = int(os.getenv("PING_IDLE_SLEEP", "30"))   # seconds when queue is empty
ENABLED_CACHE_TTL = 10                                  # seconds

_stop = False


def _log(msg: str):
    print(f"[pinger] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def _handle_signal(signum, _frame):
    global _stop
    _stop = True
    _log(f"signal {signum} received — finishing current batch then exiting")


def is_enabled(cache={"value": None, "at": 0.0}) -> bool:
    """
    Read the `pinger_enabled` settings row, cached briefly. Lets the UI pause all
    workers without a rollout, at the cost of one tiny query every 10s.
    Defaults to enabled when the row is absent.
    """
    now = time.monotonic()
    if cache["value"] is not None and now - cache["at"] < ENABLED_CACHE_TTL:
        return cache["value"]
    try:
        with engine.connect() as conn:
            row = conn.execute(sql_text(
                "SELECT value FROM settings WHERE key = 'pinger_enabled'"
            )).fetchone()
        value = (row[0].lower() != "false") if row else True
    except Exception as e:
        _log(f"enabled-check failed, assuming enabled: {e}")
        value = True
    cache["value"], cache["at"] = value, now
    return value


def _publish_heartbeat(r, **fields):
    try:
        r.hset("domains:pinger:stats", mapping={k: str(v) for k, v in fields.items()})
        r.expire("domains:pinger:stats", 300)
    except Exception:
        pass


async def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    worker_id = os.getenv("HOSTNAME", "pinger")
    r = redis_lib.from_url(REDIS_URL, decode_responses=True)

    dom.ensure_schema()
    _log(f"worker {worker_id} starting — batch={BATCH_SIZE} "
         f"concurrency={checker.DNS_CONCURRENCY} rdap_rps={checker.RDAP_RPS}")

    total_checked = 0
    async with checker.DomainChecker(r) as chk:
        _log(f"RDAP bootstrap: {len(chk._bootstrap)} TLDs mapped")
        while not _stop:
            if not is_enabled():
                _publish_heartbeat(r, worker=worker_id, state="paused", checked=total_checked)
                await asyncio.sleep(IDLE_SLEEP)
                continue

            try:
                batch = await asyncio.to_thread(dom.claim_batch, BATCH_SIZE)
            except Exception as e:
                _log(f"claim failed: {e}")
                await asyncio.sleep(10)
                continue

            if not batch:
                _publish_heartbeat(r, worker=worker_id, state="idle", checked=total_checked)
                await asyncio.sleep(IDLE_SLEEP)
                continue

            t0 = time.monotonic()
            try:
                results = await chk.check_many(batch)
            except Exception as e:
                # The lease expires on its own, so these rows come back around
                # rather than being lost.
                _log(f"batch check failed ({len(batch)} domains): {e}")
                await asyncio.sleep(5)
                continue

            try:
                await asyncio.to_thread(dom.write_results, results)
            except Exception as e:
                _log(f"write-back failed: {e}")
                await asyncio.sleep(5)
                continue

            elapsed = time.monotonic() - t0
            total_checked += len(results)
            counts = {}
            for res in results:
                counts[res["status"]] = counts.get(res["status"], 0) + 1
            _log(f"{len(results)} in {elapsed:.1f}s ({len(results)/max(elapsed,0.01):.0f}/s) {counts}")
            ids = getattr(chk, "_ids", None)
            _publish_heartbeat(
                r, worker=worker_id, state="running", checked=total_checked,
                last_batch=len(results), last_rate=round(len(results) / max(elapsed, 0.01), 1),
                last_at=time.strftime("%Y-%m-%d %H:%M:%S"),
                ids_chunks_ok=getattr(ids, "chunks_ok", 0),
                ids_chunks_failed=getattr(ids, "chunks_failed", 0),
                ids_unanswered=getattr(ids, "domains_unanswered", 0),
            )

    _log(f"stopped after {total_checked} domains")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
