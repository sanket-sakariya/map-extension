"""
Business deduplication.

The `businesses` table was keyed UNIQUE(cid, query), so the same real business
was stored once per search query that happened to return it. Measured on the
live table: 4,107,935 rows for 629,936 actual businesses — 85% duplication,
averaging 19.7 copies each and peaking at 263. Sampling showed 94% of duplicate
groups differ ONLY in the `query` column; the rest differ in review_count or
phone because copies were scraped weeks apart.

The fix keeps one row per business and moves the query association — which is a
genuine many-to-many, and which the whole Results page browses by — into its own
narrow table:

    businesses        one row per cid            (629,936 rows)
    business_queries  (cid, query) association   (4,107,935 narrow rows)

Nothing is lost: every query that ever found a business is still recorded, so
"show me everything the query 'plumber in Austin' returned" still answers
exactly as before.

cid is a safe key: on the live table cid and place_id are a perfect 1:1
bijection (629,936 distinct values each, 629,936 distinct pairs).

The migration runs online in chunks with a commit per chunk. A single statement
over 4.1M rows would hold locks long enough to stall the scrapers, and would
report no progress until it finished.
"""
import threading
import time

import redis as redis_lib

from config import REDIS_URL
from database import engine

SCHEMA_DDL = """
-- Every (business, query) pair that was previously duplicated into a whole
-- business row. Narrow on purpose: this table carries the row count so the
-- wide table (with its JSONB hours/reviews) does not have to.
CREATE TABLE IF NOT EXISTS business_queries (
    cid        TEXT NOT NULL,
    query      TEXT NOT NULL,
    scraped_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (cid, query)
);

-- "Which businesses did this query return?" and the query-list page's
-- GROUP BY query + MAX(scraped_at).
CREATE INDEX IF NOT EXISTS idx_bq_query ON business_queries(query);
CREATE INDEX IF NOT EXISTS idx_bq_query_scraped ON business_queries(query, scraped_at DESC);
-- Substring search over the query text, matching the old idx_biz_query_trgm.
CREATE INDEX IF NOT EXISTS idx_bq_query_trgm ON business_queries USING GIN (query gin_trgm_ops);
"""

# Rows per transaction. Small enough to commit quickly and report progress,
# large enough that per-statement overhead stays irrelevant.
BACKFILL_CHUNK = 250_000
DELETE_CHUNK = 50_000

LOCK_KEY = "businesses:dedupe:running"
STATE_KEY = "businesses:dedupe:state"


def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def ensure_schema():
    """Create business_queries. Safe to call on every boot."""
    with engine.begin() as conn:
        conn.exec_driver_sql("SET statement_timeout = 0")
        conn.exec_driver_sql(SCHEMA_DDL)


def _set(r, **fields):
    r.hset(STATE_KEY, mapping={k: str(v) for k, v in fields.items()})
    r.expire(STATE_KEY, 604800)


def status() -> dict:
    r = get_redis()
    s = r.hgetall(STATE_KEY) or {}
    return {
        "running": bool(r.get(LOCK_KEY)),
        "phase": s.get("phase", "idle"),
        "backfilled": int(s.get("backfilled", 0)),
        "duplicates_found": int(s.get("duplicates_found", 0)),
        "deleted": int(s.get("deleted", 0)),
        "unique_kept": int(s.get("unique_kept", 0)),
        "progress": int(s.get("progress", 0)),
        "total": int(s.get("total", 0)),
        "started_at": s.get("started_at", ""),
        "finished_at": s.get("finished_at", ""),
        "error": s.get("error", ""),
        # A failure here means the space was not reclaimed; the deduplication
        # itself had already committed.
        "reclaim_error": s.get("reclaim_error", ""),
    }


def reclaim_only():
    """
    VACUUM + REINDEX on their own, for when the dedupe committed but the
    housekeeping could not finish.
    """
    conn = engine.raw_connection()
    try:
        conn.set_isolation_level(0)
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 0")
        cur.execute("SET max_parallel_maintenance_workers = 0")
        cur.execute("VACUUM (ANALYZE) businesses")
        cur.execute("REINDEX TABLE businesses")
    finally:
        conn.close()


def _reclaim_worker():
    r = get_redis()
    _set(r, phase="reclaiming", reclaim_error="",
         started_at=time.strftime("%Y-%m-%d %H:%M:%S"), finished_at="")
    try:
        reclaim_only()
        _set(r, phase="done", finished_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        _set(r, phase="done", reclaim_error=str(e)[:300],
             finished_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        r.delete(LOCK_KEY)


def start_reclaim() -> bool:
    r = get_redis()
    if not r.set(LOCK_KEY, "1", nx=True, ex=86400):
        return False
    threading.Thread(target=_reclaim_worker, daemon=True).start()
    return True


def start(reclaim: bool = True) -> bool:
    """Kick off the migration in the background. False if one is already running."""
    r = get_redis()
    if not r.set(LOCK_KEY, "1", nx=True, ex=86400):
        return False
    threading.Thread(target=_worker, args=(reclaim,), daemon=True).start()
    return True


def _worker(reclaim: bool):
    r = get_redis()
    _set(r, phase="starting", backfilled=0, duplicates_found=0, deleted=0,
         unique_kept=0, progress=0, total=0, error="",
         started_at=time.strftime("%Y-%m-%d %H:%M:%S"), finished_at="")
    try:
        _run(r, reclaim)
        _set(r, phase="done", finished_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        _set(r, phase="failed", error=str(e)[:500],
             finished_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        r.delete(LOCK_KEY)


def _run(r, reclaim: bool):
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 0")
        cur.execute("SET work_mem = '256MB'")
        conn.commit()

        cur.execute("SELECT COALESCE(MAX(id), 0), COUNT(*) FROM businesses")
        max_id, total_rows = cur.fetchone()

        # ── Phase 1: preserve every (business, query) association ──
        # Runs before anything is deleted, so a failure here loses nothing.
        _set(r, phase="backfill", total=max_id, progress=0)
        backfilled = 0
        lo = 0
        while lo <= max_id:
            hi = lo + BACKFILL_CHUNK
            cur.execute("""
                INSERT INTO business_queries (cid, query, scraped_at)
                SELECT DISTINCT ON (cid, query) cid, query, scraped_at
                  FROM businesses
                 WHERE id >= %s AND id < %s
                   AND cid IS NOT NULL AND cid <> ''
                   AND query IS NOT NULL AND query <> ''
                 ORDER BY cid, query, scraped_at ASC
                ON CONFLICT (cid, query) DO NOTHING
            """, (lo, hi))
            backfilled += cur.rowcount
            conn.commit()
            lo = hi
            _set(r, phase="backfill", progress=min(lo, max_id), backfilled=backfilled)

        # ── Phase 2: pick the survivor for each business ──
        # Only `id` is selected, so the sort works on a narrow tuple instead of
        # dragging 9.5GB of wide rows (JSONB hours/reviews) through it.
        # Freshest wins, because copies scraped weeks apart differ in
        # review_count and rating and the newest scrape is the truthful one.
        _set(r, phase="selecting")
        cur.execute("DROP TABLE IF EXISTS dedupe_keep")
        cur.execute("""
            CREATE UNLOGGED TABLE dedupe_keep AS
            SELECT DISTINCT ON (cid) id
              FROM businesses
             WHERE cid IS NOT NULL AND cid <> ''
             ORDER BY cid, scraped_at DESC NULLS LAST, id DESC
        """)
        cur.execute("CREATE UNIQUE INDEX ON dedupe_keep(id)")
        cur.execute("ANALYZE dedupe_keep")
        cur.execute("SELECT count(*) FROM dedupe_keep")
        unique_kept = cur.fetchone()[0]
        conn.commit()

        duplicates = max(total_rows - unique_kept, 0)
        _set(r, phase="selecting", unique_kept=unique_kept, duplicates_found=duplicates)

        # ── Phase 3: drop the duplicates ──
        # Rows with a NULL/empty cid cannot be deduplicated and are kept as-is.
        _set(r, phase="deleting", total=max_id, progress=0)
        deleted = 0
        lo = 0
        while lo <= max_id:
            hi = lo + DELETE_CHUNK
            cur.execute("""
                DELETE FROM businesses b
                 WHERE b.id >= %s AND b.id < %s
                   AND b.cid IS NOT NULL AND b.cid <> ''
                   AND NOT EXISTS (SELECT 1 FROM dedupe_keep k WHERE k.id = b.id)
            """, (lo, hi))
            deleted += cur.rowcount
            conn.commit()
            lo = hi
            _set(r, phase="deleting", progress=min(lo, max_id), deleted=deleted)

        cur.execute("DROP TABLE IF EXISTS dedupe_keep")
        conn.commit()

        # ── Phase 4: make duplicates impossible from here on ──
        _set(r, phase="constraining")
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS businesses_cid_key
                ON businesses(cid) WHERE cid IS NOT NULL AND cid <> ''
        """)
        # The old key is what allowed one row per (business, query); with the
        # association moved out it is both wrong and a wasted index.
        cur.execute("ALTER TABLE businesses DROP CONSTRAINT IF EXISTS businesses_cid_query_key")
        conn.commit()

        # ── Phase 5: give the space back ──
        # A delete of this size leaves the heap and every index mostly dead
        # tuples. Without this the table still occupies its pre-dedupe size and
        # every scan reads the same number of pages.
        # Reported separately from the migration itself: by this point the
        # deduplication is committed and correct, so a housekeeping failure must
        # not be reported as "the migration failed".
        reclaim_error = ""
        if reclaim:
            try:
                _set(r, phase="reclaiming")
                conn.set_isolation_level(0)  # VACUUM cannot run in a transaction
                # Serial, not parallel. A parallel maintenance worker sizes its
                # dynamic shared memory from maintenance_work_mem and puts it in
                # /dev/shm, which containers default to 64MB — so a parallel
                # REINDEX dies with "could not resize shared memory segment".
                cur.execute("SET max_parallel_maintenance_workers = 0")
                cur.execute("VACUUM (ANALYZE) businesses")
                _set(r, phase="reindexing")
                cur.execute("REINDEX TABLE businesses")
                conn.set_isolation_level(1)
                conn.commit()
            except Exception as e:
                reclaim_error = str(e)[:300]
                try:
                    conn.set_isolation_level(1)
                    conn.rollback()
                except Exception:
                    pass

        _set(r, phase="done", deleted=deleted, unique_kept=unique_kept,
             backfilled=backfilled, duplicates_found=duplicates,
             reclaim_error=reclaim_error)
    finally:
        conn.close()
