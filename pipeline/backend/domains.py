"""
Domain registry — normalization, schema, sync-from-businesses, and the
claim/write-back primitives used by the pinger workers.

The `domains` table is a work queue as much as it is a data table: workers claim
due rows with FOR UPDATE SKIP LOCKED, so any number of pinger replicas can run
concurrently without coordinating through Redis.
"""
import re
import threading
import time
from urllib.parse import urlsplit

import redis as redis_lib
from psycopg2.extras import execute_values

from config import REDIS_URL
from database import engine

# ─── Status vocabulary ──────────────────────────────────────────────────────
# Kept as plain strings (not an enum) so they can be filtered straight from the
# query string without a translation layer.
PENDING = "pending"           # never checked
ACTIVE = "active"             # resolves in DNS and/or RDAP says registered
EXPIRED = "expired"           # RDAP 404 / past expiry / repeated NXDOMAIN
EXPIRING_SOON = "expiring_soon"  # RDAP expiry within EXPIRING_WINDOW_DAYS
ERROR = "error"               # transient resolver failures, will retry
INVALID = "invalid"           # not a syntactically usable hostname

ALL_STATUSES = [PENDING, ACTIVE, EXPIRED, EXPIRING_SOON, ERROR, INVALID]

EXPIRING_WINDOW_DAYS = 30

# Hostname must be dot-separated LDH labels. Rejects IPs, ports, paths, unicode
# leftovers — anything that survived normalization but isn't resolvable.
_HOSTNAME_RE = re.compile(
    r"^(?!-)[a-z0-9-]{1,63}(?<!-)(\.(?!-)[a-z0-9-]{1,63}(?<!-))+$"
)
_ALL_NUMERIC_RE = re.compile(r"^[0-9.]+$")


def extract_domain(raw_url: str) -> str:
    """
    Extract a clean, bare domain (no protocol, no leading www., no path/query/fragment)
    from a raw scraped website URL. Handles malformed source data gracefully:
    - Missing/duplicated protocol prefixes (e.g. "https://https:example.com")
    - Any number of leading "w" characters before a dot (e.g. "wwww.example.com")
    - "www." appearing as a non-leading label (e.g. "use.www.example.com" -> keeps
      the real registrable-looking tail, since stripping only a leading www. is safe
      but a mid-string www. is part of the actual hostname and left as-is)
    Returns "" if no usable domain can be derived.
    """
    if not raw_url:
        return ""
    url = raw_url.strip()
    # Ensure urlsplit sees a scheme so netloc parses correctly; if it already has
    # one (even a malformed doubled one) this is harmless since we only use netloc.
    if "://" not in url:
        url = "http://" + url
    try:
        netloc = urlsplit(url).netloc
    except Exception:
        netloc = ""
    if not netloc:
        # Fallback for URLs urlsplit couldn't parse at all
        netloc = url.split("//")[-1].split("/")[0]
    netloc = netloc.split("@")[-1]  # drop any userinfo (user:pass@)
    netloc = netloc.split(":")[0]   # drop port
    # Collapse any run of leading "w" characters immediately before a dot
    # (e.g. "wwww." or "ww." -> "www." is NOT assumed; we simply strip them like www.)
    netloc = re.sub(r"^w+\.", "", netloc, flags=re.IGNORECASE)
    return netloc.strip().lower()


def is_valid_domain(domain: str) -> bool:
    """True if `domain` is a syntactically resolvable hostname (not an IP)."""
    if not domain or len(domain) > 253:
        return False
    if _ALL_NUMERIC_RE.match(domain):
        return False  # bare IPv4 or garbage like "1.2"
    return bool(_HOSTNAME_RE.match(domain))


# Suffixes where the registrable domain is the last THREE labels rather than the
# last two. Not the full Public Suffix List — just the ones that actually show up
# in scraped business websites, so RDAP lookups target a real registrable name.
_MULTIPART_SUFFIXES = {
    "co.uk", "org.uk", "me.uk", "ltd.uk", "plc.uk", "net.uk", "sch.uk", "ac.uk", "gov.uk",
    "com.au", "net.au", "org.au", "edu.au", "gov.au", "id.au",
    "co.in", "net.in", "org.in", "firm.in", "gen.in", "ind.in", "ac.in", "edu.in", "gov.in",
    "co.nz", "net.nz", "org.nz", "govt.nz", "ac.nz",
    "com.br", "net.br", "org.br", "gov.br",
    "com.cn", "net.cn", "org.cn", "gov.cn", "edu.cn",
    "co.jp", "or.jp", "ne.jp", "ac.jp", "go.jp",
    "co.kr", "or.kr", "ne.kr",
    "co.za", "org.za", "net.za", "web.za",
    "com.mx", "com.ar", "com.co", "com.pe", "com.tr", "com.tw", "com.hk", "com.sg",
    "com.my", "com.ph", "com.vn", "com.pk", "com.bd", "com.ng", "com.eg", "com.sa",
    "com.ua", "com.pl", "com.ru", "com.es", "com.pt", "com.gr", "com.cy",
    "co.il", "co.id", "co.th", "co.ke", "co.ug", "co.tz",
    "org.il", "net.il", "gov.il",
}


def registrable_domain(domain: str) -> str:
    """
    Reduce a hostname to the name that is actually registered at a registrar, which
    is what RDAP can answer for. "sites.google.com" -> "google.com",
    "shop.example.co.uk" -> "example.co.uk". Returns "" when there is no dot.
    """
    if not domain:
        return ""
    parts = domain.split(".")
    if len(parts) < 2:
        return ""
    last_two = ".".join(parts[-2:])
    if last_two in _MULTIPART_SUFFIXES and len(parts) >= 3:
        return ".".join(parts[-3:])
    return last_two


def tld_of(domain: str) -> str:
    """Last label of the hostname, used for grouping/filtering in the UI."""
    return domain.rsplit(".", 1)[-1] if "." in domain else ""


# ─── Schema ─────────────────────────────────────────────────────────────────
# Applied idempotently on backend startup. init.sql only runs on a fresh volume,
# and this table has to appear on the existing (already-populated) database too.
SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS domains (
    id              BIGSERIAL PRIMARY KEY,
    domain          TEXT NOT NULL UNIQUE,
    registrable     TEXT,
    tld             TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    dns_status      TEXT,
    rdap_status     TEXT,
    registrar       TEXT,
    expiry_date     TIMESTAMP,
    created_date    TIMESTAMP,
    ns_records      JSONB,
    business_count  INTEGER DEFAULT 0,
    check_count     INTEGER DEFAULT 0,
    fail_streak     INTEGER DEFAULT 0,
    last_error      TEXT,
    last_checked_at TIMESTAMP,
    next_check_at   TIMESTAMP DEFAULT NOW(),
    first_seen_at   TIMESTAMP DEFAULT NOW()
);

-- The claim index. Partial so the permanently-parked rows ('invalid') never
-- bloat the queue scan, and worker claims stay index-only on a narrow set.
CREATE INDEX IF NOT EXISTS idx_domains_due
    ON domains(next_check_at) WHERE status <> 'invalid';

CREATE INDEX IF NOT EXISTS idx_domains_status ON domains(status);
CREATE INDEX IF NOT EXISTS idx_domains_status_domain ON domains(status, domain);
CREATE INDEX IF NOT EXISTS idx_domains_tld ON domains(tld);
CREATE INDEX IF NOT EXISTS idx_domains_registrable ON domains(registrable);

-- Sort indexes. Every list/export ORDER BY ends with ", domain ASC" as a stable
-- tiebreaker, so a single-column index does NOT satisfy it — Postgres falls back
-- to sorting the whole table, which is fine at 10k rows and fatal at 10M. These
-- composites mirror each ORDER BY (including NULLS placement) exactly so the
-- planner can walk the index and stop at LIMIT.
DROP INDEX IF EXISTS idx_domains_biz_count;
DROP INDEX IF EXISTS idx_domains_expiry;
CREATE INDEX IF NOT EXISTS idx_domains_biz_domain
    ON domains(business_count DESC NULLS LAST, domain ASC);
CREATE INDEX IF NOT EXISTS idx_domains_expiry_domain
    ON domains(expiry_date ASC NULLS LAST, domain ASC);
CREATE INDEX IF NOT EXISTS idx_domains_expiry_desc_domain
    ON domains(expiry_date DESC NULLS LAST, domain ASC);
CREATE INDEX IF NOT EXISTS idx_domains_checked_domain
    ON domains(last_checked_at DESC NULLS LAST, domain ASC);
-- Partial index for the "what is expiring / already expired" reports, which are
-- the queries a human actually runs on this page.
CREATE INDEX IF NOT EXISTS idx_domains_expiry_live
    ON domains(expiry_date) WHERE expiry_date IS NOT NULL;

-- Coverage reporting counts the never-checked rows. This partial index shrinks
-- to empty as the sweep completes, so the count stays instant at any table size.
CREATE INDEX IF NOT EXISTS idx_domains_unchecked
    ON domains(id) WHERE last_checked_at IS NULL;

-- Substring search over the domain column from the UI filter box.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_domains_trgm ON domains USING GIN (domain gin_trgm_ops);
"""


def ensure_schema():
    """Create the domains table + indexes if absent. Safe to call on every boot."""
    with engine.begin() as conn:
        # Index builds on a large table can exceed the pool's 15s statement cap.
        conn.exec_driver_sql("SET statement_timeout = 0")
        conn.exec_driver_sql(SCHEMA_DDL)


# ─── Sync: businesses.website -> domains ────────────────────────────────────

def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


SYNC_LOCK_KEY = "domains:sync:running"
SYNC_STATE_KEY = "domains:sync:state"

# Flush the in-memory dedupe map to the staging table every this many distinct
# domains. Bounds worker memory regardless of how many domains exist overall —
# Postgres does the final aggregation, not Python.
_FLUSH_EVERY = 200_000


def _set_sync_state(r, **fields):
    r.hset(SYNC_STATE_KEY, mapping={k: str(v) for k, v in fields.items()})
    r.expire(SYNC_STATE_KEY, 86400)


def sync_status() -> dict:
    r = get_redis()
    state = r.hgetall(SYNC_STATE_KEY) or {}
    return {
        "running": bool(r.get(SYNC_LOCK_KEY)),
        "phase": state.get("phase", "idle"),
        "scanned": int(state.get("scanned", 0)),
        "domains": int(state.get("domains", 0)),
        "inserted": int(state.get("inserted", 0)),
        "started_at": state.get("started_at", ""),
        "finished_at": state.get("finished_at", ""),
        "error": state.get("error", ""),
    }


def start_sync() -> bool:
    """Kick off a background sync. Returns False if one is already running."""
    r = get_redis()
    # NX lock with a TTL so a crashed sync can't wedge the feature forever.
    if not r.set(SYNC_LOCK_KEY, "1", nx=True, ex=7200):
        return False
    threading.Thread(target=_sync_worker, daemon=True).start()
    return True


def _sync_worker():
    r = get_redis()
    started = time.strftime("%Y-%m-%d %H:%M:%S")
    _set_sync_state(r, phase="scanning", scanned=0, domains=0, inserted=0,
                    started_at=started, finished_at="", error="")
    try:
        _run_sync(r)
        _set_sync_state(r, phase="done", finished_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        _set_sync_state(r, phase="failed", error=str(e)[:500],
                        finished_at=time.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        r.delete(SYNC_LOCK_KEY)


def _run_sync(r):
    """
    Stream every non-empty website, normalize it in Python (so the result is
    byte-identical to what /api/results/export-domains produces), and accumulate
    counts into an UNLOGGED staging table. Postgres then does the final GROUP BY
    and upsert, which keeps peak Python memory flat.

    Read and write use SEPARATE connections on purpose: the reader holds a
    server-side (named) cursor, and a WITHOUT HOLD cursor is destroyed the moment
    its connection commits. Flushing the staging batches on the same connection
    would kill the scan partway through the table.
    """
    read_conn = engine.raw_connection()
    write_conn = engine.raw_connection()
    try:
        wcur = write_conn.cursor()
        wcur.execute("SET statement_timeout = 0")
        wcur.execute("DROP TABLE IF EXISTS domains_stage")
        wcur.execute("CREATE UNLOGGED TABLE domains_stage (domain TEXT, cnt INTEGER)")
        write_conn.commit()

        rcur = read_conn.cursor()
        rcur.execute("SET statement_timeout = 0")
        scan = read_conn.cursor(name="website_scan")
        scan.itersize = 20000
        scan.execute(
            "SELECT website FROM businesses WHERE website IS NOT NULL AND website <> ''"
        )

        counts: dict[str, int] = {}
        scanned = 0
        flushed = 0

        def flush():
            nonlocal counts, flushed
            if not counts:
                return
            execute_values(
                wcur,
                "INSERT INTO domains_stage (domain, cnt) VALUES %s",
                list(counts.items()),
                page_size=10000,
            )
            write_conn.commit()
            flushed += len(counts)
            counts = {}

        for (website,) in scan:
            scanned += 1
            d = extract_domain(website or "")
            if d and is_valid_domain(d):
                counts[d] = counts.get(d, 0) + 1
            if len(counts) >= _FLUSH_EVERY:
                flush()
                _set_sync_state(r, scanned=scanned, domains=flushed)
            elif scanned % 100000 == 0:
                _set_sync_state(r, scanned=scanned, domains=flushed + len(counts))

        flush()
        scan.close()
        read_conn.rollback()  # release the reader's transaction
        _set_sync_state(r, phase="upserting", scanned=scanned, domains=flushed)

        wcur.execute("CREATE INDEX ON domains_stage (domain)")
        # New rows enter as 'pending' with next_check_at = NOW(), so the pingers
        # pick them up immediately. Existing rows only get their business_count
        # refreshed — their check history and schedule are preserved.
        wcur.execute("""
            INSERT INTO domains (domain, registrable, tld, business_count)
            SELECT domain,
                   NULL,
                   substring(domain from '\\.([a-z0-9-]+)$'),
                   SUM(cnt)::int
            FROM domains_stage
            GROUP BY domain
            ON CONFLICT (domain) DO UPDATE
                SET business_count = EXCLUDED.business_count
        """)
        upserted = wcur.rowcount
        # `flushed` counts rows written to staging, and a domain seen in two
        # different flush batches is counted twice. Take the real distinct figure
        # off the staging table before dropping it, so the UI stops claiming more
        # unique domains than actually exist.
        wcur.execute("SELECT COUNT(DISTINCT domain) FROM domains_stage")
        distinct_domains = wcur.fetchone()[0]
        wcur.execute("DROP TABLE IF EXISTS domains_stage")
        write_conn.commit()
        _set_sync_state(r, inserted=upserted, domains=distinct_domains)
    finally:
        try:
            read_conn.close()
        finally:
            write_conn.close()


# ─── Worker queue primitives ────────────────────────────────────────────────

# How long a claimed row stays leased before another worker may retry it. Must
# comfortably exceed the time to check one batch, or workers duplicate effort.
LEASE_MINUTES = 15


def claim_batch(limit: int) -> list[dict]:
    """
    Atomically claim up to `limit` due domains and push their next_check_at out by
    the lease window. If this worker dies mid-batch the lease simply expires and
    another worker retakes the rows — no dead-letter handling needed.
    """
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(
            """
            UPDATE domains d
               SET next_check_at = NOW() + (%s || ' minutes')::interval
              FROM (
                    SELECT id FROM domains
                     WHERE next_check_at <= NOW() AND status <> 'invalid'
                     ORDER BY next_check_at
                     LIMIT %s
                     FOR UPDATE SKIP LOCKED
                   ) AS due
             WHERE d.id = due.id
         RETURNING d.id, d.domain, d.status, d.expiry_date, d.rdap_status,
                   d.check_count, d.fail_streak
            """,
            (LEASE_MINUTES, limit),
        )
        rows = cur.fetchall()
        raw.commit()
        return [
            {
                "id": r[0], "domain": r[1], "status": r[2], "expiry_date": r[3],
                "rdap_status": r[4], "check_count": r[5] or 0, "fail_streak": r[6] or 0,
            }
            for r in rows
        ]
    finally:
        raw.close()


def write_results(results: list[dict]):
    """
    Bulk write-back of check outcomes. One UPDATE ... FROM (VALUES ...) per batch
    rather than N statements, so a 500-domain batch costs a single round trip.
    """
    if not results:
        return
    rows = [
        (
            r["id"], r["status"], r.get("dns_status"), r.get("rdap_status"),
            r.get("registrar"), r.get("expiry_date"), r.get("created_date"),
            r.get("registrable"), r.get("ns_records"), r.get("last_error"),
            r["fail_streak"], r["next_check_at"],
        )
        for r in results
    ]
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        execute_values(
            cur,
            """
            UPDATE domains d SET
                status          = v.status,
                dns_status      = v.dns_status,
                rdap_status     = COALESCE(v.rdap_status, d.rdap_status),
                registrar       = COALESCE(v.registrar, d.registrar),
                expiry_date     = COALESCE(v.expiry_date, d.expiry_date),
                created_date    = COALESCE(v.created_date, d.created_date),
                registrable     = COALESCE(v.registrable, d.registrable),
                ns_records      = COALESCE(v.ns_records, d.ns_records),
                last_error      = v.last_error,
                fail_streak     = v.fail_streak,
                check_count     = d.check_count + 1,
                last_checked_at = NOW(),
                next_check_at   = v.next_check_at
            FROM (VALUES %s) AS v (id, status, dns_status, rdap_status, registrar,
                                   expiry_date, created_date, registrable, ns_records,
                                   last_error, fail_streak, next_check_at)
            WHERE d.id = v.id
            """,
            rows,
            template="(%s::bigint, %s::text, %s::text, %s::text, %s::text, "
                     "%s::timestamp, %s::timestamp, %s::text, %s::jsonb, %s::text, "
                     "%s::int, %s::timestamp)",
            page_size=1000,
        )
        raw.commit()
    finally:
        raw.close()
