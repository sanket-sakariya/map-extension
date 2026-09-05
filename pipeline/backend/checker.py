"""
Domain liveness/expiry checking.

Three tiers, ordered by cost, each answering a different question:

  Tier 1 — DNS (cheap, unlimited): "is the name resolving right now?" An async
           NS lookup straight at public resolvers. Every registered domain has
           NS records at its TLD; a deleted domain returns NXDOMAIN. Runs on
           every domain every cycle at ~1000/s per worker.

  Tier 2 — IDS (cheap in bulk): "is the name registered at all?" The
           InstantDomainSearch bulk endpoint answers 500 domains per request,
           so this runs for every domain too. It is what actually decides
           available-vs-taken, which means expiry no longer rests on the
           two-strike NXDOMAIN heuristic. See ids_client.

  Tier 3 — RDAP (expensive, rate-limited): "when does it expire and who is the
           registrar?" The only source for those, so it stays budgeted and is
           spent where it adds something the first two tiers cannot.

All tiers are async; DNS runs concurrently, IDS runs as a batch phase, and RDAP
trickles behind its own rate limiter.
"""
import asyncio
import json
import os
import time
import warnings
from datetime import datetime, timedelta, timezone

import aiodns
import httpx
import pycares

import domains as dom
import ids_client

# ─── Tunables (env-overridable so the k8s Deployment can be retuned without a
# rebuild) ──────────────────────────────────────────────────────────────────
DNS_TIMEOUT = float(os.getenv("PING_DNS_TIMEOUT", "3.0"))
DNS_CONCURRENCY = int(os.getenv("PING_CONCURRENCY", "200"))
RDAP_RPS = float(os.getenv("RDAP_RPS", "5"))
RDAP_TIMEOUT = float(os.getenv("RDAP_TIMEOUT", "10"))
RDAP_ENRICH = os.getenv("RDAP_ENRICH", "true").lower() == "true"
# Cap on how many domains per batch may spend an RDAP call. Keeps one unlucky
# batch of NXDOMAINs from stalling behind the rate limiter for minutes.
RDAP_BUDGET_PER_BATCH = int(os.getenv("RDAP_BUDGET_PER_BATCH", "40"))

RESOLVERS = [s.strip() for s in os.getenv(
    "PING_RESOLVERS", "1.1.1.1,8.8.8.8,9.9.9.9,1.0.0.1,8.8.4.4"
).split(",") if s.strip()]

BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"
BOOTSTRAP_CACHE_KEY = "domains:rdap_bootstrap"

# c-ares error codes we care about. Anything else is treated as transient.
# aiodns 4.x added query_dns() and deprecated query(), but the two return
# DIFFERENT shapes: query() -> [AresQueryNSResult(host=...)], query_dns() ->
# pycares.DNSResult(answer=[DNSRecord(data=NSRecordData(nsdname=...))]).
# We stay on query(), whose shape is stable across both majors, and pin aiodns
# + pycares exactly (aiodns 3.x is outright broken against pycares 5.x).
warnings.filterwarnings("ignore", message="query.. is deprecated", module="aiodns")

_ARES_NOTFOUND = pycares.errno.ARES_ENOTFOUND
_ARES_NODATA = pycares.errno.ARES_ENODATA
_ARES_TIMEOUT = pycares.errno.ARES_ETIMEOUT

# RDAP domain statuses that mean the registration is already unwinding.
_DYING_STATUSES = {"pending delete", "redemption period", "pending deletion"}


def _utcnow() -> datetime:
    """Naive UTC — the timestamp columns are `TIMESTAMP WITHOUT TIME ZONE`."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class RateLimiter:
    """Token bucket. Serializes RDAP calls to RDAP_RPS across the whole process."""

    def __init__(self, rps: float):
        self._interval = 1.0 / rps if rps > 0 else 0.0
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        if self._interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next = max(now, self._next) + self._interval


class DomainChecker:
    """
    Owns the resolver pool, the RDAP client and the TLD bootstrap map.
    Construct once per process; `check_many` is the batch entry point.
    """

    def __init__(self, redis_client=None):
        # One resolver pinned per nameserver, round-robined in Python. c-ares'
        # own rotation is per-channel and less predictable than doing it here.
        self._resolvers = [
            aiodns.DNSResolver(nameservers=[ns], timeout=DNS_TIMEOUT, tries=1)
            for ns in RESOLVERS
        ]
        self._rr = 0
        self._sem = asyncio.Semaphore(DNS_CONCURRENCY)
        self._rdap_limiter = RateLimiter(RDAP_RPS)
        self._http: httpx.AsyncClient | None = None
        self._bootstrap: dict[str, str] = {}
        self._redis = redis_client
        self._ids: ids_client.IDSClient | None = None

    def _next_resolver(self):
        r = self._resolvers[self._rr % len(self._resolvers)]
        self._rr += 1
        return r

    async def __aenter__(self):
        self._http = httpx.AsyncClient(
            timeout=RDAP_TIMEOUT,
            follow_redirects=True,
            headers={"Accept": "application/rdap+json, application/json"},
            limits=httpx.Limits(max_connections=20),
        )
        await self._load_bootstrap()
        if ids_client.ENABLED:
            # Its own httpx client: the IDS endpoint needs browser-ish headers
            # that would be wrong to send to registry RDAP servers.
            self._ids = await ids_client.IDSClient().__aenter__()
        return self

    async def __aexit__(self, *exc):
        if self._ids:
            await self._ids.__aexit__(*exc)
        if self._http:
            await self._http.aclose()

    # ─── RDAP bootstrap ─────────────────────────────────────────────────────
    async def _load_bootstrap(self):
        """
        Build {tld: rdap_base_url} from IANA. Cached in Redis for a day so N
        pinger replicas restarting together don't each hit IANA, and so a
        transient IANA outage doesn't disable tier 2.
        """
        if self._redis:
            try:
                cached = self._redis.get(BOOTSTRAP_CACHE_KEY)
                if cached:
                    self._bootstrap = json.loads(cached)
                    return
            except Exception:
                pass
        try:
            resp = await self._http.get(BOOTSTRAP_URL)
            resp.raise_for_status()
            data = resp.json()
            mapping = {}
            for entry in data.get("services", []):
                tlds, urls = entry[0], entry[1]
                base = next((u for u in urls if u.startswith("https://")), urls[0] if urls else None)
                if not base:
                    continue
                for t in tlds:
                    mapping[t.lower().lstrip(".")] = base.rstrip("/")
            self._bootstrap = mapping
            if self._redis:
                try:
                    self._redis.set(BOOTSTRAP_CACHE_KEY, json.dumps(mapping), ex=86400)
                except Exception:
                    pass
        except Exception:
            # No bootstrap => tier 2 reports 'unsupported' and we fall back to
            # DNS-only evidence. Degraded, not broken.
            self._bootstrap = {}

    # ─── Tier 1: DNS ────────────────────────────────────────────────────────
    async def _dns_check(self, domain: str) -> tuple[str, list[str] | None]:
        """
        Returns (dns_status, ns_records).
        dns_status ∈ ok | nxdomain | no_ns | timeout | servfail

        A timeout is retried once, and the round-robin sends the retry to a
        different resolver. At this concurrency a dropped UDP packet is routine,
        and without the retry every drop would mark a healthy domain 'error' and
        churn the queue. NXDOMAIN is never retried here — it is a real answer,
        and the two-strike rule in _classify already guards against acting on one.
        """
        status, ns = await self._dns_query(domain)
        if status == "timeout":
            status, ns = await self._dns_query(domain)
        return status, ns

    async def _dns_query(self, domain: str) -> tuple[str, list[str] | None]:
        try:
            res = await self._next_resolver().query(domain, "NS")
            return "ok", sorted({r.host.rstrip(".").lower() for r in res})[:8]
        except aiodns.error.DNSError as e:
            code = e.args[0] if e.args else None
            if code == _ARES_NOTFOUND:
                return "nxdomain", None
            if code == _ARES_NODATA:
                # A hostname below the zone apex (e.g. "shop.example.com") has no
                # NS of its own. An A record still proves it is live.
                try:
                    await self._next_resolver().query(domain, "A")
                    return "ok", None
                except aiodns.error.DNSError as e2:
                    code2 = e2.args[0] if e2.args else None
                    if code2 == _ARES_NOTFOUND:
                        return "nxdomain", None
                    if code2 == _ARES_NODATA:
                        return "no_ns", None
                    return "timeout" if code2 == _ARES_TIMEOUT else "servfail", None
            if code == _ARES_TIMEOUT:
                return "timeout", None
            return "servfail", None
        except Exception:
            return "servfail", None

    # ─── Tier 2: RDAP ───────────────────────────────────────────────────────
    async def _rdap_check(self, registrable: str) -> dict:
        """
        Returns {rdap_status, expiry_date, created_date, registrar, rdap_flags}.
        rdap_status ∈ registered | available | unsupported | error
        """
        out = {"rdap_status": "unsupported", "expiry_date": None,
               "created_date": None, "registrar": None, "rdap_flags": []}
        if not registrable:
            return out
        tld = dom.tld_of(registrable)
        base = self._bootstrap.get(tld)
        if not base:
            return out

        await self._rdap_limiter.acquire()
        try:
            resp = await self._http.get(f"{base}/domain/{registrable}")
        except Exception as e:
            out["rdap_status"] = "error"
            out["error"] = str(e)[:200]
            return out

        if resp.status_code == 404:
            # The TLD's own registry says no such registration exists. This is
            # the strongest possible expiry signal.
            out["rdap_status"] = "available"
            return out
        if resp.status_code == 429:
            out["rdap_status"] = "error"
            out["error"] = "rdap rate limited"
            return out
        if resp.status_code != 200:
            out["rdap_status"] = "error"
            out["error"] = f"rdap http {resp.status_code}"
            return out

        try:
            data = resp.json()
        except Exception:
            out["rdap_status"] = "error"
            out["error"] = "rdap bad json"
            return out

        out["rdap_status"] = "registered"
        out["rdap_flags"] = [str(s).lower() for s in data.get("status", [])]
        for ev in data.get("events", []):
            action = (ev.get("eventAction") or "").lower()
            when = _parse_rdap_date(ev.get("eventDate"))
            if not when:
                continue
            if action == "expiration":
                out["expiry_date"] = when
            elif action == "registration":
                out["created_date"] = when
        out["registrar"] = _rdap_registrar(data)
        return out

    # ─── Orchestration ──────────────────────────────────────────────────────
    async def check_many(self, rows: list[dict]) -> list[dict]:
        """
        Check a claimed batch as a pipeline, cheapest evidence first.

          1. DNS   — every domain, concurrently.
          2. IDS   — every domain, in bulk. 500 per request means the whole
                     batch costs a handful of requests, so the authoritative
                     "is it registered?" answer is affordable for everyone
                     rather than rationed.
          3. RDAP  — only where it adds something DNS and IDS cannot: the
                     expiry date and registrar. Still budgeted per batch.

        Running IDS as a batch phase (rather than per domain) is the whole point:
        2000 domains cost 4 requests instead of 2000.
        """
        now = _utcnow()
        valid, out = [], {}

        for row in rows:
            if not dom.is_valid_domain(row["domain"]):
                # Same key set as every other result, so callers can index
                # rather than .get() their way around a special case.
                out[row["id"]] = {
                    "id": row["id"], "status": dom.INVALID,
                    "dns_status": None, "ids_status": None, "rdap_status": None,
                    "registrar": None, "expiry_date": None, "created_date": None,
                    "registrable": None, "ns_records": None,
                    "last_error": "not a resolvable hostname", "fail_streak": 0,
                    "next_check_at": now + timedelta(days=365),
                }
            else:
                valid.append(row)

        if not valid:
            return [out[r["id"]] for r in rows]

        # ── Phase 1: DNS ──
        dns_results = await asyncio.gather(*(self._dns_with_sem(r["domain"]) for r in valid))

        registrables = [dom.registrable_domain(r["domain"]) for r in valid]

        # ── Phase 2: IDS bulk availability ──
        ids_map = {}
        if self._ids:
            try:
                ids_map = await self._ids.check([r for r in registrables if r])
            except Exception as e:
                print(f"[checker] IDS batch failed: {type(e).__name__}: {e}", flush=True)

        # ── Phase 3: RDAP, budgeted ──
        budget = [RDAP_BUDGET_PER_BATCH]
        rdap_jobs = []
        for row, (dns_status, _ns), registrable in zip(valid, dns_results, registrables):
            ids_status = ids_map.get(registrable)
            if RDAP_ENRICH and _needs_rdap(dns_status, row, now, ids_status):
                if budget[0] > 0:
                    budget[0] -= 1
                    rdap_jobs.append((row["id"], registrable))
                else:
                    out.setdefault(row["id"], {})["_rdap_deferred"] = True

        rdap_by_id = {}
        if rdap_jobs:
            answers = await asyncio.gather(*(self._rdap_check(reg) for _id, reg in rdap_jobs))
            rdap_by_id = {job[0]: ans for job, ans in zip(rdap_jobs, answers)}

        # ── Phase 4: fold the evidence ──
        results = []
        for row, (dns_status, ns), registrable in zip(valid, dns_results, registrables):
            rdap = rdap_by_id.get(row["id"])
            ids_status = ids_map.get(registrable)
            rdap_deferred = bool(out.get(row["id"], {}).get("_rdap_deferred"))
            last_error = rdap.get("error") if rdap else None
            fail_streak = row.get("fail_streak", 0) or 0

            status, fail_streak, last_error = _classify(
                dns_status, rdap, row, fail_streak, last_error, now, ids_status
            )
            results.append({
                "id": row["id"],
                "status": status,
                "dns_status": dns_status,
                "ids_status": ids_status,
                "rdap_status": rdap.get("rdap_status") if rdap else None,
                "registrar": rdap.get("registrar") if rdap else None,
                "expiry_date": rdap.get("expiry_date") if rdap else None,
                "created_date": rdap.get("created_date") if rdap else None,
                "registrable": registrable or None,
                "ns_records": json.dumps(ns) if ns else None,
                "last_error": last_error,
                "fail_streak": fail_streak,
                "next_check_at": _next_check(status, rdap, row, fail_streak, now,
                                             rdap_deferred, ids_status),
            })

        by_id = {r["id"]: r for r in results}
        return [by_id.get(r["id"]) or out[r["id"]] for r in rows]

    async def _dns_with_sem(self, domain: str):
        async with self._sem:
            return await self._dns_check(domain)

    async def check_one(self, row: dict) -> dict:
        """Single-domain path, used by the UI's on-demand check."""
        return (await self.check_many([row]))[0]


# ─── Pure decision logic (kept module-level so it is unit-testable without a
# resolver, an event loop or a database) ────────────────────────────────────

def _needs_rdap(dns_status: str, row: dict, now: datetime, ids_status=None) -> bool:
    """
    RDAP is the scarce resource, so spend it only where it changes the answer:
      1. IDS already says the name is unregistered — nothing left to learn, and
         a registry has no expiry date for a name nobody holds. Skip it.
      2. DNS says the name is gone but IDS could not confirm — fall back to RDAP.
      3. We have never enriched this domain — one-time backfill of expiry data.
      4. The expiry we hold is near or past — re-verify before alerting.
    """
    if ids_status == ids_client.AVAILABLE:
        return False
    if dns_status in ("nxdomain", "no_ns") and ids_status != ids_client.REGISTERED:
        return True
    if not row.get("rdap_status"):
        return True
    expiry = row.get("expiry_date")
    if expiry and expiry <= now + timedelta(days=dom.EXPIRING_WINDOW_DAYS + 15):
        return True
    return False


def _classify(dns_status, rdap, row, fail_streak, last_error, now, ids_status=None):
    """
    Fold DNS + IDS + RDAP evidence into one status.
    Returns (status, fail_streak, last_error).
    """
    # IDS answers the availability question directly for every domain, so when
    # it says the name is unregistered we can call it expired on the first pass
    # — no two-strike NXDOMAIN wait, no RDAP call.
    if ids_status == ids_client.AVAILABLE:
        return dom.EXPIRED, 0, "not registered — available to register"

    # RDAP is authoritative when we have it.
    if rdap and rdap.get("rdap_status") == "available":
        return dom.EXPIRED, 0, "not registered (RDAP 404)"

    if rdap and rdap.get("rdap_status") == "registered":
        flags = set(rdap.get("rdap_flags") or [])
        if flags & _DYING_STATUSES:
            return dom.EXPIRED, 0, f"registry status: {', '.join(sorted(flags & _DYING_STATUSES))}"
        expiry = rdap.get("expiry_date")
        if expiry:
            if expiry <= now:
                return dom.EXPIRED, 0, "past expiry date"
            if expiry <= now + timedelta(days=dom.EXPIRING_WINDOW_DAYS):
                return dom.EXPIRING_SOON, 0, None
        return dom.ACTIVE, 0, None

    # No usable RDAP — fall back to DNS evidence.
    if dns_status == "ok":
        return dom.ACTIVE, 0, last_error

    if dns_status in ("nxdomain", "no_ns"):
        if ids_status == ids_client.REGISTERED:
            # The registry says it is held; DNS just isn't publishing for it
            # (parked, suspended, or between nameservers). Not expired.
            return dom.ACTIVE, 0, "registered but not resolving"
        # One NXDOMAIN can be a resolver hiccup or a blocked query. Require two
        # consecutive failures before declaring a domain expired on DNS alone.
        streak = fail_streak + 1
        if streak >= 2:
            return dom.EXPIRED, streak, "does not resolve (NXDOMAIN, unconfirmed by RDAP)"
        return row.get("status") or dom.PENDING, streak, "NXDOMAIN — awaiting confirmation"

    # timeout / servfail — transient, back off and retry.
    streak = fail_streak + 1
    return dom.ERROR, streak, last_error or f"dns {dns_status}"


def _next_check(status, rdap, row, fail_streak, now, rdap_deferred=False, ids_status=None) -> datetime:
    """
    Adaptive scheduling. This is what makes millions of domains sustainable: a
    healthy domain is touched ~12 times a year, not continuously, so steady-state
    load is a small fraction of the initial sweep.
    """
    if status == dom.ERROR:
        # Exponential backoff, capped at a week.
        hours = min(2 ** min(fail_streak, 8), 168)
        return now + timedelta(hours=hours)

    if status == dom.EXPIRED:
        # Expired names get re-registered; keep watching, just slowly.
        return now + timedelta(days=14)

    if status == dom.EXPIRING_SOON:
        return now + timedelta(days=3)

    if fail_streak > 0:
        # An unconfirmed NXDOMAIN. The status was left alone so a single resolver
        # hiccup never alarms, but we must come back fast to settle it.
        return now + timedelta(hours=12)

    if status == dom.PENDING:
        return now + timedelta(hours=12)

    if rdap_deferred and ids_status != ids_client.AVAILABLE:
        # Wanted enrichment, lost the budget draw. Retry within the day.
        return now + timedelta(hours=18)


    # ACTIVE: if we know when it expires, wake up shortly before that.
    expiry = (rdap or {}).get("expiry_date") or row.get("expiry_date")
    if expiry:
        target = expiry - timedelta(days=dom.EXPIRING_WINDOW_DAYS - 5)
        if target > now:
            return min(target, now + timedelta(days=30))
    return now + timedelta(days=30)


def _parse_rdap_date(value) -> datetime | None:
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(str(value)[:19], fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _rdap_registrar(data: dict) -> str | None:
    """Pull the registrar's display name out of the RDAP jCard soup."""
    for ent in data.get("entities", []) or []:
        roles = [str(r).lower() for r in ent.get("roles", [])]
        if "registrar" not in roles:
            continue
        vcard = ent.get("vcardArray")
        if isinstance(vcard, list) and len(vcard) > 1:
            for field in vcard[1]:
                if isinstance(field, list) and len(field) >= 4 and field[0] == "fn":
                    return str(field[3])[:200]
        if ent.get("handle"):
            return str(ent["handle"])[:200]
    return None


async def check_domain_now(domain: str, redis_client=None) -> dict:
    """
    One-off check for a single domain, used by the UI's "Check now" button.
    Runs the full DNS -> IDS -> RDAP pipeline; RDAP is skipped only when IDS
    already reports the name as unregistered, since a registry holds no expiry
    date for a name nobody owns.
    """
    row = {"id": 0, "domain": domain, "status": None, "expiry_date": None,
           "rdap_status": None, "check_count": 0, "fail_streak": 1}
    async with DomainChecker(redis_client) as checker:
        result = await checker.check_one(row)
    result["domain"] = domain
    result["ns_records"] = json.loads(result["ns_records"]) if result["ns_records"] else None
    for key in ("expiry_date", "created_date", "next_check_at"):
        if result.get(key):
            result[key] = str(result[key])
    result.pop("id", None)
    return result
