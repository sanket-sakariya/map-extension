"""
InstantDomainSearch bulk availability client.

This is the fastest availability oracle we have: it answers "is this name
registered?" for up to 500 domains in a single ~0.3s request (~1800 domains/s),
against RDAP's one-domain-per-request at a few requests per second. That makes
it practical to ask the question for EVERY domain on every sweep instead of
rationing it to a handful of suspicious ones.

It does not report expiry dates or registrars, so it complements RDAP rather
than replacing it:

    DNS   -> is the name resolving right now?
    IDS   -> is the name registered at all?   (this module)
    RDAP  -> when does it expire, and who is the registrar?

Request shape (reverse-engineered from the site's own client bundle):

    POST https://cloud.instantdomainsearch.com/services/bulk-check
    {"names": [{"name": "<label>", "hash": "<signed int32>", "tlds": ["com"]}]}

The `hash` field is validated server-side — a wrong value is rejected with
"invalid hash" — and is the site's own `hashCode(label, NOMINL_HASH_SEED)`:
a Java-style 31-multiplier rolling hash over code points, truncated to signed
32-bit, seeded with 42.
"""
import asyncio
import json
import os
import time

import httpx

BASE_URL = os.getenv("IDS_URL", "https://cloud.instantdomainsearch.com/services/bulk-check")
# The site's own client chunks at 500; verified working at that size.
BATCH = int(os.getenv("IDS_BATCH", "500"))
# Requests per second, NOT domains per second — each request carries up to BATCH
# domains, so 5 here is ~2500 domains/s while staying a polite caller.
RPS = float(os.getenv("IDS_RPS", "5"))
TIMEOUT = float(os.getenv("IDS_TIMEOUT", "30"))
ENABLED = os.getenv("IDS_ENABLED", "true").lower() == "true"
# A failed chunk silently costs availability data for up to BATCH domains, and
# they then wait out a full recheck cycle. Retrying is far cheaper than losing
# them, so give each chunk a couple of attempts with a short backoff.
RETRIES = int(os.getenv("IDS_RETRIES", "2"))

HASH_SEED = int(os.getenv("IDS_HASH_SEED", "42"))

# The site sends browser headers and rejects requests without a matching Origin.
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://instantdomainsearch.com",
    "Referer": "https://instantdomainsearch.com/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
                   "(KHTML, like Gecko) Version/18.5 Safari/605.1.15"),
}

AVAILABLE = "available"     # not registered -> expired or never taken
REGISTERED = "registered"
UNKNOWN = "unknown"         # asked but got no usable answer


def hash_code(label: str, seed: int = HASH_SEED) -> str:
    """
    Port of the site's hashCode(): h = h*31 + codePoint, folded to signed int32.

    Iterates code points (not UTF-16 units), matching JS `for (const c of str)`.
    Returned as a string because the API expects it as a JSON string.
    """
    h = seed & 0xFFFFFFFF
    for ch in label:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return str(h - (1 << 32) if h >= (1 << 31) else h)


def split_label_tld(registrable: str) -> tuple[str, str] | None:
    """
    Split a registrable domain into the (label, tld) pair the API expects.
    "example.co.uk" -> ("example", "co.uk"). Returns None if unusable.
    """
    if not registrable or "." not in registrable:
        return None
    label, _, tld = registrable.partition(".")
    if not label or not tld:
        return None
    return label, tld


class RateLimiter:
    """Token bucket over REQUESTS (each carrying up to BATCH domains)."""

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


class IDSClient:
    """
    Bulk availability lookups. One instance per worker process; `check` takes the
    full batch and handles chunking, rate limiting and partial failure.
    """

    def __init__(self, http: httpx.AsyncClient | None = None):
        self._http = http
        self._own_http = http is None
        self._limiter = RateLimiter(RPS)
        self.last_error: str | None = None
        # Counters so a degraded run is visible in the worker heartbeat rather
        # than showing up days later as a hole in the data.
        self.chunks_ok = 0
        self.chunks_failed = 0
        self.domains_unanswered = 0

    async def __aenter__(self):
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS,
                                           limits=httpx.Limits(max_connections=10))
        return self

    async def __aexit__(self, *exc):
        if self._own_http and self._http:
            await self._http.aclose()

    async def check(self, registrables: list[str]) -> dict[str, str]:
        """
        Map each registrable domain to AVAILABLE / REGISTERED / UNKNOWN.

        Domains the API cannot be asked about (no dot, empty label) are simply
        absent from the result, and callers fall back to their other evidence.
        """
        if not ENABLED or not registrables:
            return {}

        # Deduplicate and index by (label, tld) so the response can be mapped back.
        wanted: dict[tuple[str, str], str] = {}
        for reg in registrables:
            parts = split_label_tld(reg)
            if parts:
                wanted[parts] = reg
        if not wanted:
            return {}

        items = [{"name": lbl, "hash": hash_code(lbl), "tlds": [tld]}
                 for (lbl, tld) in wanted]
        chunks = [items[i:i + BATCH] for i in range(0, len(items), BATCH)]

        results: dict[str, str] = {}
        for chunk in chunks:
            answers = []
            for attempt in range(RETRIES + 1):
                await self._limiter.acquire()
                answers = await self._post(chunk)
                if answers:
                    break
                if attempt < RETRIES:
                    await asyncio.sleep(1.0 * (attempt + 1))

            if answers:
                self.chunks_ok += 1
            else:
                # Loud, not silent: an unlogged empty chunk is how 45k domains
                # ended up with no availability answer and nobody noticed.
                self.chunks_failed += 1
                self.domains_unanswered += len(chunk)
                print(f"[ids] chunk of {len(chunk)} unanswered after "
                      f"{RETRIES + 1} attempts: {self.last_error}", flush=True)

            for label, tld, registered in answers:
                reg = wanted.get((label, tld))
                if reg:
                    results[reg] = REGISTERED if registered else AVAILABLE

        missing = len(wanted) - len(results)
        if missing > 0:
            print(f"[ids] {missing}/{len(wanted)} domains had no answer this batch",
                  flush=True)
        return results

    async def _post(self, items: list[dict]) -> list[tuple[str, str, bool]]:
        """One request. Returns [] on any failure — never raises into the sweep."""
        try:
            resp = await self._http.post(BASE_URL, content=json.dumps({"names": items}))
        except Exception as e:
            self.last_error = f"ids request failed: {type(e).__name__}: {e}"[:200]
            return []
        if resp.status_code != 200:
            self.last_error = f"ids http {resp.status_code}: {resp.text[:120]}"
            return []
        try:
            payload = resp.json()
        except Exception:
            self.last_error = f"ids bad json: {resp.text[:120]}"
            return []

        out = []
        for r in payload.get("results", []):
            label, tld = r.get("label"), r.get("tld")
            registered = r.get("isRegistered")
            if label and tld and isinstance(registered, bool):
                out.append((label, tld, registered))
        if not out:
            self.last_error = f"ids returned no usable results: {resp.text[:120]}"
        return out
