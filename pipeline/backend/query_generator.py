"""
Query generator — fetches business categories and cities.
All external API calls cached in Redis (24h TTL).
"""
import json
import httpx
import redis as redis_lib
from pathlib import Path
from config import REDIS_URL

BLUTEC_API = "https://blutec-scout-be.blutec.ai/api/auth"
CACHE_TTL = 86400  # 24 hours


def _redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def get_business_categories() -> list[str]:
    """Return all GMB business categories. Cached in Redis."""
    r = _redis()
    cached = r.get("cache:categories")
    if cached:
        return json.loads(cached)

    # Load from file
    cat_file = Path(__file__).parent / "data" / "categories.json"
    if cat_file.exists():
        cats = json.loads(cat_file.read_text())
        r.set("cache:categories", json.dumps(cats), ex=CACHE_TTL)
        return cats

    return []


def get_countries() -> list[str]:
    """Return all countries. Cached in Redis."""
    r = _redis()
    cached = r.get("cache:countries")
    if cached:
        return json.loads(cached)

    country_file = Path(__file__).parent / "data" / "countries.json"
    if country_file.exists():
        countries = json.loads(country_file.read_text())
        r.set("cache:countries", json.dumps(countries), ex=CACHE_TTL)
        return countries

    return []


def get_cities(country: str) -> list[dict]:
    """Fetch cities for a country. Cached in Redis for 24h."""
    r = _redis()
    cache_key = f"cache:cities:{country}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    # Fetch from API
    try:
        resp = httpx.get(f"{BLUTEC_API}/getStateDistricts", params={"country": country}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            cities = data.get("results", [])
            # Cache it
            r.set(cache_key, json.dumps(cities), ex=CACHE_TTL)
            return cities
    except:
        pass

    return []


def generate_queries(
    mode: str,
    businesses: list[str],
    locations: list[str],
    modifier: str = "in"
) -> list[str]:
    """Generate queries from business × location combinations."""
    queries = []
    for biz in businesses:
        for loc in locations:
            queries.append(f"{biz} {modifier} {loc}")
    return queries
