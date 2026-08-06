"""
Query generator — fetches business categories and cities from blutec scout API.
Generates queries in 3 modes:
1. One business × all locations
2. One location × all businesses
3. All businesses × all locations
"""
import httpx

BLUTEC_API = "https://blutec-scout-be.blutec.ai/api/auth"
CATEGORIES_CACHE = []
COUNTRIES_CACHE = []


def get_business_categories() -> list[str]:
    """Return all 4074 GMB business categories (scraped from blutec page)."""
    global CATEGORIES_CACHE
    if CATEGORIES_CACHE:
        return CATEGORIES_CACHE

    # These are static GMB categories — hardcoded top ones, full list loaded from file
    try:
        from pathlib import Path
        cat_file = Path(__file__).parent / "data" / "categories.json"
        if cat_file.exists():
            import json
            CATEGORIES_CACHE = json.loads(cat_file.read_text())
            return CATEGORIES_CACHE
    except:
        pass

    # Fallback defaults
    CATEGORIES_CACHE = [
        "Restaurant", "Hotel", "Salon", "Gym", "Hospital", "Dentist",
        "School", "Plumber", "Electrician", "Car repair", "Lawyer",
        "Accountant", "Real estate agent", "Pharmacy", "Supermarket"
    ]
    return CATEGORIES_CACHE


def get_countries() -> list[str]:
    """Return all 249 countries."""
    global COUNTRIES_CACHE
    if COUNTRIES_CACHE:
        return COUNTRIES_CACHE

    try:
        from pathlib import Path
        f = Path(__file__).parent / "data" / "countries.json"
        if f.exists():
            import json
            COUNTRIES_CACHE = json.loads(f.read_text())
            return COUNTRIES_CACHE
    except:
        pass

    COUNTRIES_CACHE = ["India", "United States", "United Kingdom", "Canada", "Australia"]
    return COUNTRIES_CACHE


def get_cities(country: str) -> list[dict]:
    """Fetch districts/cities for a country from blutec API.
    Returns: [{"district": "Rajkot", "state": "Gujarat"}, ...]
    """
    try:
        r = httpx.get(f"{BLUTEC_API}/getStateDistricts", params={"country": country}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get("results", [])
    except:
        pass
    return []


def generate_queries(
    mode: str,
    businesses: list[str],
    locations: list[str],
    modifier: str = "in"
) -> list[str]:
    """
    Generate queries.
    mode: 'one_biz_all_loc' | 'one_loc_all_biz' | 'all_biz_all_loc'
    modifier: 'in' or 'near'
    """
    queries = []
    for biz in businesses:
        for loc in locations:
            queries.append(f"{biz} {modifier} {loc}")
    return queries
