"""
DB Writer worker — consumes result_queue, formats data, inserts into PostgreSQL.
Parses rating to float, review_count to int, extracts city/state from address.
"""
import json
import re
import time
import threading
import redis as redis_lib
from config import REDIS_URL
from database import SessionLocal
from models import Business
from domains import extract_domain
from sqlalchemy import text as sql_text

_running = False
_thread = None


def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def start():
    global _running, _thread
    if _running:
        return
    _running = True
    _thread = threading.Thread(target=_loop, daemon=True)
    _thread.start()


def stop():
    global _running
    _running = False


def parse_rating(raw: str) -> float | None:
    """'4.7' → 4.7"""
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def parse_review_count(raw: str) -> int:
    """'(583)' or '(7,837)' → 583 or 7837"""
    if not raw:
        return 0
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else 0


def extract_city_state(address: str) -> tuple[str, str]:
    """
    Extract city and state from Google Maps address.
    Typical format: '...., CityName, StateName PINCODE, Country'
    """
    if not address:
        return "", ""

    # Split by comma, work backwards
    parts = [p.strip() for p in address.split(",")]

    city = ""
    state = ""

    # Try to find Indian state pattern: 'Gujarat 360001' or 'Maharashtra'
    indian_states = [
        "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
        "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
        "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
        "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
        "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
        "Delhi", "Chandigarh", "Puducherry", "Jammu and Kashmir", "Ladakh"
    ]

    for i, part in enumerate(parts):
        for st in indian_states:
            if st.lower() in part.lower():
                state = st
                # City is usually the part before state
                if i > 0:
                    city = parts[i - 1].strip()
                break
        if state:
            break

    # Fallback: if address has "India" at end, city is 3rd from last
    if not city and len(parts) >= 3:
        # Check if last part is country
        if "india" in parts[-1].lower():
            # state+pin is second to last, city is third to last
            state_part = parts[-2].strip()
            for st in indian_states:
                if st.lower() in state_part.lower():
                    state = st
                    break
            city = parts[-3].strip() if len(parts) >= 3 else ""
        else:
            # Generic: second to last is city
            city = parts[-2].strip() if len(parts) >= 2 else ""

    # Clean pin code from city
    city = re.sub(r"\d{6}", "", city).strip()

    return city, state


# Columns refreshed when a business is seen again. A later scrape is the
# truthful one for volatile fields, but an EMPTY value in a later scrape means
# "not captured this time", not "no longer has one" — so empty never overwrites
# a value we already hold.
_MERGE_TEXT = ["name", "place_id", "category", "phone", "website", "address",
               "city", "state", "plus_code", "current_status", "identifies_as",
               "maps_url", "domain"]


def _build(item: dict, query: str) -> Business:
    """Map a scraped item onto a Business row."""
    address = item.get("address", "")
    city, state = extract_city_state(address)
    return Business(
        name=item.get("name", ""),
        cid=item.get("cid", ""),
        place_id=item.get("placeId", ""),
        category=item.get("category", ""),
        rating=parse_rating(item.get("rating", "")),
        review_count=parse_review_count(item.get("reviewCount", "")),
        phone=item.get("phone", ""),
        website=item.get("website", ""),
        address=address,
        city=city,
        state=state,
        plus_code=item.get("plusCode", ""),
        current_status=item.get("currentStatus", ""),
        identifies_as=item.get("identifiesAs", ""),
        hours=item.get("hours"),
        reviews=item.get("reviews"),
        maps_url=item.get("url", ""),
        query=query,
        domain=extract_domain(item.get("website", "")),
    )


def _upsert(db, item: dict, cid: str, query: str):
    """
    Insert the business, or refresh it if we already have it, and record that
    this query found it. One row per business, one row per (business, query) —
    which is what stops the table growing a fresh copy for every search that
    happens to return the same place.
    """
    address = item.get("address", "")
    city, state = extract_city_state(address)
    params = {
        "cid": cid,
        "name": item.get("name", ""),
        "place_id": item.get("placeId", ""),
        "category": item.get("category", ""),
        "rating": parse_rating(item.get("rating", "")),
        "review_count": parse_review_count(item.get("reviewCount", "")),
        "phone": item.get("phone", ""),
        "website": item.get("website", ""),
        "address": address,
        "city": city,
        "state": state,
        "plus_code": item.get("plusCode", ""),
        "current_status": item.get("currentStatus", ""),
        "identifies_as": item.get("identifiesAs", ""),
        "hours": json.dumps(item.get("hours")) if item.get("hours") is not None else None,
        "reviews": json.dumps(item.get("reviews")) if item.get("reviews") is not None else None,
        "maps_url": item.get("url", ""),
        "query": query,
        "domain": extract_domain(item.get("website", "")),
    }
    merge = ",\n                ".join(
        f"{c} = COALESCE(NULLIF(EXCLUDED.{c}, ''), businesses.{c})" for c in _MERGE_TEXT
    )
    db.execute(sql_text(f"""
        INSERT INTO businesses (
            cid, name, place_id, category, rating, review_count, phone, website,
            address, city, state, plus_code, current_status, identifies_as,
            hours, reviews, maps_url, query, domain, scraped_at
        ) VALUES (
            :cid, :name, :place_id, :category, :rating, :review_count, :phone, :website,
            :address, :city, :state, :plus_code, :current_status, :identifies_as,
            CAST(:hours AS JSONB), CAST(:reviews AS JSONB), :maps_url, :query, :domain, NOW()
        )
        ON CONFLICT (cid) WHERE cid IS NOT NULL AND cid <> '' DO UPDATE SET
                {merge},
                rating       = COALESCE(EXCLUDED.rating, businesses.rating),
                review_count = COALESCE(NULLIF(EXCLUDED.review_count, 0), businesses.review_count),
                hours        = COALESCE(EXCLUDED.hours, businesses.hours),
                reviews      = COALESCE(EXCLUDED.reviews, businesses.reviews),
                query        = EXCLUDED.query,
                scraped_at   = NOW()
    """), params)

    # The association that used to be encoded by duplicating the whole row.
    db.execute(sql_text("""
        INSERT INTO business_queries (cid, query, scraped_at)
        VALUES (:cid, :query, NOW())
        ON CONFLICT (cid, query) DO NOTHING
    """), {"cid": cid, "query": query})


def _loop():
    global _running
    r = get_redis()

    while _running:
        try:
            raw = r.lpop("result_queue")
            if not raw:
                time.sleep(3)
                continue

            data = json.loads(raw)
            query = data.get("query", "")
            results = data.get("results", [])

            if not results:
                continue

            db = SessionLocal()
            inserted = 0
            try:
                # A scrape batch can itself return the same business twice; keep
                # the last occurrence so the loop below never self-conflicts.
                by_cid = {}
                no_cid = []
                for item in results:
                    if not item.get("name"):
                        continue
                    cid = (item.get("cid") or "").strip()
                    if cid:
                        by_cid[cid] = item
                    else:
                        no_cid.append(item)

                for cid, item in by_cid.items():
                    _upsert(db, item, cid, query)
                    inserted += 1

                # Without a cid there is nothing to deduplicate on, so these are
                # inserted as-is. Google always supplies one; this is a fallback.
                for item in no_cid:
                    db.add(_build(item, query))
                    inserted += 1

                db.commit()
                r.incrby("stats:total_inserted", inserted)
            except Exception:
                db.rollback()
            finally:
                db.close()

        except Exception:
            time.sleep(3)
