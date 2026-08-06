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
                for item in results:
                    cid = item.get("cid", "")
                    name = item.get("name", "")
                    if not name:
                        continue

                    # Skip duplicates
                    if cid:
                        existing = db.query(Business).filter_by(cid=cid, query=query).first()
                        if existing:
                            continue

                    address = item.get("address", "")
                    city, state = extract_city_state(address)

                    biz = Business(
                        name=name,
                        cid=cid,
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
                        query=query
                    )
                    db.add(biz)
                    inserted += 1

                db.commit()
                r.incrby("stats:total_inserted", inserted)
            except Exception:
                db.rollback()
            finally:
                db.close()

        except Exception:
            time.sleep(3)
