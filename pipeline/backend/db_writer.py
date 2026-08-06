"""
DB Writer worker — runs as a background thread.
Consumes result_queue from Redis, formats data, inserts into PostgreSQL.
"""
import json
import time
import threading
import redis as redis_lib
from sqlalchemy import text
from config import REDIS_URL, DATABASE_URL
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
                    # Upsert: skip if cid+query already exists
                    cid = item.get("cid", "")
                    if not cid and not item.get("name"):
                        continue

                    existing = db.query(Business).filter_by(cid=cid, query=query).first() if cid else None
                    if existing:
                        continue

                    biz = Business(
                        name=item.get("name", ""),
                        rating=item.get("rating", ""),
                        review_count=item.get("reviewCount", ""),
                        category=item.get("category", ""),
                        address=item.get("address", ""),
                        phone=item.get("phone", ""),
                        plus_code=item.get("plusCode", ""),
                        website=item.get("website", ""),
                        hours=item.get("hours"),
                        current_status=item.get("currentStatus", ""),
                        identifies_as=item.get("identifiesAs", ""),
                        cid=cid,
                        place_id=item.get("placeId", ""),
                        maps_url=item.get("url", ""),
                        reviews=item.get("reviews"),
                        query=query
                    )
                    db.add(biz)
                    inserted += 1

                db.commit()
                # Track stats
                r.incrby("stats:total_inserted", inserted)
            except Exception as e:
                db.rollback()
            finally:
                db.close()

        except Exception:
            time.sleep(3)
