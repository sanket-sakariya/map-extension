"""Maps Scraping Pipeline — FastAPI Backend"""
import json
import time
from contextlib import asynccontextmanager

import redis as redis_lib
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session

from config import REDIS_URL
from database import get_db, engine, Base
from models import Business, ScrapeJob
import github_client
import query_generator
import orchestrator
import db_writer

# State
_pat = ""


def get_pat():
    return _pat


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Base.metadata.create_all(bind=engine)
    db_writer.start()
    orchestrator.start(get_pat)
    yield
    # Shutdown
    orchestrator.stop()
    db_writer.stop()


app = FastAPI(title="Maps Scraping Pipeline", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


# ─── Models ─────────────────────────────────────────────────────────────────

class ConfigRequest(BaseModel):
    pat: str

class WorkflowStartRequest(BaseModel):
    count: int = 1

class QueryGenerateRequest(BaseModel):
    cities: list[str] = query_generator.DEFAULT_CITIES
    businesses: list[str] = query_generator.DEFAULT_BUSINESSES


# ─── Endpoints ──────────────────────────────────────────────────────────────

@app.post("/api/config")
def set_config(req: ConfigRequest):
    global _pat
    result = github_client.validate_pat(req.pat)
    if result["valid"]:
        _pat = req.pat
        return {"status": "ok", "message": "PAT validated and saved"}
    return {"status": "error", "message": result.get("error", "Invalid PAT")}


@app.get("/api/config")
def get_config():
    return {"pat_set": bool(_pat)}


@app.get("/api/workflows")
def get_workflows():
    if not _pat:
        return {"error": "PAT not configured"}
    scrapers = github_client.get_active_scrapers(_pat)
    # Update active_scrapers in Redis
    r = get_redis()
    r.delete("active_scrapers")
    for s in scrapers:
        if s["tunnel_url"]:
            r.sadd("active_scrapers", json.dumps(s))
    return {"active": len(scrapers), "scrapers": scrapers}


@app.post("/api/workflows/start")
def start_workflows(req: WorkflowStartRequest):
    if not _pat:
        return {"error": "PAT not configured"}
    results = []
    for _ in range(req.count):
        res = github_client.trigger_workflow(_pat)
        results.append(res)
        time.sleep(1)  # avoid rate limit
    return {"triggered": req.count, "results": results}


@app.post("/api/workflows/stop")
def stop_workflows():
    if not _pat:
        return {"error": "PAT not configured"}
    runs = github_client.list_runs(_pat, "in_progress")
    cancelled = 0
    for run in runs:
        if run.get("name") == "Maps Scraper VNC":
            if github_client.cancel_run(_pat, run["id"]):
                cancelled += 1
    r = get_redis()
    r.delete("active_scrapers")
    return {"cancelled": cancelled}


@app.post("/api/queries/generate")
def generate_queries(req: QueryGenerateRequest):
    queries = query_generator.generate_queries(req.cities, req.businesses)
    r = get_redis()
    for q in queries:
        r.rpush("query_queue", q)
    return {"generated": len(queries), "queries": queries[:20]}


@app.get("/api/pipeline/status")
def pipeline_status():
    r = get_redis()
    return {
        "query_queue": r.llen("query_queue"),
        "active_jobs": r.llen("active_jobs"),
        "result_queue": r.llen("result_queue"),
        "active_scrapers": r.scard("active_scrapers"),
        "total_inserted": int(r.get("stats:total_inserted") or 0)
    }


@app.get("/api/results")
def get_results(
    limit: int = 50,
    offset: int = 0,
    search: str = "",
    query: str = "",
    city: str = "",
    state: str = "",
    category: str = "",
    min_rating: float = 0,
    min_reviews: int = 0,
    phone_only: bool = False,
    db: Session = Depends(get_db)
):
    """
    Fast filtered search across all businesses.
    - search: full-text search on name+address+category
    - query: filter by scrape query
    - city/state/category: exact match filters
    - min_rating/min_reviews: numeric filters
    - phone_only: only businesses with phone numbers
    """
    from sqlalchemy import text as sql_text, func as sql_func

    q = db.query(Business)

    # Full-text search (uses GIN index)
    if search:
        q = q.filter(sql_text(
            "to_tsvector('english', coalesce(name,'') || ' ' || coalesce(address,'') || ' ' || coalesce(category,'')) "
            "@@ plainto_tsquery('english', :search)"
        ).bindparams(search=search))

    if query:
        q = q.filter(Business.query.ilike(f"%{query}%"))
    if city:
        q = q.filter(Business.city.ilike(f"%{city}%"))
    if state:
        q = q.filter(Business.state.ilike(f"%{state}%"))
    if category:
        q = q.filter(Business.category.ilike(f"%{category}%"))
    if min_rating > 0:
        q = q.filter(Business.rating >= min_rating)
    if min_reviews > 0:
        q = q.filter(Business.review_count >= min_reviews)
    if phone_only:
        q = q.filter(Business.phone != "", Business.phone.isnot(None))

    total = q.count()
    rows = q.order_by(Business.rating.desc().nulls_last()).offset(offset).limit(limit).all()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": [
            {
                "id": b.id, "name": b.name, "rating": float(b.rating) if b.rating else None,
                "review_count": b.review_count, "category": b.category,
                "address": b.address, "city": b.city, "state": b.state,
                "phone": b.phone, "website": b.website, "cid": b.cid,
                "place_id": b.place_id, "maps_url": b.maps_url,
                "query": b.query, "scraped_at": str(b.scraped_at)
            }
            for b in rows
        ]
    }
