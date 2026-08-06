"""Maps Scraping Pipeline — FastAPI Backend"""
import json
import time
from contextlib import asynccontextmanager

import redis as redis_lib
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text as sql_text

from config import REDIS_URL
from database import get_db, engine, Base, SessionLocal
from models import Business, ScrapeJob, ActiveScraper
import github_client
import query_generator
import orchestrator
import db_writer


def get_pat() -> str:
    """Read PAT from DB."""
    db = SessionLocal()
    try:
        row = db.execute(sql_text("SELECT value FROM settings WHERE key = 'github_pat'")).fetchone()
        return row[0] if row else ""
    finally:
        db.close()


def save_pat(pat: str):
    """Persist PAT in DB."""
    db = SessionLocal()
    try:
        db.execute(sql_text(
            "INSERT INTO settings (key, value, updated_at) VALUES ('github_pat', :pat, NOW()) "
            "ON CONFLICT (key) DO UPDATE SET value = :pat, updated_at = NOW()"
        ), {"pat": pat})
        db.commit()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    db_writer.start()
    orchestrator.start(get_pat)
    yield
    orchestrator.stop()
    db_writer.stop()


app = FastAPI(title="Maps Scraping Pipeline", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


# ─── Request Models ─────────────────────────────────────────────────────────

class ConfigRequest(BaseModel):
    pat: str

class WorkflowStartRequest(BaseModel):
    count: int = 1

class ScraperRegisterRequest(BaseModel):
    run_id: int
    tunnel_url: str

class ScraperDeregisterRequest(BaseModel):
    run_id: int


# ─── Config (PAT persisted in DB) ──────────────────────────────────────────

@app.post("/api/config")
def set_config(req: ConfigRequest):
    result = github_client.validate_pat(req.pat)
    if result["valid"]:
        save_pat(req.pat)
        return {"status": "ok", "message": "PAT validated and saved to DB"}
    return {"status": "error", "message": result.get("error", "Invalid PAT")}


@app.get("/api/config")
def get_config():
    pat = get_pat()
    # Also get tunnel URL
    db = SessionLocal()
    try:
        row = db.execute(sql_text("SELECT value FROM settings WHERE key = 'tunnel_url'")).fetchone()
        tunnel_url = row[0] if row else ""
    finally:
        db.close()
    return {"pat_set": bool(pat), "pat_preview": f"{pat[:10]}...{pat[-4:]}" if pat else "", "tunnel_url": tunnel_url}


@app.post("/api/config/tunnel")
def set_tunnel_url(data: dict):
    """Store the public tunnel URL (called by startup script or manually)."""
    url = data.get("url", "").strip()
    if not url:
        return {"status": "error", "message": "Missing url"}
    db = SessionLocal()
    try:
        db.execute(sql_text(
            "INSERT INTO settings (key, value, updated_at) VALUES ('tunnel_url', :url, NOW()) "
            "ON CONFLICT (key) DO UPDATE SET value = :url, updated_at = NOW()"
        ), {"url": url})
        db.commit()
    finally:
        db.close()
    return {"status": "ok", "tunnel_url": url}


# ─── Scraper Registration (called by workflows) ────────────────────────────

@app.post("/api/scrapers/register")
def register_scraper(req: ScraperRegisterRequest, db: Session = Depends(get_db)):
    """Workflow calls this on startup to register its tunnel URL."""
    existing = db.query(ActiveScraper).filter_by(run_id=req.run_id).first()
    if existing:
        existing.tunnel_url = req.tunnel_url
        existing.last_heartbeat = sql_text("NOW()")
    else:
        db.add(ActiveScraper(run_id=req.run_id, tunnel_url=req.tunnel_url))
    db.commit()

    # Also update Redis for orchestrator
    r = get_redis()
    r.sadd("active_scrapers", json.dumps({"run_id": req.run_id, "tunnel_url": req.tunnel_url}))
    return {"status": "registered", "run_id": req.run_id, "tunnel_url": req.tunnel_url}


@app.post("/api/scrapers/deregister")
def deregister_scraper(req: ScraperDeregisterRequest, db: Session = Depends(get_db)):
    """Workflow calls this on shutdown to remove itself."""
    scraper = db.query(ActiveScraper).filter_by(run_id=req.run_id).first()
    tunnel_url = ""
    if scraper:
        tunnel_url = scraper.tunnel_url
        db.delete(scraper)
        db.commit()

    # Remove from Redis
    r = get_redis()
    for member in r.smembers("active_scrapers"):
        try:
            data = json.loads(member)
            if data.get("run_id") == req.run_id:
                r.srem("active_scrapers", member)
        except:
            pass
    # Clean up busy lock
    if tunnel_url:
        r.delete(f"scraper_busy:{tunnel_url}")

    return {"status": "deregistered", "run_id": req.run_id}


@app.get("/api/scrapers")
def list_scrapers(db: Session = Depends(get_db)):
    """List all registered scrapers."""
    scrapers = db.query(ActiveScraper).all()
    return {
        "count": len(scrapers),
        "scrapers": [
            {"run_id": s.run_id, "tunnel_url": s.tunnel_url, "registered_at": str(s.registered_at)}
            for s in scrapers
        ]
    }


# ─── Workflow Management ────────────────────────────────────────────────────

@app.get("/api/workflows")
def get_workflows(db: Session = Depends(get_db)):
    pat = get_pat()
    if not pat:
        return {"error": "PAT not configured"}
    # Return scrapers from DB (more reliable than GitHub API)
    scrapers = db.query(ActiveScraper).all()
    return {
        "active": len(scrapers),
        "scrapers": [
            {"run_id": s.run_id, "tunnel_url": s.tunnel_url, "registered_at": str(s.registered_at)}
            for s in scrapers
        ]
    }


@app.post("/api/workflows/start")
def start_workflows(req: WorkflowStartRequest):
    pat = get_pat()
    if not pat:
        return {"error": "PAT not configured"}

    # Get tunnel URL to pass to workflows for self-registration
    db = SessionLocal()
    try:
        row = db.execute(sql_text("SELECT value FROM settings WHERE key = 'tunnel_url'")).fetchone()
        pipeline_url = row[0] if row else ""
    finally:
        db.close()

    results = []
    for _ in range(req.count):
        res = github_client.trigger_workflow(pat, pipeline_url)
        results.append(res)
        time.sleep(1)
    return {"triggered": req.count, "pipeline_url": pipeline_url, "results": results}


@app.post("/api/workflows/stop")
def stop_workflows(db: Session = Depends(get_db)):
    pat = get_pat()
    if not pat:
        return {"error": "PAT not configured"}
    runs = github_client.list_runs(pat, "in_progress")
    cancelled = 0
    for run in runs:
        if run.get("name") == "Maps Scraper VNC":
            if github_client.cancel_run(pat, run["id"]):
                cancelled += 1
    # Clear all scrapers from DB (they'll deregister anyway on cleanup)
    db.query(ActiveScraper).delete()
    db.commit()
    r = get_redis()
    r.delete("active_scrapers")
    return {"cancelled": cancelled}


# ─── Query Generation ───────────────────────────────────────────────────────

class QueryGenerateRequest(BaseModel):
    mode: str = "all_biz_all_loc"  # one_biz_all_loc, one_loc_all_biz, all_biz_all_loc
    businesses: list[str] = []
    locations: list[str] = []
    modifier: str = "in"


@app.get("/api/categories")
def get_categories():
    """Return all 4074 GMB business categories."""
    cats = query_generator.get_business_categories()
    return {"count": len(cats), "categories": cats}


@app.get("/api/countries")
def get_countries():
    """Return all 249 countries."""
    countries = query_generator.get_countries()
    return {"count": len(countries), "countries": countries}


@app.get("/api/cities")
def get_cities(country: str = "India"):
    """Return all cities/districts for a country."""
    cities = query_generator.get_cities(country)
    return {"country": country, "count": len(cities), "cities": cities}


@app.post("/api/queries/generate")
def generate_queries(req: QueryGenerateRequest):
    """Generate queries and push to queue.
    mode: one_biz_all_loc | one_loc_all_biz | all_biz_all_loc
    """
    queries = query_generator.generate_queries(
        mode=req.mode,
        businesses=req.businesses,
        locations=req.locations,
        modifier=req.modifier
    )
    r = get_redis()
    for q in queries:
        r.rpush("query_queue", q)
    return {"generated": len(queries), "preview": queries[:20]}


# ─── Pipeline Status ────────────────────────────────────────────────────────

@app.get("/api/pipeline/status")
def pipeline_status(db: Session = Depends(get_db)):
    r = get_redis()
    scraper_count = db.query(ActiveScraper).count()
    return {
        "query_queue": r.llen("query_queue"),
        "active_jobs": r.llen("active_jobs"),
        "result_queue": r.llen("result_queue"),
        "active_scrapers": scraper_count,
        "total_inserted": int(r.get("stats:total_inserted") or 0)
    }


@app.get("/api/results/queries")
def get_result_queries(search: str = "", db: Session = Depends(get_db)):
    """Get all unique queries with result counts. Optionally filter by search term."""
    if search:
        rows = db.execute(sql_text(
            "SELECT query, COUNT(*) as count, MAX(scraped_at) as last_scraped "
            "FROM businesses WHERE query ILIKE :search GROUP BY query ORDER BY MAX(scraped_at) DESC"
        ), {"search": f"%{search}%"}).fetchall()
    else:
        rows = db.execute(sql_text(
            "SELECT query, COUNT(*) as count, MAX(scraped_at) as last_scraped "
            "FROM businesses GROUP BY query ORDER BY MAX(scraped_at) DESC"
        )).fetchall()
    return {
        "total": len(rows),
        "queries": [{"query": r[0], "count": r[1], "last_scraped": str(r[2])} for r in rows]
    }


# ─── Results (optimized with filters) ──────────────────────────────────────

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
    no_phone: bool = False,
    db: Session = Depends(get_db)
):
    q = db.query(Business)

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
    if no_phone:
        q = q.filter((Business.phone == "") | (Business.phone.is_(None)))

    total = q.count()
    rows = q.order_by(Business.rating.desc().nulls_last()).offset(offset).limit(limit).all()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": [
            {
                "id": b.id,
                "name": b.name,
                "rating": float(b.rating) if b.rating else None,
                "review_count": b.review_count,
                "category": b.category,
                "address": b.address,
                "city": b.city,
                "state": b.state,
                "phone": b.phone,
                "website": b.website,
                "cid": b.cid,
                "place_id": b.place_id,
                "plus_code": b.plus_code,
                "maps_url": b.maps_url,
                "hours": b.hours,
                "current_status": b.current_status,
                "identifies_as": b.identifies_as,
                "reviews": b.reviews,
                "query": b.query,
                "scraped_at": str(b.scraped_at)
            }
            for b in rows
        ]
    }
