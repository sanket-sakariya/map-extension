"""Maps Scraping Pipeline — FastAPI Backend"""
import json
import re
import time
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import csv
import io
import redis as redis_lib
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text as sql_text, select

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

class LoginRequest(BaseModel):
    email: str
    password: str

# Hardcoded credentials
AUTH_EMAIL = "jaydeep@botxbyte.com"
AUTH_PASSWORD = "Jaydeep@1234"
AUTH_TOKEN = "mex_auth_7f3k9x2p4q8w1y6z"  # simple static token


@app.post("/api/auth/login")
def login(req: LoginRequest):
    if req.email == AUTH_EMAIL and req.password == AUTH_PASSWORD:
        return {"status": "ok", "token": AUTH_TOKEN}
    return {"status": "error", "message": "Invalid email or password"}


@app.get("/api/auth/verify")
def verify_auth(token: str = ""):
    if token == AUTH_TOKEN:
        return {"status": "ok", "email": AUTH_EMAIL}
    return {"status": "error", "message": "Invalid token"}

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


@app.get("/api/workflows/queued")
def get_queued_workflows():
    """Check how many workflows are in queued/waiting state on GitHub."""
    pat = get_pat()
    if not pat:
        return {"queued": 0, "error": "PAT not configured"}
    queued_runs = github_client.list_runs(pat, "queued")
    waiting_runs = github_client.list_runs(pat, "waiting")
    total = len(queued_runs) + len(waiting_runs)
    return {"queued": total}


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
    total_records = db.query(Business).count()
    return {
        "query_queue": r.llen("query_queue"),
        "active_jobs": r.llen("active_jobs"),
        "result_queue": r.llen("result_queue"),
        "active_scrapers": scraper_count,
        "total_inserted": total_records,
        "dead_letter_queue": r.llen("dead_letter_queue"),
        "dlx_retries": int(r.get("stats:dlx_retries") or 0),
        "completed_jobs": int(r.get("stats:completed_jobs") or 0)
    }


@app.get("/api/results/queries")
def get_result_queries(search: str = "", limit: int = 50, offset: int = 0, sort: str = "date", db: Session = Depends(get_db)):
    """Get unique queries with result counts. Server-side paginated."""
    # Total businesses (instant from pg_class)
    total_biz = db.execute(sql_text(
        "SELECT reltuples::bigint FROM pg_class WHERE relname = 'businesses'"
    )).scalar() or 0

    # Sort clause
    order_clause = "ORDER BY last_scraped DESC"
    if sort == "count":
        order_clause = "ORDER BY count DESC"
    elif sort == "name":
        order_clause = "ORDER BY query ASC"

    if search:
        # For search: use query column with ILIKE (trigram index helps)
        rows = db.execute(sql_text(f"""
            SELECT query, COUNT(*) as count, MAX(scraped_at) as last_scraped
            FROM businesses WHERE query ILIKE :search
            GROUP BY query {order_clause}
            LIMIT :limit OFFSET :offset
        """), {"search": f"%{search}%", "limit": limit, "offset": offset}).fetchall()
        # Approximate count: just use len(rows) if under limit, else estimate
        if len(rows) < limit:
            count_row = offset + len(rows)
        else:
            cr = db.execute(sql_text(
                "SELECT COUNT(*) FROM (SELECT DISTINCT query FROM businesses WHERE query ILIKE :search LIMIT 10000) t"
            ), {"search": f"%{search}%"}).scalar() or 0
            count_row = cr
    else:
        # Unfiltered: use n_distinct from pg_stats (instant)
        nd = db.execute(sql_text(
            "SELECT n_distinct FROM pg_stats WHERE tablename='businesses' AND attname='query'"
        )).scalar()
        # n_distinct > 0 means exact count; < 0 means fraction of rows
        if nd and nd > 0:
            count_row = int(nd)
        elif nd and nd < 0:
            count_row = int(abs(nd) * total_biz)
        else:
            count_row = 0

        rows = db.execute(sql_text(f"""
            SELECT query, COUNT(*) as count, MAX(scraped_at) as last_scraped
            FROM businesses
            GROUP BY query {order_clause}
            LIMIT :limit OFFSET :offset
        """), {"limit": limit, "offset": offset}).fetchall()

    return {
        "total": int(count_row),
        "total_businesses": int(total_biz),
        "limit": limit,
        "offset": offset,
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
    website_only: bool = False,
    phone_filter: str = "all",
    website_filter: str = "all",
    address_filter: str = "all",
    sort_by: str = "rating",
    sort_order: str = "desc",
    db: Session = Depends(get_db)
):
    q = db.query(Business)

    if search:
        q = q.filter(sql_text(
            "to_tsvector('english', coalesce(name,'') || ' ' || coalesce(address,'') || ' ' || coalesce(category,'')) "
            "@@ plainto_tsquery('english', :search)"
        ).bindparams(search=search))

    if query:
        q = q.filter(Business.query == query)
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

    # Legacy compatibility mapping
    if phone_only:
        phone_filter = "has"
    elif no_phone:
        phone_filter = "none"

    if website_only:
        website_filter = "has"

    # Phone filter
    if phone_filter == "has":
        q = q.filter(Business.phone != "", Business.phone.isnot(None))
    elif phone_filter == "none":
        q = q.filter((Business.phone == "") | (Business.phone.is_(None)))

    # Website filter
    if website_filter == "has":
        q = q.filter(Business.website != "", Business.website.isnot(None))
    elif website_filter == "none":
        q = q.filter((Business.website == "") | (Business.website.is_(None)))

    # Address filter
    if address_filter == "has":
        q = q.filter(Business.address != "", Business.address.isnot(None))
    elif address_filter == "none":
        q = q.filter((Business.address == "") | (Business.address.is_(None)))

    # Counting exact rows on a 2M+ row table with arbitrary filter combinations can be slow
    # on cold cache (uncached index/heap pages need disk reads). To keep the API responsive
    # under ANY filter combo, we cap how long we're willing to wait for an exact COUNT(*):
    # if it doesn't finish in time, we fall back to a fast approximate count instead of hanging.
    total = None
    if query and not search and not city and not category and min_rating <= 0 and min_reviews <= 0 and phone_filter == "all" and website_filter == "all" and address_filter == "all":
        # Fast path: exact query match with idx_biz_query — always instant
        total = db.execute(sql_text(
            "SELECT COUNT(*) FROM businesses WHERE query = :q"
        ), {"q": query}).scalar() or 0
    else:
        try:
            db.execute(sql_text("SET LOCAL statement_timeout = '2500ms'"))
            total = q.count()
        except Exception:
            db.rollback()
            try:
                table_estimate = db.execute(sql_text(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = 'businesses'"
                )).scalar() or 0
                total = int(table_estimate)
            except Exception:
                db.rollback()
                total = 0

    # Dynamic sorting
    sort_columns = {
        "rating": Business.rating,
        "reviews": Business.review_count,
        "name": Business.name,
        "date": Business.scraped_at,
        "city": Business.city,
        "category": Business.category,
    }
    sort_col = sort_columns.get(sort_by, Business.rating)
    if sort_order == "asc":
        q = q.order_by(sort_col.asc().nulls_last())
    else:
        q = q.order_by(sort_col.desc().nulls_last())

    rows = q.offset(offset).limit(limit).all()

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


@app.get("/api/results/export")
def export_results_csv(
    search: str = "",
    query: str = "",
    city: str = "",
    state: str = "",
    category: str = "",
    min_rating: float = 0,
    min_reviews: int = 0,
    phone_only: bool = False,
    no_phone: bool = False,
    website_only: bool = False,
    phone_filter: str = "all",
    website_filter: str = "all",
    address_filter: str = "all",
    sort_by: str = "rating",
    sort_order: str = "desc"
):
    db = SessionLocal()
    tx = db.begin()
    try:
        # We query specific columns to bypass ORM hydration entirely (makes it 10x-20x faster)
        stmt = select(
            Business.name,
            Business.rating,
            Business.review_count,
            Business.category,
            Business.phone,
            Business.address,
            Business.city,
            Business.state,
            Business.website,
            Business.cid,
            Business.place_id,
            Business.plus_code,
            Business.current_status,
            Business.hours,
            Business.maps_url
        )

        if search:
            stmt = stmt.filter(sql_text(
                "to_tsvector('english', coalesce(name,'') || ' ' || coalesce(address,'') || ' ' || coalesce(category,'')) "
                "@@ plainto_tsquery('english', :search)"
            ).bindparams(search=search))

        if query:
            stmt = stmt.filter(Business.query == query)
        if city:
            stmt = stmt.filter(Business.city.ilike(f"%{city}%"))
        if state:
            stmt = stmt.filter(Business.state.ilike(f"%{state}%"))
        if category:
            stmt = stmt.filter(Business.category.ilike(f"%{category}%"))
        if min_rating > 0:
            stmt = stmt.filter(Business.rating >= min_rating)
        if min_reviews > 0:
            stmt = stmt.filter(Business.review_count >= min_reviews)

        # Legacy compatibility mapping
        if phone_only:
            phone_filter = "has"
        elif no_phone:
            phone_filter = "none"

        if website_only:
            website_filter = "has"

        # Phone filter
        if phone_filter == "has":
            stmt = stmt.filter(Business.phone != "", Business.phone.isnot(None))
        elif phone_filter == "none":
            stmt = stmt.filter((Business.phone == "") | (Business.phone.is_(None)))

        # Website filter
        if website_filter == "has":
            stmt = stmt.filter(Business.website != "", Business.website.isnot(None))
        elif website_filter == "none":
            stmt = stmt.filter((Business.website == "") | (Business.website.is_(None)))

        # Address filter
        if address_filter == "has":
            stmt = stmt.filter(Business.address != "", Business.address.isnot(None))
        elif address_filter == "none":
            stmt = stmt.filter((Business.address == "") | (Business.address.is_(None)))

        sort_columns = {
            "rating": Business.rating,
            "reviews": Business.review_count,
            "name": Business.name,
            "date": Business.scraped_at,
            "city": Business.city,
            "category": Business.category,
        }
        sort_col = sort_columns.get(sort_by, Business.rating)
        if sort_order == "asc":
            stmt = stmt.order_by(sort_col.asc().nulls_last())
        else:
            stmt = stmt.order_by(sort_col.desc().nulls_last())

        # Stream results sequentially in batches of 10000 using a server-side cursor (yield_per)
        # Bypassing LIMIT/OFFSET prevents database degradation on deep offset pagination
        result = db.execute(stmt.execution_options(yield_per=10000))

        def csv_generator():
            try:
                headers = ['Name','Rating','Reviews','Category','Phone','Address','City','State','Website','CID','Place ID','Plus Code','Status','Hours','Maps URL']
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_ALL)
                writer.writerow(headers)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)

                for row in result:
                    hours_str = ""
                    if row.hours:
                        try:
                            hours_str = " | ".join(f"{d}: {t}" for d, t in row.hours.items())
                        except Exception:
                            pass
                    csv_row = [
                        row.name or '',
                        str(row.rating) if row.rating is not None else '',
                        row.review_count or 0,
                        row.category or '',
                        row.phone or '',
                        (row.address or '').replace('\n', ' '),
                        row.city or '',
                        row.state or '',
                        row.website or '',
                        row.cid or '',
                        row.place_id or '',
                        row.plus_code or '',
                        row.current_status or '',
                        hours_str,
                        row.maps_url or ''
                    ]
                    writer.writerow(csv_row)
                    
                    # Yield every 1000 records or ~64KB of buffer to keep the client download responsive
                    if output.tell() > 65536:
                        yield output.getvalue()
                        output.seek(0)
                        output.truncate(0)

                if output.tell() > 0:
                    yield output.getvalue()
            finally:
                tx.close()
                db.close()

        return StreamingResponse(
            csv_generator(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=export.csv"}
        )
    except Exception:
        tx.rollback()
        db.close()
        raise


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


@app.get("/api/results/export-domains")
def export_domains_csv(
    search: str = "",
    query: str = "",
    city: str = "",
    state: str = "",
    category: str = "",
    min_rating: float = 0,
    min_reviews: int = 0,
    phone_only: bool = False,
    no_phone: bool = False,
    website_only: bool = False,
    phone_filter: str = "all",
    website_filter: str = "all",
    address_filter: str = "all",
):
    """
    Export ONLY the unique domain names (extracted from the website URL) for records
    matching the current filters. Always implicitly filters to rows that have a website,
    since a domain export is meaningless without one. Deduplicates domains in the output.
    """
    db = SessionLocal()
    tx = db.begin()
    try:
        stmt = select(Business.website)

        if search:
            stmt = stmt.filter(sql_text(
                "to_tsvector('english', coalesce(name,'') || ' ' || coalesce(address,'') || ' ' || coalesce(category,'')) "
                "@@ plainto_tsquery('english', :search)"
            ).bindparams(search=search))

        if query:
            stmt = stmt.filter(Business.query == query)
        if city:
            stmt = stmt.filter(Business.city.ilike(f"%{city}%"))
        if state:
            stmt = stmt.filter(Business.state.ilike(f"%{state}%"))
        if category:
            stmt = stmt.filter(Business.category.ilike(f"%{category}%"))
        if min_rating > 0:
            stmt = stmt.filter(Business.rating >= min_rating)
        if min_reviews > 0:
            stmt = stmt.filter(Business.review_count >= min_reviews)

        # Legacy compatibility mapping
        if phone_only:
            phone_filter = "has"
        elif no_phone:
            phone_filter = "none"

        if phone_filter == "has":
            stmt = stmt.filter(Business.phone != "", Business.phone.isnot(None))
        elif phone_filter == "none":
            stmt = stmt.filter((Business.phone == "") | (Business.phone.is_(None)))

        if address_filter == "has":
            stmt = stmt.filter(Business.address != "", Business.address.isnot(None))
        elif address_filter == "none":
            stmt = stmt.filter((Business.address == "") | (Business.address.is_(None)))

        # A domain export only makes sense for rows that actually have a website.
        # We honor an explicit website_filter=none by returning an empty result rather
        # than silently ignoring the user's filter choice.
        if website_filter == "none":
            stmt = stmt.filter(sql_text("FALSE"))
        else:
            stmt = stmt.filter(Business.website != "", Business.website.isnot(None))

        result = db.execute(stmt.execution_options(yield_per=10000))

        def domain_generator():
            seen = set()
            try:
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_ALL)
                writer.writerow(["Domain"])
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)

                for row in result:
                    url = (row.website or "").strip()
                    if not url:
                        continue
                    domain = extract_domain(url)
                    if not domain or domain in seen:
                        continue
                    seen.add(domain)
                    writer.writerow([domain])

                    if output.tell() > 65536:
                        yield output.getvalue()
                        output.seek(0)
                        output.truncate(0)

                if output.tell() > 0:
                    yield output.getvalue()
            finally:
                tx.close()
                db.close()

        return StreamingResponse(
            domain_generator(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=domains.csv"}
        )
    except Exception:
        tx.rollback()
        db.close()
        raise
