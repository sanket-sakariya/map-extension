from sqlalchemy import Column, Integer, Text, TIMESTAMP, BigInteger, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from database import Base


class ActiveScraper(Base):
    __tablename__ = "active_scrapers"
    id = Column(Integer, primary_key=True)
    run_id = Column(BigInteger, unique=True)
    tunnel_url = Column(Text, nullable=False)
    registered_at = Column(TIMESTAMP, server_default=func.now())
    last_heartbeat = Column(TIMESTAMP, server_default=func.now())


class Business(Base):
    __tablename__ = "businesses"
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    cid = Column(Text)
    place_id = Column(Text)
    category = Column(Text)
    rating = Column(Numeric(2, 1))
    review_count = Column(Integer, default=0)
    phone = Column(Text)
    website = Column(Text)
    address = Column(Text)
    city = Column(Text)
    state = Column(Text)
    plus_code = Column(Text)
    current_status = Column(Text)
    identifies_as = Column(Text)
    hours = Column(JSONB)
    reviews = Column(JSONB)
    maps_url = Column(Text)
    query = Column(Text)
    scraped_at = Column(TIMESTAMP, server_default=func.now())


class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"
    id = Column(Integer, primary_key=True)
    job_id = Column(Text, nullable=False)
    query = Column(Text, nullable=False)
    workflow_run_id = Column(BigInteger)
    tunnel_url = Column(Text)
    status = Column(Text, default="queued")
    total = Column(Integer, default=0)
    progress = Column(Integer, default=0)
    results_count = Column(Integer, default=0)
    error = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.now())
    completed_at = Column(TIMESTAMP)
