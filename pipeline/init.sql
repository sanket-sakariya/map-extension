CREATE TABLE IF NOT EXISTS businesses (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    rating TEXT,
    review_count TEXT,
    category TEXT,
    address TEXT,
    phone TEXT,
    plus_code TEXT,
    website TEXT,
    hours JSONB,
    current_status TEXT,
    identifies_as TEXT,
    cid TEXT,
    place_id TEXT,
    maps_url TEXT,
    reviews JSONB,
    query TEXT,
    scraped_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(cid, query)
);

CREATE TABLE IF NOT EXISTS scrape_jobs (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    query TEXT NOT NULL,
    workflow_run_id BIGINT,
    tunnel_url TEXT,
    status TEXT DEFAULT 'queued',
    total INTEGER DEFAULT 0,
    progress INTEGER DEFAULT 0,
    results_count INTEGER DEFAULT 0,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_businesses_cid ON businesses(cid);
CREATE INDEX IF NOT EXISTS idx_businesses_query ON businesses(query);
CREATE INDEX IF NOT EXISTS idx_scrape_jobs_status ON scrape_jobs(status);
