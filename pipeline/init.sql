-- Optimized schema for fast lookups on any field

-- Settings (PAT token, config)
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Active scrapers (self-registered by workflows)
CREATE TABLE IF NOT EXISTS active_scrapers (
    id SERIAL PRIMARY KEY,
    run_id BIGINT UNIQUE,
    tunnel_url TEXT NOT NULL,
    registered_at TIMESTAMP DEFAULT NOW(),
    last_heartbeat TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scrapers_tunnel ON active_scrapers(tunnel_url);

CREATE TABLE IF NOT EXISTS businesses (
    id SERIAL PRIMARY KEY,

    -- Core identity
    name TEXT NOT NULL,
    cid TEXT,
    place_id TEXT,

    -- Business info
    category TEXT,
    rating NUMERIC(2,1),
    review_count INTEGER DEFAULT 0,
    phone TEXT,
    website TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    plus_code TEXT,

    -- Status
    current_status TEXT,
    identifies_as TEXT,

    -- Structured data (JSONB for flexible querying)
    hours JSONB,
    reviews JSONB,

    -- URLs
    maps_url TEXT,

    -- Pipeline metadata
    query TEXT,
    scraped_at TIMESTAMP DEFAULT NOW(),

    -- Prevent duplicates: same CID for same query
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

-- ═══════════════════════════════════════════════════════════════════
-- INDEXES: Cover all common query patterns
-- ═══════════════════════════════════════════════════════════════════

-- Identity lookups (exact match)
CREATE INDEX IF NOT EXISTS idx_biz_cid ON businesses(cid);
CREATE INDEX IF NOT EXISTS idx_biz_place_id ON businesses(place_id);
CREATE INDEX IF NOT EXISTS idx_biz_phone ON businesses(phone);

-- Category/geo filtering (most common queries)
CREATE INDEX IF NOT EXISTS idx_biz_category ON businesses(category);
CREATE INDEX IF NOT EXISTS idx_biz_city ON businesses(city);
CREATE INDEX IF NOT EXISTS idx_biz_state ON businesses(state);
CREATE INDEX IF NOT EXISTS idx_biz_city_category ON businesses(city, category);

-- Rating sorting/filtering
CREATE INDEX IF NOT EXISTS idx_biz_rating ON businesses(rating DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_biz_review_count ON businesses(review_count DESC NULLS LAST);

-- Query tracking
CREATE INDEX IF NOT EXISTS idx_biz_query ON businesses(query);
CREATE INDEX IF NOT EXISTS idx_biz_scraped_at ON businesses(scraped_at DESC);

-- Full-text search on name + address + category
CREATE INDEX IF NOT EXISTS idx_biz_fts ON businesses
    USING GIN (to_tsvector('english', coalesce(name,'') || ' ' || coalesce(address,'') || ' ' || coalesce(category,'')));

-- JSONB indexes for querying inside hours/reviews
CREATE INDEX IF NOT EXISTS idx_biz_hours ON businesses USING GIN (hours);
CREATE INDEX IF NOT EXISTS idx_biz_reviews ON businesses USING GIN (reviews);

-- Partial indexes for has/none filters (website, phone, address) — makes COUNT(*) instant
CREATE INDEX IF NOT EXISTS idx_biz_has_website ON businesses(id) WHERE website IS NOT NULL AND website != '';
CREATE INDEX IF NOT EXISTS idx_biz_no_website ON businesses(id) WHERE website IS NULL OR website = '';
CREATE INDEX IF NOT EXISTS idx_biz_has_phone ON businesses(id) WHERE phone IS NOT NULL AND phone != '';
CREATE INDEX IF NOT EXISTS idx_biz_no_phone ON businesses(id) WHERE phone IS NULL OR phone = '';
CREATE INDEX IF NOT EXISTS idx_biz_has_address ON businesses(id) WHERE address IS NOT NULL AND address != '';
CREATE INDEX IF NOT EXISTS idx_biz_no_address ON businesses(id) WHERE address IS NULL OR address = '';

-- Scrape jobs
CREATE INDEX IF NOT EXISTS idx_jobs_status ON scrape_jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON scrape_jobs(created_at DESC);
