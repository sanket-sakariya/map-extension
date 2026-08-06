# Plan: Maps Scraping Pipeline

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          FRONTEND (port 3000)                        │
│  - Map Scraper tab: start/stop workflows, set concurrency            │
│  - Provide GitHub PAT → validates & shows available runners          │
│  - Pipeline view: query queue → scraping → results → DB             │
└─────────────┬───────────────────────────────────────────────────────┘
              │ HTTP
┌─────────────▼───────────────────────────────────────────────────────┐
│                        BACKEND / API (port 8000)                      │
│  FastAPI                                                             │
│  - POST /config        → save PAT, set concurrency                   │
│  - GET  /workflows     → list running GitHub workflows               │
│  - POST /workflows/start  → trigger N workflows                      │
│  - POST /workflows/stop   → cancel workflows                         │
│  - POST /queries/generate → generate queries (city × business)       │
│  - GET  /pipeline/status  → queue lengths, active workers            │
│  - GET  /results          → query DB for scraped data                │
└──────┬──────────────────────────┬───────────────────────┬───────────┘
       │                          │                       │
       ▼                          ▼                       ▼
┌──────────────┐  ┌───────────────────────────┐  ┌───────────────────┐
│  Redis Queue │  │  Workflow Orchestrator     │  │  PostgreSQL       │
│              │  │  (background thread)       │  │  (port 5432)      │
│  queues:     │  │  - watches query_queue     │  │                   │
│  - queries   │  │  - assigns to workflows   │  │  tables:          │
│  - results   │  │  - polls job status        │  │  - businesses     │
│              │  │  - pushes to result_queue  │  │  - scrape_jobs    │
└──────────────┘  └───────────────────────────┘  └───────────────────┘
       │                                                  ▲
       ▼                                                  │
┌──────────────────────────────────────┐                  │
│  DB Writer Worker                     │                  │
│  - consumes result_queue              │──────────────────┘
│  - formats JSON → inserts to postgres │
└──────────────────────────────────────┘
```

## Flow

1. User opens frontend → enters PAT → backend validates (checks Actions API)
2. User sets concurrency (e.g., 3 workflows) → backend triggers 3 `maps-scraper.yml` runs
3. User generates queries (city list × business list) → pushed to Redis `query_queue`
4. Orchestrator worker picks queries from `query_queue`, assigns to available workflow tunnels
5. When a workflow scrape job completes, result JSON → pushed to `result_queue`
6. DB writer worker picks from `result_queue`, formats, inserts into PostgreSQL

## File Scope

```
pipeline/
├── docker-compose.yml
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py              # FastAPI app
│   ├── config.py            # Settings
│   ├── models.py            # SQLAlchemy models
│   ├── database.py          # DB connection
│   ├── github_client.py     # GitHub API (trigger/cancel/list workflows)
│   ├── query_generator.py   # Generate city×business queries
│   ├── orchestrator.py      # Background worker: assign queries → workflows
│   └── db_writer.py         # Background worker: result_queue → postgres
├── frontend/
│   ├── Dockerfile
│   ├── nginx.conf
│   └── index.html           # Single-page UI (vanilla JS)
└── init.sql                 # PostgreSQL schema
```

## Acceptance Criteria
- `docker compose up` starts everything
- Open http://localhost:3000 → see pipeline UI
- Enter PAT → shows workflow count
- Start/stop workflows
- Generate queries → see them flow through pipeline → land in postgres
- Query DB via API or psql to see scraped business data
