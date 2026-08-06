import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/mapscraper")
GITHUB_REPO = os.getenv("GITHUB_REPO", "sanket-sakariya/map-extension")
WORKFLOW_FILE = os.getenv("WORKFLOW_FILE", "maps-scraper.yml")
