import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/mapscraper")
GITHUB_REPO = os.getenv("GITHUB_REPO", "aug-026/map")
WORKFLOW_FILE = os.getenv("WORKFLOW_FILE", "map-vnc.yaml")
