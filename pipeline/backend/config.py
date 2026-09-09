import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/mapscraper")
GITHUB_REPO = os.getenv("GITHUB_REPO", "aug-026/map")
WORKFLOW_FILE = os.getenv("WORKFLOW_FILE", "map-vnc.yaml")

# Public base URL the scrapers use to call back into this API. Scrapers run on
# GitHub Actions runners, so this must be reachable from the public internet.
#
# It lives here (and in the Deployment) rather than only in the settings table
# because the DB value survived a cluster migration and kept pointing at the old
# cluster's load balancer, so every dispatched scraper registered against a dead
# ELB. Prefer the stable domain over a raw *.elb.amazonaws.com hostname, which
# changes whenever the LoadBalancer is recreated.
PIPELINE_PUBLIC_URL = os.getenv("PIPELINE_PUBLIC_URL", "https://map.articleinnovator.com")
