"""GitHub Actions API client — trigger, cancel, list workflows, download artifacts."""
import httpx
import time
from config import GITHUB_REPO, WORKFLOW_FILE

API = "https://api.github.com"


def headers(pat: str):
    return {"Authorization": f"token {pat}", "Accept": "application/vnd.github+json"}


def validate_pat(pat: str) -> dict:
    """Check PAT is valid and has Actions access."""
    r = httpx.get(f"{API}/repos/{GITHUB_REPO}/actions/workflows", headers=headers(pat), timeout=10)
    if r.status_code == 200:
        return {"valid": True, "workflows": r.json().get("total_count", 0)}
    return {"valid": False, "error": r.text}


def trigger_workflow(pat: str, pipeline_url: str = "") -> dict:
    """Trigger maps-scraper workflow_dispatch with pipeline_url input."""
    inputs = {}
    if pipeline_url:
        inputs["pipeline_url"] = pipeline_url

    r = httpx.post(
        f"{API}/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches",
        headers=headers(pat),
        json={"ref": "main", "inputs": inputs},
        timeout=10
    )
    if r.status_code == 204:
        return {"triggered": True}
    return {"triggered": False, "error": r.text}


def list_runs(pat: str, status: str = "in_progress") -> list:
    """List workflow runs by status."""
    r = httpx.get(
        f"{API}/repos/{GITHUB_REPO}/actions/runs",
        headers=headers(pat),
        params={"status": status, "per_page": 20},
        timeout=10
    )
    if r.status_code != 200:
        return []
    return r.json().get("workflow_runs", [])


def cancel_run(pat: str, run_id: int) -> bool:
    """Cancel a workflow run."""
    r = httpx.post(
        f"{API}/repos/{GITHUB_REPO}/actions/runs/{run_id}/cancel",
        headers=headers(pat),
        timeout=10
    )
    return r.status_code == 202


def get_tunnel_url(pat: str, run_id: int) -> str:
    """Download tunnel-urls artifact and extract scraper URL."""
    # List artifacts for this run
    r = httpx.get(
        f"{API}/repos/{GITHUB_REPO}/actions/runs/{run_id}/artifacts",
        headers=headers(pat),
        timeout=10
    )
    if r.status_code != 200:
        return ""

    artifacts = r.json().get("artifacts", [])
    tunnel_artifact = next((a for a in artifacts if a["name"] == "tunnel-urls"), None)
    if not tunnel_artifact:
        return ""

    # Download artifact zip
    dl_url = tunnel_artifact["archive_download_url"]
    r = httpx.get(dl_url, headers=headers(pat), follow_redirects=True, timeout=30)
    if r.status_code != 200:
        return ""

    # Extract scraper-url.txt from zip
    import zipfile, io
    z = zipfile.ZipFile(io.BytesIO(r.content))
    if "scraper-url.txt" in z.namelist():
        return z.read("scraper-url.txt").decode().strip()
    return ""


def get_active_scrapers(pat: str) -> list:
    """Get all running workflows with their tunnel URLs."""
    runs = list_runs(pat, "in_progress")
    scrapers = []
    for run in runs:
        if run.get("name") == "Maps Scraper VNC":
            tunnel = get_tunnel_url(pat, run["id"])
            scrapers.append({
                "run_id": run["id"],
                "status": run["status"],
                "started_at": run["created_at"],
                "tunnel_url": tunnel
            })
    return scrapers
