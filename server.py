"""
Maps Scraper API Server — Async Job Pattern
POST /api/v1/scrape starts a job, returns job_id immediately
GET /api/v1/status/<job_id> polls for results
This avoids Cloudflare's 100s timeout on free tunnels.
"""

import json
import os
import time
import threading
import urllib.parse
import uuid
from pathlib import Path

from flask import Flask, request, jsonify
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

app = Flask(__name__)

PROJECT_DIR = Path(__file__).parent
CHROMIUM_BIN = PROJECT_DIR / "vendor" / "ungoogled-chromium" / "chrome"
CHROMEDRIVER_BIN = PROJECT_DIR / "vendor" / "ungoogled-chromium" / "chromedriver"
EXTENSION_DIR = PROJECT_DIR

# Job store: {job_id: {status, query, progress, total, results, error}}
jobs = {}
scrape_lock = threading.Lock()


def create_driver():
    opts = Options()
    opts.binary_location = str(CHROMIUM_BIN)
    opts.add_argument(f"--load-extension={EXTENSION_DIR}")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=en-US")
    display = os.environ.get("DISPLAY")
    if not display:
        opts.add_argument("--headless=new")
    service = Service(str(CHROMEDRIVER_BIN))
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(5)
    return driver


def collect_listing_urls(driver, query):
    encoded_query = urllib.parse.quote_plus(query)
    url = f"https://www.google.com/maps/search/{encoded_query}/"
    driver.get(url)
    time.sleep(3)

    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "a.hfpxzc") or
                      d.find_elements(By.CSS_SELECTOR, "h1.DUwDvf")
        )
    except:
        return []

    single_place = driver.find_elements(By.CSS_SELECTOR, "h1.DUwDvf")
    feed_links = driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")

    if single_place and not feed_links:
        return [driver.current_url]

    if not feed_links:
        return []

    # Scroll to load all
    driver.set_script_timeout(120)
    driver.execute_async_script("""
        const cb = arguments[arguments.length - 1];
        (async () => {
          const feed = document.querySelector("div[role='feed']") ||
                       document.querySelector(".m6QErb.DxyBCb.kA9KIf.dS8AEf");
          if (!feed) { cb(); return; }
          let prevCount = 0, sameStreak = 0;
          while (sameStreak < 3) {
            const count = document.querySelectorAll("a.hfpxzc").length;
            if (count === prevCount) sameStreak++;
            else { sameStreak = 0; prevCount = count; }
            feed.scrollTop = feed.scrollHeight;
            await new Promise(r => setTimeout(r, 800));
            if (document.querySelector(".HlvSq")) break;
          }
        })().then(cb);
    """)

    links = driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")
    return [l.get_attribute("href") for l in links if l.get_attribute("href")]


def extract_listing_detail(driver):
    return driver.execute_script("""
    function extract() {
      const txt = (sel) => document.querySelector(sel)?.textContent?.trim() || "";
      const name = txt("h1.DUwDvf");
      if (!name) return null;
      const rating = txt(".F7nice span[aria-hidden='true']");
      const reviewCount = txt(".F7nice span[role='img'][aria-label*='reviews']") || txt(".F7nice .UY7F9");
      const category = txt("button.DkEaL");
      const address = txt("button[data-item-id='address'] .Io6YTe");
      const phoneBtn = document.querySelector("button[data-item-id^='phone']");
      const phone = phoneBtn ? phoneBtn.querySelector(".Io6YTe")?.textContent?.trim() || "" : "";
      const plusCode = txt("button[data-item-id='oloc'] .Io6YTe");
      const websiteLink = document.querySelector("a[data-item-id='authority']");
      const website = websiteLink ? websiteLink.href : "";
      const hours = {};
      document.querySelectorAll(".t39EBf.GUrTXd table tr.y0skZc").forEach((row) => {
        const day = row.querySelector(".ylH6lf")?.textContent?.trim();
        const t = row.querySelector(".mxowUb")?.getAttribute("aria-label") ||
                  row.querySelector(".G8aQO")?.textContent?.trim();
        if (day) hours[day] = t || "";
      });
      const hoursToggle = document.querySelector(".OMl5r.hH0dDd.jBYmhd");
      if (hoursToggle && hoursToggle.getAttribute("aria-expanded") === "false") hoursToggle.click();
      const currentStatus = document.querySelector(".ZDu9vd span")?.textContent?.trim() || "";
      const identifiesAs = txt("div[data-item-id='place-info-links:'] .Io6YTe");
      const reviews = [];
      document.querySelectorAll(".jftiEf[data-review-id]").forEach((rev) => {
        reviews.push({
          reviewer: rev.querySelector(".d4r55")?.textContent?.trim() || "",
          stars: rev.querySelector(".kvMYJc")?.getAttribute("aria-label") || "",
          time: rev.querySelector(".rsqaWe")?.textContent?.trim() || "",
          text: rev.querySelector(".wiI7pd")?.textContent?.trim() || "",
          ownerReply: rev.querySelector(".CDe7pd .wiI7pd")?.textContent?.trim() || ""
        });
      });
      const url = window.location.href;
      let cid = "", placeId = "";
      const cidMatch = url.match(/!1s(0x[0-9a-f]+:0x([0-9a-f]+))/i);
      if (cidMatch) { placeId = cidMatch[1]; try { cid = BigInt("0x" + cidMatch[2]).toString(); } catch(e) {} }
      const chijMatch = url.match(/!1s(ChIJ[A-Za-z0-9_-]+)/);
      const googlePlaceId = chijMatch ? chijMatch[1] : "";
      return { name, rating, reviewCount, category, address, phone, plusCode,
               website, hours, currentStatus, identifiesAs, reviews, url,
               cid, placeId: googlePlaceId || placeId };
    }
    return extract();
    """)


def run_scrape_job(job_id, query):
    """Background thread: scrapes all listings and updates job store."""
    driver = None
    try:
        with scrape_lock:
            driver = create_driver()
            listing_urls = collect_listing_urls(driver, query)

            if not listing_urls:
                jobs[job_id].update({"status": "error", "error": "No listings found"})
                return

            total = len(listing_urls)
            jobs[job_id].update({"status": "running", "total": total, "progress": 0})
            results = []

            for i, listing_url in enumerate(listing_urls):
                try:
                    driver.get(listing_url)
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.DUwDvf"))
                        )
                    except:
                        try:
                            consent = driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Accept']")
                            consent.click()
                            time.sleep(2)
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.DUwDvf"))
                            )
                        except:
                            continue
                    time.sleep(0.5)
                    detail = extract_listing_detail(driver)
                    if detail and detail.get("name"):
                        detail["_index"] = i + 1
                        results.append(detail)
                except Exception:
                    continue
                finally:
                    jobs[job_id]["progress"] = i + 1
                    jobs[job_id]["results"] = results

            jobs[job_id].update({
                "status": "done",
                "results": results,
                "total": total,
                "progress": total
            })
    except Exception as e:
        jobs[job_id].update({"status": "error", "error": str(e)})
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


@app.route("/api/v1/scrape", methods=["POST"])
def scrape():
    """Start a scrape job. Returns immediately with job_id for polling."""
    data = request.get_json(force=True)
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "Missing 'query' field"}), 400

    if not CHROMIUM_BIN.exists():
        return jsonify({"status": "error", "message": "Chromium not found"}), 500

    # Check if a job is already running
    running = [j for j in jobs.values() if j["status"] == "running"]
    if running:
        return jsonify({
            "status": "busy",
            "message": "Another scrape in progress. Poll /api/v1/status/<job_id>",
            "running_job": next((k for k, v in jobs.items() if v["status"] == "running"), None)
        }), 429

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "queued", "query": query, "progress": 0, "total": 0, "results": []}

    # Start background thread
    t = threading.Thread(target=run_scrape_job, args=(job_id, query), daemon=True)
    t.start()

    return jsonify({
        "status": "started",
        "job_id": job_id,
        "query": query,
        "poll_url": f"/api/v1/status/{job_id}"
    })


@app.route("/api/v1/status/<job_id>", methods=["GET"])
def status(job_id):
    """Poll job status. When done, includes full results."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "error", "message": "Job not found"}), 404

    response = {
        "status": job["status"],
        "query": job.get("query", ""),
        "progress": job.get("progress", 0),
        "total": job.get("total", 0)
    }

    if job["status"] == "done":
        response["results"] = job["results"]
    elif job["status"] == "error":
        response["error"] = job.get("error", "Unknown error")
    elif job["status"] == "running":
        # Include partial results
        response["results_so_far"] = len(job.get("results", []))

    return jsonify(response)


@app.route("/api/v1/results/<job_id>", methods=["GET"])
def results(job_id):
    """Get full results for a completed job."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "error", "message": "Job not found"}), 404
    if job["status"] != "done":
        return jsonify({"status": job["status"], "message": "Job not complete yet"}), 202
    return jsonify({
        "status": "success",
        "query": job["query"],
        "total": job["total"],
        "results": job["results"]
    })


@app.route("/api/v1/health", methods=["GET"])
def health():
    running_jobs = [k for k, v in jobs.items() if v["status"] == "running"]
    return jsonify({
        "status": "ok",
        "chromium_available": CHROMIUM_BIN.exists(),
        "extension_loaded": (EXTENSION_DIR / "manifest.json").exists(),
        "active_jobs": len(running_jobs),
        "total_jobs": len(jobs)
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8815))
    print(f"🚀 Maps Scraper API starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
