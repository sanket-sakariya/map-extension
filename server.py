"""
Maps Scraper API Server
Flask API that:
1. Receives a search query via POST /api/v1/scrape
2. Launches ungoogled-chromium with the maps extension loaded
3. Navigates to Google Maps, searches the query
4. Triggers the extension content script to scrape all listings
5. Returns structured JSON

Usage:
  python3 server.py
  POST http://localhost:8815/api/v1/scrape {"query": "beauty parlour Rajkot"}
"""

import json
import os
import subprocess
import sys
import time
import threading
import tempfile
import shutil
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
EXTENSION_DIR = PROJECT_DIR  # The extension is in project root (manifest.json, content.js, etc.)

# Lock to serialize browser sessions (one at a time)
scrape_lock = threading.Lock()


def create_driver():
    """Launch ungoogled-chromium with the maps extension loaded."""
    opts = Options()
    opts.binary_location = str(CHROMIUM_BIN)

    # Load our extension
    opts.add_argument(f"--load-extension={EXTENSION_DIR}")

    # Standard headless-friendly flags
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=en-US")

    # Use display if available (VNC), else headless
    display = os.environ.get("DISPLAY")
    if not display:
        opts.add_argument("--headless=new")

    service = Service(str(CHROMEDRIVER_BIN))
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(5)
    return driver


def search_google_maps(driver, query):
    """Navigate to Google Maps search results directly via URL."""
    import urllib.parse
    encoded_query = urllib.parse.quote_plus(query)
    url = f"https://www.google.com/maps/search/{encoded_query}/"
    driver.get(url)

    # Wait for results to load (listing cards appear)
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "a.hfpxzc"))
    )
    time.sleep(2)


def run_scraper_script(driver):
    """
    Inject and run the scraping logic directly via executeScript
    (more reliable than messaging the extension in headless).
    """
    # Read our content.js scraping functions
    content_js = (PROJECT_DIR / "content.js").read_text()

    # We'll inject the core functions and call scrapeListings() directly
    # Strip the chrome.runtime.onMessage listener and just define + call
    inject_script = """
    // --- Injected scraper functions ---
    function waitForInject(selector, timeout = 4000) {
      return new Promise((resolve) => {
        const el = document.querySelector(selector);
        if (el) return resolve(el);
        const observer = new MutationObserver(() => {
          const el = document.querySelector(selector);
          if (el) { observer.disconnect(); resolve(el); }
        });
        observer.observe(document.body, { childList: true, subtree: true });
        setTimeout(() => { observer.disconnect(); resolve(null); }, timeout);
      });
    }

    function waitMs(ms) { return new Promise(r => setTimeout(r, ms)); }

    async function scrollToLoadAllInject() {
      const feed = document.querySelector("div[role='feed']") ||
                   document.querySelector(".m6QErb.DxyBCb.kA9KIf.dS8AEf");
      if (!feed) return;
      let prevCount = 0, sameStreak = 0;
      while (sameStreak < 3) {
        const count = document.querySelectorAll("a.hfpxzc").length;
        if (count === prevCount) sameStreak++;
        else { sameStreak = 0; prevCount = count; }
        feed.scrollTop = feed.scrollHeight;
        await waitMs(800);
        if (document.querySelector(".HlvSq")) break;
      }
    }

    function extractDetailInject() {
      const txt = (sel) => document.querySelector(sel)?.textContent?.trim() || "";
      const name = txt("h1.DUwDvf");
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
        const time = row.querySelector(".mxowUb")?.getAttribute("aria-label") ||
                     row.querySelector(".G8aQO")?.textContent?.trim();
        if (day) hours[day] = time || "";
      });

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
      if (cidMatch) {
        placeId = cidMatch[1];
        try { cid = BigInt("0x" + cidMatch[2]).toString(); } catch(e) {}
      }
      const chijMatch = url.match(/!1s(ChIJ[A-Za-z0-9_-]+)/);
      const googlePlaceId = chijMatch ? chijMatch[1] : "";

      return {
        name, rating, reviewCount, category, address, phone, plusCode,
        website, hours, currentStatus, identifiesAs, reviews, url,
        cid, placeId: googlePlaceId || placeId
      };
    }

    async function scrapeAllInject() {
      await scrollToLoadAllInject();
      const totalLinks = document.querySelectorAll("a.hfpxzc").length;
      if (!totalLinks) return { error: "No listings found", results: [] };

      const results = [];
      for (let i = 0; i < totalLinks; i++) {
        const currentLinks = document.querySelectorAll("a.hfpxzc");
        if (!currentLinks[i]) break;
        currentLinks[i].scrollIntoView({ block: "center" });
        await waitMs(100);
        currentLinks[i].click();
        const nameEl = await waitForInject("h1.DUwDvf", 5000);
        if (!nameEl) {
          const backBtn = document.querySelector("button[aria-label='Back']");
          if (backBtn) { backBtn.click(); await waitMs(800); }
          continue;
        }
        await waitMs(300);
        const hoursToggle = document.querySelector(".OMl5r.hH0dDd.jBYmhd");
        if (hoursToggle && hoursToggle.getAttribute("aria-expanded") === "false") {
          hoursToggle.click();
          await waitMs(300);
        }
        const data = extractDetailInject();
        data._index = i + 1;
        results.push(data);
        const backBtn = document.querySelector("button[aria-label='Back']");
        if (backBtn) {
          backBtn.click();
          await waitForInject("a.hfpxzc", 4000);
          await waitMs(200);
        }
      }
      return { results, total: totalLinks };
    }

    return scrapeAllInject();
    """

    # Execute as async — Selenium handles promises via execute_async_script
    driver.set_script_timeout(600)  # 10 min max for large result sets
    result = driver.execute_async_script(f"""
        const callback = arguments[arguments.length - 1];
        (async () => {{
            {inject_script}
        }})().then(callback).catch(e => callback({{error: e.message}}));
    """)
    return result


@app.route("/api/v1/scrape", methods=["POST"])
def scrape():
    """
    POST /api/v1/scrape
    Body: {"query": "beauty parlour Rajkot", "max_results": 20}
    Returns: {"status": "success", "query": "...", "results": [...], "total": N}
    """
    data = request.get_json(force=True)
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "Missing 'query' field"}), 400

    if not CHROMIUM_BIN.exists():
        return jsonify({"status": "error", "message": "Chromium not found. Run tools/setup_vendor.sh first"}), 500

    # Serialize — one scrape at a time
    acquired = scrape_lock.acquire(timeout=5)
    if not acquired:
        return jsonify({"status": "busy", "message": "Another scrape is in progress. Try again shortly."}), 429

    driver = None
    try:
        driver = create_driver()
        search_google_maps(driver, query)
        result = run_scraper_script(driver)

        if not result or result.get("error"):
            return jsonify({
                "status": "error",
                "query": query,
                "message": result.get("error", "Unknown error during scraping")
            }), 500

        return jsonify({
            "status": "success",
            "query": query,
            "total": result.get("total", len(result.get("results", []))),
            "results": result.get("results", [])
        })
    except Exception as e:
        return jsonify({"status": "error", "query": query, "message": str(e)}), 500
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        scrape_lock.release()


@app.route("/api/v1/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "chromium_available": CHROMIUM_BIN.exists(),
        "extension_loaded": (EXTENSION_DIR / "manifest.json").exists()
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8815))
    print(f"🚀 Maps Scraper API starting on port {port}")
    print(f"   Chromium: {CHROMIUM_BIN}")
    print(f"   Extension: {EXTENSION_DIR}")
    app.run(host="0.0.0.0", port=port, debug=False)
