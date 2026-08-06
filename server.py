"""
Maps Scraper API Server
Flask API that:
1. Receives a search query via POST /api/v1/scrape
2. Launches ungoogled-chromium with the maps extension loaded
3. Navigates to Google Maps, searches the query
4. Collects all listing URLs, visits each one, extracts data
5. Returns structured JSON
"""

import json
import os
import time
import threading
import urllib.parse
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

scrape_lock = threading.Lock()


def create_driver():
    """Launch ungoogled-chromium."""
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
    """Phase 1: Navigate to search, scroll to load all, collect all listing hrefs."""
    encoded_query = urllib.parse.quote_plus(query)
    url = f"https://www.google.com/maps/search/{encoded_query}/"
    driver.get(url)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "a.hfpxzc"))
    )
    time.sleep(2)

    # Scroll to load all listings
    scroll_script = """
    async function scrollAll() {
      const feed = document.querySelector("div[role='feed']") ||
                   document.querySelector(".m6QErb.DxyBCb.kA9KIf.dS8AEf");
      if (!feed) return;
      let prevCount = 0, sameStreak = 0;
      while (sameStreak < 3) {
        const count = document.querySelectorAll("a.hfpxzc").length;
        if (count === prevCount) sameStreak++;
        else { sameStreak = 0; prevCount = count; }
        feed.scrollTop = feed.scrollHeight;
        await new Promise(r => setTimeout(r, 800));
        if (document.querySelector(".HlvSq")) break;
      }
    }
    return scrollAll();
    """
    driver.set_script_timeout(120)
    driver.execute_async_script(f"""
        const cb = arguments[arguments.length - 1];
        (async () => {{ {scroll_script} }})().then(cb);
    """)

    # Collect all hrefs
    links = driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")
    urls = []
    for link in links:
        href = link.get_attribute("href")
        if href:
            urls.append(href)
    return urls


def extract_listing_detail(driver):
    """Phase 2: Extract data from a listing detail page."""
    extract_script = """
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
        const time = row.querySelector(".mxowUb")?.getAttribute("aria-label") ||
                     row.querySelector(".G8aQO")?.textContent?.trim();
        if (day) hours[day] = time || "";
      });

      // Expand hours if collapsed
      const hoursToggle = document.querySelector(".OMl5r.hH0dDd.jBYmhd");
      if (hoursToggle && hoursToggle.getAttribute("aria-expanded") === "false") {
        hoursToggle.click();
      }

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
    return extract();
    """
    return driver.execute_script(extract_script)


@app.route("/api/v1/scrape", methods=["POST"])
def scrape():
    data = request.get_json(force=True)
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"status": "error", "message": "Missing 'query' field"}), 400

    if not CHROMIUM_BIN.exists():
        return jsonify({"status": "error", "message": "Chromium not found. Run tools/setup_vendor.sh"}), 500

    acquired = scrape_lock.acquire(timeout=5)
    if not acquired:
        return jsonify({"status": "busy", "message": "Another scrape in progress."}), 429

    driver = None
    try:
        driver = create_driver()

        # Phase 1: collect all listing URLs
        listing_urls = collect_listing_urls(driver, query)
        if not listing_urls:
            return jsonify({"status": "error", "query": query, "message": "No listings found"}), 404

        total = len(listing_urls)
        results = []

        # Phase 2: visit each URL directly and extract
        for i, listing_url in enumerate(listing_urls):
            try:
                driver.get(listing_url)
                # Wait for detail panel name to appear
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h1.DUwDvf"))
                )
                time.sleep(0.5)  # brief settle

                detail = extract_listing_detail(driver)
                if detail and detail.get("name"):
                    detail["_index"] = i + 1
                    results.append(detail)
            except Exception:
                continue  # skip failed listings

        return jsonify({
            "status": "success",
            "query": query,
            "total": total,
            "results": results
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
