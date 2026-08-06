// content.js — fast scraping with smart waits, reports progress to background service worker

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === "start") {
    scrapeListings();
  }
});

function reportProgress(current, total) {
  chrome.runtime.sendMessage({ action: "scrapeProgress", current, total });
}

function reportDone(results, total) {
  chrome.runtime.sendMessage({ action: "scrapeDone", results, total });
}

function reportError(error) {
  chrome.runtime.sendMessage({ action: "scrapeError", error });
}

// Wait for a selector to appear (max timeout)
function waitFor(selector, timeout = 4000) {
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

function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function scrollToLoadAll() {
  const feed = document.querySelector("div[role='feed']") ||
               document.querySelector(".m6QErb.DxyBCb.kA9KIf.dS8AEf");
  if (!feed) return;

  let prevCount = 0;
  let sameCountStreak = 0;

  while (sameCountStreak < 3) {
    const count = document.querySelectorAll("a.hfpxzc").length;
    if (count === prevCount) {
      sameCountStreak++;
    } else {
      sameCountStreak = 0;
      prevCount = count;
    }
    feed.scrollTop = feed.scrollHeight;
    await wait(800);

    // End of list marker
    if (document.querySelector(".HlvSq")) break;
  }
}

async function scrapeListings() {
  try {
    // Scroll to load all listings first
    await scrollToLoadAll();

    const totalLinks = document.querySelectorAll("a.hfpxzc").length;
    if (!totalLinks) {
      reportError("No listings found. Make sure you have Maps search results open.");
      return;
    }

    reportProgress(0, totalLinks);
    const results = [];

    for (let i = 0; i < totalLinks; i++) {
      const currentLinks = document.querySelectorAll("a.hfpxzc");
      if (!currentLinks[i]) break;

      currentLinks[i].scrollIntoView({ block: "center" });
      await wait(100);
      currentLinks[i].click();

      // Wait for detail panel to load (h1 with business name)
      const nameEl = await waitFor("h1.DUwDvf", 5000);
      if (!nameEl) {
        // Skip this one if panel didn't load
        const backBtn = document.querySelector("button[aria-label='Back']");
        if (backBtn) { backBtn.click(); await wait(800); }
        continue;
      }
      await wait(300); // tiny settle

      // Expand hours
      const hoursToggle = document.querySelector(".OMl5r.hH0dDd.jBYmhd");
      if (hoursToggle && hoursToggle.getAttribute("aria-expanded") === "false") {
        hoursToggle.click();
        await wait(300);
      }

      const data = extractDetail();
      data._index = i + 1;
      results.push(data);
      reportProgress(i + 1, totalLinks);

      // Go back
      const backBtn = document.querySelector("button[aria-label='Back']");
      if (backBtn) {
        backBtn.click();
        // Wait for listing feed to reappear
        await waitFor("a.hfpxzc", 4000);
        await wait(200);
      }
    }

    reportDone(results, totalLinks);
  } catch (e) {
    reportError(e.message);
  }
}

function extractDetail() {
  const txt = (sel) => document.querySelector(sel)?.textContent?.trim() || "";

  const name = txt("h1.DUwDvf");
  const rating = txt(".F7nice span[aria-hidden='true']");
  const reviewCount = txt(".F7nice span[role='img'][aria-label*='reviews']") ||
                      txt(".F7nice .UY7F9");
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

  // Extract CID and Place ID from URL
  // URL contains patterns like !1s0x...:0x<CID_HEX>!
  let cid = "";
  let placeId = "";
  const cidMatch = url.match(/!1s(0x[0-9a-f]+:0x([0-9a-f]+))/i);
  if (cidMatch) {
    placeId = cidMatch[1]; // full hex place reference
    cid = BigInt("0x" + cidMatch[2]).toString(); // convert hex CID to decimal
  }

  // Also try to get the canonical Place ID (ChIJ... format) from meta or data attributes
  const canonicalLink = document.querySelector("link[href*='/maps/place/']");
  const metaPlaceId = document.querySelector("meta[itemprop='placeId']")?.content || "";
  // Fallback: parse from the URL data segment — !1sChIJ...!
  const chijMatch = url.match(/!1s(ChIJ[A-Za-z0-9_-]+)/);
  const googlePlaceId = metaPlaceId || (chijMatch ? chijMatch[1] : "");

  return {
    name, rating, reviewCount, category, address, phone, plusCode,
    website, hours, currentStatus, identifiesAs, reviews, url,
    cid, placeId: googlePlaceId || placeId
  };
}
