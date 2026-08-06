// background.js — persists scraping state even when popup closes

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === "startScraping") {
    startScraping(msg.tabId);
    sendResponse({ ok: true });
  }
  if (msg.action === "getStatus") {
    chrome.storage.local.get(["scrapeState"], (d) => sendResponse(d.scrapeState || {}));
    return true;
  }
  if (msg.action === "scrapeDone") {
    chrome.storage.local.set({ scrapeState: { status: "done", results: msg.results, total: msg.total } });
  }
  if (msg.action === "scrapeProgress") {
    chrome.storage.local.set({ scrapeState: { status: "running", current: msg.current, total: msg.total } });
  }
  if (msg.action === "scrapeError") {
    chrome.storage.local.set({ scrapeState: { status: "error", error: msg.error } });
  }
  if (msg.action === "clearResults") {
    chrome.storage.local.remove("scrapeState");
    sendResponse({ ok: true });
  }
});

async function startScraping(tabId) {
  chrome.storage.local.set({ scrapeState: { status: "running", current: 0, total: 0 } });

  // Inject content script
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      files: ["content.js"]
    });
  } catch (e) {
    chrome.storage.local.set({ scrapeState: { status: "error", error: e.message } });
    return;
  }

  // Small delay then tell content script to go
  setTimeout(() => {
    chrome.tabs.sendMessage(tabId, { action: "start" });
  }, 300);
}
