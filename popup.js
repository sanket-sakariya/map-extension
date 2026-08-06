const startBtn = document.getElementById("start");
const dlBtn = document.getElementById("download");
const clearBtn = document.getElementById("clear");
const status = document.getElementById("status");
const progress = document.getElementById("progress");
const progressBar = document.getElementById("progressBar");
const resultsEl = document.getElementById("results");

let pollInterval = null;

// On popup open, immediately check state
checkStatus();

startBtn.addEventListener("click", async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab.url?.includes("google.com/maps")) {
    status.textContent = "Navigate to Google Maps first!";
    return;
  }
  startBtn.disabled = true;
  chrome.runtime.sendMessage({ action: "startScraping", tabId: tab.id });
  status.textContent = "Starting...";
  startPolling();
});

dlBtn.addEventListener("click", () => {
  chrome.storage.local.get(["scrapeState"], (d) => {
    const data = d.scrapeState?.results;
    if (!data) return;
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `maps-data-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  });
});

clearBtn.addEventListener("click", () => {
  chrome.runtime.sendMessage({ action: "clearResults" }, () => {
    resetUI();
  });
});

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(checkStatus, 1000);
}

function checkStatus() {
  chrome.runtime.sendMessage({ action: "getStatus" }, (state) => {
    if (!state || !state.status) {
      resetUI();
      return;
    }
    if (state.status === "running") {
      startBtn.disabled = true;
      progress.style.display = "block";
      dlBtn.style.display = "none";
      clearBtn.style.display = "none";
      resultsEl.style.display = "none";
      const pct = state.total ? Math.round((state.current / state.total) * 100) : 0;
      progressBar.style.width = pct + "%";
      status.textContent = `Scraping... ${state.current}/${state.total} listings`;
      if (!pollInterval) startPolling();
    } else if (state.status === "done") {
      stopPolling();
      startBtn.disabled = false;
      progress.style.display = "none";
      dlBtn.style.display = "inline-block";
      clearBtn.style.display = "inline-block";
      resultsEl.style.display = "block";
      status.textContent = `Done! ${state.results.length} of ${state.total} listings scraped.`;
      resultsEl.textContent = JSON.stringify(state.results, null, 2);
    } else if (state.status === "error") {
      stopPolling();
      startBtn.disabled = false;
      progress.style.display = "none";
      status.textContent = "Error: " + state.error;
    }
  });
}

function stopPolling() {
  if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
}

function resetUI() {
  startBtn.disabled = false;
  progress.style.display = "none";
  dlBtn.style.display = "none";
  clearBtn.style.display = "none";
  resultsEl.style.display = "none";
  status.textContent = "";
  stopPolling();
}
