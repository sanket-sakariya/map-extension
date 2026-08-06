# Plan: Google Maps Listing Scraper

## Architecture

```
[External Client] --POST /api/v1/scrape {"query":"..."}-->
  [Cloudflare Tunnel] -->
    [Flask server.py :8815] -->
      [Ungoogled-Chromium + Extension loaded] -->
        [Navigate to Google Maps, search, scroll, click each listing, extract data] -->
          [Return JSON with name, rating, phone, address, hours, reviews, CID, placeId]
```

## Files
- `manifest.json` — MV3 Chrome extension manifest
- `content.js` — content script (clicks listings, extracts data, reports progress)
- `background.js` — service worker (persists state for popup)
- `popup.html` / `popup.js` — manual scrape trigger UI
- `server.py` — Flask API server for headless/automated scraping
- `requirements.txt` — Python dependencies (flask, selenium)
- `tools/setup_vendor.sh` — downloads ungoogled-chromium portable to vendor/
- `.github/workflows/maps-scraper.yml` — GitHub Actions workflow (XFCE+VNC+Cloudflare)

## API

### POST /api/v1/scrape
```json
{"query": "beauty parlour Rajkot"}
```

### Response
```json
{
  "status": "success",
  "query": "beauty parlour Rajkot",
  "total": 20,
  "results": [
    {
      "name": "Dhruvan beauty parlour",
      "rating": "4.7",
      "reviewCount": "(16)",
      "category": "Beautician",
      "address": "Prajapti soc. 3, 40 ft. road...",
      "phone": "090997 90535",
      "plusCode": "7Q8G+H8 Rajkot, Gujarat",
      "website": "",
      "hours": {"Wednesday": "9 am to 7 pm", ...},
      "currentStatus": "Open · Closes 7 pm",
      "identifiesAs": "Identifies as women-owned",
      "cid": "14870697541577025007",
      "placeId": "ChIJbQRwttTLWTkR7-UYulnKKs4",
      "reviews": [{...}],
      "url": "https://www.google.com/maps/place/..."
    }
  ]
}
```

### GET /api/v1/health
Returns chromium/extension status.

## Workflow
- `workflow_dispatch` trigger
- Sets up XFCE + TurboVNC + noVNC
- Downloads ungoogled-chromium via `tools/setup_vendor.sh`
- Starts Flask server.py on port 8815
- Exposes both VNC and API via Cloudflare quick tunnels
- Auto-restarts services in keep-alive loop
