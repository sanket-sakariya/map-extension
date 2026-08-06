# Plan: Google Maps Listing Scraper Extension

## Objective
Chrome extension that clicks each Google Maps listing card (`a.hfpxzc` inside `div.Nv2PK`) in the search results sidebar, waits for the detail panel to load, extracts business info, then moves to the next listing.

## File Scope
- `manifest.json` — MV3 extension manifest
- `content.js` — content script: clicks listings, extracts data from detail panel
- `popup.html` — simple UI with Start button and results table
- `popup.js` — triggers scraping via message to content script, displays results

## Step-by-Step Implementation
1. Create `manifest.json` (MV3, permissions: activeTab, scripting; matches google.com/maps)
2. Create `content.js`:
   - Listen for "start" message from popup
   - Gather all `a.hfpxzc` links in the sidebar
   - For each: click → wait for detail panel → extract (name, rating, reviews, category, address, hours, phone, website) → click back
   - Send collected data back to popup
3. Create `popup.html` + `popup.js`:
   - Start button
   - Display extracted data as JSON / table

## Acceptance Criteria
- Load extension in chrome://extensions (developer mode)
- Go to Google Maps, search for a business category
- Click extension icon → "Start Scraping"
- Extension clicks through each visible listing and returns structured data
