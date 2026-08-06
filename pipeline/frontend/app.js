// ─── State ──────────────────────────────────────────────────────────────────
let currentPage = 'dashboard';
let currentStep = 1;
let queryMode = 'one_biz_all_loc';
let allCategories = [];
let allCities = [];
let resultsPage = 1;
const RESULTS_PER_PAGE = 25;

// ─── Init ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  renderNav();
  renderIcons();
  loadConfig();
  loadCategories();
  loadCountries();
  refreshStatus();
  setInterval(refreshStatus, 8000);
});

// ─── API Helper ─────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  try {
    const r = await fetch(path, { headers: {'Content-Type':'application/json'}, ...opts });
    if (!r.ok) return { error: `HTTP ${r.status}` };
    return await r.json();
  } catch(e) { return { error: e.message }; }
}

// ─── Navigation ─────────────────────────────────────────────────────────────
const pages = [
  { id: 'dashboard', label: 'Dashboard', icon: 'dashboard' },
  { id: 'scrapers', label: 'Scrapers', icon: 'server' },
  { id: 'queries', label: 'Query Generator', icon: 'search' },
  { id: 'results', label: 'Results', icon: 'database' },
  { id: 'settings', label: 'Settings', icon: 'settings' },
];

function renderNav() {
  const nav = document.getElementById('nav');
  nav.innerHTML = pages.map(p =>
    `<div class="nav-item ${p.id === currentPage ? 'active' : ''}" onclick="goTo('${p.id}')">${icons[p.icon]}<span>${p.label}</span></div>`
  ).join('');

  // Logo
  document.getElementById('logo-text').innerHTML = `${icons.map} Maps Pipeline`;
  document.getElementById('mobile-logo').innerHTML = `${icons.map} Maps Pipeline`;
  document.getElementById('menu-btn').innerHTML = icons.menu;
}

function goTo(pageId) {
  currentPage = pageId;
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById(`page-${pageId}`).classList.add('active');
  renderNav();
  closeSidebar();
  if (pageId === 'scrapers') refreshWorkflows();
  if (pageId === 'results') loadResults();
}

function toggleSidebar() { document.getElementById('sidebar').classList.toggle('open'); }
function closeSidebar() { document.getElementById('sidebar').classList.remove('open'); }

// ─── Render Icons into buttons ──────────────────────────────────────────────
function renderIcons() {
  // Dashboard
  document.getElementById('scrapers-title').innerHTML = `${icons.server} Active Scrapers`;

  // Scrapers page
  document.getElementById('wf-title').innerHTML = `${icons.zap} Workflow Management`;
  document.getElementById('btn-start').innerHTML = `${icons.play} Start`;
  document.getElementById('btn-stop').innerHTML = `${icons.stop} Stop All`;
  document.getElementById('btn-refresh-wf').innerHTML = `${icons.refresh} Refresh`;

  // Query generator
  document.getElementById('mode-title').innerHTML = `${icons.zap} Generation Mode`;
  renderModeButtons();
  renderStepper();

  // Results
  document.getElementById('filter-title').innerHTML = `${icons.filter} Filters`;
  document.getElementById('btn-search').innerHTML = `${icons.search} Search`;

  // Settings
  document.getElementById('pat-title').innerHTML = `${icons.key} GitHub Authentication`;
  document.getElementById('tunnel-title').innerHTML = `${icons.globe} Public Tunnel`;
  document.getElementById('btn-save-pat').innerHTML = `${icons.check} Save Token`;
  document.getElementById('btn-generate').innerHTML = `${icons.zap} Generate & Queue`;

  // Biz/Loc titles
  document.getElementById('biz-title').innerHTML = `${icons.search} Business Categories`;
  document.getElementById('loc-title').innerHTML = `${icons.globe} Locations`;
}

// ─── Stepper ────────────────────────────────────────────────────────────────
function renderStepper() {
  const steps = ['Select Mode', 'Choose Data', 'Generate'];
  const stepper = document.getElementById('query-stepper');
  stepper.innerHTML = steps.map((s, i) => {
    const num = i + 1;
    const cls = num < currentStep ? 'done' : num === currentStep ? 'active' : '';
    return `<div class="step ${cls}"><div class="step-num">${num < currentStep ? icons.check : num}</div><span>${s}</span></div>${i < steps.length-1 ? '<div class="step-line"></div>' : ''}`;
  }).join('');

  // Show/hide steps
  document.querySelectorAll('.query-step').forEach((el, i) => {
    el.style.display = (i + 1 === currentStep) ? 'block' : 'none';
  });

  // Step navigation
  const nav = document.getElementById('step-nav');
  nav.innerHTML = '';
  if (currentStep > 1) nav.innerHTML += `<button class="btn btn-outline" onclick="setStep(${currentStep-1})">${icons.chevronLeft} Back</button>`;
  if (currentStep < 3) nav.innerHTML += `<button class="btn btn-primary" onclick="setStep(${currentStep+1})">Next ${icons.chevronRight}</button>`;
}

function setStep(n) {
  currentStep = n;
  renderStepper();
  if (n === 3) renderPreview();
}

// ─── Mode Selection ─────────────────────────────────────────────────────────
function renderModeButtons() {
  const modes = [
    { id: 'one_biz_all_loc', label: '1 Business × All Locations', desc: 'Pick one category, apply to all selected cities' },
    { id: 'one_loc_all_biz', label: '1 Location × All Businesses', desc: 'Pick one city, apply all selected categories' },
    { id: 'all_biz_all_loc', label: 'All × All', desc: 'Full cross-product of selections' },
  ];
  document.getElementById('mode-buttons').innerHTML = modes.map(m =>
    `<button class="btn ${m.id === queryMode ? 'btn-primary' : 'btn-outline'}" onclick="setMode('${m.id}')" style="flex:1;min-width:160px;flex-direction:column;padding:12px;"><span style="font-weight:600;">${m.label}</span><span style="font-size:11px;opacity:0.7;margin-top:2px;">${m.desc}</span></button>`
  ).join('');
}

function setMode(m) { queryMode = m; renderModeButtons(); }

// ─── Dashboard ──────────────────────────────────────────────────────────────
async function refreshStatus() {
  const d = await api('/api/pipeline/status');
  if (d.error) return;
  const stats = [
    { num: d.active_scrapers || 0, label: 'Scrapers' },
    { num: d.query_queue || 0, label: 'Query Queue' },
    { num: d.active_jobs || 0, label: 'Active Jobs' },
    { num: d.result_queue || 0, label: 'Result Queue' },
    { num: d.total_inserted || 0, label: 'DB Records' },
  ];
  document.getElementById('stats-grid').innerHTML = stats.map(s =>
    `<div class="stat-card"><div class="stat-num">${s.num}</div><div class="stat-label">${s.label}</div></div>`
  ).join('');

  // Scrapers list on dashboard
  const sc = await api('/api/scrapers');
  const list = document.getElementById('scrapers-list');
  if (!sc.scrapers?.length) {
    list.innerHTML = '<p style="color:var(--text-muted);font-size:13px;">No active scrapers. Start some from the Scrapers page.</p>';
  } else {
    list.innerHTML = sc.scrapers.map(s =>
      `<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--border);"><span class="tag tag-green">Running</span><span style="font-size:13px;color:var(--text);">${s.tunnel_url}</span><span class="tag tag-blue">#${s.run_id}</span></div>`
    ).join('');
  }
}

// ─── Scrapers Page ──────────────────────────────────────────────────────────
async function startWorkflows() {
  const btn = document.getElementById('btn-start');
  btn.innerHTML = `<span class="spinner"></span> Starting...`;
  btn.disabled = true;
  const count = parseInt(document.getElementById('wf-count').value);
  const d = await api('/api/workflows/start', { method:'POST', body: JSON.stringify({count}) });
  btn.innerHTML = `${icons.play} Start`;
  btn.disabled = false;
  if (d.error) { toast(d.error, 'error'); return; }
  toast(`${count} workflow(s) triggered`, 'success');
  setTimeout(refreshWorkflows, 5000);
}

async function stopWorkflows() {
  const btn = document.getElementById('btn-stop');
  btn.innerHTML = `<span class="spinner"></span> Stopping...`;
  btn.disabled = true;
  const d = await api('/api/workflows/stop', { method:'POST' });
  btn.innerHTML = `${icons.stop} Stop All`;
  btn.disabled = false;
  toast(`Cancelled ${d.cancelled || 0} workflow(s)`, 'success');
  setTimeout(refreshWorkflows, 3000);
}

async function refreshWorkflows() {
  const d = await api('/api/scrapers');
  const list = document.getElementById('wf-scrapers-list');
  if (!d.scrapers?.length) {
    list.innerHTML = '<p style="color:var(--text-muted);font-size:13px;">No active scrapers registered.</p>';
    return;
  }
  list.innerHTML = `<div class="table-wrap"><table><thead><tr><th>Run ID</th><th>Tunnel URL</th><th>Registered</th><th>Status</th></tr></thead><tbody>${
    d.scrapers.map(s => `<tr><td><span class="tag tag-blue">${s.run_id}</span></td><td style="font-size:12px;">${s.tunnel_url}</td><td style="font-size:12px;">${new Date(s.registered_at).toLocaleString()}</td><td><span class="tag tag-green">Active</span></td></tr>`).join('')
  }</tbody></table></div>`;
}

// ─── Query Generator ────────────────────────────────────────────────────────
async function loadCategories() {
  const d = await api('/api/categories');
  if (d.categories) {
    allCategories = d.categories;
    renderSelectOptions('businesses', allCategories);
    document.getElementById('biz-count').textContent = `${allCategories.length} categories available`;
  }
}

async function loadCountries() {
  const d = await api('/api/countries');
  const sel = document.getElementById('country');
  if (d.countries?.length) {
    sel.innerHTML = '<option value="">Select country...</option>' + d.countries.map(c => `<option value="${c}">${c}</option>`).join('');
  } else {
    sel.innerHTML = '<option value="">Failed to load</option>';
    setTimeout(loadCountries, 3000);
  }
}

async function loadCities() {
  const country = document.getElementById('country').value;
  if (!country) return;
  const sel = document.getElementById('locations');
  sel.innerHTML = '<option>Loading...</option>';
  const d = await api(`/api/cities?country=${encodeURIComponent(country)}`);
  allCities = (d.cities || []).map(c => `${c.district}, ${c.state}`);
  renderSelectOptions('locations', allCities);
  document.getElementById('loc-count').textContent = `${allCities.length} cities loaded`;
}

function renderSelectOptions(id, items) {
  document.getElementById(id).innerHTML = items.map(c => `<option value="${c}">${c}</option>`).join('');
}

function filterList(id, query) {
  const source = id === 'businesses' ? allCategories : allCities;
  const filtered = query ? source.filter(s => s.toLowerCase().includes(query.toLowerCase())) : source;
  renderSelectOptions(id, filtered);
}

function selectAllLocs() {
  Array.from(document.getElementById('locations').options).forEach(o => o.selected = true);
  toast('All locations selected', 'success');
}

function renderPreview() {
  const biz = Array.from(document.getElementById('businesses').selectedOptions).map(o => o.value);
  const loc = Array.from(document.getElementById('locations').selectedOptions).map(o => o.value);
  const mod = document.getElementById('modifier').value;
  const total = biz.length * loc.length;
  const sample = [];
  for (let i = 0; i < Math.min(5, biz.length); i++) {
    for (let j = 0; j < Math.min(2, loc.length); j++) {
      sample.push(`${biz[i]} ${mod} ${loc[j]}`);
    }
  }
  document.getElementById('gen-preview').innerHTML = `
    <div style="font-size:14px;margin-bottom:8px;"><strong>${total}</strong> queries will be generated</div>
    <div style="font-size:12px;color:var(--text-muted);margin-bottom:4px;">Preview:</div>
    <div style="background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:12px;max-height:120px;overflow-y:auto;">
      ${sample.map(s => `<div style="padding:2px 0;">${s}</div>`).join('')}
      ${total > sample.length ? `<div style="color:var(--text-muted);margin-top:4px;">... and ${total - sample.length} more</div>` : ''}
    </div>
  `;
}

async function generateQueries() {
  const btn = document.getElementById('btn-generate');
  btn.innerHTML = `<span class="spinner"></span> Generating...`;
  btn.disabled = true;

  const businesses = Array.from(document.getElementById('businesses').selectedOptions).map(o => o.value);
  const locations = Array.from(document.getElementById('locations').selectedOptions).map(o => o.value);
  const modifier = document.getElementById('modifier').value;

  if (!businesses.length || !locations.length) {
    toast('Select at least one business and one location', 'error');
    btn.innerHTML = `${icons.zap} Generate & Queue`;
    btn.disabled = false;
    return;
  }

  const d = await api('/api/queries/generate', {
    method: 'POST',
    body: JSON.stringify({ mode: queryMode, businesses, locations, modifier })
  });

  btn.innerHTML = `${icons.zap} Generate & Queue`;
  btn.disabled = false;

  if (d.generated) {
    toast(`${d.generated} queries added to queue`, 'success');
    refreshStatus();
  } else {
    toast('Failed to generate queries', 'error');
  }
}

// ─── Results Page ───────────────────────────────────────────────────────────
async function loadResults() {
  const search = document.getElementById('r-search').value;
  const city = document.getElementById('r-city').value;
  const category = document.getElementById('r-category').value;
  const min_rating = document.getElementById('r-rating').value || 0;
  const phone_only = document.getElementById('r-phone').checked;
  const offset = (resultsPage - 1) * RESULTS_PER_PAGE;

  const params = new URLSearchParams({
    limit: RESULTS_PER_PAGE, offset, search, city, category, min_rating, phone_only
  });

  const d = await api(`/api/results?${params}`);
  document.getElementById('results-info').textContent = `${d.total || 0} results found`;

  const tbody = document.getElementById('results-tbody');
  if (!d.results?.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:24px;">No results found</td></tr>';
  } else {
    tbody.innerHTML = d.results.map(r =>
      `<tr><td style="font-weight:500;">${r.name || '-'}</td><td>${r.rating || '-'}</td><td>${r.review_count || 0}</td><td><span class="tag tag-blue">${r.category || '-'}</span></td><td>${r.phone || '-'}</td><td>${r.city || '-'}</td><td style="font-size:11px;color:var(--text-muted);">${r.cid || '-'}</td></tr>`
    ).join('');
  }

  // Pagination
  const totalPages = Math.ceil((d.total || 0) / RESULTS_PER_PAGE);
  renderPagination(totalPages);
}

function renderPagination(totalPages) {
  const container = document.getElementById('pagination');
  if (totalPages <= 1) { container.innerHTML = ''; return; }

  let html = '';
  html += `<div class="page-btn" onclick="goPage(${resultsPage - 1})" ${resultsPage <= 1 ? 'style="opacity:0.3;pointer-events:none;"' : ''}>${icons.chevronLeft}</div>`;

  const start = Math.max(1, resultsPage - 2);
  const end = Math.min(totalPages, resultsPage + 2);
  for (let i = start; i <= end; i++) {
    html += `<div class="page-btn ${i === resultsPage ? 'active' : ''}" onclick="goPage(${i})">${i}</div>`;
  }

  html += `<div class="page-btn" onclick="goPage(${resultsPage + 1})" ${resultsPage >= totalPages ? 'style="opacity:0.3;pointer-events:none;"' : ''}>${icons.chevronRight}</div>`;
  container.innerHTML = html;
}

function goPage(n) { resultsPage = n; loadResults(); }

// ─── Settings ───────────────────────────────────────────────────────────────
async function loadConfig() {
  const d = await api('/api/config');
  if (d.pat_set) {
    document.getElementById('pat').placeholder = d.pat_preview || 'Token saved';
    document.getElementById('pat-status').innerHTML = `<span class="tag tag-green">Active</span>`;
  }
  if (d.tunnel_url) {
    document.getElementById('tunnel-info').innerHTML = `<a href="${d.tunnel_url}" target="_blank" style="color:var(--primary);">${d.tunnel_url}</a>`;
  } else {
    document.getElementById('tunnel-info').textContent = 'Not configured. Run wait-for-tunnel.sh after starting docker.';
  }
}

async function savePat() {
  const btn = document.getElementById('btn-save-pat');
  const pat = document.getElementById('pat').value.trim();
  if (!pat) { toast('Enter a PAT token', 'error'); return; }

  btn.innerHTML = `<span class="spinner"></span> Validating...`;
  btn.disabled = true;

  const d = await api('/api/config', { method:'POST', body: JSON.stringify({pat}) });

  btn.innerHTML = `${icons.check} Save Token`;
  btn.disabled = false;

  if (d.status === 'ok') {
    toast('Token validated and saved', 'success');
    document.getElementById('pat').value = '';
    loadConfig();
  } else {
    toast(d.message || 'Invalid token', 'error');
  }
}

// ─── Toast ──────────────────────────────────────────────────────────────────
function toast(message, type = 'success') {
  const container = document.getElementById('toast-container');
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `${type === 'success' ? icons.check : icons.x}<span>${message}</span>`;
  container.appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 3000);
}
