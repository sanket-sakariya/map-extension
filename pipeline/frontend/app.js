// ─── State ──────────────────────────────────────────────────────────────────
let currentPage = 'dashboard';
let currentStep = 1;
let queryMode = 'one_biz_all_loc';
let allCategories = [];
let allCities = [];
let resultsPage = 1;

// ─── Helpers ────────────────────────────────────────────────────────────────
let _queryFilterTimer = null;
function debounceQueryFilter() {
  clearTimeout(_queryFilterTimer);
  _queryFilterTimer = setTimeout(() => { queryListPage = 1; loadQueryList(); }, 400);
}

// ─── Auth ───────────────────────────────────────────────────────────────────
function getToken() { return localStorage.getItem('mex_token') || ''; }
function setToken(t) { localStorage.setItem('mex_token', t); }
function clearToken() { localStorage.removeItem('mex_token'); }

async function checkAuth() {
  const token = getToken();
  if (!token) { showLogin(); return; }
  const d = await api(`/api/auth/verify?token=${token}`);
  if (d.status === 'ok') { hideLogin(); } else { showLogin(); }
}

function showLogin() {
  document.getElementById('login-screen').style.display = 'flex';
  document.getElementById('login-logo').innerHTML = icons.map;
}

function hideLogin() {
  document.getElementById('login-screen').style.display = 'none';
}

async function doLogin() {
  const btn = document.getElementById('btn-login');
  const email = document.getElementById('login-email').value.trim();
  const password = document.getElementById('login-password').value;
  const errEl = document.getElementById('login-error');
  errEl.style.display = 'none';

  if (!email || !password) { errEl.textContent = 'Please enter email and password'; errEl.style.display = 'block'; return; }

  btn.innerHTML = '<span class="spinner"></span> Signing in...';
  btn.disabled = true;

  const d = await api('/api/auth/login', { method: 'POST', body: JSON.stringify({ email, password }) });

  btn.innerHTML = 'Sign In';
  btn.disabled = false;

  if (d.token) {
    setToken(d.token);
    hideLogin();
    initApp();
  } else {
    errEl.textContent = d.message || 'Invalid credentials';
    errEl.style.display = 'block';
  }
}

function logout() {
  clearToken();
  showLogin();
}

// ─── Init ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  checkAuth();
  initApp();
});

function initApp() {
  if (!getToken()) return;
  renderNav();
  renderIcons();
  loadConfig();
  loadCategories();
  loadCountries();
  refreshStatus();
  setInterval(refreshStatus, 8000);
}

// ─── API Helper ─────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  try {
    const r = await fetch(path, { headers: {'Content-Type':'application/json'}, ...opts });
    if (!r.ok) return { error: `HTTP ${r.status}` };
    return await r.json();
  } catch(e) {
    if (e.name === 'AbortError') throw e; // let callers distinguish cancellation from real errors
    return { error: e.message };
  }
}

// ─── Navigation ─────────────────────────────────────────────────────────────
const pages = [
  { id: 'dashboard', label: 'Dashboard', icon: 'dashboard' },
  { id: 'scrapers', label: 'Scrapers', icon: 'server' },
  { id: 'queries', label: 'Query Generator', icon: 'search' },
  { id: 'results', label: 'Results', icon: 'database' },
  { id: 'alldata', label: 'All Data', icon: 'filter' },
  { id: 'settings', label: 'Settings', icon: 'settings' },
];

function renderNav() {
  const nav = document.getElementById('nav');
  nav.innerHTML = pages.map(p =>
    `<div class="nav-item ${p.id === currentPage ? 'active' : ''}" onclick="goTo('${p.id}')">${icons[p.icon]}<span>${p.label}</span></div>`
  ).join('') + `<div class="nav-item" onclick="logout()" style="margin-top:auto;border-top:1px solid var(--border);padding-top:12px;color:var(--danger);">${icons.x}<span>Logout</span></div>`;

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
  if (pageId === 'alldata') loadAllData();
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
  document.getElementById('btn-search').innerHTML = `${icons.search} Search`;
  document.getElementById('btn-back').innerHTML = `${icons.chevronLeft} Back to Queries`;
  document.getElementById('btn-export').innerHTML = `${icons.download} Export All`;
  document.getElementById('btn-export-filtered').innerHTML = `${icons.download} Export Filtered`;

  // Settings
  document.getElementById('pat-title').innerHTML = `${icons.key} GitHub Authentication`;
  document.getElementById('tunnel-title').innerHTML = `${icons.globe} Public Tunnel`;
  document.getElementById('btn-save-pat').innerHTML = `${icons.check} Save Token`;
  document.getElementById('btn-generate').innerHTML = `${icons.zap} Generate & Queue`;

  // All Data
  document.getElementById('alldata-filter-title').innerHTML = `${icons.filter} Global Filters`;
  document.getElementById('btn-search-alldata').innerHTML = `${icons.search} Search`;
  document.getElementById('btn-reset-alldata').innerHTML = `${icons.refresh} Reset`;
  document.getElementById('btn-export-alldata').innerHTML = `${icons.download} Export Filtered`;
  document.getElementById('btn-export-domains').innerHTML = `${icons.globe} Export Domains`;

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
    { num: d.completed_jobs || 0, label: 'Completed' },
    { num: d.dead_letter_queue || 0, label: 'Failed (DLX)' },
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
let selectedQuery = '';
let queryListPage = 1;
let resultsSort = { by: 'rating', order: 'desc' };

function getPerPage() { return parseInt(document.getElementById('r-per-page')?.value || 25); }
function changePerPage() { resultsPage = 1; queryListPage = 1; loadResults(); }

async function loadResults() {
  if (!selectedQuery) { await loadQueryList(); return; }
  await loadQueryResults();
}

async function loadQueryList() {
  const filterText = document.getElementById('q-filter').value;
  const sortBy = document.getElementById('q-sort').value;
  const perPage = getPerPage();
  const offset = (queryListPage - 1) * perPage;

  const tbody = document.getElementById('results-tbody');
  const info = document.getElementById('results-info');
  document.getElementById('results-filters').style.display = 'none';
  document.getElementById('results-back').style.display = 'none';
  document.getElementById('query-filters').style.display = 'flex';

  info.innerHTML = `<span class="spinner"></span> Loading queries...`;
  tbody.innerHTML = Array.from({length:5}).map(()=>`<tr class="skeleton-row"><td colspan="4"><div class="skeleton-bar"></div></td></tr>`).join('');

  const params = new URLSearchParams({ search: filterText, limit: perPage, offset, sort: sortBy });
  const d = await api(`/api/results/queries?${params}`);

  if (!d.queries?.length) {
    info.textContent = d.total ? 'No matching queries' : 'No scraped data yet';
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:24px;">No data scraped yet.</td></tr>';
    document.getElementById('pagination').innerHTML = '';
    return;
  }

  const totalPages = Math.ceil((d.total || 0) / perPage);
  info.textContent = `${(d.total||0).toLocaleString()} queries — ${(d.total_businesses||0).toLocaleString()} businesses — page ${queryListPage}/${totalPages}`;
  document.getElementById('results-thead').innerHTML = '<tr><th>Query</th><th>Results</th><th>Last Scraped</th><th>Action</th></tr>';
  tbody.innerHTML = d.queries.map(q => `
    <tr style="cursor:pointer;" onclick="openQuery('${q.query.replace(/'/g, "\\'")}')">
      <td style="font-weight:500;">${q.query}</td>
      <td><span class="tag tag-blue">${q.count}</span></td>
      <td style="font-size:12px;color:var(--text-muted);">${new Date(q.last_scraped).toLocaleString()}</td>
      <td><button class="btn btn-outline" style="padding:4px 10px;font-size:11px;" onclick="event.stopPropagation();openQuery('${q.query.replace(/'/g, "\\'")}')">${icons.chevronRight}</button></td>
    </tr>`).join('');
  renderPag(totalPages, queryListPage, 'goQueryPage');
}
function goQueryPage(n) { queryListPage = n; loadQueryList(); }

function openQuery(query) {
  selectedQuery = query;
  resultsPage = 1;
  resultsSort = { by: 'rating', order: 'desc' };
  document.getElementById('query-filters').style.display = 'none';
  document.getElementById('results-filters').style.display = 'flex';
  document.getElementById('results-back').style.display = 'flex';
  loadQueryResults();
}
function backToQueries() { selectedQuery = ''; loadQueryList(); }

function sortCol(field) {
  if (resultsSort.by === field) resultsSort.order = resultsSort.order === 'desc' ? 'asc' : 'desc';
  else resultsSort = { by: field, order: 'desc' };
  resultsPage = 1;
  loadQueryResults();
}

function buildSortHeaders() {
  const cols = [
    { f: 'name', l: 'Name' }, { f: 'rating', l: 'Rating' }, { f: 'reviews', l: 'Reviews' },
    { f: 'category', l: 'Category' }, { f: 'city', l: 'City' }, { f: null, l: 'Phone' }, { f: null, l: 'CID' }
  ];
  return '<tr>' + cols.map(c => {
    if (!c.f) return `<th>${c.l}</th>`;
    const active = resultsSort.by === c.f;
    const arrow = active ? (resultsSort.order === 'asc' ? ' &#9650;' : ' &#9660;') : ' <span style="opacity:0.3;">&#9650;&#9660;</span>';
    const cls = active ? `sortable ${resultsSort.order}` : 'sortable';
    return `<th class="${cls}" onclick="sortCol('${c.f}')">${c.l}${arrow}</th>`;
  }).join('') + '</tr>';
}

async function loadQueryResults() {
  const search = document.getElementById('r-search').value;
  const city = document.getElementById('r-city').value;
  const category = document.getElementById('r-category').value;
  const min_rating = document.getElementById('r-rating').value || 0;
  const phone_only = document.getElementById('r-phone').checked;
  const no_phone = document.getElementById('r-no-phone').checked;
  const website_only = document.getElementById('r-website').checked;
  const perPage = getPerPage();
  const offset = (resultsPage - 1) * perPage;

  const params = new URLSearchParams({
    limit: perPage, offset, search, city, category, min_rating, phone_only, no_phone, website_only,
    sort_by: resultsSort.by, sort_order: resultsSort.order, query: selectedQuery
  });

  const searchBtn = document.getElementById('btn-search');
  const tbody = document.getElementById('results-tbody');
  const originalBtnHtml = searchBtn.innerHTML;
  searchBtn.disabled = true;
  searchBtn.innerHTML = `<span class="spinner"></span> Searching...`;
  tbody.innerHTML = Array.from({length: 5}).map(() =>
    `<tr class="skeleton-row"><td colspan="7"><div class="skeleton-bar"></div></td></tr>`
  ).join('');

  const d = await api(`/api/results?${params}`);

  searchBtn.disabled = false;
  searchBtn.innerHTML = originalBtnHtml;

  document.getElementById('results-info').innerHTML = `<strong style="color:var(--primary);">${selectedQuery}</strong> — ${(d.total || 0).toLocaleString()} results`;
  document.getElementById('results-thead').innerHTML = buildSortHeaders();

  if (!d.results?.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:24px;">No results</td></tr>';
  } else {
    tbody.innerHTML = d.results.map((r, i) => `
      <tr style="cursor:pointer;" onclick="toggleDetail(${i})">
        <td style="font-weight:500;">${r.name||'-'}</td><td>${r.rating||'-'}</td><td>${r.review_count||0}</td>
        <td><span class="tag tag-blue">${r.category||'-'}</span></td><td>${r.city||'-'}</td><td>${r.phone||'-'}</td>
        <td style="font-size:11px;color:var(--text-muted);">${r.cid||'-'}</td>
      </tr>
      <tr id="detail-${i}" style="display:none;">
        <td colspan="7" style="padding:12px;background:var(--primary-50);border-radius:8px;">
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px;">
            <div><strong>Address:</strong> ${r.address||'-'}</div>
            <div><strong>Place ID:</strong> ${r.place_id||'-'}</div>
            <div><strong>Plus Code:</strong> ${r.plus_code||'-'}</div>
            <div><strong>Website:</strong> ${r.website?`<a href="${r.website}" target="_blank" style="color:var(--primary);">${r.website}</a>`:'-'}</div>
            <div><strong>Status:</strong> ${r.current_status||'-'}</div>
            <div><strong>Identifies As:</strong> ${r.identifies_as||'-'}</div>
            <div><strong>Maps:</strong> ${r.maps_url?`<a href="${r.maps_url}" target="_blank" style="color:var(--primary);">Open</a>`:'-'}</div>
            <div><strong>CID:</strong> ${r.cid||'-'}</div>
            <div style="grid-column:1/-1;"><strong>Hours:</strong> ${r.hours?Object.entries(r.hours).map(([d,t])=>`${d}: ${t}`).join(' | '):'-'}</div>
            ${r.reviews?.length?`<div style="grid-column:1/-1;"><strong>Reviews (${r.reviews.length}):</strong><div style="max-height:80px;overflow-y:auto;margin-top:4px;padding:6px;background:var(--card);border:1px solid var(--border);border-radius:6px;font-size:11px;">${r.reviews.slice(0,3).map(rv=>`<div style="margin-bottom:4px;"><b>${rv.reviewer}</b> ${rv.stars} <i>${rv.time}</i><br>${(rv.text||'').slice(0,120)}</div>`).join('')}</div></div>`:''}
          </div>
        </td>
      </tr>`).join('');
  }
  const totalPages = Math.ceil((d.total||0)/perPage);
  renderPag(totalPages, resultsPage, 'goPage');
}
function toggleDetail(i) { const r=document.getElementById(`detail-${i}`); r.style.display=r.style.display==='none'?'table-row':'none'; }
function goPage(n) { resultsPage = n; loadQueryResults(); }

// ─── Pagination (shared) ────────────────────────────────────────────────────
function renderPag(totalPages, current, fnName) {
  const c = document.getElementById('pagination');
  if (totalPages <= 1) { c.innerHTML = ''; return; }
  let h = '';
  // First
  h += `<div class="page-btn" onclick="${fnName}(1)" ${current<=1?'style="opacity:0.3;pointer-events:none;"':''} title="First">&#171;</div>`;
  // Prev
  h += `<div class="page-btn" onclick="${fnName}(${current-1})" ${current<=1?'style="opacity:0.3;pointer-events:none;"':''}>${icons.chevronLeft}</div>`;
  // Numbers
  const s = Math.max(1, current-2), e = Math.min(totalPages, current+2);
  if (s > 1) h += `<div class="page-btn" onclick="${fnName}(1)">1</div><span style="padding:0 3px;color:var(--text-muted);">..</span>`;
  for (let i=s;i<=e;i++) h += `<div class="page-btn ${i===current?'active':''}" onclick="${fnName}(${i})">${i}</div>`;
  if (e < totalPages) h += `<span style="padding:0 3px;color:var(--text-muted);">..</span><div class="page-btn" onclick="${fnName}(${totalPages})">${totalPages}</div>`;
  // Next
  h += `<div class="page-btn" onclick="${fnName}(${current+1})" ${current>=totalPages?'style="opacity:0.3;pointer-events:none;"':''}>${icons.chevronRight}</div>`;
  // Last
  h += `<div class="page-btn" onclick="${fnName}(${totalPages})" ${current>=totalPages?'style="opacity:0.3;pointer-events:none;"':''} title="Last">&#187;</div>`;
  c.innerHTML = h;
}

// ─── Export CSV ─────────────────────────────────────────────────────────────
async function exportCSV() {
  if (!selectedQuery) return;
  const btn = document.getElementById('btn-export');
  btn.innerHTML = `<span class="spinner"></span> Exporting...`;
  btn.disabled = true;

  const params = new URLSearchParams({ query: selectedQuery });
  const a = document.createElement('a');
  a.href = `/api/results/export?${params.toString()}`;
  a.download = `${selectedQuery}_all.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  btn.innerHTML = `${icons.download} Export All`;
  btn.disabled = false;
}

async function exportFilteredCSV() {
  if (!selectedQuery) return;
  const btn = document.getElementById('btn-export-filtered');
  btn.innerHTML = `<span class="spinner"></span> Exporting...`;
  btn.disabled = true;

  // Use exact same filters as the current view
  const search = document.getElementById('r-search').value;
  const city = document.getElementById('r-city').value;
  const category = document.getElementById('r-category').value;
  const min_rating = document.getElementById('r-rating').value || 0;
  const phone_only = document.getElementById('r-phone').checked;
  const no_phone = document.getElementById('r-no-phone').checked;
  const website_only = document.getElementById('r-website').checked;

  const params = new URLSearchParams({
    search, city, category, min_rating, phone_only, no_phone, website_only,
    sort_by: resultsSort.by, sort_order: resultsSort.order, query: selectedQuery
  });

  // Build filename from active filters
  let filterName = selectedQuery;
  if (phone_only) filterName += '_phone-only';
  if (no_phone) filterName += '_no-phone';
  if (website_only) filterName += '_website-only';
  if (city) filterName += `_${city}`;
  if (category) filterName += `_${category}`;
  if (min_rating > 0) filterName += `_min${min_rating}`;

  const a = document.createElement('a');
  a.href = `/api/results/export?${params.toString()}`;
  a.download = `${filterName}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  btn.innerHTML = `${icons.download} Export Filtered`;
  btn.disabled = false;
}

function downloadCSV(results, filename) {
  if (!results?.length) {
    toast('No data to export', 'error');
    return;
  }

  const headers = ['Name','Rating','Reviews','Category','Phone','Address','City','State','Website','CID','Place ID','Plus Code','Status','Hours','Maps URL'];
  const rows = results.map(r => [
    r.name || '',
    r.rating || '',
    r.review_count || '',
    r.category || '',
    r.phone || '',
    (r.address || '').replace(/,/g, ' '),
    r.city || '',
    r.state || '',
    r.website || '',
    r.cid || '',
    r.place_id || '',
    r.plus_code || '',
    r.current_status || '',
    r.hours ? Object.entries(r.hours).map(([d,t]) => `${d}: ${t}`).join(' | ') : '',
    r.maps_url || ''
  ]);

  const csvContent = [headers, ...rows].map(row =>
    row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(',')
  ).join('\n');

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${filename.replace(/[^a-zA-Z0-9_-]/g, '_')}.csv`;
  a.click();
  URL.revokeObjectURL(url);
  toast(`Exported ${results.length} records`, 'success');
}

// ─── All Data Page ──────────────────────────────────────────────────────────
let allDataPage = 1;
let allDataAbortController = null;

async function loadAllData() {
  const search = document.getElementById('ad-search').value;
  const city = document.getElementById('ad-city').value;
  const category = document.getElementById('ad-category').value;
  const query = document.getElementById('ad-query').value;
  const min_rating = document.getElementById('ad-rating').value || 0;
  const min_reviews = document.getElementById('ad-reviews').value || 0;
  const phone_filter = document.getElementById('ad-phone-filter').value;
  const website_filter = document.getElementById('ad-website-filter').value;
  const address_filter = document.getElementById('ad-address-filter').value;
  const sort_by = document.getElementById('ad-sort').value;
  const sort_order = document.getElementById('ad-order').value;
  const perPage = parseInt(document.getElementById('ad-per-page').value);
  const offset = (allDataPage - 1) * perPage;

  const params = new URLSearchParams({
    limit: perPage, offset, search, query, city, category, min_rating, min_reviews,
    phone_filter, website_filter, address_filter, sort_by, sort_order
  });

  // Cancel any in-flight search so filters don't race each other
  if (allDataAbortController) allDataAbortController.abort();
  allDataAbortController = new AbortController();

  const searchBtn = document.getElementById('btn-search-alldata');
  const tbody = document.getElementById('alldata-tbody');
  const info = document.getElementById('alldata-info');

  searchBtn.disabled = true;
  const originalBtnHtml = searchBtn.innerHTML;
  searchBtn.innerHTML = `<span class="spinner"></span> Searching...`;
  info.innerHTML = `<span class="spinner"></span> Filtering across all records — this can take a few seconds on large datasets...`;
  tbody.innerHTML = Array.from({length: 6}).map(() =>
    `<tr class="skeleton-row"><td colspan="8"><div class="skeleton-bar"></div></td></tr>`
  ).join('');
  document.getElementById('alldata-pagination').innerHTML = '';

  let d;
  try {
    d = await api(`/api/results?${params}`, { signal: allDataAbortController.signal });
  } catch (e) {
    if (e.name === 'AbortError') return; // superseded by a newer search
    info.textContent = 'Search failed. Please try again.';
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--danger);padding:24px;">Error loading results</td></tr>';
    searchBtn.disabled = false;
    searchBtn.innerHTML = originalBtnHtml;
    return;
  }

  searchBtn.disabled = false;
  searchBtn.innerHTML = originalBtnHtml;

  if (d.error) {
    info.textContent = 'Search failed. Please try again.';
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--danger);padding:24px;">Error loading results</td></tr>';
    return;
  }

  info.textContent = `${(d.total || 0).toLocaleString()} records found`;

  if (!d.results?.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:24px;">No records found</td></tr>';
  } else {
    tbody.innerHTML = d.results.map(r =>
      `<tr>
        <td style="font-weight:500;">${r.name || '-'}</td>
        <td>${r.rating || '-'}</td>
        <td>${r.review_count || 0}</td>
        <td><span class="tag tag-blue">${r.category || '-'}</span></td>
        <td>${r.phone || '-'}</td>
        <td>${r.website ? `<a href="${r.website}" target="_blank" style="color:var(--primary);">${r.website.replace('https://', '').replace('http://', '').split('/')[0]}</a>` : '-'}</td>
        <td>${r.city || '-'}</td>
        <td style="font-size:11px;color:var(--text-muted);">${r.query || '-'}</td>
      </tr>`
    ).join('');
  }

  const totalPages = Math.ceil((d.total || 0) / perPage);
  renderAllDataPag(totalPages);
}

function renderAllDataPag(totalPages) {
  const c = document.getElementById('alldata-pagination');
  if (totalPages <= 1) { c.innerHTML = ''; return; }
  let h = '';
  h += `<div class="page-btn" onclick="goAllDataPage(1)" ${allDataPage<=1?'style="opacity:0.3;pointer-events:none;"':''}>&#171;</div>`;
  h += `<div class="page-btn" onclick="goAllDataPage(${allDataPage-1})" ${allDataPage<=1?'style="opacity:0.3;pointer-events:none;"':''}>${icons.chevronLeft}</div>`;
  const s = Math.max(1, allDataPage-2), e = Math.min(totalPages, allDataPage+2);
  if (s > 1) h += `<div class="page-btn" onclick="goAllDataPage(1)">1</div><span style="padding:0 3px;color:var(--text-muted);">..</span>`;
  for (let i=s;i<=e;i++) h += `<div class="page-btn ${i===allDataPage?'active':''}" onclick="goAllDataPage(${i})">${i}</div>`;
  if (e < totalPages) h += `<span style="padding:0 3px;color:var(--text-muted);">..</span><div class="page-btn" onclick="goAllDataPage(${totalPages})">${totalPages}</div>`;
  h += `<div class="page-btn" onclick="goAllDataPage(${allDataPage+1})" ${allDataPage>=totalPages?'style="opacity:0.3;pointer-events:none;"':''}>${icons.chevronRight}</div>`;
  h += `<div class="page-btn" onclick="goAllDataPage(${totalPages})" ${allDataPage>=totalPages?'style="opacity:0.3;pointer-events:none;"':''}>&#187;</div>`;
  c.innerHTML = h;
}

function goAllDataPage(n) { allDataPage = n; loadAllData(); }

async function exportAllDataCSV() {
  const btn = document.getElementById('btn-export-alldata');
  btn.innerHTML = `<span class="spinner"></span> Exporting...`;
  btn.disabled = true;

  const search = document.getElementById('ad-search').value;
  const city = document.getElementById('ad-city').value;
  const category = document.getElementById('ad-category').value;
  const query = document.getElementById('ad-query').value;
  const min_rating = document.getElementById('ad-rating').value || 0;
  const min_reviews = document.getElementById('ad-reviews').value || 0;
  const phone_filter = document.getElementById('ad-phone-filter').value;
  const website_filter = document.getElementById('ad-website-filter').value;
  const address_filter = document.getElementById('ad-address-filter').value;
  const sort_by = document.getElementById('ad-sort').value;
  const sort_order = document.getElementById('ad-order').value;

  const params = new URLSearchParams({
    search, query, city, category, min_rating, min_reviews,
    phone_filter, website_filter, address_filter, sort_by, sort_order
  });

  let filename = 'all_data';
  if (phone_filter !== 'all') filename += `_phone-${phone_filter}`;
  if (website_filter !== 'all') filename += `_website-${website_filter}`;
  if (address_filter !== 'all') filename += `_address-${address_filter}`;
  if (city) filename += `_${city}`;
  if (category) filename += `_${category}`;
  if (min_rating > 0) filename += `_min${min_rating}`;

  const a = document.createElement('a');
  a.href = `/api/results/export?${params.toString()}`;
  a.download = `${filename}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  btn.innerHTML = `${icons.download} Export Filtered`;
  btn.disabled = false;
}

async function exportDomainsCSV() {
  const btn = document.getElementById('btn-export-domains');
  btn.innerHTML = `<span class="spinner"></span> Exporting...`;
  btn.disabled = true;

  const search = document.getElementById('ad-search').value;
  const city = document.getElementById('ad-city').value;
  const category = document.getElementById('ad-category').value;
  const query = document.getElementById('ad-query').value;
  const min_rating = document.getElementById('ad-rating').value || 0;
  const min_reviews = document.getElementById('ad-reviews').value || 0;
  const phone_filter = document.getElementById('ad-phone-filter').value;
  const website_filter = document.getElementById('ad-website-filter').value;
  const address_filter = document.getElementById('ad-address-filter').value;

  if (website_filter === 'none') {
    toast('Website filter is set to "No Website" — there are no domains to export for this filter', 'error');
    btn.innerHTML = `${icons.globe} Export Domains`;
    btn.disabled = false;
    return;
  }

  const params = new URLSearchParams({
    search, query, city, category, min_rating, min_reviews,
    phone_filter, website_filter, address_filter
  });

  let filename = 'domains';
  if (city) filename += `_${city}`;
  if (category) filename += `_${category}`;
  if (min_rating > 0) filename += `_min${min_rating}`;

  const a = document.createElement('a');
  a.href = `/api/results/export-domains?${params.toString()}`;
  a.download = `${filename}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  btn.innerHTML = `${icons.globe} Export Domains`;
  btn.disabled = false;
  toast('Downloading unique domains list...', 'success');
}

function resetAllDataFilters() {
  document.getElementById('ad-search').value = '';
  document.getElementById('ad-city').value = '';
  document.getElementById('ad-category').value = '';
  document.getElementById('ad-query').value = '';
  document.getElementById('ad-rating').value = '';
  document.getElementById('ad-reviews').value = '';
  document.getElementById('ad-phone-filter').value = 'all';
  document.getElementById('ad-website-filter').value = 'all';
  document.getElementById('ad-address-filter').value = 'all';
  document.getElementById('ad-sort').value = 'rating';
  document.getElementById('ad-order').value = 'desc';
  allDataPage = 1;
  loadAllData();
}

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
  const icon = type === 'success' ? icons.check : icons.x;
  el.innerHTML = `${icon}<span>${message}</span>`;
  container.appendChild(el);
  setTimeout(() => {
    el.classList.add('hiding');
    setTimeout(() => el.remove(), 300);
  }, 3000);
}
