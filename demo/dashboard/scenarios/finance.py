"""
pg_trickle demo — financial risk pipeline scenario: HTML, DAG, and data queries.

Exposes:
  HTML         — complete page HTML with {{ dag }} placeholder
  DAG_DIAGRAM  — ASCII art DAG to substitute into {{ dag }}
  get_data(conn, safe_query, serialize) → dict for /api/data
"""

# ── Full page HTML ─────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>pg_trickle — Real-time Financial Risk Pipeline</title>
  <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
  <style>
    :root {
      --bg:      #0d1117;
      --card:    #161b22;
      --border:  #30363d;
      --hdr:     #21262d;
      --muted:   #8b949e;
      --green:   #3fb950;
      --yellow:  #d29922;
      --red:     #da3633;
      --blue:    #58a6ff;
      --purple:  #bc8cff;
      --text:    #e6edf3;
    }
    html, body { background: var(--bg); color: var(--text); font-size: 14px; }
    a { color: var(--blue); }
    .g-card { background: var(--card); border: 1px solid var(--border);
               border-radius: 8px; overflow: hidden; }
    .g-card-hdr { background: var(--hdr); padding: 8px 14px;
                  font-size: 13px; font-weight: 600; letter-spacing: .4px; }
    .g-table { width: 100%; border-collapse: collapse; }
    .g-table th { color: var(--muted); font-weight: 500; font-size: 12px;
                  border-bottom: 1px solid var(--border); padding: 6px 10px; }
    .g-table td { padding: 5px 10px; border-bottom: 1px solid var(--hdr); }
    .g-table tr:last-child td { border-bottom: none; }
    .kpi-val { font-size: 2.2rem; font-weight: 700; line-height: 1; }
    .kpi-lbl { font-size: 11px; color: var(--muted); margin-top: 4px; }
    .dag-pre { font-family: 'SFMono-Regular', Consolas, monospace;
               font-size: 11px; color: var(--muted); background: var(--bg);
               padding: 14px; border-radius: 6px; white-space: pre;
               overflow-x: auto; margin: 0; }
    .live-dot { width: 9px; height: 9px; border-radius: 50%;
                background: var(--green); display: inline-block;
                margin-right: 8px; animation: blink 1.8s ease-in-out infinite; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.25} }
    .nav-tabs { border-bottom-color: var(--border); }
    .nav-tabs .nav-link { color: var(--muted); border: 1px solid transparent;
                          border-radius: 6px 6px 0 0; padding: 6px 16px; }
    .nav-tabs .nav-link:hover { color: var(--text); border-color: var(--border);
                                background: var(--hdr); }
    .nav-tabs .nav-link.active { color: var(--text); background: var(--card);
                                 border-color: var(--border) var(--border) var(--card); }
    .g-card-hdr code { font-size: 11px; background: rgba(99,110,123,0.2);
                       border-radius: 3px; padding: 1px 5px; font-weight: 400; }
    .g-table td code { font-size: 11px; }
    .score-keep   { color: var(--muted); }
    .score-switch { color: var(--yellow); }
    .conf-high    { color: var(--green); }
    .conf-medium  { color: var(--yellow); }
    .conf-low     { color: var(--muted); }
    .pnl-pos { color: var(--green); }
    .pnl-neg { color: var(--red); }
    .pnl-flat{ color: var(--muted); }
    .status-ok      { color: var(--green); font-weight: 600; }
    .status-warn    { color: var(--yellow); font-weight: 600; }
    .status-breach  { color: var(--red); font-weight: 700; }
    .level-badge {
      display: inline-block; font-size: 10px; font-weight: 700;
      border-radius: 4px; padding: 1px 5px; margin-right: 4px;
      background: rgba(88,166,255,0.15); color: var(--blue);
    }
  </style>
</head>
<body>
<div class="container-fluid px-4 py-3">

  <!-- Header -->
  <div class="d-flex align-items-center mb-3 gap-3">
    <div>
      <span class="live-dot"></span>
      <span style="font-size:18px; font-weight:700;">pg_trickle</span>
      <span class="text-muted ms-1">Real-time Financial Risk Pipeline</span>
    </div>
    <span class="ms-auto text-muted" id="ts" style="font-size:12px;">connecting…</span>
  </div>

  <!-- Tab navigation -->
  <ul class="nav nav-tabs mb-3" id="mainTabs" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="tab-main-btn"
              data-bs-toggle="tab" data-bs-target="#tab-main"
              type="button" role="tab">📈 Risk Dashboard</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="tab-internals-btn"
              data-bs-toggle="tab" data-bs-target="#tab-internals"
              type="button" role="tab">⚙️ pg_trickle Internals</button>
    </li>
  </ul>

  <div class="tab-content">
  <div class="tab-pane fade show active" id="tab-main" role="tabpanel">

  <!-- KPI row -->
  <div class="row g-3 mb-3">
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-positions">—</div>
        <div class="kpi-lbl">Active Positions</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-exposure" style="color:var(--blue)">—</div>
        <div class="kpi-lbl">Total Book Exposure</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-pnl" style="color:var(--green)">—</div>
        <div class="kpi-lbl">Unrealized P&amp;L</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-var" style="color:var(--yellow)">—</div>
        <div class="kpi-lbl">Total Book VaR (95%)</div>
      </div>
    </div>
  </div>

  <!-- Row 2: breach dashboard + sector exposure -->
  <div class="row g-3 mb-3">
    <div class="col-7">
      <div class="g-card h-100">
        <div class="g-card-hdr">
          🚨 Capital Breach Dashboard
          <span class="level-badge">L10</span>
          <small style="color:var(--green);font-size:11px;margin-left:4px">
            DIFFERENTIAL showcase — LIMIT 10, change ratio ~0.02–0.1
          </small>
        </div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>#</th><th>Portfolio</th><th>Manager</th><th>Strategy</th>
                  <th>Book Value</th><th>VaR 95%</th><th>Req. Capital</th>
                  <th>Capital %</th><th>Status</th></tr>
            </thead>
            <tbody id="tb-breach"></tbody>
          </table>
        </div>
        <div class="p-2" style="font-size:11px;color:var(--muted);border-top:1px solid var(--border)">
          10-level DAG: trades → net_positions → position_values → account_pnl →
          portfolio_pnl → sector_exposure → var_contributions → account_var →
          portfolio_var → regulatory_capital → <strong>breach_dashboard</strong>.
          Fixed cardinality (10 rows): rank shifts are rare — KEEP DIFFERENTIAL.
        </div>
      </div>
    </div>
    <div class="col-5">
      <div class="g-card h-100">
        <div class="g-card-hdr">
          🏭 Sector Exposure
          <span class="level-badge">L5</span>
        </div>
        <div class="overflow-auto" style="max-height:310px;">
          <table class="g-table">
            <thead>
              <tr><th>Sector</th><th>Instruments</th><th>Exposure</th><th>P&amp;L</th><th>% Book</th></tr>
            </thead>
            <tbody id="tb-sector"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Row 3: portfolio P&L + portfolio VaR -->
  <div class="row g-3 mb-3">
    <div class="col-6">
      <div class="g-card h-100">
        <div class="g-card-hdr">
          💼 Portfolio P&amp;L
          <span class="level-badge">L4</span>
        </div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>Portfolio</th><th>Manager</th><th>Strategy</th>
                  <th>Accounts</th><th>Book Value</th><th>Unrealized P&amp;L</th><th>Return %</th></tr>
            </thead>
            <tbody id="tb-portfolio-pnl"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-6">
      <div class="g-card h-100">
        <div class="g-card-hdr">
          📊 Portfolio VaR
          <span class="level-badge">L8</span>
        </div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>Portfolio</th><th>Strategy</th><th>Book Value</th>
                  <th>VaR 95%</th><th>Stressed VaR 99%</th><th>VaR % NAV</th></tr>
            </thead>
            <tbody id="tb-portfolio-var"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Row 4: price snapshot (DIFFERENTIAL showcase) -->
  <div class="row g-3 mb-3">
    <div class="col-12">
      <div class="g-card">
        <div class="g-card-hdr">
          📡 Live Price Snapshot
          <span class="level-badge">L1</span>
          <small style="color:var(--green);font-size:11px;margin-left:8px">
            DIFFERENTIAL showcase — 1 of 30 instruments ticks per cycle, change ratio ~0.033
          </small>
        </div>
        <div class="overflow-auto" style="max-height:220px;">
          <table class="g-table">
            <thead>
              <tr><th>Ticker</th><th>Name</th><th>Sector</th>
                  <th>Bid</th><th>Ask</th><th>Mid</th>
                  <th>Base</th><th>Drift $</th><th>Drift %</th><th>Updated</th></tr>
            </thead>
            <tbody id="tb-prices"></tbody>
          </table>
        </div>
        <div class="p-2" style="font-size:11px;color:var(--muted);border-top:1px solid var(--border)">
          Generator ticks one instrument per cycle (~1 s). Only that instrument's
          positions cascade through all 10 DAG levels. Change ratio at L2 ≈ 0.02.
          Mode Advisor recommends <span style="color:var(--green)">KEEP DIFFERENTIAL</span>
          at every level.
        </div>
      </div>
    </div>
  </div>

  <!-- Row 5: stream table status + DAG -->
  <div class="row g-3">
    <div class="col-4">
      <div class="g-card">
        <div class="g-card-hdr">⚡ Stream Table Status</div>
        <div class="overflow-auto" style="max-height:260px;">
          <table class="g-table">
            <thead>
              <tr><th>Name</th><th>Level</th><th>Mode</th><th>Schedule</th><th>Status</th></tr>
            </thead>
            <tbody id="tb-sts"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-8">
      <div class="g-card">
        <div class="g-card-hdr" style="cursor:pointer"
             data-bs-toggle="collapse" data-bs-target="#dag-collapse">
          📊 DAG Topology (10 levels) — click to expand
        </div>
        <div class="collapse show" id="dag-collapse">
          <pre class="dag-pre" id="dag-pre">{{ dag }}</pre>
        </div>
      </div>
    </div>
  </div>

  </div><!-- /tab-pane tab-main -->

  <!-- ═══════════════════ TAB 2: pg_trickle Internals ═══════════════════ -->
  <div class="tab-pane fade" id="tab-internals" role="tabpanel">

    <div class="g-card mb-3">
      <div class="g-card-hdr">🏥 Stream Table Health — <code>pgtrickle.pgt_status()</code></div>
      <div class="overflow-auto">
        <table class="g-table">
          <thead>
            <tr><th>Name</th><th>Mode</th><th>Schedule</th><th>Populated</th>
                <th>Last Refresh</th><th>Duration</th><th>Rows Δ</th><th>Status</th></tr>
          </thead>
          <tbody id="tb-int-health"></tbody>
        </table>
      </div>
    </div>

    <div class="row g-3 mb-3">
      <div class="col-4">
        <div class="g-card h-100">
          <div class="g-card-hdr">🌳 Dependency Tree — <code>pgtrickle.dependency_tree()</code></div>
          <pre class="dag-pre" id="pre-dep-tree">loading…</pre>
        </div>
      </div>
      <div class="col-8">
        <div class="g-card h-100">
          <div class="g-card-hdr">📜 Refresh History — <code>pgtrickle.pgt_refresh_history</code></div>
          <div class="overflow-auto" style="max-height:320px;">
            <table class="g-table">
              <thead>
                <tr><th>Table</th><th>Action</th><th>Started</th><th>ms</th><th>Rows Δ</th><th>Status</th></tr>
              </thead>
              <tbody id="tb-int-history"></tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <div class="g-card mb-3">
      <div class="g-card-hdr">⚡ Refresh Efficiency — <code>pgtrickle.refresh_efficiency()</code></div>
      <div class="overflow-auto">
        <table class="g-table">
          <thead>
            <tr><th>Stream Table</th><th>Total Refreshes</th><th>DIFF</th><th>FULL</th>
                <th>Avg DIFF ms</th><th>Avg FULL ms</th><th>DIFF Speedup</th><th>Avg Δ Ratio</th></tr>
          </thead>
          <tbody id="tb-int-eff"></tbody>
        </table>
      </div>
    </div>

    <div class="g-card">
      <div class="g-card-hdr">🎯 Refresh Mode Advisor — <code>pgtrickle.refresh_mode_advisor()</code></div>
      <div class="overflow-auto">
        <table class="g-table">
          <thead>
            <tr><th>Stream Table</th><th>Current</th><th>Effective</th>
                <th>Recommendation</th><th>Confidence</th><th>Reason</th></tr>
          </thead>
          <tbody id="tb-int-advisor"></tbody>
        </table>
      </div>
    </div>

  </div><!-- /tab-pane tab-internals -->
  </div><!-- /tab-content -->
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
const esc = s => s == null ? '—' :
  String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

function fmt(n)  { return n == null ? '—' : Number(n).toLocaleString(); }
function fmtM(n) {
  if (n == null) return '—';
  const v = Math.abs(Number(n));
  if (v >= 1e9)  return (Number(n)/1e9).toFixed(2)  + 'B';
  if (v >= 1e6)  return (Number(n)/1e6).toFixed(2)  + 'M';
  if (v >= 1e3)  return (Number(n)/1e3).toFixed(1)  + 'K';
  return Number(n).toFixed(2);
}
function fmtPnl(n) {
  if (n == null) return '—';
  const cls = Number(n) > 0.01 ? 'pnl-pos' : Number(n) < -0.01 ? 'pnl-neg' : 'pnl-flat';
  const sign = Number(n) > 0.01 ? '+' : '';
  return `<span class="${cls}">${sign}${fmtM(n)}</span>`;
}
function setRows(id, html) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = html;
}

const LEVEL_MAP = {
  price_snapshot:     'L1',
  net_positions:      'L1',
  position_values:    'L2',
  account_pnl:        'L3',
  portfolio_pnl:      'L4',
  sector_exposure:    'L5',
  var_contributions:  'L6',
  account_var:        'L7',
  portfolio_var:      'L8',
  regulatory_capital: 'L9',
  breach_dashboard:   'L10',
};

async function refresh() {
  let d;
  try {
    const r = await fetch('/api/data');
    d = await r.json();
  } catch(e) { return; }

  document.getElementById('ts').textContent =
    'Last update: ' + new Date().toLocaleTimeString();

  // ── KPIs ──────────────────────────────────────────────────────────────────
  const posRows = d.position_values || [];
  const totalExp = posRows.reduce((s,r)=>s+(r.market_value||0),0);
  const totalPnl = posRows.reduce((s,r)=>s+(r.unrealized_pnl||0),0);
  document.getElementById('kpi-positions').textContent = fmt(posRows.length);
  document.getElementById('kpi-exposure').textContent = fmtM(totalExp);
  const pnlEl = document.getElementById('kpi-pnl');
  pnlEl.textContent = (totalPnl >= 0 ? '+' : '') + fmtM(totalPnl);
  pnlEl.style.color = totalPnl >= 0 ? 'var(--green)' : 'var(--red)';
  const totalVar = (d.portfolio_var||[]).reduce((s,r)=>s+(r.portfolio_var_95||0),0);
  document.getElementById('kpi-var').textContent = fmtM(totalVar);

  // ── Breach dashboard (L10) ────────────────────────────────────────────────
  setRows('tb-breach', (d.breach_dashboard||[]).map(r => {
    const sc = r.capital_status === 'BREACH' ? 'status-breach'
             : r.capital_status === 'WARNING' ? 'status-warn' : 'status-ok';
    return `<tr>
       <td style="color:var(--muted)">${esc(r.rank)}</td>
       <td style="font-weight:600">${esc(r.portfolio_name)}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.manager)}</td>
       <td style="font-size:11px">${esc(r.strategy)}</td>
       <td style="text-align:right">${fmtM(r.total_market_value)}</td>
       <td style="text-align:right;color:var(--yellow)">${fmtM(r.portfolio_var_95)}</td>
       <td style="text-align:right;color:var(--red)">${fmtM(r.required_capital)}</td>
       <td style="text-align:right">${r.capital_ratio_pct != null ? Number(r.capital_ratio_pct).toFixed(2)+'%' : '—'}</td>
       <td class="${sc}">${esc(r.capital_status)}</td>
     </tr>`;
  }).join(''));

  // ── Sector exposure (L5) ──────────────────────────────────────────────────
  const sortedSec = [...(d.sector_exposure||[])].sort((a,b)=>
    Math.abs(b.total_exposure||0)-Math.abs(a.total_exposure||0));
  setRows('tb-sector', sortedSec.map(r =>
    `<tr>
       <td style="font-weight:600">${esc(r.sector)}</td>
       <td style="text-align:right;color:var(--muted)">${fmt(r.instrument_count)}</td>
       <td style="text-align:right">${fmtM(r.total_exposure)}</td>
       <td>${fmtPnl(r.total_unrealized_pnl)}</td>
       <td style="text-align:right;color:var(--muted)">${r.pct_of_total_book != null ? Number(r.pct_of_total_book).toFixed(1)+'%' : '—'}</td>
     </tr>`).join(''));

  // ── Portfolio P&L (L4) ────────────────────────────────────────────────────
  setRows('tb-portfolio-pnl', (d.portfolio_pnl||[]).map(r => {
    const retPct = r.portfolio_return_pct;
    const cls = retPct > 0.01 ? 'pnl-pos' : retPct < -0.01 ? 'pnl-neg' : 'pnl-flat';
    const sign = retPct > 0.01 ? '+' : '';
    return `<tr>
       <td style="font-weight:600">${esc(r.portfolio_name)}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.manager)}</td>
       <td style="font-size:11px">${esc(r.strategy)}</td>
       <td style="text-align:right;color:var(--muted)">${fmt(r.account_count)}</td>
       <td style="text-align:right">${fmtM(r.total_market_value)}</td>
       <td>${fmtPnl(r.total_unrealized_pnl)}</td>
       <td class="${cls}" style="text-align:right">${retPct != null ? sign+Number(retPct).toFixed(2)+'%' : '—'}</td>
     </tr>`;
  }).join(''));

  // ── Portfolio VaR (L8) ────────────────────────────────────────────────────
  setRows('tb-portfolio-var', (d.portfolio_var||[]).map(r =>
    `<tr>
       <td style="font-weight:600">${esc(r.portfolio_name)}</td>
       <td style="font-size:11px">${esc(r.strategy)}</td>
       <td style="text-align:right">${fmtM(r.total_market_value)}</td>
       <td style="text-align:right;color:var(--yellow)">${fmtM(r.portfolio_var_95)}</td>
       <td style="text-align:right;color:var(--red)">${fmtM(r.sum_stressed_var_99)}</td>
       <td style="text-align:right;color:var(--muted)">${r.var_pct_nav != null ? Number(r.var_pct_nav).toFixed(3)+'%' : '—'}</td>
     </tr>`).join(''));

  // ── Price snapshot (L1) ───────────────────────────────────────────────────
  const sortedPx = [...(d.price_snapshot||[])].sort((a,b)=>
    Math.abs(b.drift_pct||0) - Math.abs(a.drift_pct||0));
  setRows('tb-prices', sortedPx.map(r => {
    const dp = r.drift_pct || 0;
    const dcls = dp > 0.01 ? 'pnl-pos' : dp < -0.01 ? 'pnl-neg' : 'pnl-flat';
    const sign = dp > 0.01 ? '+' : '';
    const ts = r.price_updated_at ? new Date(r.price_updated_at).toLocaleTimeString() : '—';
    return `<tr>
       <td style="font-family:monospace;font-weight:700">${esc(r.ticker)}</td>
       <td style="font-size:11px;color:var(--muted)">${esc(r.instrument_name)}</td>
       <td style="font-size:11px">${esc(r.sector)}</td>
       <td style="text-align:right;color:var(--muted)">${r.bid != null ? Number(r.bid).toFixed(4) : '—'}</td>
       <td style="text-align:right;color:var(--muted)">${r.ask != null ? Number(r.ask).toFixed(4) : '—'}</td>
       <td style="text-align:right;font-weight:600">${r.mid_price != null ? Number(r.mid_price).toFixed(4) : '—'}</td>
       <td style="text-align:right;color:var(--muted)">${r.base_price != null ? Number(r.base_price).toFixed(4) : '—'}</td>
       <td class="${dcls}" style="text-align:right">${r.price_drift != null ? (r.price_drift>0?'+':'')+Number(r.price_drift).toFixed(4) : '—'}</td>
       <td class="${dcls}" style="text-align:right">${sign}${Number(dp).toFixed(2)}%</td>
       <td style="color:var(--muted);font-size:11px">${ts}</td>
     </tr>`;
  }).join(''));

  // ── Stream table status ────────────────────────────────────────────────────
  setRows('tb-sts', (d.st_status||[]).map(r => {
    const dot = r.is_populated
      ? '<span style="color:var(--green)">●</span>'
      : '<span style="color:var(--yellow)">○</span>';
    const level = LEVEL_MAP[r.name] || '';
    return `<tr>
       <td style="font-family:monospace;font-size:12px">${esc(r.name)}</td>
       <td style="font-size:10px;color:var(--blue)">${level}</td>
       <td style="font-size:11px;color:var(--muted)">${esc(r.refresh_mode)}</td>
       <td style="font-size:11px;color:var(--muted)">${esc(r.schedule||'calc')}</td>
       <td>${dot} ${esc(r.status)}</td>
     </tr>`;
  }).join(''));
}

refresh();
setInterval(refresh, 2000);
</script>
<script src="/static/internals.js"></script>
</body>
</html>
"""

# ── DAG diagram ────────────────────────────────────────────────────────────────

DAG_DIAGRAM = r"""
  Base tables (LEAF schedule)    L1 ── CALCULATED downstream ──────────────────────────────────────────────────────────────

  ┌─────────────────┐  2s        ┌──────────────────┐
  │  market_prices  │──────────►│  price_snapshot  │─────────────────────────────────────────────────────────────────────┐
  │ (tick: 1/30     │            │  (DIFF, 2s)      │                                                                     │
  │  instruments    │            └──────────────────┘                                                                     │
  │  per cycle)     │                                                                                                     │
  └─────────────────┘                                                                                                     │
                                                                                                                          │ L2
  ┌─────────────────┐  1s        ┌──────────────────┐            ┌──────────────────┐                                   ▼
  │     trades      │──────────►│  net_positions   │───────────►│ position_values  │ (DIFF, calc) joins price_snapshot
  │ (append-only    │            │  (DIFF, 1s)      │            │ MV = qty × price │
  │  order stream)  │            └──────────────────┘            └────────┬─────────┘
  └─────────────────┘                                                     │
                                                                          │ L3                        L5
                                                                          ├──────────────────────────►┌──────────────────┐
                                                                          │                           │ sector_exposure  │ (DIFF, calc)
                                                                          ▼                           └──────────────────┘
                                                                 ┌──────────────────┐
                                                                 │   account_pnl    │ (DIFF, calc)
                                                                 └────────┬─────────┘
                                                                          │ L4
                                                                          ▼                           L6
                                                                 ┌──────────────────┐    position_values
                                                                 │  portfolio_pnl   │    ──────────────►┌──────────────────┐
                                                                 │  (DIFF, calc)    │                   │ var_contributions│ (DIFF, calc)
                                                                 └──────────────────┘                   └────────┬─────────┘
                                                                                                                 │ L7
                                                                                                                 ▼
                                                                                                        ┌──────────────────┐
                                                                                                        │   account_var    │ (DIFF, calc)
                                                                                                        └────────┬─────────┘
                                                                                                                 │ L8
                                                                                                                 ▼
                                                                                                        ┌──────────────────┐
                                                                                                        │  portfolio_var   │ (DIFF, calc)
                                                                                                        └────────┬─────────┘
                                                                                                                 │ L9
                                                                                                                 ▼
                                                                                                        ┌──────────────────────┐
                                                                                                        │ regulatory_capital   │ (DIFF, calc)
                                                                                                        └────────┬─────────────┘
                                                                                                                 │ L10
                                                                                                                 ▼
                                                                                                        ┌──────────────────────┐
                                                                                                        │  breach_dashboard    │ ← DIFF SHOWCASE
                                                                                                        │  LIMIT 10 portfolios │   change ratio ~0.02
                                                                                                        └──────────────────────┘

  Only market_prices (2s) and trades (1s) have CALCULATED schedules.
  All 9 derived levels propagate via schedule => 'calculated'.
  One price tick → ~30 of 1,500 positions change (ratio ≈ 0.02).
"""


# ── Data queries ───────────────────────────────────────────────────────────────

def get_data(conn, safe_query, serialize) -> dict:
    price_snapshot = safe_query(conn, """
        SELECT ticker, instrument_name, sector, asset_class,
               bid, ask, mid_price, base_price, price_drift, drift_pct,
               price_updated_at
        FROM   price_snapshot
        ORDER  BY ticker
    """)

    position_values = safe_query(conn, """
        SELECT account_id, account_name, portfolio_id, ticker, sector,
               asset_class, net_quantity, avg_cost_basis, mid_price,
               market_value, unrealized_pnl, pnl_pct
        FROM   position_values
        WHERE  market_value IS NOT NULL
        ORDER  BY ABS(market_value) DESC NULLS LAST
    """)

    sector_exposure = safe_query(conn, """
        SELECT sector, asset_class, instrument_count, account_count,
               total_exposure, total_unrealized_pnl, sector_return_pct, pct_of_total_book
        FROM   sector_exposure
        ORDER  BY ABS(total_exposure) DESC NULLS LAST
    """)

    portfolio_pnl = safe_query(conn, """
        SELECT portfolio_id, portfolio_name, manager, strategy,
               account_count, total_positions, total_market_value,
               total_unrealized_pnl, portfolio_return_pct
        FROM   portfolio_pnl
        ORDER  BY total_market_value DESC NULLS LAST
    """)

    portfolio_var = safe_query(conn, """
        SELECT portfolio_id, portfolio_name, manager, strategy,
               total_market_value, total_pnl,
               sum_var_95, sum_stressed_var_99,
               portfolio_var_95, var_pct_nav
        FROM   portfolio_var
        ORDER  BY portfolio_var_95 DESC NULLS LAST
    """)

    breach_dashboard = safe_query(conn, """
        SELECT rank, portfolio_name, manager, strategy,
               total_market_value, total_pnl,
               portfolio_var_95, required_capital,
               capital_limit, capital_headroom,
               capital_ratio_pct, capital_status
        FROM   breach_dashboard
        ORDER  BY rank
    """)

    st_status = safe_query(conn, """
        SELECT name, status, refresh_mode, is_populated, schedule
        FROM   pgtrickle.pgt_status()
        ORDER  BY name
    """)

    return {
        "price_snapshot":    serialize(price_snapshot),
        "position_values":   serialize(position_values),
        "sector_exposure":   serialize(sector_exposure),
        "portfolio_pnl":     serialize(portfolio_pnl),
        "portfolio_var":     serialize(portfolio_var),
        "breach_dashboard":  serialize(breach_dashboard),
        "st_status":         serialize(st_status),
    }
