"""
pg_trickle demo — fraud detection scenario: HTML, DAG, and data queries.

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
  <title>pg_trickle — Real-time Fraud Detection</title>
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
      --text:    #e6edf3;
    }
    html, body { background: var(--bg); color: var(--text); font-size: 14px; }
    a { color: var(--green); }
    .g-card { background: var(--card); border: 1px solid var(--border);
               border-radius: 8px; overflow: hidden; }
    .g-card-hdr { background: var(--hdr); padding: 8px 14px;
                  font-size: 13px; font-weight: 600; letter-spacing: .4px; }
    .g-table { width: 100%; border-collapse: collapse; }
    .g-table th { color: var(--muted); font-weight: 500; font-size: 12px;
                  border-bottom: 1px solid var(--border); padding: 6px 10px; }
    .g-table td { padding: 5px 10px; border-bottom: 1px solid var(--hdr); }
    .g-table tr:last-child td { border-bottom: none; }
    .g-table tr.risk-HIGH  td { color: #ff7b72; }
    .g-table tr.risk-MED   td { color: #e3b341; }
    .kpi-val { font-size: 2.2rem; font-weight: 700; line-height: 1; }
    .kpi-lbl { font-size: 11px; color: var(--muted); margin-top: 4px; }
    .rbadge { display: inline-block; padding: 2px 7px; border-radius: 12px;
              font-size: 11px; font-weight: 700; }
    .rbadge-LOW    { background: #1a4620; color: var(--green); }
    .rbadge-MEDIUM { background: #422d09; color: var(--yellow); }
    .rbadge-HIGH   { background: #3d1114; color: var(--red); }
    .dag-pre { font-family: 'SFMono-Regular', Consolas, monospace;
               font-size: 12px; color: var(--muted); background: var(--bg);
               padding: 14px; border-radius: 6px; white-space: pre;
               overflow-x: auto; margin: 0; }
    .live-dot { width: 9px; height: 9px; border-radius: 50%;
                background: var(--green); display: inline-block;
                margin-right: 8px; animation: blink 1.8s ease-in-out infinite; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.25} }
    .risk-bar { height: 8px; border-radius: 4px; }
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
  </style>
</head>
<body>
<div class="container-fluid px-4 py-3">

  <!-- Header -->
  <div class="d-flex align-items-center mb-3 gap-3">
    <div>
      <span class="live-dot"></span>
      <span style="font-size:18px; font-weight:700;">pg_trickle</span>
      <span class="text-muted ms-1">Real-time Fraud Detection Pipeline</span>
    </div>
    <span class="ms-auto text-muted" id="ts" style="font-size:12px;">connecting…</span>
  </div>

  <!-- Tab navigation -->
  <ul class="nav nav-tabs mb-3" id="mainTabs" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="tab-main-btn"
              data-bs-toggle="tab" data-bs-target="#tab-main"
              type="button" role="tab">🔍 Fraud Detection</button>
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
        <div class="kpi-val" id="kpi-total">—</div>
        <div class="kpi-lbl">Total Transactions</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-low" style="color:var(--green)">—</div>
        <div class="kpi-lbl">LOW Risk</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-med" style="color:var(--yellow)">—</div>
        <div class="kpi-lbl">MEDIUM Risk</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-high" style="color:var(--red)">—</div>
        <div class="kpi-lbl">HIGH Risk</div>
      </div>
    </div>
  </div>

  <!-- Risk bar -->
  <div class="mb-3 g-card p-3">
    <div class="d-flex mb-1" style="font-size:12px;">
      <span class="text-success me-auto">LOW</span>
      <span class="text-warning">MEDIUM</span>
      <span class="text-danger ms-auto">HIGH</span>
    </div>
    <div class="d-flex gap-1" id="risk-bar" style="height:10px; border-radius:6px; overflow:hidden;"></div>
  </div>

  <!-- Row 2: alerts + risky merchants -->
  <div class="row g-3 mb-3">
    <div class="col-7">
      <div class="g-card h-100">
        <div class="g-card-hdr">⚠️ Recent HIGH / MEDIUM Risk Alerts</div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>#</th><th>User</th><th>Merchant</th>
                  <th>Category</th><th>Amount</th><th>Risk</th></tr>
            </thead>
            <tbody id="tb-alerts"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-5">
      <div class="g-card h-100">
        <div class="g-card-hdr">🏪 Merchant Risk Leaderboard</div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>Merchant</th><th>Cat.</th><th>High</th><th>Med</th><th>Risk%</th></tr>
            </thead>
            <tbody id="tb-merchants"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Row 3: user velocity + country risk + category volume -->
  <div class="row g-3 mb-3">
    <div class="col-4">
      <div class="g-card h-100">
        <div class="g-card-hdr">👤 User Velocity (top 10)</div>
        <div class="overflow-auto" style="max-height:240px;">
          <table class="g-table">
            <thead>
              <tr><th>User</th><th>🌍</th><th>Txns</th><th>Avg $</th></tr>
            </thead>
            <tbody id="tb-velocity"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-4">
      <div class="g-card h-100">
        <div class="g-card-hdr">🌍 Country Overview</div>
        <div class="overflow-auto" style="max-height:240px;">
          <table class="g-table">
            <thead>
              <tr><th>Country</th><th>Users</th><th>Txns</th><th>Volume $</th></tr>
            </thead>
            <tbody id="tb-country"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-4">
      <div class="g-card h-100">
        <div class="g-card-hdr">📦 Category Volume</div>
        <div class="overflow-auto" style="max-height:240px;">
          <table class="g-table">
            <thead>
              <tr><th>Category</th><th>Txns</th><th>Avg $</th><th>Users</th></tr>
            </thead>
            <tbody id="tb-category"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Row 3b: Differential efficiency showcase #1 -->
  <div class="row g-3 mb-3">
    <div class="col-7">
      <div class="g-card h-100">
        <div class="g-card-hdr">🔬 Merchant Tier Stats
          <small style="color:var(--green);font-size:11px;margin-left:8px">
            DIFFERENTIAL showcase — change ratio ~0.07
          </small>
        </div>
        <div class="overflow-auto" style="max-height:220px;">
          <table class="g-table">
            <thead>
              <tr><th>Merchant</th><th>Cat.</th><th>Tier</th><th>Risk</th><th>Last Changed</th></tr>
            </thead>
            <tbody id="tb-tier-stats"></tbody>
          </table>
        </div>
        <div class="p-2" style="font-size:11px;color:var(--muted);border-top:1px solid var(--border)">
          Only tier rotations trigger output changes (~every 30 cycles). No
          fast-growing sources — only <code>merchant_risk_tier</code> (15 rows).
          Mode Advisor recommends
          <span style="color:var(--green)">KEEP DIFFERENTIAL</span> here.
        </div>
      </div>
    </div>
    <div class="col-5">
      <div class="g-card h-100">
        <div class="g-card-hdr">🏷️ Live Merchant Risk Tiers
          <small style="color:var(--muted);font-size:11px;margin-left:8px">rotates ~every 30 cycles</small>
        </div>
        <div class="overflow-auto" style="max-height:260px;">
          <table class="g-table">
            <thead>
              <tr><th>Merchant</th><th>Category</th><th>Tier</th></tr>
            </thead>
            <tbody id="tb-tiers"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Row 3c: Top-10 leaderboard showcase #2 -->
  <div class="row g-3 mb-3">
    <div class="col-12">
      <div class="g-card">
        <div class="g-card-hdr">🏆 Top 10 Risky Merchants Leaderboard
          <small style="color:var(--green);font-size:11px;margin-left:8px">
            DIFFERENTIAL showcase — change ratio ~0.2–0.3
          </small>
        </div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>Rank</th><th>Merchant</th><th>Category</th><th>Total Txns</th><th>High</th><th>Medium</th><th>Risk%</th></tr>
            </thead>
            <tbody id="tb-top-10"></tbody>
          </table>
        </div>
        <div class="p-2" style="font-size:11px;color:var(--muted);border-top:1px solid var(--border)">
          Fixed cardinality (10 rows) means only rank shifts change, not 100% of
          output rows. Change ratio ≈ 0.2–0.3 (typically 2–3 merchants move in/out
          per cycle). Mode Advisor recommends
          <span style="color:var(--green)">KEEP DIFFERENTIAL</span> here.
        </div>
      </div>
    </div>
  </div>

  <!-- Row 4: alert_summary + risk by category -->
  <div class="row g-3 mb-3">
    <div class="col-5">
      <div class="g-card h-100">
        <div class="g-card-hdr">🎯 Alert Summary</div>
        <table class="g-table">
          <thead>
            <tr><th>Risk Level</th><th>Transactions</th><th>Avg Amount</th><th>Total Volume</th></tr>
          </thead>
          <tbody id="tb-alert-summary"></tbody>
        </table>
      </div>
    </div>
    <div class="col-7">
      <div class="g-card h-100">
        <div class="g-card-hdr">🔍 Risk Distribution by Category</div>
        <div class="overflow-auto" style="max-height:200px;">
          <table class="g-table">
            <thead>
              <tr><th>Category</th><th>Low</th><th>Medium</th><th>High</th><th>High%</th></tr>
            </thead>
            <tbody id="tb-risk-by-cat"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Stream table status + DAG -->
  <div class="row g-3">
    <div class="col-5">
      <div class="g-card">
        <div class="g-card-hdr">⚡ Stream Table Status</div>
        <div class="overflow-auto" style="max-height:220px;">
          <table class="g-table">
            <thead>
              <tr><th>Name</th><th>Mode</th><th>Schedule</th><th>Status</th></tr>
            </thead>
            <tbody id="tb-sts"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-7">
      <div class="g-card">
        <div class="g-card-hdr" style="cursor:pointer"
             data-bs-toggle="collapse" data-bs-target="#dag-collapse">
          📊 DAG Topology — click to expand
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
            <tr><th>Table</th><th>Total</th><th>DIFF</th><th>FULL</th>
                <th>Avg DIFF ms</th><th>Avg FULL ms</th><th>Speedup</th><th>Avg Change Ratio</th></tr>
          </thead>
          <tbody id="tb-int-eff"></tbody>
        </table>
      </div>
    </div>

    <div class="g-card">
      <div class="g-card-hdr">🤖 Refresh Mode Advisor — <code>pgtrickle.recommend_refresh_mode()</code></div>
      <div class="overflow-auto">
        <table class="g-table">
          <thead>
            <tr><th>Table</th><th>Current</th><th>Effective</th><th>Recommendation</th>
                <th>Confidence</th><th>Reason</th></tr>
          </thead>
          <tbody id="tb-int-advisor"></tbody>
        </table>
      </div>
    </div>

  </div><!-- /tab-pane tab-internals -->

  </div><!-- /tab-content -->

</div><!-- /container -->

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
"use strict";

const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const fmt  = (n, d=0) => n == null ? '—' : Number(n).toLocaleString('en-US', {maximumFractionDigits:d});
const fmtM = n => n == null ? '—' : '$' + Number(n).toLocaleString('en-US', {minimumFractionDigits:2,maximumFractionDigits:2});
const badge = lvl => `<span class="rbadge rbadge-${esc(lvl)}">${esc(lvl)}</span>`;

function setRows(tbodyId, html) {
  $(tbodyId).innerHTML = html || '<tr><td colspan="99" class="text-muted">waiting for data…</td></tr>';
}

async function refresh() {
  let d;
  try {
    const r = await fetch('/api/data');
    d = await r.json();
  } catch(e) {
    $('ts').textContent = 'Error: ' + e.message;
    return;
  }

  $('ts').textContent = 'Last refresh: ' + new Date().toLocaleTimeString();

  let low=0, med=0, high=0, total=0;
  (d.alert_summary||[]).forEach(r => {
    const n = r.txn_count|0;
    if (r.risk_level==='LOW')    low  = n;
    if (r.risk_level==='MEDIUM') med  = n;
    if (r.risk_level==='HIGH')   high = n;
    total += n;
  });
  $('kpi-total').textContent = fmt(total);
  $('kpi-low').textContent   = fmt(low);
  $('kpi-med').textContent   = fmt(med);
  $('kpi-high').textContent  = fmt(high);

  if (total > 0) {
    const lp = (low/total*100).toFixed(1), mp = (med/total*100).toFixed(1), hp = (100-parseFloat(lp)-parseFloat(mp)).toFixed(1);
    $('risk-bar').innerHTML =
      `<div class="risk-bar flex-fill" style="background:#1a4620;width:${lp}%" title="LOW ${lp}%"></div>` +
      `<div class="risk-bar flex-fill" style="background:#422d09;width:${mp}%" title="MED ${mp}%"></div>` +
      `<div class="risk-bar flex-fill" style="background:#3d1114;width:${hp}%" title="HIGH ${hp}%"></div>`;
  }

  setRows('tb-alerts', (d.recent_alerts||[]).map(r =>
    `<tr class="risk-${r.risk_level==='HIGH'?'HIGH':r.risk_level==='MEDIUM'?'MED':''}">
       <td>#${esc(r.txn_id)}</td><td>${esc(r.user_name)}</td><td>${esc(r.merchant_name)}</td>
       <td>${esc(r.merchant_category)}</td><td>${fmtM(r.amount)}</td><td>${badge(r.risk_level)}</td>
     </tr>`).join(''));

  const sortedM = [...(d.top_risky_merchants||[])].sort(
    (a,b) => (b.high_risk_count-a.high_risk_count) || (b.medium_risk_count-a.medium_risk_count));
  setRows('tb-merchants', sortedM.slice(0,10).map(r =>
    `<tr>
       <td>${esc(r.merchant_name)}</td>
       <td><span class="rbadge" style="background:#21262d;color:#ccc">${esc(r.merchant_category)}</span></td>
       <td style="color:var(--red)">${r.high_risk_count}</td>
       <td style="color:var(--yellow)">${r.medium_risk_count}</td>
       <td>${r.risk_rate_pct}%</td>
     </tr>`).join(''));

  const sortedU = [...(d.user_velocity||[])].sort((a,b)=>b.txn_count-a.txn_count);
  setRows('tb-velocity', sortedU.slice(0,10).map(r =>
    `<tr>
       <td>${esc(r.user_name)}</td><td>${esc(r.country)}</td>
       <td>${r.txn_count}</td><td>${fmtM(r.avg_txn_amount)}</td>
     </tr>`).join(''));

  const sortedC = [...(d.country_risk||[])].sort((a,b)=>b.total_txns-a.total_txns);
  setRows('tb-country', sortedC.slice(0,10).map(r =>
    `<tr>
       <td>${esc(r.country)}</td><td>${r.user_count}</td>
       <td>${r.total_txns}</td><td>${fmtM(r.total_volume)}</td>
     </tr>`).join(''));

  const sortedCat = [...(d.category_volume||[])].sort((a,b)=>b.total_volume-a.total_volume);
  setRows('tb-category', sortedCat.map(r =>
    `<tr>
       <td>${esc(r.category)}</td><td>${r.txn_count}</td>
       <td>${fmtM(r.avg_txn_amount)}</td><td>${r.unique_users}</td>
     </tr>`).join(''));

  const riskColors = { HIGH: 'var(--red)', MEDIUM: 'var(--yellow)', LOW: 'var(--green)' };
  setRows('tb-alert-summary', (d.alert_summary||[]).map(r => {
    const col = riskColors[r.risk_level] || 'inherit';
    return `<tr>
       <td><span class="rbadge rbadge-${esc(r.risk_level)}">${esc(r.risk_level)}</span></td>
       <td style="color:${col}">${fmt(r.txn_count)}</td>
       <td>${fmtM(r.avg_amount)}</td><td>${fmtM(r.total_amount)}</td>
     </tr>`;
  }).join(''));

  setRows('tb-risk-by-cat', (d.risk_by_category||[]).map(r =>
    `<tr>
       <td><span class="rbadge" style="background:#21262d;color:#ccc">${esc(r.merchant_category)}</span></td>
       <td style="color:var(--green)">${fmt(r.low_count)}</td>
       <td style="color:var(--yellow)">${fmt(r.medium_count)}</td>
       <td style="color:var(--red)">${fmt(r.high_count)}</td>
       <td>${r.high_pct}%</td>
     </tr>`).join(''));

  setRows('tb-sts', (d.st_status||[]).map(r => {
    const dot = r.is_populated
      ? '<span style="color:var(--green)">●</span>'
      : '<span style="color:var(--yellow)">○</span>';
    return `<tr>
       <td style="font-family:monospace;font-size:12px">${esc(r.name)}</td>
       <td style="font-size:11px;color:var(--muted)">${esc(r.refresh_mode)}</td>
       <td style="font-size:11px;color:var(--muted)">${esc(r.schedule||'calc')}</td>
       <td>${dot} ${esc(r.status)}</td>
     </tr>`;
  }).join(''));

  const tierColors = { HIGH: 'var(--red)', ELEVATED: 'var(--yellow)', STANDARD: 'var(--green)' };
  setRows('tb-tier-stats', (d.merchant_tier_stats||[]).map(r => {
    const col = tierColors[r.merchant_tier] || 'inherit';
    const ts = r.tier_last_changed ? new Date(r.tier_last_changed).toLocaleTimeString() : '—';
    return `<tr>
       <td>${esc(r.merchant_name)}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.category||'')}</td>
       <td style="color:${col};font-weight:bold">${esc(r.merchant_tier)}</td>
       <td style="text-align:right;color:var(--muted)">${r.risk_score}</td>
       <td style="color:var(--muted);font-size:11px">${ts}</td>
     </tr>`;
  }).join(''));

  setRows('tb-tiers', (d.merchant_tiers||[]).map(r => {
    const col = tierColors[r.tier] || 'inherit';
    return `<tr>
       <td>${esc(r.merchant_name)}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.category||'')}</td>
       <td style="color:${col};font-weight:bold">${esc(r.tier)}</td>
     </tr>`;
  }).join(''));

  setRows('tb-top-10', (d.top_10_risky||[]).map(r => {
    const riskClass = r.risk_rate_pct >= 50 ? 'color:var(--red)'
                    : r.risk_rate_pct >= 25 ? 'color:var(--yellow)'
                    : 'color:var(--green)';
    return `<tr>
       <td style="font-weight:bold;text-align:center">#${r.rank}</td>
       <td>${esc(r.merchant_name)}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.merchant_category)}</td>
       <td style="text-align:right">${fmt(r.total_txns)}</td>
       <td style="color:var(--red);text-align:right">${r.high_risk_count}</td>
       <td style="color:var(--yellow);text-align:right">${r.medium_risk_count}</td>
       <td style="${riskClass};text-align:right;font-weight:bold">${r.risk_rate_pct}%</td>
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
  Base tables            Layer 1 — Silver        Layer 2 — Gold          Layer 3 — Platinum
  ────────────           ──────────────────────   ─────────────────────   ──────────────────────

  ┌────────────┐         ┌──────────────────┐
  │   users    │────────►│  user_velocity   │─────────────────────────►┌──────────────────┐
  └────────────┘         │  (DIFFERENTIAL)  │                          │   country_risk   │
                         └──────┬───────────┘                          │  (DIFFERENTIAL)  │
                                │                                       └──────────────────┘
  ┌────────────┐                │  ┌──────────────────┐
  │transactions│────────────────┼─►│  merchant_stats  │
  │ (stream)   │                │  │  (DIFFERENTIAL)  │
  └────────────┘                │  └──────┬───────────┘
        │                       │         │
        │         ┌─────────────┼─────────┘ ← DIAMOND DEPENDENCY
        │         │             │
        │         ▼             ▼                                       ┌───────────────────────┐
        │    ┌────────────────────────┐                                 │    alert_summary      │
        │    │      risk_scores       │────────────────────────────────►│    (DIFFERENTIAL)     │
        │    │   (FULL, calculated)   │                                 └───────────────────────┘
        │    └────────────────────────┘
        │                                                               ┌───────────────────────┐
        │                                                               │  top_risky_merchants  │
        └──────────────────────────────────────────────────────────────►│    (DIFFERENTIAL)     │
                                                                        └───────────────────────┘
  ┌────────────┐         ┌──────────────────┐
  │ merchants  │────────►│ category_volume  │
  └────────────┘         │  (DIFFERENTIAL)  │
                         └──────────────────┘

  ┌───────────────────┐   ┌─────────────────────┐
  │ merchant_risk_tier│──►│ merchant_tier_stats │  ← DIFFERENTIAL SHOWCASE #1
  │ (slowly-changing) │   │   (DIFFERENTIAL 5s) │    change ratio ~0.07
  └───────────────────┘   └─────────────────────┘

  ┌──────────────────────┐   ┌─────────────────────┐
  │ top_risky_merchants  │──►│ top_10_risky_       │  ← DIFFERENTIAL SHOWCASE #2
  │  (DIFFERENTIAL)      │   │  merchants          │    change ratio ~0.2–0.3
  └──────────────────────┘   │  (DIFFERENTIAL 5s)  │
                             └─────────────────────┘
"""


# ── Data queries ───────────────────────────────────────────────────────────────

def get_data(conn, safe_query, serialize) -> dict:
    recent_alerts = safe_query(conn, """
        SELECT txn_id, user_name, merchant_name, merchant_category,
               amount, risk_level
        FROM   risk_scores
        WHERE  risk_level IN ('HIGH', 'MEDIUM')
        ORDER  BY txn_id DESC
        LIMIT  15
    """)

    alert_summary = safe_query(conn, """
        SELECT risk_level, txn_count, total_amount, avg_amount
        FROM   alert_summary
        ORDER  BY CASE risk_level
                      WHEN 'HIGH'   THEN 0
                      WHEN 'MEDIUM' THEN 1
                      ELSE 2 END
    """)

    top_risky_merchants = safe_query(conn, """
        SELECT merchant_name, merchant_category, total_txns,
               high_risk_count, medium_risk_count, risk_rate_pct
        FROM   top_risky_merchants
    """)

    user_velocity = safe_query(conn, """
        SELECT user_name, country, txn_count, total_spent, avg_txn_amount
        FROM   user_velocity
        WHERE  txn_count > 0
    """)

    country_risk = safe_query(conn, """
        SELECT country, user_count, total_txns, total_volume
        FROM   country_risk
    """)

    category_volume = safe_query(conn, """
        SELECT category, txn_count, total_volume, avg_txn_amount, unique_users
        FROM   category_volume
        WHERE  txn_count > 0
    """)

    st_status = safe_query(conn, """
        SELECT name, status, refresh_mode, is_populated, schedule
        FROM   pgtrickle.pgt_status()
        ORDER  BY name
    """)

    risk_by_category = safe_query(conn, """
        SELECT merchant_category,
               COUNT(*) FILTER (WHERE risk_level = 'LOW')    AS low_count,
               COUNT(*) FILTER (WHERE risk_level = 'MEDIUM') AS medium_count,
               COUNT(*) FILTER (WHERE risk_level = 'HIGH')   AS high_count,
               ROUND(100.0 * COUNT(*) FILTER (WHERE risk_level = 'HIGH')
                     / NULLIF(COUNT(*), 0), 1)               AS high_pct
        FROM   risk_scores
        GROUP  BY merchant_category
        ORDER  BY high_pct DESC NULLS LAST
    """)

    merchant_tier_stats = safe_query(conn, """
        SELECT merchant_id, merchant_name, category,
               merchant_tier, risk_score, tier_last_changed
        FROM   merchant_tier_stats
        ORDER  BY merchant_id
    """)

    merchant_tiers = safe_query(conn, """
        SELECT mrt.merchant_id, m.name AS merchant_name, mrt.tier, mrt.updated_at
        FROM   merchant_risk_tier mrt
        JOIN   merchants m ON m.id = mrt.merchant_id
        ORDER  BY mrt.merchant_id
    """)

    top_10_risky = safe_query(conn, """
        SELECT rank, merchant_name, merchant_category,
               total_txns, high_risk_count, medium_risk_count, risk_rate_pct
        FROM   top_10_risky_merchants
        ORDER  BY rank
    """)

    return {
        "recent_alerts":        serialize(recent_alerts),
        "alert_summary":        serialize(alert_summary),
        "top_risky_merchants":  serialize(top_risky_merchants),
        "user_velocity":        serialize(user_velocity),
        "country_risk":         serialize(country_risk),
        "category_volume":      serialize(category_volume),
        "st_status":            serialize(st_status),
        "risk_by_category":     serialize(risk_by_category),
        "merchant_tier_stats":  serialize(merchant_tier_stats),
        "merchant_tiers":       serialize(merchant_tiers),
        "top_10_risky":         serialize(top_10_risky),
    }
