"""
pg_trickle demo — e-commerce analytics scenario: HTML, DAG, and data queries.

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
  <title>pg_trickle — Real-time E-commerce Analytics</title>
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
               font-size: 12px; color: var(--muted); background: var(--bg);
               padding: 14px; border-radius: 6px; white-space: pre;
               overflow-x: auto; margin: 0; }
    .live-dot { width: 9px; height: 9px; border-radius: 50%;
                background: var(--green); display: inline-block;
                margin-right: 8px; animation: blink 1.8s ease-in-out infinite; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.25} }
    .rev-bar  { height: 8px; border-radius: 4px; background: var(--blue); }
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
    .delta-up   { color: var(--red); }
    .delta-down { color: var(--green); }
    .delta-flat { color: var(--muted); }
  </style>
</head>
<body>
<div class="container-fluid px-4 py-3">

  <!-- Header -->
  <div class="d-flex align-items-center mb-3 gap-3">
    <div>
      <span class="live-dot"></span>
      <span style="font-size:18px; font-weight:700;">pg_trickle</span>
      <span class="text-muted ms-1">Real-time E-commerce Analytics</span>
    </div>
    <span class="ms-auto text-muted" id="ts" style="font-size:12px;">connecting…</span>
  </div>

  <!-- Tab navigation -->
  <ul class="nav nav-tabs mb-3" id="mainTabs" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="tab-main-btn"
              data-bs-toggle="tab" data-bs-target="#tab-main"
              type="button" role="tab">🛒 E-commerce Analytics</button>
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
        <div class="kpi-val" id="kpi-orders">—</div>
        <div class="kpi-lbl">Total Orders</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-revenue" style="color:var(--blue)">—</div>
        <div class="kpi-lbl">Total Revenue</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-aov" style="color:var(--green)">—</div>
        <div class="kpi-lbl">Avg Order Value</div>
      </div>
    </div>
    <div class="col-3">
      <div class="g-card p-3 text-center">
        <div class="kpi-val" id="kpi-customers" style="color:var(--yellow)">—</div>
        <div class="kpi-lbl">Active Customers</div>
      </div>
    </div>
  </div>

  <!-- Row 2: category revenue + top products -->
  <div class="row g-3 mb-3">
    <div class="col-6">
      <div class="g-card h-100">
        <div class="g-card-hdr">📦 Revenue by Category</div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>Category</th><th>Orders</th><th>Units</th><th>Revenue</th><th>Avg Price</th></tr>
            </thead>
            <tbody id="tb-cat-revenue"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col-6">
      <div class="g-card h-100">
        <div class="g-card-hdr">🏆 Top Products by Revenue</div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>Product</th><th>Category</th><th>Orders</th><th>Units</th><th>Revenue</th></tr>
            </thead>
            <tbody id="tb-products"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Row 3: top customers (DIFF showcase #2) + country revenue -->
  <div class="row g-3 mb-3">
    <div class="col-7">
      <div class="g-card h-100">
        <div class="g-card-hdr">👤 Top 10 Customers
          <small style="color:var(--green);font-size:11px;margin-left:8px">
            DIFFERENTIAL showcase — change ratio ~0.1–0.2
          </small>
        </div>
        <div class="overflow-auto" style="max-height:280px;">
          <table class="g-table">
            <thead>
              <tr><th>Rank</th><th>Customer</th><th>🌍</th><th>Orders</th><th>Total Spent</th><th>Avg Order</th></tr>
            </thead>
            <tbody id="tb-top-customers"></tbody>
          </table>
        </div>
        <div class="p-2" style="font-size:11px;color:var(--muted);border-top:1px solid var(--border)">
          Fixed cardinality (10 rows): only rank shifts trigger output changes.
          Mode Advisor recommends
          <span style="color:var(--green)">KEEP DIFFERENTIAL</span> here.
        </div>
      </div>
    </div>
    <div class="col-5">
      <div class="g-card h-100">
        <div class="g-card-hdr">🌍 Revenue by Country</div>
        <div class="overflow-auto" style="max-height:310px;">
          <table class="g-table">
            <thead>
              <tr><th>Country</th><th>Customers</th><th>Orders</th><th>Revenue</th></tr>
            </thead>
            <tbody id="tb-country-revenue"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Row 4: catalog price impact (DIFF showcase #1) -->
  <div class="row g-3 mb-3">
    <div class="col-12">
      <div class="g-card">
        <div class="g-card-hdr">🏷️ Catalog Price Impact
          <small style="color:var(--green);font-size:11px;margin-left:8px">
            DIFFERENTIAL showcase — change ratio ~0.07
          </small>
        </div>
        <div class="overflow-auto" style="max-height:240px;">
          <table class="g-table">
            <thead>
              <tr><th>Product</th><th>Category</th><th>Base Price</th><th>Current Price</th><th>Delta $</th><th>Delta %</th><th>Last Updated</th></tr>
            </thead>
            <tbody id="tb-price-impact"></tbody>
          </table>
        </div>
        <div class="p-2" style="font-size:11px;color:var(--muted);border-top:1px solid var(--border)">
          Depends only on <code>product_catalog</code> (1 of 15 products repriced per ~30 cycles).
          Change ratio ≈ 0.07. Mode Advisor recommends
          <span style="color:var(--green)">KEEP DIFFERENTIAL</span> here.
        </div>
      </div>
    </div>
  </div>

  <!-- Stream table status + DAG -->
  <div class="row g-3">
    <div class="col-4">
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
    <div class="col-8">
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

  // ── KPIs from customer_stats ───────────────────────────────────────────
  let totalOrders = 0, totalRevenue = 0, activeCustomers = 0;
  (d.customer_stats||[]).forEach(r => {
    totalOrders   += r.order_count|0;
    totalRevenue  += r.total_spent || 0;
    if ((r.order_count|0) > 0) activeCustomers++;
  });
  const aov = activeCustomers > 0 ? totalRevenue / activeCustomers : 0;
  $('kpi-orders').textContent    = fmt(totalOrders);
  $('kpi-revenue').textContent   = fmtM(totalRevenue);
  $('kpi-aov').textContent       = fmtM(aov);
  $('kpi-customers').textContent = fmt(activeCustomers);

  // ── Category revenue ───────────────────────────────────────────────────
  const sortedCat = [...(d.category_revenue||[])].sort((a,b)=>b.revenue-a.revenue);
  setRows('tb-cat-revenue', sortedCat.map(r =>
    `<tr>
       <td>${esc(r.category)}</td>
       <td style="text-align:right">${fmt(r.order_count)}</td>
       <td style="text-align:right">${fmt(r.units_sold)}</td>
       <td style="text-align:right;color:var(--blue)">${fmtM(r.revenue)}</td>
       <td style="text-align:right;color:var(--muted)">${fmtM(r.avg_price)}</td>
     </tr>`).join(''));

  // ── Top products ───────────────────────────────────────────────────────
  const sortedP = [...(d.product_sales||[])].sort((a,b)=>b.revenue-a.revenue);
  setRows('tb-products', sortedP.slice(0,10).map(r =>
    `<tr>
       <td>${esc(r.product_name)}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.category)}</td>
       <td style="text-align:right">${fmt(r.order_count)}</td>
       <td style="text-align:right">${fmt(r.units_sold)}</td>
       <td style="text-align:right;color:var(--blue)">${fmtM(r.revenue)}</td>
     </tr>`).join(''));

  // ── Top 10 customers (DIFFERENTIAL showcase) ──────────────────────────
  setRows('tb-top-customers', (d.top_10_customers||[]).map(r =>
    `<tr>
       <td style="font-weight:bold;text-align:center">#${r.rank}</td>
       <td>${esc(r.customer_name)}</td>
       <td style="color:var(--muted)">${esc(r.country)}</td>
       <td style="text-align:right">${fmt(r.order_count)}</td>
       <td style="text-align:right;color:var(--blue)">${fmtM(r.total_spent)}</td>
       <td style="text-align:right;color:var(--muted)">${fmtM(r.avg_order_value)}</td>
     </tr>`).join(''));

  // ── Country revenue ────────────────────────────────────────────────────
  const sortedCo = [...(d.country_revenue||[])].sort((a,b)=>b.total_revenue-a.total_revenue);
  setRows('tb-country-revenue', sortedCo.map(r =>
    `<tr>
       <td>${esc(r.country)}</td>
       <td style="text-align:right">${fmt(r.customer_count)}</td>
       <td style="text-align:right">${fmt(r.total_orders)}</td>
       <td style="text-align:right;color:var(--blue)">${fmtM(r.total_revenue)}</td>
     </tr>`).join(''));

  // ── Catalog price impact (DIFFERENTIAL showcase) ──────────────────────
  setRows('tb-price-impact', (d.catalog_price_impact||[]).map(r => {
    const delta = r.price_delta || 0;
    const pct   = r.pct_change  || 0;
    const cls   = delta > 0.01 ? 'delta-up' : delta < -0.01 ? 'delta-down' : 'delta-flat';
    const sign  = delta > 0.01 ? '+' : '';
    const ts    = r.price_last_updated ? new Date(r.price_last_updated).toLocaleTimeString() : '—';
    return `<tr>
       <td>${esc(r.product_name)}</td>
       <td style="color:var(--muted);font-size:11px">${esc(r.category)}</td>
       <td style="text-align:right;color:var(--muted)">${fmtM(r.base_price)}</td>
       <td style="text-align:right">${fmtM(r.current_price)}</td>
       <td class="${cls}" style="text-align:right">${sign}${fmtM(delta)}</td>
       <td class="${cls}" style="text-align:right">${sign}${Number(pct).toFixed(1)}%</td>
       <td style="color:var(--muted);font-size:11px">${ts}</td>
     </tr>`;
  }).join(''));

  // ── Stream table status ────────────────────────────────────────────────
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
  Base tables          Layer 1 — Silver           Layer 2 — Gold         Layer 3 — Platinum
  ────────────         ──────────────────────      ─────────────────────  ──────────────────────

  ┌────────────┐       ┌──────────────────┐
  │ customers  │──────►│ customer_stats   │──────────────────────────────►┌──────────────────┐
  └────────────┘       │  (DIFFERENTIAL)  │                               │ country_revenue  │
                       └──────────────────┘                               │  (DIFFERENTIAL)  │
                                │                                          └──────────────────┘
  ┌────────────┐                │
  │  orders    │────────────────┘
  │ (stream)   │
  └──────┬─────┘
         │        ┌────────────────┐
         └───────►│ product_sales  │
  ┌────────────┐  │ (DIFFERENTIAL) │
  │  products  │─►│                │
  │ categories │  └────────────────┘
  └──────┬─────┘
         │        ┌─────────────────┐
         └───────►│ category_revenue│
                  │  (DIFFERENTIAL) │
                  └─────────────────┘

  ┌────────────────────┐   ┌──────────────────────┐
  │  product_catalog   │──►│ catalog_price_impact │  ← DIFFERENTIAL SHOWCASE #1
  │  (slowly-changing) │   │   (DIFFERENTIAL 5s)  │    change ratio ~0.07
  └────────────────────┘   └──────────────────────┘

  ┌──────────────────┐   ┌──────────────────┐
  │  customer_stats  │──►│  top_10_customers│  ← DIFFERENTIAL SHOWCASE #2
  │  (DIFFERENTIAL)  │   │  (DIFFERENTIAL)  │    change ratio ~0.1–0.2
  └──────────────────┘   │  LIMIT 10        │    (fixed-cardinality output)
                         └──────────────────┘
"""


# ── Data queries ───────────────────────────────────────────────────────────────

def get_data(conn, safe_query, serialize) -> dict:
    category_revenue = safe_query(conn, """
        SELECT category_name AS category, order_count, units_sold, revenue, avg_price, unique_customers
        FROM   category_revenue
        ORDER  BY revenue DESC NULLS LAST
    """)

    product_sales = safe_query(conn, """
        SELECT product_name, category_name AS category, order_count, units_sold, revenue, avg_selling_price
        FROM   product_sales
        ORDER  BY revenue DESC NULLS LAST
    """)

    customer_stats = safe_query(conn, """
        SELECT customer_name, country, order_count, total_spent, avg_order_value
        FROM   customer_stats
        WHERE  order_count > 0
    """)

    top_10_customers = safe_query(conn, """
        SELECT rank, customer_name, country, order_count, total_spent, avg_order_value
        FROM   top_10_customers
        ORDER  BY rank
    """)

    country_revenue = safe_query(conn, """
        SELECT country, customer_count, total_orders, total_revenue, avg_order_value
        FROM   country_revenue
        ORDER  BY total_revenue DESC NULLS LAST
    """)

    catalog_price_impact = safe_query(conn, """
        SELECT product_name, category_name AS category, base_price, current_price,
               price_delta, pct_change, price_last_updated
        FROM   catalog_price_impact
        ORDER  BY product_name
    """)

    st_status = safe_query(conn, """
        SELECT name, status, refresh_mode, is_populated, schedule
        FROM   pgtrickle.pgt_status()
        ORDER  BY name
    """)

    return {
        "category_revenue":     serialize(category_revenue),
        "product_sales":        serialize(product_sales),
        "customer_stats":       serialize(customer_stats),
        "top_10_customers":     serialize(top_10_customers),
        "country_revenue":      serialize(country_revenue),
        "catalog_price_impact": serialize(catalog_price_impact),
        "st_status":            serialize(st_status),
    }
