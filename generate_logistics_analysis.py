"""
generate_logistics_analysis.py
-------------------------------
Dashboard Iniciativas SDX — análise mensal multi-país (MLB, MLC, MLM, MLA, MLU)

Abas:
  Visão Geral        — Volume · Not Delivered · Lead Time por mês e país + por método de envio
  Qualidade & SLA    — Top 3 estados por país · Pareto de motivos ND

USO:
    .venv/Scripts/python generate_logistics_analysis.py

SAÍDA:
    sdx_dashboard.html

DEPENDÊNCIAS:
    pip install google-cloud-bigquery pandas db-dtypes
"""

from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime

# ── CONFIGURAÇÃO ──────────────────────────────────────────────────────────────

PROJECT   = "meli-bi-data"
DATE_FROM = "2025-01-01"
DATE_TO   = "2026-04-10"
OUTPUT    = "sdx_dashboard.html"

TABLE_MLB   = "`meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`"
TABLE_MULTI = "`meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS`"

# ── CTE BASE — UNION MLB + multi-país ─────────────────────────────────────────

_BASE_CTE = f"""
  all_orders AS (
    SELECT
      'MLB'                                                                AS site,
      ORDER_SHIPPING_NUMBER,
      DATE_CREATED,
      COALESCE(DATE_FIRST_VISIT, DATE_DELIVERED)                           AS delivery_date,
      SHP_STATUS_TYPE,
      COALESCE(NULLIF(TRIM(SHP_PICKING_TYPE_ID), ''), 'unknown')          AS picking_type,
      COALESCE(NULLIF(TRIM(SHP_ADD_STATE_ID), ''), 'unknown')             AS state,
      COALESCE(NULLIF(TRIM(MOTIVO_NO_ENTREGA_NAME_1), ''), 'sem motivo')  AS motivo_nd
    FROM {TABLE_MLB}
    WHERE DATE(DATE_CREATED) BETWEEN '{DATE_FROM}' AND '{DATE_TO}'

    UNION ALL

    SELECT
      SIT_SITE_ID                                                          AS site,
      ORDER_SHIPPING_NUMBER,
      DATE_CREATED,
      COALESCE(DATE_FIRST_VISIT, DATE_DELIVERED)                           AS delivery_date,
      SHP_STATUS_TYPE,
      COALESCE(NULLIF(TRIM(SHP_PICKING_TYPE_ID), ''), 'unknown')          AS picking_type,
      COALESCE(NULLIF(TRIM(SHP_ADD_STATE_ID), ''), 'unknown')             AS state,
      COALESCE(NULLIF(TRIM(MOTIVO_NO_ENTREGA_NAME_1), ''), 'sem motivo')  AS motivo_nd
    FROM {TABLE_MULTI}
    WHERE DATE(DATE_CREATED) BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
      AND SIT_SITE_ID IN ('MLC', 'MLM', 'MLA', 'MLU')
  )
"""

# Fragmento reutilizável de lead time médio (sem alias de tabela)
_LT_CASE = "DATE_DIFF(DATE(delivery_date), DATE(DATE_CREATED), DAY)"
_LT_AVG  = f"""ROUND(AVG(
    CASE
      WHEN delivery_date IS NOT NULL
       AND {_LT_CASE} BETWEEN 1 AND 59
      THEN {_LT_CASE}
    END
  ), 2)"""

# Lead time com alias qualificado para queries com JOIN
_LT_CASE_Q = "DATE_DIFF(DATE(all_orders.delivery_date), DATE(all_orders.DATE_CREATED), DAY)"
_LT_AVG_Q  = f"""ROUND(AVG(
    CASE
      WHEN all_orders.delivery_date IS NOT NULL
       AND {_LT_CASE_Q} BETWEEN 1 AND 59
      THEN {_LT_CASE_Q}
    END
  ), 2)"""

# ── QUERIES — Visão Geral ─────────────────────────────────────────────────────

QUERY_KPIS = f"""
WITH {_BASE_CTE}
SELECT
  COUNT(ORDER_SHIPPING_NUMBER)                                                                    AS total_sold,
  COUNTIF(delivery_date IS NOT NULL)                                                              AS total_delivered,
  COUNTIF(SHP_STATUS_TYPE = 'not_delivered')                                                      AS total_nd,
  ROUND(SAFE_DIVIDE(COUNTIF(SHP_STATUS_TYPE = 'not_delivered'), COUNT(ORDER_SHIPPING_NUMBER)) * 100, 2) AS nd_pct,
  {_LT_AVG}                                                                                       AS avg_lead_time
FROM all_orders
"""

QUERY_BY_MONTH_COUNTRY = f"""
WITH {_BASE_CTE}
SELECT
  FORMAT_DATE('%Y-%m', DATE(DATE_CREATED))                                                        AS month,
  site,
  COUNT(ORDER_SHIPPING_NUMBER)                                                                    AS sold_orders,
  COUNTIF(delivery_date IS NOT NULL)                                                              AS delivered_orders,
  COUNTIF(SHP_STATUS_TYPE = 'not_delivered')                                                      AS not_delivered,
  ROUND(SAFE_DIVIDE(COUNTIF(SHP_STATUS_TYPE = 'not_delivered'), COUNT(ORDER_SHIPPING_NUMBER)) * 100, 2) AS nd_pct,
  {_LT_AVG}                                                                                       AS avg_lead_time
FROM all_orders
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_BY_PICKING = f"""
WITH {_BASE_CTE}
SELECT
  FORMAT_DATE('%Y-%m', DATE(DATE_CREATED))                                                        AS month,
  site,
  picking_type,
  COUNT(ORDER_SHIPPING_NUMBER)                                                                    AS total_orders,
  COUNTIF(SHP_STATUS_TYPE = 'not_delivered')                                                      AS not_delivered,
  ROUND(SAFE_DIVIDE(COUNTIF(SHP_STATUS_TYPE = 'not_delivered'), COUNT(ORDER_SHIPPING_NUMBER)) * 100, 2) AS nd_pct,
  {_LT_AVG}                                                                                       AS avg_lead_time
FROM all_orders
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# ── QUERIES — Qualidade & SLA ─────────────────────────────────────────────────

# Gráfico 1 — Top 3 estados por país: volume + lead time médio
QUERY_TOP3_STATES = f"""
WITH {_BASE_CTE},
agg AS (
  SELECT
    site,
    state,
    COUNT(ORDER_SHIPPING_NUMBER)  AS total_orders,
    {_LT_AVG}                     AS avg_lead_time
  FROM all_orders
  GROUP BY 1, 2
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY site ORDER BY total_orders DESC) AS rn
  FROM agg
)
SELECT site, state, total_orders, avg_lead_time, rn
FROM ranked
WHERE rn <= 3
ORDER BY site, rn
"""

# Gráfico 2 — Top 3 estados por país com lead time médio aberto por método de envio
QUERY_TOP3_BY_PICKING = f"""
WITH {_BASE_CTE},
vol AS (
  SELECT site, state, COUNT(ORDER_SHIPPING_NUMBER) AS vol
  FROM all_orders
  GROUP BY 1, 2
),
top3 AS (
  SELECT site, state
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY site ORDER BY vol DESC) AS rn
    FROM vol
  )
  WHERE rn <= 3
)
SELECT
  all_orders.site,
  all_orders.state,
  all_orders.picking_type,
  COUNT(all_orders.ORDER_SHIPPING_NUMBER)  AS total_orders,
  {_LT_AVG_Q}                              AS avg_lead_time
FROM all_orders
INNER JOIN top3
  ON all_orders.site  = top3.site
 AND all_orders.state = top3.state
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# Gráfico 3 — Pareto de motivos de not_delivered
QUERY_PARETO_ND = f"""
WITH {_BASE_CTE},
counts AS (
  SELECT
    site,
    motivo_nd                          AS motivo,
    COUNT(ORDER_SHIPPING_NUMBER)        AS cnt
  FROM all_orders
  WHERE SHP_STATUS_TYPE = 'not_delivered'
  GROUP BY 1, 2
),
totals AS (
  SELECT site, SUM(cnt) AS total FROM counts GROUP BY 1
)
SELECT
  c.site,
  c.motivo,
  c.cnt,
  t.total                                              AS site_total,
  ROUND(SAFE_DIVIDE(c.cnt, t.total) * 100, 2)          AS pct
FROM counts c
JOIN totals t ON c.site = t.site
ORDER BY c.site, c.cnt DESC
"""

# ── HELPERS ───────────────────────────────────────────────────────────────────

def run_query(client: bigquery.Client, sql: str, label: str) -> pd.DataFrame:
    print(f"  [{label}] executando...")
    df = client.query(sql).to_dataframe()
    for col in df.select_dtypes(include=["object"]).columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    print(f"  [{label}] {len(df)} linhas")
    return df


def df_to_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records", force_ascii=False))


# ── HTML TEMPLATE ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Dashboard Iniciativas SDX — {date_from} a {date_to}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {{
  --blue:#2D73F5; --orange:#FF7A00; --green:#00C48C; --red:#FF4D4F;
  --purple:#9747FF; --teal:#00BCD4; --yellow:#FFC107; --lime:#8BC34A;
  --gray:#607D8B; --pink:#E91E63;
  --bg:#F4F6FA; --card:#FFFFFF; --text:#1A1A2E; --muted:#7A8099; --border:#E8EAEE;
}}
*{{ box-sizing:border-box; margin:0; padding:0; }}
body{{ font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--text); }}

.header{{ background:var(--blue); color:#fff; padding:16px 28px; display:flex; align-items:center; gap:12px; flex-wrap:wrap; }}
.header h1{{ font-size:1.2rem; font-weight:700; }}
.header p{{ font-size:.82rem; opacity:.85; margin-top:2px; }}
.badge{{ background:rgba(255,255,255,.2); border-radius:20px; padding:3px 12px; font-size:.73rem; margin-left:auto; white-space:nowrap; }}

.tab-bar{{ position:sticky; top:0; z-index:100; background:#fff;
           border-bottom:2px solid var(--border); padding:0 28px;
           display:flex; gap:0; overflow-x:auto; }}
.tab-btn{{ padding:12px 22px; font-size:.84rem; font-weight:600; color:var(--muted);
           border:none; background:none; cursor:pointer;
           border-bottom:3px solid transparent; margin-bottom:-2px;
           white-space:nowrap; transition:color .15s; }}
.tab-btn:hover{{ color:var(--blue); }}
.tab-btn.active{{ color:var(--blue); border-bottom-color:var(--blue); }}

.content{{ padding:24px 28px; max-width:1600px; margin:0 auto; }}

.kpi-grid{{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr));
            gap:12px; margin-bottom:36px; }}
.kpi{{ background:var(--card); border-radius:10px; padding:18px 20px;
       box-shadow:0 1px 4px rgba(0,0,0,.07); }}
.kpi .lbl{{ font-size:.74rem; color:var(--muted); margin-bottom:6px; }}
.kpi .val{{ font-size:1.85rem; font-weight:700; line-height:1; }}
.kpi .sub{{ font-size:.71rem; color:var(--muted); margin-top:5px; }}
.kpi.c-blue .val{{ color:var(--blue); }}
.kpi.c-green .val{{ color:var(--green); }}
.kpi.c-red .val{{ color:var(--red); }}
.kpi.c-orange .val{{ color:var(--orange); }}

.section{{ margin-bottom:44px; scroll-margin-top:55px; }}
.section-title{{ font-size:.82rem; font-weight:700; color:var(--muted);
                 text-transform:uppercase; letter-spacing:.06em;
                 margin-bottom:14px; padding-bottom:6px;
                 border-bottom:2px solid var(--border);
                 display:flex; align-items:center; gap:8px; }}
.section-num{{ background:var(--blue); color:#fff; border-radius:50%;
               width:22px; height:22px; display:inline-flex; align-items:center;
               justify-content:center; font-size:.72rem; font-weight:700; flex-shrink:0; }}

.chart-grid{{ display:grid; gap:16px; }}
.chart-grid.cols-2{{ grid-template-columns:repeat(auto-fit,minmax(460px,1fr)); }}
.chart-grid.cols-1{{ grid-template-columns:1fr; }}
.chart-card{{ background:var(--card); border-radius:10px; padding:20px 22px;
              box-shadow:0 1px 4px rgba(0,0,0,.07); }}
.chart-card h3{{ font-size:.92rem; font-weight:600; margin-bottom:4px; color:var(--text); }}
.chart-card .subtitle{{ font-size:.74rem; color:var(--muted); margin-bottom:14px; }}
.chart-card canvas{{ max-height:310px; }}
.chart-card canvas.tall{{ max-height:380px; }}

.footer{{ text-align:center; padding:20px; font-size:.71rem; color:var(--muted); margin-top:20px; }}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Dashboard Iniciativas SDX</h1>
    <p>Período: {date_from} a {date_to} &nbsp;·&nbsp; Sites: MLB · MLC · MLM · MLA · MLU</p>
  </div>
  <span class="badge">Gerado em: {updated_at}</span>
</div>

<nav class="tab-bar">
  <button class="tab-btn active" data-tab="tab-geral"
          onclick="showTab('tab-geral')">Visão Geral</button>
  <button class="tab-btn" data-tab="tab-sla"
          onclick="showTab('tab-sla')">Qualidade &amp; SLA — Points</button>
</nav>

<div class="content">

  <!-- KPIs — sempre visíveis -->
  <div id="sec-kpis" style="padding-top:28px;">
    <div class="kpi-grid" id="kpi-grid"></div>
  </div>

  <!-- ══ ABA: Visão Geral ══════════════════════════════════════════════════ -->
  <div id="tab-geral" class="tab-pane">

    <!-- Seção 1 — Volume -->
    <div class="section">
      <div class="section-title"><span class="section-num">1</span>Volume de Pedidos por Mês e País</div>
      <div class="chart-grid cols-2">
        <div class="chart-card">
          <h3>Pedidos Vendidos por Mês</h3>
          <p class="subtitle">COUNT(ORDER_SHIPPING_NUMBER) · por mês e site</p>
          <canvas id="chart-sold" class="tall"></canvas>
        </div>
        <div class="chart-card">
          <h3>Pedidos Entregues por Mês</h3>
          <p class="subtitle">Pedidos com COALESCE(DATE_FIRST_VISIT, DATE_DELIVERED) preenchida</p>
          <canvas id="chart-delivered" class="tall"></canvas>
        </div>
      </div>
    </div>

    <!-- Seção 2 — Not Delivered -->
    <div class="section">
      <div class="section-title"><span class="section-num">2</span>Taxa Not Delivered (%) por Mês e País</div>
      <div class="chart-grid cols-1">
        <div class="chart-card">
          <h3>% Not Delivered por Mês e País</h3>
          <p class="subtitle">SHP_STATUS_TYPE = 'not_delivered' / COUNT(ORDER_SHIPPING_NUMBER)</p>
          <canvas id="chart-nd-pct" class="tall"></canvas>
        </div>
      </div>
    </div>

    <!-- Seção 3 — Lead Time -->
    <div class="section">
      <div class="section-title"><span class="section-num">3</span>Lead Time Médio (dias corridos) por Mês e País</div>
      <div class="chart-grid cols-1">
        <div class="chart-card">
          <h3>Lead Time Médio por Mês e País</h3>
          <p class="subtitle">DATE_DIFF(COALESCE(DATE_FIRST_VISIT, DATE_DELIVERED), DATE_CREATED, DAY)</p>
          <canvas id="chart-lt" class="tall"></canvas>
        </div>
      </div>
    </div>

    <!-- Seção 4 — LT por Método -->
    <div class="section">
      <div class="section-title"><span class="section-num">4</span>Lead Time Médio por Método de Envio por Mês — por País</div>
      <div class="chart-grid cols-2" id="section-lt-pick"></div>
    </div>

    <!-- Seção 5 — ND por Método -->
    <div class="section">
      <div class="section-title"><span class="section-num">5</span>% Not Delivered por Método de Envio por Mês — por País</div>
      <div class="chart-grid cols-2" id="section-nd-pick"></div>
    </div>

  </div><!-- /tab-geral -->

  <!-- ══ ABA: Qualidade & SLA ══════════════════════════════════════════════ -->
  <div id="tab-sla" class="tab-pane" style="display:none">

    <!-- Seção 1 — Top 3 estados: volume + lead time -->
    <div class="section">
      <div class="section-title">
        <span class="section-num">1</span>Top 3 Estados por País — Volume e Lead Time Médio
      </div>
      <div class="chart-grid cols-2" id="sec-sla-1-grid"></div>
    </div>

    <!-- Seção 2 — Top 3 estados: lead time por método de envio -->
    <div class="section">
      <div class="section-title">
        <span class="section-num">2</span>Top 3 Estados por País — Lead Time Médio por Método de Envio
      </div>
      <div class="chart-grid cols-2" id="sec-sla-2-grid"></div>
    </div>

    <!-- Seção 3 — Pareto de motivos ND -->
    <div class="section">
      <div class="section-title">
        <span class="section-num">3</span>Pareto de Motivos de Falha de Entrega (Not Delivered) por País
      </div>
      <div class="chart-grid cols-2" id="sec-sla-3-grid"></div>
    </div>

  </div><!-- /tab-sla -->

</div><!-- /content -->

<div class="footer">
  Fonte: meli-bi-data &nbsp;·&nbsp;
  SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB &amp; TBL_LK_SDX_BASE_ORDERS &nbsp;·&nbsp;
  {date_from} a {date_to}
</div>

<script>
const DATA = {data_json};

// ── Paleta ───────────────────────────────────────────────────────────────────
const PALETTE = [
  "#2D73F5","#FF7A00","#00C48C","#9747FF","#FF4D4F",
  "#607D8B","#FFC107","#8BC34A","#00BCD4","#E91E63"
];
const SITE_COLOR = {{
  MLB:"#2D73F5", MLC:"#FF7A00", MLM:"#00C48C", MLA:"#9747FF", MLU:"#FF4D4F"
}};
function ax(c, a) {{ return c + Math.round(a*255).toString(16).padStart(2,"0"); }}
function colorFor(i) {{ return PALETTE[i % PALETTE.length]; }}
function fmtN(n) {{
  if (n == null) return "—";
  if (n >= 1e6) return (n/1e6).toFixed(1)+"M";
  if (n >= 1e3) return (n/1e3).toFixed(1)+"K";
  return typeof n === "number" ? n.toLocaleString("pt-BR") : String(n);
}}

// ── Sistema de abas ──────────────────────────────────────────────────────────
const INITED = {{}};
function showTab(id) {{
  document.querySelectorAll(".tab-pane").forEach(p => p.style.display = "none");
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.getElementById(id).style.display = "block";
  document.querySelector("[data-tab='" + id + "']").classList.add("active");
  if (!INITED[id]) {{ INITED[id] = true; INIT_FNS[id](); }}
}}

// ── KPIs ─────────────────────────────────────────────────────────────────────
const kpis = DATA.kpis;
document.getElementById("kpi-grid").innerHTML = [
  {{ cls:"c-blue",   lbl:"Pedidos Vendidos",  val:fmtN(kpis.total_sold),          sub:"Todos os países · período completo" }},
  {{ cls:"c-green",  lbl:"Pedidos Entregues", val:fmtN(kpis.total_delivered),     sub:"Com data de entrega registrada" }},
  {{ cls:"c-red",    lbl:"Not Delivered",      val:(kpis.nd_pct||0)+"%",           sub:fmtN(kpis.total_nd)+" pedidos" }},
  {{ cls:"c-orange", lbl:"Lead Time Médio",    val:(kpis.avg_lead_time||"—")+"d", sub:"Dias corridos · todos os países" }},
].map(k =>
  '<div class="kpi ' + k.cls + '">' +
  '<div class="lbl">' + k.lbl + '</div>' +
  '<div class="val">' + k.val + '</div>' +
  '<div class="sub">' + k.sub + '</div></div>'
).join("");

// ── Helpers de gráfico de linhas ─────────────────────────────────────────────
function mkLineChart(id, datasets, yLabel, isPct) {{
  new Chart(document.getElementById(id), {{
    type: "line",
    data: {{ labels: DATA._months, datasets: datasets }},
    options: {{
      responsive: true,
      interaction: {{ mode:"index", intersect:false }},
      plugins: {{
        legend: {{ position:"top", labels:{{ font:{{ size:11 }}, boxWidth:14 }} }},
        tooltip: {{ callbacks: {{ label: ctx =>
          " " + ctx.dataset.label + ": " + (isPct ? ctx.parsed.y+"%" : fmtN(ctx.parsed.y))
        }} }}
      }},
      scales: {{
        x: {{ ticks:{{ maxRotation:45, font:{{ size:10 }} }} }},
        y: {{ beginAtZero:true, title:{{ display:true, text:yLabel, font:{{ size:11 }} }} }}
      }}
    }}
  }});
}}

function siteDatasets(key) {{
  return DATA._sites.map((site, i) => {{
    const vals = DATA._months.map(m => {{
      const r = DATA.by_month_country.find(x => x.month === m && x.site === site);
      return r ? r[key] : null;
    }});
    const c = SITE_COLOR[site] || colorFor(i);
    return {{
      label: site, data: vals,
      borderColor: c, backgroundColor: ax(c, 0.08),
      borderWidth: 2, tension: 0.3, spanGaps: true,
      pointRadius: 3, pointHoverRadius: 5
    }};
  }});
}}

// ── Pré-calcula listas globais ───────────────────────────────────────────────
DATA._months = [...new Set(DATA.by_month_country.map(r => r.month))].sort();
DATA._sites  = [...new Set(DATA.by_month_country.map(r => r.site))].sort();

// ── Init: Visão Geral ────────────────────────────────────────────────────────
function initGeral() {{
  mkLineChart("chart-sold",      siteDatasets("sold_orders"),      "Pedidos",       false);
  mkLineChart("chart-delivered", siteDatasets("delivered_orders"), "Pedidos",       false);
  mkLineChart("chart-nd-pct",    siteDatasets("nd_pct"),           "% ND",          true);
  mkLineChart("chart-lt",        siteDatasets("avg_lead_time"),    "Dias corridos", false);

  const pick   = DATA.by_picking;
  const pMonths = [...new Set(pick.map(r => r.month))].sort();
  const pSites  = [...new Set(pick.map(r => r.site))].sort();

  function buildPickSection(containerId, valueKey, yLabel, isPct) {{
    const container = document.getElementById(containerId);
    pSites.forEach(site => {{
      const sd    = pick.filter(r => r.site === site);
      const types = [...new Set(sd.map(r => r.picking_type))].sort();
      const cid   = "chart-" + containerId + "-" + site;
      const card  = document.createElement("div");
      card.className = "chart-card";
      card.innerHTML =
        "<h3>" + site + "</h3>" +
        '<p class="subtitle">Por método de envio (SHP_PICKING_TYPE_ID) · mês a mês</p>' +
        '<canvas id="' + cid + '" class="tall"></canvas>';
      container.appendChild(card);

      const datasets = types.map((pt, i) => {{
        const vals = pMonths.map(m => {{
          const row = sd.find(x => x.month === m && x.picking_type === pt);
          return row ? row[valueKey] : null;
        }});
        const c = colorFor(i);
        return {{
          label: pt, data: vals,
          borderColor: c, backgroundColor: ax(c, 0.08),
          borderWidth: 2, tension: 0.3, spanGaps: true,
          pointRadius: 3, pointHoverRadius: 5
        }};
      }});

      new Chart(document.getElementById(cid), {{
        type: "line",
        data: {{ labels: pMonths, datasets: datasets }},
        options: {{
          responsive: true,
          interaction: {{ mode:"index", intersect:false }},
          plugins: {{
            legend: {{ position:"top", labels:{{ font:{{ size:10 }}, boxWidth:12 }} }},
            tooltip: {{ callbacks: {{ label: ctx =>
              " " + ctx.dataset.label + ": " + (isPct ? ctx.parsed.y+"%" : fmtN(ctx.parsed.y))
            }} }}
          }},
          scales: {{
            x: {{ ticks:{{ maxRotation:45, font:{{ size:10 }} }} }},
            y: {{ beginAtZero:true, title:{{ display:true, text:yLabel, font:{{ size:11 }} }} }}
          }}
        }}
      }});
    }});
  }}

  buildPickSection("section-lt-pick", "avg_lead_time", "Dias corridos", false);
  buildPickSection("section-nd-pick", "nd_pct",        "% ND",          true);
}}

// ── Init: Qualidade & SLA ────────────────────────────────────────────────────
function initSLA() {{
  const top3     = DATA.top3_states;
  const top3pick = DATA.top3_by_picking;
  const pareto   = DATA.pareto_nd;

  const slaSites = [...new Set(top3.map(r => r.site))].sort();

  // ── Gráfico 1: Top 3 estados — volume (barras) + lead time (linha) ──────
  slaSites.forEach(site => {{
    const sd  = top3.filter(r => r.site === site).sort((a,b) => a.rn - b.rn);
    const labels  = sd.map(r => r.state);
    const volumes = sd.map(r => r.total_orders);
    const leads   = sd.map(r => r.avg_lead_time);
    const c = SITE_COLOR[site] || colorFor(0);
    const cid = "chart-top3-" + site;

    const card = document.createElement("div");
    card.className = "chart-card";
    card.innerHTML =
      "<h3>" + site + "</h3>" +
      '<p class="subtitle">Top 3 estados por volume · lead time médio (eixo direito)</p>' +
      '<canvas id="' + cid + '" class="tall"></canvas>';
    document.getElementById("sec-sla-1-grid").appendChild(card);

    new Chart(document.getElementById(cid), {{
      data: {{
        labels: labels,
        datasets: [
          {{
            type: "bar",
            label: "Pedidos",
            data: volumes,
            backgroundColor: ax(c, 0.70),
            borderColor: c,
            borderWidth: 1,
            yAxisID: "y"
          }},
          {{
            type: "line",
            label: "Lead Time Médio (dias)",
            data: leads,
            borderColor: PALETTE[4],
            backgroundColor: ax(PALETTE[4], 0.10),
            borderWidth: 2,
            pointRadius: 6,
            pointHoverRadius: 8,
            tension: 0,
            yAxisID: "y2"
          }}
        ]
      }},
      options: {{
        responsive: true,
        interaction: {{ mode:"index", intersect:false }},
        plugins: {{
          legend: {{ position:"top", labels:{{ font:{{ size:11 }}, boxWidth:14 }} }},
          tooltip: {{ callbacks: {{
            label: ctx => {{
              if (ctx.datasetIndex === 0) return " " + ctx.dataset.label + ": " + fmtN(ctx.parsed.y);
              return " " + ctx.dataset.label + ": " + ctx.parsed.y + "d";
            }}
          }} }}
        }},
        scales: {{
          y:  {{ beginAtZero:true, title:{{ display:true, text:"Pedidos",        font:{{ size:11 }} }} }},
          y2: {{ position:"right", beginAtZero:true,
                title:{{ display:true, text:"Lead Time (dias)", font:{{ size:11 }} }},
                grid:{{ drawOnChartArea:false }} }}
        }}
      }}
    }});
  }});

  // ── Gráfico 2: Top 3 estados — lead time agrupado por método de envio ────
  const p2Sites = [...new Set(top3pick.map(r => r.site))].sort();
  p2Sites.forEach(site => {{
    const sd = top3pick.filter(r => r.site === site);

    // Ordena os estados pelo volume total desc
    const stateVol = {{}};
    sd.forEach(r => {{ stateVol[r.state] = (stateVol[r.state]||0) + r.total_orders; }});
    const states = [...new Set(sd.map(r => r.state))].sort((a,b) => stateVol[b] - stateVol[a]);
    const picks  = [...new Set(sd.map(r => r.picking_type))].sort();

    const cid  = "chart-top3pick-" + site;
    const card = document.createElement("div");
    card.className = "chart-card";
    card.innerHTML =
      "<h3>" + site + "</h3>" +
      '<p class="subtitle">Lead Time médio por método de envio · top 3 estados em volume</p>' +
      '<canvas id="' + cid + '" class="tall"></canvas>';
    document.getElementById("sec-sla-2-grid").appendChild(card);

    const datasets = picks.map((pt, i) => {{
      const vals = states.map(state => {{
        const row = sd.find(r => r.state === state && r.picking_type === pt);
        return row ? row.avg_lead_time : null;
      }});
      const c = colorFor(i);
      return {{
        label: pt, data: vals,
        backgroundColor: ax(c, 0.72), borderColor: c, borderWidth: 1
      }};
    }});

    new Chart(document.getElementById(cid), {{
      type: "bar",
      data: {{ labels: states, datasets: datasets }},
      options: {{
        responsive: true,
        interaction: {{ mode:"index", intersect:false }},
        plugins: {{
          legend: {{ position:"top", labels:{{ font:{{ size:10 }}, boxWidth:12 }} }},
          tooltip: {{ callbacks: {{
            afterLabel: ctx => {{
              const row = sd.find(r =>
                r.state === states[ctx.dataIndex] && r.picking_type === picks[ctx.datasetIndex]);
              return row ? "   Volume: " + fmtN(row.total_orders) + " pedidos" : "";
            }}
          }} }}
        }},
        scales: {{
          y: {{ beginAtZero:true, title:{{ display:true, text:"Lead Time (dias)", font:{{ size:11 }} }} }}
        }}
      }}
    }});
  }});

  // ── Gráfico 3: Pareto de motivos ND ──────────────────────────────────────
  const p3Sites = [...new Set(pareto.map(r => r.site))].sort();
  p3Sites.forEach(site => {{
    const sd = pareto.filter(r => r.site === site)
                     .sort((a,b) => b.cnt - a.cnt)
                     .slice(0, 15);   // top 15 motivos

    // Acumulado %
    let cum = 0;
    const cumPcts = sd.map(r => {{ cum += r.pct; return Math.round(cum * 100) / 100; }});

    const cid  = "chart-pareto-" + site;
    const card = document.createElement("div");
    card.className = "chart-card";
    card.innerHTML =
      "<h3>" + site + "</h3>" +
      '<p class="subtitle">MOTIVO_NO_ENTREGA_NAME_1 · pedidos not_delivered · % sobre total ND do site</p>' +
      '<canvas id="' + cid + '" class="tall"></canvas>';
    document.getElementById("sec-sla-3-grid").appendChild(card);

    new Chart(document.getElementById(cid), {{
      data: {{
        labels: sd.map(r => r.motivo),
        datasets: [
          {{
            type: "bar",
            label: "% ND",
            data: sd.map(r => r.pct),
            backgroundColor: ax(PALETTE[0], 0.70),
            borderColor: PALETTE[0],
            borderWidth: 1,
            yAxisID: "y"
          }},
          {{
            type: "line",
            label: "Acumulado %",
            data: cumPcts,
            borderColor: PALETTE[4],
            backgroundColor: "transparent",
            borderWidth: 2,
            pointRadius: 3,
            tension: 0.1,
            yAxisID: "y2"
          }}
        ]
      }},
      options: {{
        responsive: true,
        interaction: {{ mode:"index", intersect:false }},
        plugins: {{
          legend: {{ position:"top", labels:{{ font:{{ size:11 }}, boxWidth:14 }} }},
          tooltip: {{ callbacks: {{
            label: ctx => {{
              if (ctx.datasetIndex === 0) {{
                const row = sd[ctx.dataIndex];
                return " % ND: " + ctx.parsed.y + "% (" + fmtN(row.cnt) + " pedidos)";
              }}
              return " Acumulado: " + ctx.parsed.y + "%";
            }}
          }} }}
        }},
        scales: {{
          x: {{ ticks:{{ maxRotation:55, font:{{ size:9 }} }} }},
          y:  {{ beginAtZero:true,
                title:{{ display:true, text:"% do total ND", font:{{ size:11 }} }} }},
          y2: {{ position:"right", min:0, max:100,
                title:{{ display:true, text:"Acumulado %", font:{{ size:11 }} }},
                grid:{{ drawOnChartArea:false }} }}
        }}
      }}
    }});
  }});
}}

// ── Mapa de funções de init por aba ──────────────────────────────────────────
const INIT_FNS = {{ "tab-geral": initGeral, "tab-sla": initSLA }};

// Ativa a aba inicial
showTab("tab-geral");
</script>
</body>
</html>
"""


# ── GERAR HTML ────────────────────────────────────────────────────────────────

def generate_html(kpis: dict, by_month_country: list, by_picking: list,
                  top3_states: list, top3_by_picking: list, pareto_nd: list,
                  output: str):
    payload = {
        "kpis":             kpis,
        "by_month_country": by_month_country,
        "by_picking":       by_picking,
        "top3_states":      top3_states,
        "top3_by_picking":  top3_by_picking,
        "pareto_nd":        pareto_nd,
    }
    data_json = json.dumps(payload, ensure_ascii=False, default=str)
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    html = HTML_TEMPLATE.format(
        date_from=DATE_FROM,
        date_to=DATE_TO,
        updated_at=now,
        data_json=data_json,
    )
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nDashboard gerado: {output}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Conectando ao BigQuery ({PROJECT})...")
    client = bigquery.Client(project=PROJECT)

    print("\nExecutando queries...")
    df_kpis        = run_query(client, QUERY_KPIS,             "kpis")
    df_monthly     = run_query(client, QUERY_BY_MONTH_COUNTRY, "monthly")
    df_picking     = run_query(client, QUERY_BY_PICKING,       "picking")
    df_top3        = run_query(client, QUERY_TOP3_STATES,      "top3-states")
    df_top3pick    = run_query(client, QUERY_TOP3_BY_PICKING,  "top3-picking")
    df_pareto      = run_query(client, QUERY_PARETO_ND,        "pareto-nd")

    kpis = df_kpis.iloc[0].to_dict() if len(df_kpis) > 0 else {}

    print("\nMontando HTML...")
    generate_html(
        kpis=kpis,
        by_month_country=df_to_records(df_monthly),
        by_picking=df_to_records(df_picking),
        top3_states=df_to_records(df_top3),
        top3_by_picking=df_to_records(df_top3pick),
        pareto_nd=df_to_records(df_pareto),
        output=OUTPUT,
    )

    print(f"\nAbra o arquivo '{OUTPUT}' no navegador para visualizar.")
    print("Para publicar no GitHub Pages:")
    print(f"  git add {OUTPUT} && git commit -m 'update sdx dashboard' && git push")


if __name__ == "__main__":
    main()
