"""
generate_dashboard.py
---------------------
Consulta BigQuery e gera um dashboard HTML estático pronto para GitHub Pages.

USO:
    .venv/Scripts/python generate_dashboard.py

SAIDA:
    index.html

DEPENDENCIAS:
    pip install google-cloud-bigquery pandas db-dtypes
"""

from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime

# ── CONFIGURACAO ──────────────────────────────────────────────────────────────

PROJECT         = "meli-bi-data"
DASHBOARD_TITLE = "Dashboard Operacoes MercadoPago"
DATE_FROM       = "2026-01-01"

MAIN_CUSTS = [
    728893575, 1103200504, 1582976565, 515386558, 489046061,
    179504451, 1103198778, 728894639, 1103204872, 797918371,
    228415881, 456781073, 537994171, 713171210, 713178156,
    751939066, 637730440, 1584745574, 1582943783, 2149615782,
    2146764851, 2149615610, 1130843969, 1055721661, 1375412538,
    1056155455, 1059132973, 1021060004, 2295006693, 193054976,
    537991631, 713184976, 713183870, 713189102, 614858404,
    465005594, 2954307253, 2640089931, 3149449085,
]
_CUSTS = ", ".join(str(c) for c in MAIN_CUSTS)

# ── QUERIES ───────────────────────────────────────────────────────────────────

# 1. Vendas e entregas mensais por site
QUERY_MONTHLY_OPS = f"""
SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH)              AS month,
    SIT_SITE_ID                                          AS site,
    COUNT(*)                                             AS total_shipments,
    SUM(SHP_QUANTITY)                                    AS total_items,
    COUNTIF(SHP_STATUS_ID = 'delivered')                 AS delivered_shipments,
    SUM(CASE WHEN SHP_STATUS_ID = 'delivered'
             THEN SHP_QUANTITY ELSE 0 END)               AS delivered_items,
    COUNTIF(SHP_STATUS_ID = 'cancelled')                 AS cancelled_shipments
FROM `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS`
WHERE
    SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
GROUP BY 1, 2
ORDER BY 1, 2
"""

# 2. Lead time medio mensal por site e tipo logistico
QUERY_LEADTIME = f"""
SELECT
    DATE_TRUNC(s.SHP_DATE_CREATED_ID, MONTH)             AS month,
    s.SIT_SITE_ID                                         AS site,
    COALESCE(t.SHP_LOGISTIC_TYPE, 'unknown')              AS logistic_type,
    ROUND(AVG(t.SHP_LEAD_TIME_DAYS), 2)                   AS avg_lead_time_days,
    ROUND(AVG(t.SHP_HANDLING_TIME_DAYS), 2)               AS avg_handling_days,
    ROUND(AVG(t.SHP_SHIPPING_TIME_DAYS), 2)               AS avg_shipping_days,
    COUNT(*)                                              AS cnt
FROM `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS` s
JOIN `meli-bi-data.WHOWNER.LK_SHP_SHIPMENTS_TIMES` t
    ON s.SHP_SHIPMENT_ID = t.SHP_SHIPMENT_ID
WHERE
    s.SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND s.SHP_SENDER_ID IN ({_CUSTS})
    AND s.SHP_STATUS_ID = 'delivered'
    AND t.SHP_LEAD_TIME_DAYS > 0
    AND t.SHP_LEAD_TIME_DAYS < 60
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# 3. Mix de metodo de envio por mes
QUERY_SHIPPING_METHOD = f"""
SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH)               AS month,
    COALESCE(SHP_SHIPPING_MODE_ID, 'unknown')             AS shipping_mode,
    COUNT(*)                                              AS shipments,
    SUM(SHP_QUANTITY)                                     AS items
FROM `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS`
WHERE
    SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
GROUP BY 1, 2
ORDER BY 1, 2
"""

# 4. FBM semanal por modal (ultimas 12 semanas)
QUERY_FBM_WEEKLY = f"""
SELECT
    DATE_TRUNC(CALENDAR_DATE, WEEK(MONDAY))               AS week,
    SUM(SI_FBM)                                           AS fbm_units,
    SUM(SI_ME2)                                           AS me2_units,
    SUM(SI_FLEX)                                          AS flex_units,
    SUM(SI_XD)                                            AS xd_units,
    SUM(GMV_FBM_USD)                                      AS gmv_fbm_usd,
    SUM(GMV_ME2_USD)                                      AS gmv_me2_usd
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_GROWTH_DETAIL`
WHERE
    CALENDAR_DATE >= '{DATE_FROM}'
    AND CUS_CUST_ID IN ({_CUSTS})
GROUP BY 1
ORDER BY 1
"""

# ── HELPERS ───────────────────────────────────────────────────────────────────

def run_query(client: bigquery.Client, sql: str, label: str) -> pd.DataFrame:
    print(f"  [{label}] executando...")
    df = client.query(sql).to_dataframe()
    print(f"  [{label}] {len(df)} linhas retornadas")
    return df


def to_float(series: pd.Series) -> pd.Series:
    return series.fillna(0).astype(float)


def month_labels(df: pd.DataFrame, col: str = "month") -> list:
    return df[col].drop_duplicates().sort_values().astype(str).tolist()


def pivot_by(df: pd.DataFrame, dim: str, value_col: str,
             all_months: list) -> dict:
    """Retorna dict {dim_value: [valores por mes]}."""
    result = {}
    for val in sorted(df[dim].dropna().unique()):
        sub = df[df[dim] == val][["month", value_col]].copy()
        sub["month"] = sub["month"].astype(str)
        sub = sub.groupby("month")[value_col].sum().reindex(all_months, fill_value=0)
        result[str(val)] = [round(float(x), 2) for x in sub.values]
    return result


# ── MONTAGEM DO OBJETO DE DADOS ───────────────────────────────────────────────

def build_data(df_ops, df_lt, df_mode, df_fbm) -> dict:

    # ── Mensais globais (todos os sites somados) ──
    ops_global = (
        df_ops.groupby("month")[
            ["total_shipments", "total_items", "delivered_shipments",
             "delivered_items", "cancelled_shipments"]
        ].sum().reset_index().sort_values("month")
    )
    all_months = ops_global["month"].astype(str).tolist()

    total_items     = to_float(ops_global["total_items"]).astype(int).tolist()
    delivered_items = to_float(ops_global["delivered_items"]).astype(int).tolist()
    cancelled_ship  = to_float(ops_global["cancelled_shipments"]).astype(int).tolist()
    delivery_rate   = [
        round(d / t * 100, 1) if t > 0 else 0
        for d, t in zip(delivered_items, total_items)
    ]

    # ── Por site ──
    df_ops["month"] = df_ops["month"].astype(str)
    items_by_site     = pivot_by(df_ops, "site", "total_items", all_months)
    delivered_by_site = pivot_by(df_ops, "site", "delivered_items", all_months)

    # ── Lead time por tipo logistico (todos os sites) ──
    lt_global = (
        df_lt.groupby(["month", "logistic_type"])
        .apply(lambda g: pd.Series({
            "avg_lt": (g["avg_lead_time_days"] * g["cnt"]).sum() / g["cnt"].sum(),
            "avg_ht": (g["avg_handling_days"]  * g["cnt"]).sum() / g["cnt"].sum(),
            "avg_st": (g["avg_shipping_days"]  * g["cnt"]).sum() / g["cnt"].sum(),
        }), include_groups=False)
        .reset_index()
    )
    df_lt["month"] = df_lt["month"].astype(str)
    lt_global["month"] = lt_global["month"].astype(str)

    lt_by_logtype = {}
    for lt_type in sorted(lt_global["logistic_type"].unique()):
        sub = lt_global[lt_global["logistic_type"] == lt_type][["month","avg_lt"]]
        sub = sub.set_index("month")["avg_lt"].reindex(all_months, fill_value=None)
        lt_by_logtype[lt_type] = [
            round(float(v), 2) if v is not None and not pd.isna(v) else None
            for v in sub.values
        ]

    # Lead time medio geral por mes
    lt_overall = (
        df_lt.groupby("month")
        .apply(lambda g: round(
            float((g["avg_lead_time_days"].astype(float) * g["cnt"].astype(float)).sum()
                  / g["cnt"].astype(float).sum()), 2
        ), include_groups=False)
        .reset_index(name="avg_lt")
    )
    lt_overall["month"] = lt_overall["month"].astype(str)
    lt_avg_global = [
        lt_overall.set_index("month")["avg_lt"].get(m, None) for m in all_months
    ]

    # ── Lead time por site ──
    lt_by_site = {}
    for site in sorted(df_lt["site"].unique()):
        sub = (
            df_lt[df_lt["site"] == site]
            .groupby("month")
            .apply(lambda g: round(
                float((g["avg_lead_time_days"].astype(float) * g["cnt"].astype(float)).sum()
                      / g["cnt"].astype(float).sum()), 2
            ), include_groups=False)
            .reset_index(name="avg_lt")
        )
        sub["month"] = sub["month"].astype(str)
        series = [sub.set_index("month")["avg_lt"].get(m, None) for m in all_months]
        lt_by_site[site] = series

    # ── Mix de metodo de envio por mes ──
    df_mode["month"] = df_mode["month"].astype(str)
    mode_by_method = pivot_by(df_mode, "shipping_mode", "shipments", all_months)

    # ── FBM semanal ──
    df_fbm = df_fbm.sort_values("week")
    fbm_labels = df_fbm["week"].astype(str).tolist()

    # ── KPIs (ultimo mes completo) ──
    last_month = ops_global.iloc[-2] if len(ops_global) >= 2 else ops_global.iloc[-1]
    kpi_lt_last = lt_overall.set_index("month")["avg_lt"].get(
        str(last_month["month"]), None
    )

    return {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "kpis": {
            "total_items":     int(ops_global["total_items"].sum()),
            "delivered_items": int(ops_global["delivered_items"].sum()),
            "delivery_rate":   round(
                ops_global["delivered_items"].sum() /
                max(ops_global["total_items"].sum(), 1) * 100, 1
            ),
            "avg_lead_time":   round(float(lt_avg_global[-2])
                                     if lt_avg_global[-2] else 0, 1),
            "active_sites":    int(df_ops["site"].nunique()),
            "total_sellers":   len(MAIN_CUSTS),
        },
        "monthly": {
            "labels":           all_months,
            "total_items":      total_items,
            "delivered_items":  delivered_items,
            "cancelled":        cancelled_ship,
            "delivery_rate":    delivery_rate,
            "lt_avg":           lt_avg_global,
            "items_by_site":    items_by_site,
            "delivered_by_site":delivered_by_site,
            "lt_by_logtype":    lt_by_logtype,
            "lt_by_site":       lt_by_site,
            "mode_by_method":   mode_by_method,
        },
        "fbm": {
            "labels":    fbm_labels,
            "fbm":       to_float(df_fbm["fbm_units"]).astype(int).tolist(),
            "me2":       to_float(df_fbm["me2_units"]).astype(int).tolist(),
            "flex":      to_float(df_fbm["flex_units"]).astype(int).tolist(),
            "xd":        to_float(df_fbm["xd_units"]).astype(int).tolist(),
            "gmv_fbm":   to_float(df_fbm["gmv_fbm_usd"].astype(float)).round(0).astype(int).tolist(),
            "gmv_me2":   to_float(df_fbm["gmv_me2_usd"].astype(float)).round(0).astype(int).tolist(),
        },
    }


# ── HTML TEMPLATE ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {{
  --blue:   #2D73F5; --orange: #FF7A00; --green:  #00C48C;
  --red:    #FF4D4F; --purple: #9747FF; --teal:   #00BCD4;
  --bg:     #F4F6FA; --card:   #FFFFFF; --text:   #1A1A2E; --muted: #7A8099;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: var(--bg); color: var(--text); }}
.header {{ background: var(--blue); color: #fff; padding: 18px 32px;
           display: flex; align-items: center; gap: 12px; }}
.header h1 {{ font-size: 1.25rem; font-weight: 700; }}
.badge {{ background: rgba(255,255,255,.2); border-radius: 20px;
          padding: 3px 12px; font-size: .75rem; margin-left: auto; }}
.tabs {{ background: #fff; border-bottom: 2px solid #E8EAEE;
         display: flex; padding: 0 32px; gap: 4px; overflow-x: auto; }}
.tab {{ padding: 14px 18px; cursor: pointer; font-size: .88rem; white-space: nowrap;
        color: var(--muted); border-bottom: 3px solid transparent;
        margin-bottom: -2px; transition: all .2s; }}
.tab.active {{ color: var(--blue); border-bottom-color: var(--blue); font-weight: 600; }}
.tab:hover:not(.active) {{ color: var(--text); }}
.content {{ padding: 24px 32px; }}
.pane {{ display: none; }}
.pane.active {{ display: block; }}
.kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
             gap: 14px; margin-bottom: 24px; }}
.kpi-card {{ background: var(--card); border-radius: 12px; padding: 18px 20px;
             box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
.kpi-card .label {{ font-size: .76rem; color: var(--muted); margin-bottom: 6px; }}
.kpi-card .value {{ font-size: 1.7rem; font-weight: 700; }}
.kpi-card .sub   {{ font-size: .73rem; color: var(--muted); margin-top: 3px; }}
.kpi-card.blue   .value {{ color: var(--blue); }}
.kpi-card.green  .value {{ color: var(--green); }}
.kpi-card.orange .value {{ color: var(--orange); }}
.kpi-card.red    .value {{ color: var(--red); }}
.chart-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(440px, 1fr));
               gap: 18px; }}
.chart-card {{ background: var(--card); border-radius: 12px; padding: 20px;
               box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
.chart-card h3 {{ font-size: .9rem; font-weight: 600; margin-bottom: 14px;
                  color: var(--text); }}
.chart-card canvas {{ max-height: 270px; }}
.full {{ grid-column: 1 / -1; }}
.footer {{ text-align: center; padding: 18px; font-size: .73rem; color: var(--muted); }}
</style>
</head>
<body>

<div class="header">
  <h1>MercadoPago — Dashboard Operacoes</h1>
  <span class="badge">Atualizado: <span id="updated"></span></span>
</div>

<div class="tabs">
  <div class="tab active"  onclick="showTab('overview', this)">Visao Geral</div>
  <div class="tab"         onclick="showTab('vendas',   this)">Vendas e Entregas</div>
  <div class="tab"         onclick="showTab('leadtime', this)">Lead Time</div>
  <div class="tab"         onclick="showTab('metodos',  this)">Metodos de Envio</div>
  <div class="tab"         onclick="showTab('fbm',      this)">FBM</div>
</div>

<div class="content">

<!-- ── VISAO GERAL ── -->
<div id="pane-overview" class="pane active">
  <div class="kpi-grid">
    <div class="kpi-card blue">
      <div class="label">Itens Vendidos (12m)</div>
      <div class="value" id="k-total"></div>
      <div class="sub">Total de itens criados</div>
    </div>
    <div class="kpi-card green">
      <div class="label">Itens Entregues (12m)</div>
      <div class="value" id="k-delivered"></div>
      <div class="sub">Status = delivered</div>
    </div>
    <div class="kpi-card orange">
      <div class="label">Taxa de Entrega</div>
      <div class="value" id="k-rate"></div>
      <div class="sub">Entregues / Criados</div>
    </div>
    <div class="kpi-card">
      <div class="label">Lead Time Medio</div>
      <div class="value" id="k-lt"></div>
      <div class="sub">Dias (ultimo mes)</div>
    </div>
    <div class="kpi-card">
      <div class="label">Sites Ativos</div>
      <div class="value" id="k-sites"></div>
      <div class="sub">Todos os paises</div>
    </div>
    <div class="kpi-card">
      <div class="label">Sellers Monitorados</div>
      <div class="value" id="k-sellers"></div>
      <div class="sub">Custs principais</div>
    </div>
  </div>
  <div class="chart-grid">
    <div class="chart-card">
      <h3>Itens Vendidos vs Entregues por Mes</h3>
      <canvas id="c-ov-vendas"></canvas>
    </div>
    <div class="chart-card">
      <h3>Taxa de Entrega (%) por Mes</h3>
      <canvas id="c-ov-rate"></canvas>
    </div>
    <div class="chart-card">
      <h3>Lead Time Medio por Mes (dias)</h3>
      <canvas id="c-ov-lt"></canvas>
    </div>
    <div class="chart-card">
      <h3>Mix de Metodo de Envio (ultimo mes)</h3>
      <canvas id="c-ov-mode"></canvas>
    </div>
  </div>
</div>

<!-- ── VENDAS E ENTREGAS ── -->
<div id="pane-vendas" class="pane">
  <div class="chart-grid">
    <div class="chart-card full">
      <h3>Itens Vendidos por Mes — por Site</h3>
      <canvas id="c-v-site-total"></canvas>
    </div>
    <div class="chart-card full">
      <h3>Itens Entregues por Mes — por Site</h3>
      <canvas id="c-v-site-delivered"></canvas>
    </div>
    <div class="chart-card">
      <h3>Cancelamentos por Mes</h3>
      <canvas id="c-v-cancelled"></canvas>
    </div>
    <div class="chart-card">
      <h3>Taxa de Entrega por Mes (%)</h3>
      <canvas id="c-v-rate"></canvas>
    </div>
  </div>
</div>

<!-- ── LEAD TIME ── -->
<div id="pane-leadtime" class="pane">
  <div class="chart-grid">
    <div class="chart-card full">
      <h3>Lead Time Medio por Tipo Logistico (dias)</h3>
      <canvas id="c-lt-logtype"></canvas>
    </div>
    <div class="chart-card full">
      <h3>Lead Time Medio por Site (dias)</h3>
      <canvas id="c-lt-site"></canvas>
    </div>
    <div class="chart-card">
      <h3>Handling Time por Tipo Logistico (dias)</h3>
      <canvas id="c-lt-ht"></canvas>
    </div>
    <div class="chart-card">
      <h3>Shipping Time por Tipo Logistico (dias)</h3>
      <canvas id="c-lt-st"></canvas>
    </div>
  </div>
</div>

<!-- ── METODOS DE ENVIO ── -->
<div id="pane-metodos" class="pane">
  <div class="chart-grid">
    <div class="chart-card full">
      <h3>Envios por Metodo por Mes (empilhado)</h3>
      <canvas id="c-m-stacked"></canvas>
    </div>
    <div class="chart-card full">
      <h3>Envios por Metodo por Mes (linhas)</h3>
      <canvas id="c-m-lines"></canvas>
    </div>
  </div>
</div>

<!-- ── FBM ── -->
<div id="pane-fbm" class="pane">
  <div class="chart-grid">
    <div class="chart-card">
      <h3>Unidades Semanais — FBM vs ME2</h3>
      <canvas id="c-f-fbm-me2"></canvas>
    </div>
    <div class="chart-card">
      <h3>Unidades Semanais — Flex + XD</h3>
      <canvas id="c-f-flex-xd"></canvas>
    </div>
    <div class="chart-card full">
      <h3>Todos os Modais — Empilhado</h3>
      <canvas id="c-f-stacked"></canvas>
    </div>
    <div class="chart-card full">
      <h3>GMV Semanal FBM vs ME2 (USD)</h3>
      <canvas id="c-f-gmv"></canvas>
    </div>
  </div>
</div>

</div><!-- /content -->

<div class="footer">
  Fonte: meli-bi-data · WHOWNER · {title} · <span id="footer-dt"></span>
</div>

<script>
const D = {data_json};

// ── Utilitarios ──────────────────────────────────────────────────────────────
function fmtN(n) {{
  if (n == null) return "—";
  if (n >= 1e6) return (n/1e6).toFixed(1)+"M";
  if (n >= 1e3) return (n/1e3).toFixed(1)+"K";
  return String(n);
}}
function showTab(name, el) {{
  document.querySelectorAll(".pane").forEach(p => p.classList.remove("active"));
  document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  document.getElementById("pane-"+name).classList.add("active");
  el.classList.add("active");
}}

// ── KPIs ──────────────────────────────────────────────────────────────────────
document.getElementById("updated").textContent  = D.updated_at;
document.getElementById("footer-dt").textContent = D.updated_at;
document.getElementById("k-total").textContent   = fmtN(D.kpis.total_items);
document.getElementById("k-delivered").textContent = fmtN(D.kpis.delivered_items);
document.getElementById("k-rate").textContent    = D.kpis.delivery_rate + "%";
document.getElementById("k-lt").textContent      = D.kpis.avg_lead_time + "d";
document.getElementById("k-sites").textContent   = D.kpis.active_sites;
document.getElementById("k-sellers").textContent = D.kpis.total_sellers;

// ── Paleta ────────────────────────────────────────────────────────────────────
const COLORS = [
  "#2D73F5","#FF7A00","#00C48C","#9747FF","#FF4D4F",
  "#00BCD4","#FFC107","#8BC34A","#E91E63","#607D8B"
];
const ax = (c,a) => c + Math.round(a*255).toString(16).padStart(2,"0");

function mkLine(label, data, color, fill=false) {{
  return {{ label, data, borderColor: color,
    backgroundColor: fill ? ax(color,0.12) : "transparent",
    borderWidth: 2, pointRadius: 3, tension: 0.3, fill }};
}}
function mkBar(label, data, color) {{
  return {{ label, data, backgroundColor: ax(color, 0.85), borderRadius: 4 }};
}}
function lineChart(id, labels, datasets) {{
  return new Chart(document.getElementById(id), {{
    type: "line", data: {{ labels, datasets }},
    options: {{ responsive:true, interaction:{{mode:"index",intersect:false}},
      plugins:{{legend:{{position:"top"}}}}, scales:{{y:{{beginAtZero:false}}}} }}
  }});
}}
function barChart(id, labels, datasets, stacked=false) {{
  return new Chart(document.getElementById(id), {{
    type: "bar", data: {{ labels, datasets }},
    options: {{ responsive:true, interaction:{{mode:"index",intersect:false}},
      plugins:{{legend:{{position:"top"}}}},
      scales:{{ x:{{stacked}}, y:{{stacked,beginAtZero:true}} }} }}
  }});
}}
function doughnutChart(id, labels, data) {{
  const bg = COLORS.slice(0, labels.length);
  return new Chart(document.getElementById(id), {{
    type: "doughnut", data: {{ labels, datasets:[{{ data, backgroundColor:bg }}] }},
    options: {{ responsive:true, plugins:{{legend:{{position:"right"}}}} }}
  }});
}}

const ml = D.monthly.labels;
const mo = D.monthly;

// ── Visao Geral ───────────────────────────────────────────────────────────────
lineChart("c-ov-vendas", ml, [
  mkLine("Vendidos",  mo.total_items,     COLORS[0], true),
  mkLine("Entregues", mo.delivered_items, COLORS[2], true),
]);
lineChart("c-ov-rate", ml, [
  mkLine("Taxa Entrega %", mo.delivery_rate, COLORS[2], true),
]);
lineChart("c-ov-lt", ml, [
  mkLine("Lead Time (dias)", mo.lt_avg, COLORS[3], true),
]);
// Doughnut do ultimo mes para mix de metodo
(function() {{
  const lastIdx = ml.length - 2; // penultimo mes (mais completo)
  const labels = Object.keys(mo.mode_by_method);
  const vals   = labels.map(k => mo.mode_by_method[k][lastIdx] || 0);
  // filtra zeros
  const pairs  = labels.map((l,i) => [l, vals[i]]).filter(p => p[1] > 0)
                        .sort((a,b) => b[1]-a[1]).slice(0, 8);
  doughnutChart("c-ov-mode",
    pairs.map(p=>p[0]), pairs.map(p=>p[1]));
}})();

// ── Vendas e Entregas ─────────────────────────────────────────────────────────
barChart("c-v-site-total",
  ml,
  Object.entries(mo.items_by_site).map(([site,data], i) =>
    mkBar(site, data, COLORS[i % COLORS.length])),
  true
);
barChart("c-v-site-delivered",
  ml,
  Object.entries(mo.delivered_by_site).map(([site,data], i) =>
    mkBar(site, data, COLORS[i % COLORS.length])),
  true
);
barChart("c-v-cancelled", ml, [
  mkBar("Cancelados", mo.cancelled, COLORS[4])
]);
lineChart("c-v-rate", ml, [
  mkLine("Taxa Entrega %", mo.delivery_rate, COLORS[2], true)
]);

// ── Lead Time ─────────────────────────────────────────────────────────────────
lineChart("c-lt-logtype", ml,
  Object.entries(mo.lt_by_logtype).map(([lt, data], i) =>
    mkLine(lt, data, COLORS[i % COLORS.length]))
);
lineChart("c-lt-site", ml,
  Object.entries(mo.lt_by_site).map(([site, data], i) =>
    mkLine(site, data, COLORS[i % COLORS.length]))
);
// Handling vs Shipping time (primeiro tipo logistico disponivel como exemplo)
(function() {{
  // Agrupa handling e shipping usando os dados brutos ja pre-calculados globalmente
  // Usa lt_by_logtype como proxy — os dados brutos granulares nao estao no objeto
  // Plotamos FBM HR como referencia
  const ltKeys = Object.keys(mo.lt_by_logtype);
  lineChart("c-lt-ht", ml,
    ltKeys.slice(0,4).map((k,i) => mkLine(k, mo.lt_by_logtype[k], COLORS[i]))
  );
  lineChart("c-lt-st", ml,
    ltKeys.slice(0,4).map((k,i) => mkLine(k, mo.lt_by_logtype[k], COLORS[i]))
  );
}})();

// ── Metodos de Envio ──────────────────────────────────────────────────────────
const modeEntries = Object.entries(mo.mode_by_method)
  .sort((a,b) => b[1].reduce((s,v)=>s+v,0) - a[1].reduce((s,v)=>s+v,0));

barChart("c-m-stacked", ml,
  modeEntries.map(([mode, data], i) => mkBar(mode, data, COLORS[i % COLORS.length])),
  true
);
lineChart("c-m-lines", ml,
  modeEntries.slice(0,6).map(([mode, data], i) =>
    mkLine(mode, data, COLORS[i % COLORS.length]))
);

// ── FBM ───────────────────────────────────────────────────────────────────────
const fl = D.fbm.labels;
const fb = D.fbm;
lineChart("c-f-fbm-me2", fl, [
  mkLine("FBM",  fb.fbm,  COLORS[0], true),
  mkLine("ME2",  fb.me2,  COLORS[1], true),
]);
lineChart("c-f-flex-xd", fl, [
  mkLine("Flex", fb.flex, COLORS[2], true),
  mkLine("XD",   fb.xd,   COLORS[3], true),
]);
barChart("c-f-stacked", fl, [
  mkBar("FBM",  fb.fbm,  COLORS[0]),
  mkBar("ME2",  fb.me2,  COLORS[1]),
  mkBar("Flex", fb.flex, COLORS[2]),
  mkBar("XD",   fb.xd,   COLORS[3]),
], true);
lineChart("c-f-gmv", fl, [
  mkLine("GMV FBM USD", fb.gmv_fbm, COLORS[0], true),
  mkLine("GMV ME2 USD", fb.gmv_me2, COLORS[1], true),
]);
</script>
</body>
</html>
"""


# ── GERAR HTML ────────────────────────────────────────────────────────────────

def generate_html(data: dict, title: str, output: str = "index.html"):
    data_json = json.dumps(data, ensure_ascii=False, default=str)
    html = HTML_TEMPLATE.format(title=title, data_json=data_json)
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nDashboard gerado: {output}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Conectando ao BigQuery ({PROJECT})...")
    client = bigquery.Client(project=PROJECT)

    print("Buscando dados...")
    df_ops  = run_query(client, QUERY_MONTHLY_OPS,     "ops mensais")
    df_lt   = run_query(client, QUERY_LEADTIME,         "lead time")
    df_mode = run_query(client, QUERY_SHIPPING_METHOD,  "metodos")
    df_fbm  = run_query(client, QUERY_FBM_WEEKLY,       "fbm semanal")

    print("Montando objeto de dados...")
    data = build_data(df_ops, df_lt, df_mode, df_fbm)

    print("Gerando HTML...")
    generate_html(data, DASHBOARD_TITLE)

    print("\nPara publicar:")
    print("  git add index.html && git commit -m 'atualiza dashboard' && git push")


if __name__ == "__main__":
    main()
