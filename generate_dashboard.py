"""
generate_dashboard.py
---------------------
Consulta BigQuery e gera um dashboard HTML estatico pronto para GitHub Pages.

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
_CUSTS              = ", ".join(str(c) for c in MAIN_CUSTS)
_SITES_GERAL        = "'MLA', 'MLC', 'MLM', 'MLU'"   # Points & Others
_SITES_GERAL_CARDS  = "'MLA', 'MLC', 'MLU'"           # Cards (MLM sem dados)

# ── QUERIES — ABA 1: Points & Others ─────────────────────────────────────────

QUERY_POINTS_SALES = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH)                       AS month,
    SIT_SITE_ID,
    FLAG_DELIVERED                                            AS delivered_flag,
    COALESCE(Q_DEVICES, 1)                                    AS items
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
  UNION ALL
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH),
    SIT_SITE_ID,
    FLAG_DELIVERED,
    COALESCE(Q_DEVICES, 1)
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL})
)
SELECT
  month,
  SIT_SITE_ID                                                 AS site,
  COUNT(*)                                                    AS total_orders,
  SUM(items)                                                  AS total_items,
  COUNTIF(delivered_flag = 1)                                 AS delivered_orders,
  SUM(CASE WHEN delivered_flag = 1 THEN items ELSE 0 END)     AS delivered_items
FROM base
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_POINTS_LT = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH)                       AS month,
    SIT_SITE_ID,
    COALESCE(NULLIF(SHP_PICKING_TYPE_ID, ''), 'unknown')      AS picking_type,
    LEAD_TIME_DIAS_HABILES                                    AS lead_time_days
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND FLAG_DELIVERED = 1
    AND LEAD_TIME_DIAS_HABILES > 0
    AND LEAD_TIME_DIAS_HABILES < 30
  UNION ALL
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH),
    SIT_SITE_ID,
    COALESCE(NULLIF(SHP_PICKING_TYPE_ID, ''), 'unknown'),
    LEAD_TIME_DIAS_HABILES
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL})
    AND FLAG_DELIVERED = 1
    AND LEAD_TIME_DIAS_HABILES > 0
    AND LEAD_TIME_DIAS_HABILES < 30
)
SELECT
  month,
  SIT_SITE_ID                                                 AS site,
  picking_type,
  ROUND(AVG(lead_time_days), 2)                               AS avg_lead_time,
  COUNT(*)                                                    AS cnt
FROM base
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# ── QUERIES — ABA 2: Cards ────────────────────────────────────────────────────

QUERY_CARDS_SALES = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH)                    AS month,
    SIT_SITE_ID,
    IF(SHP_STATUS_ID = 'delivered', 1, 0)                     AS delivered_flag,
    COALESCE(SHP_QUANTITY, 1)                                 AS items
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID_MLB`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
  UNION ALL
  SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH),
    SIT_SITE_ID,
    IF(SHP_STATUS_ID = 'delivered', 1, 0),
    COALESCE(SHP_QUANTITY, 1)
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL_CARDS})
)
SELECT
  month,
  SIT_SITE_ID                                                 AS site,
  COUNT(*)                                                    AS total_orders,
  SUM(items)                                                  AS total_items,
  COUNTIF(delivered_flag = 1)                                 AS delivered_orders,
  SUM(CASE WHEN delivered_flag = 1 THEN items ELSE 0 END)     AS delivered_items
FROM base
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_CARDS_LT = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH)                              AS month,
    SIT_SITE_ID,
    COALESCE(NULLIF(OP_LOGISTICO, ''), 'unknown')                       AS picking_type,
    DATE_DIFF(DATE(SHP_DATETIME_DELIVERED_ID), SHP_DATE_CREATED_ID, DAY) AS lead_time_days
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID_MLB`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SHP_STATUS_ID = 'delivered'
    AND SHP_DATETIME_DELIVERED_ID IS NOT NULL
    AND DATE_DIFF(DATE(SHP_DATETIME_DELIVERED_ID), SHP_DATE_CREATED_ID, DAY) > 0
    AND DATE_DIFF(DATE(SHP_DATETIME_DELIVERED_ID), SHP_DATE_CREATED_ID, DAY) < 60

  UNION ALL

  SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH),
    SIT_SITE_ID,
    COALESCE(NULLIF(OP_LOGISTICO, ''), 'unknown'),
    DATE_DIFF(DATE(SHP_DATETIME_DELIVERED_ID), SHP_DATE_CREATED_ID, DAY)
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL_CARDS})
    AND SHP_STATUS_ID = 'delivered'
    AND SHP_DATETIME_DELIVERED_ID IS NOT NULL
    AND DATE_DIFF(DATE(SHP_DATETIME_DELIVERED_ID), SHP_DATE_CREATED_ID, DAY) > 0
    AND DATE_DIFF(DATE(SHP_DATETIME_DELIVERED_ID), SHP_DATE_CREATED_ID, DAY) < 60
)
SELECT
  month,
  SIT_SITE_ID                                                           AS site,
  picking_type,
  ROUND(AVG(lead_time_days), 2)                                         AS avg_lead_time,
  COUNT(*)                                                              AS cnt
FROM base
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
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


def wavg(df, val_col, weight_col):
    total = float(df[weight_col].sum())
    if total == 0:
        return None
    return round(float((df[val_col].astype(float) * df[weight_col].astype(float)).sum() / total), 2)


# ── MONTAGEM — reutilizavel para qualquer aba ─────────────────────────────────

def build_tab_data(df_sales: pd.DataFrame, df_lt: pd.DataFrame) -> dict:
    df_sales = df_sales.copy()
    df_lt    = df_lt.copy()
    df_sales["month"] = df_sales["month"].astype(str)
    df_lt["month"]    = df_lt["month"].astype(str)

    all_months = sorted(df_sales["month"].unique())
    all_sites  = sorted(df_sales["site"].unique())

    by_site = {}
    for site in all_sites:
        s  = df_sales[df_sales["site"] == site].set_index("month")
        lt = df_lt[df_lt["site"] == site]

        sold      = [int(s.loc[m, "total_items"])    if m in s.index else 0 for m in all_months]
        delivered = [int(s.loc[m, "delivered_items"]) if m in s.index else 0 for m in all_months]
        rate      = [round(d / t * 100, 1) if t > 0 else 0 for d, t in zip(delivered, sold)]

        lt_avg = []
        for m in all_months:
            sub = lt[lt["month"] == m]
            lt_avg.append(wavg(sub, "avg_lead_time", "cnt") if len(sub) > 0 else None)

        lt_by_picking = {}
        for pt in sorted(lt["picking_type"].unique()):
            sub_pt = lt[lt["picking_type"] == pt]
            series = []
            for m in all_months:
                sub_m = sub_pt[sub_pt["month"] == m]
                series.append(wavg(sub_m, "avg_lead_time", "cnt") if len(sub_m) > 0 else None)
            lt_by_picking[pt] = series

        by_site[site] = {
            "sold":          sold,
            "delivered":     delivered,
            "delivery_rate": rate,
            "lead_time":     lt_avg,
            "lt_by_picking": lt_by_picking,
        }

    total_sold      = int(df_sales["total_items"].sum())
    total_delivered = int(df_sales["delivered_items"].sum())
    overall_lt      = wavg(df_lt, "avg_lead_time", "cnt") or 0

    return {
        "labels":  all_months,
        "sites":   all_sites,
        "by_site": by_site,
        "kpis": {
            "total_sold":      total_sold,
            "total_delivered": total_delivered,
            "delivery_rate":   round(total_delivered / max(total_sold, 1) * 100, 1),
            "avg_lead_time":   overall_lt,
            "active_sites":    len(all_sites),
            "total_sellers":   len(MAIN_CUSTS),
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
  --blue:#2D73F5; --orange:#FF7A00; --green:#00C48C; --red:#FF4D4F;
  --purple:#9747FF; --teal:#00BCD4; --yellow:#FFC107; --lime:#8BC34A;
  --bg:#F4F6FA; --card:#FFFFFF; --text:#1A1A2E; --muted:#7A8099; --border:#E8EAEE;
}}
*{{ box-sizing:border-box; margin:0; padding:0; }}
body{{ font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
       background:var(--bg); color:var(--text); }}
.header{{ background:var(--blue); color:#fff; padding:16px 28px;
          display:flex; align-items:center; gap:12px; }}
.header h1{{ font-size:1.2rem; font-weight:700; }}
.badge{{ background:rgba(255,255,255,.2); border-radius:20px;
         padding:3px 12px; font-size:.73rem; margin-left:auto; white-space:nowrap; }}
.tabs{{ background:#fff; border-bottom:2px solid var(--border);
        display:flex; padding:0 28px; gap:2px; overflow-x:auto; }}
.tab{{ padding:13px 16px; cursor:pointer; font-size:.86rem; white-space:nowrap;
       color:var(--muted); border-bottom:3px solid transparent;
       margin-bottom:-2px; transition:.15s; user-select:none; }}
.tab.active{{ color:var(--blue); border-bottom-color:var(--blue); font-weight:600; }}
.tab:hover:not(.active){{ color:var(--text); }}
.content{{ padding:22px 28px; }}
.pane{{ display:none; }}
.pane.active{{ display:block; }}
.kpi-grid{{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
            gap:12px; margin-bottom:22px; }}
.kpi{{ background:var(--card); border-radius:10px; padding:16px 18px;
       box-shadow:0 1px 4px rgba(0,0,0,.07); }}
.kpi .lbl{{ font-size:.74rem; color:var(--muted); margin-bottom:5px; }}
.kpi .val{{ font-size:1.65rem; font-weight:700; line-height:1; }}
.kpi .sub{{ font-size:.71rem; color:var(--muted); margin-top:4px; }}
.kpi.c-blue   .val{{ color:var(--blue); }}
.kpi.c-green  .val{{ color:var(--green); }}
.kpi.c-orange .val{{ color:var(--orange); }}
.kpi.c-purple .val{{ color:var(--purple); }}
.section-title{{ font-size:.82rem; font-weight:700; color:var(--muted);
                 text-transform:uppercase; letter-spacing:.06em;
                 margin:24px 0 12px; padding-bottom:6px;
                 border-bottom:1px solid var(--border); }}
.chart-grid{{ display:grid; grid-template-columns:repeat(auto-fit,minmax(420px,1fr)); gap:16px; }}
.chart-grid.cols-1{{ grid-template-columns:1fr; }}
.chart-card{{ background:var(--card); border-radius:10px; padding:18px 20px;
              box-shadow:0 1px 4px rgba(0,0,0,.07); }}
.chart-card h3{{ font-size:.88rem; font-weight:600; margin-bottom:12px; }}
.chart-card canvas{{ max-height:260px; }}
.picking-grid{{ display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:16px; }}
.picking-grid .chart-card canvas{{ max-height:200px; }}
.footer{{ text-align:center; padding:16px; font-size:.71rem; color:var(--muted); }}
</style>
</head>
<body>

<div class="header">
  <h1>MercadoPago — Dashboard Operacoes</h1>
  <span class="badge">Atualizado: <span id="updated"></span></span>
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('points',this)">Visao Geral Points &amp; Others</div>
  <div class="tab"        onclick="showTab('cards', this)">Visao Geral Cards</div>
  <div class="tab"        onclick="showTab('tab3',  this)">Em breve</div>
  <div class="tab"        onclick="showTab('tab4',  this)">Em breve</div>
</div>

<div class="content">

<!-- ── ABA 1: Points & Others ── -->
<div id="pane-points" class="pane active">
  <div class="kpi-grid">
    <div class="kpi c-blue">  <div class="lbl">Itens Vendidos (2026)</div>   <div class="val" id="p-k-sold"></div>  <div class="sub">Total de ordens criadas</div></div>
    <div class="kpi c-green"> <div class="lbl">Itens Entregues (2026)</div>  <div class="val" id="p-k-del"></div>   <div class="sub">FLAG_DELIVERED = 1</div></div>
    <div class="kpi c-orange"><div class="lbl">Taxa de Entrega</div>          <div class="val" id="p-k-rate"></div>  <div class="sub">Entregues / Vendidos</div></div>
    <div class="kpi c-purple"><div class="lbl">Lead Time Medio</div>          <div class="val" id="p-k-lt"></div>    <div class="sub">Dias habeis (entregues)</div></div>
    <div class="kpi">         <div class="lbl">Paises Ativos</div>            <div class="val" id="p-k-sites"></div> <div class="sub">Sites com dados 2026</div></div>
    <div class="kpi">         <div class="lbl">Sellers Monitorados</div>      <div class="val" id="p-k-sell"></div>  <div class="sub">Custs da lista principal</div></div>
  </div>
  <div class="section-title">Grafico 1 — Itens Vendidos vs Entregues por Mes (por Pais)</div>
  <div class="chart-grid">
    <div class="chart-card"><h3>Itens Vendidos por Mes</h3>   <canvas id="p-c1-sold"></canvas></div>
    <div class="chart-card"><h3>Itens Entregues por Mes</h3>  <canvas id="p-c1-del"></canvas></div>
  </div>
  <div class="section-title">Grafico 2 — Taxa de Entrega por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Taxa de Entrega (%) por Mes e Pais</h3><canvas id="p-c2-rate"></canvas></div>
  </div>
  <div class="section-title">Grafico 3 — Lead Time Medio por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Lead Time Medio — Dias Habeis (entregues)</h3><canvas id="p-c3-lt"></canvas></div>
  </div>
  <div class="section-title">Grafico 4 — Lead Time por Picking Type e Pais</div>
  <div class="picking-grid" id="p-c4"></div>
</div>

<!-- ── ABA 2: Cards ── -->
<div id="pane-cards" class="pane">
  <div class="kpi-grid">
    <div class="kpi c-blue">  <div class="lbl">Cards Vendidos (2026)</div>   <div class="val" id="c-k-sold"></div>  <div class="sub">Total de ordens criadas</div></div>
    <div class="kpi c-green"> <div class="lbl">Cards Entregues (2026)</div>  <div class="val" id="c-k-del"></div>   <div class="sub">SHP_STATUS = delivered</div></div>
    <div class="kpi c-orange"><div class="lbl">Taxa de Entrega</div>          <div class="val" id="c-k-rate"></div>  <div class="sub">Entregues / Vendidos</div></div>
    <div class="kpi c-purple"><div class="lbl">Lead Time Medio</div>          <div class="val" id="c-k-lt"></div>    <div class="sub">Dias habeis (entregues)</div></div>
    <div class="kpi">         <div class="lbl">Paises Ativos</div>            <div class="val" id="c-k-sites"></div> <div class="sub">Sites com dados 2026</div></div>
    <div class="kpi">         <div class="lbl">Sellers Monitorados</div>      <div class="val" id="c-k-sell"></div>  <div class="sub">Custs da lista principal</div></div>
  </div>
  <div class="section-title">Grafico 1 — Cards Vendidos vs Entregues por Mes (por Pais)</div>
  <div class="chart-grid">
    <div class="chart-card"><h3>Cards Vendidos por Mes</h3>   <canvas id="c-c1-sold"></canvas></div>
    <div class="chart-card"><h3>Cards Entregues por Mes</h3>  <canvas id="c-c1-del"></canvas></div>
  </div>
  <div class="section-title">Grafico 2 — Taxa de Entrega por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Taxa de Entrega (%) por Mes e Pais</h3><canvas id="c-c2-rate"></canvas></div>
  </div>
  <div class="section-title">Grafico 3 — Lead Time Medio por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Lead Time Medio — Dias Habeis (entregues)</h3><canvas id="c-c3-lt"></canvas></div>
  </div>
  <div class="section-title">Grafico 4 — Lead Time por Picking Type e Pais</div>
  <div class="picking-grid" id="c-c4"></div>
</div>

<!-- Placeholders -->
<div id="pane-tab3" class="pane"><p style="padding:40px;color:var(--muted);text-align:center">Em construcao...</p></div>
<div id="pane-tab4" class="pane"><p style="padding:40px;color:var(--muted);text-align:center">Em construcao...</p></div>

</div><!-- /content -->
<div class="footer">Fonte: meli-bi-data · SBOX_OPER_MP · a partir de {date_from} · <span id="footer-dt"></span></div>

<script>
const D = {data_json};

// ── Utilitarios ───────────────────────────────────────────────────────────────
function fmtN(n){{
  if(n==null) return "—";
  if(n>=1e6) return (n/1e6).toFixed(1)+"M";
  if(n>=1e3) return (n/1e3).toFixed(1)+"K";
  return String(n);
}}
function showTab(name,el){{
  document.querySelectorAll(".pane").forEach(p=>p.classList.remove("active"));
  document.querySelectorAll(".tab") .forEach(t=>t.classList.remove("active"));
  document.getElementById("pane-"+name).classList.add("active");
  el.classList.add("active");
}}

// ── Paleta ────────────────────────────────────────────────────────────────────
const SITE_COLORS = {{MLB:"#2D73F5",MLA:"#FF7A00",MLC:"#00C48C",MLM:"#9747FF",MLU:"#FF4D4F"}};
const PICK_COLORS = ["#2D73F5","#FF7A00","#00C48C","#9747FF","#FF4D4F","#607D8B","#FFC107","#8BC34A"];
function sc(site){{ return SITE_COLORS[site]||"#607D8B"; }}
function ax(c,a){{ return c+Math.round(a*255).toString(16).padStart(2,"0"); }}

// ── Builders de dataset ───────────────────────────────────────────────────────
function mkLine(label,data,c,fill=false){{
  return{{label,data,borderColor:c,backgroundColor:fill?ax(c,0.12):"transparent",
    borderWidth:2,pointRadius:3,tension:0.35,spanGaps:true,fill}};
}}

// ── Chart factories ───────────────────────────────────────────────────────────
function lineChart(id,labels,datasets,yLabel=""){{
  return new Chart(document.getElementById(id),{{
    type:"line",data:{{labels,datasets}},
    options:{{responsive:true,interaction:{{mode:"index",intersect:false}},
      plugins:{{legend:{{position:"top"}}}},
      scales:{{y:{{beginAtZero:false,title:{{display:!!yLabel,text:yLabel,font:{{size:11}}}}}}}}
    }}
  }});
}}
function lineChartEl(el,labels,datasets,yLabel=""){{
  return new Chart(el,{{
    type:"line",data:{{labels,datasets}},
    options:{{responsive:true,interaction:{{mode:"index",intersect:false}},
      plugins:{{legend:{{position:"top"}}}},
      scales:{{y:{{beginAtZero:false,title:{{display:!!yLabel,text:yLabel,font:{{size:11}}}}}}}}
    }}
  }});
}}

// ── Renderiza os 4 graficos de uma aba ────────────────────────────────────────
function renderTab(prefix, tabData){{
  const ml    = tabData.labels;
  const bs    = tabData.by_site;
  const sites = tabData.sites;

  // Grafico 1
  lineChart(prefix+"-c1-sold", ml, sites.map(s=>mkLine(s, bs[s].sold,      sc(s))), "Itens");
  lineChart(prefix+"-c1-del",  ml, sites.map(s=>mkLine(s, bs[s].delivered, sc(s))), "Itens");

  // Grafico 2
  lineChart(prefix+"-c2-rate", ml, sites.map(s=>mkLine(s, bs[s].delivery_rate, sc(s), true)), "%");

  // Grafico 3
  lineChart(prefix+"-c3-lt", ml, sites.map(s=>mkLine(s, bs[s].lead_time, sc(s))), "Dias habeis");

  // Grafico 4 — um chart por pais
  const container = document.getElementById(prefix+"-c4");
  sites.forEach(site => {{
    const picks = Object.keys(bs[site].lt_by_picking);
    if(!picks.length) return;
    const card = document.createElement("div");
    card.className = "chart-card";
    card.innerHTML = `<h3>Lead Time — ${{site}}</h3><canvas></canvas>`;
    container.appendChild(card);
    lineChartEl(card.querySelector("canvas"), ml,
      picks.map((pt,i)=>mkLine(pt, bs[site].lt_by_picking[pt], PICK_COLORS[i%PICK_COLORS.length])),
      "Dias habeis");
  }});
}}

// ── KPIs ──────────────────────────────────────────────────────────────────────
function setKpis(prefix, kpis){{
  document.getElementById(prefix+"-k-sold") .textContent = fmtN(kpis.total_sold);
  document.getElementById(prefix+"-k-del")  .textContent = fmtN(kpis.total_delivered);
  document.getElementById(prefix+"-k-rate") .textContent = kpis.delivery_rate+"%";
  document.getElementById(prefix+"-k-lt")   .textContent = kpis.avg_lead_time+"d";
  document.getElementById(prefix+"-k-sites").textContent = kpis.active_sites;
  document.getElementById(prefix+"-k-sell") .textContent = kpis.total_sellers;
}}

// ── Init ──────────────────────────────────────────────────────────────────────
document.getElementById("updated")  .textContent = D.updated_at;
document.getElementById("footer-dt").textContent = D.updated_at;

setKpis("p", D.points.kpis);
setKpis("c", D.cards.kpis);
renderTab("p", D.points);
renderTab("c", D.cards);
</script>
</body>
</html>
"""


# ── GERAR HTML ────────────────────────────────────────────────────────────────

def generate_html(points_data: dict, cards_data: dict, title: str, output: str = "index.html"):
    payload = {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "points":     points_data,
        "cards":      cards_data,
    }
    data_json = json.dumps(payload, ensure_ascii=False, default=str)
    html = HTML_TEMPLATE.format(title=title, data_json=data_json, date_from=DATE_FROM)
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nDashboard gerado: {output}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Conectando ao BigQuery ({PROJECT})...")
    client = bigquery.Client(project=PROJECT)

    print("\n[ABA 1] Points & Others...")
    df_p_sales = run_query(client, QUERY_POINTS_SALES, "points-vendas")
    df_p_lt    = run_query(client, QUERY_POINTS_LT,    "points-leadtime")

    print("\n[ABA 2] Cards...")
    df_c_sales = run_query(client, QUERY_CARDS_SALES,  "cards-vendas")
    df_c_lt    = run_query(client, QUERY_CARDS_LT,     "cards-leadtime")

    print("\nMontando dados...")
    points_data = build_tab_data(df_p_sales, df_p_lt)
    cards_data  = build_tab_data(df_c_sales, df_c_lt)

    print("Gerando HTML...")
    generate_html(points_data, cards_data, DASHBOARD_TITLE)

    print("\nPara publicar:")
    print("  git add index.html && git commit -m 'aba cards' && git push")


if __name__ == "__main__":
    main()
