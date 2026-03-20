"""
generate_stock_dashboard.py
---------------------------
Dashboard de gestão de estoques — Produtos MLB FBM
Gera dashboard_estoque.html

USO:
    .venv/Scripts/python generate_stock_dashboard.py

SAIDA:
    dashboard_estoque.html
"""

from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime

# ── CONFIGURACAO ──────────────────────────────────────────────────────────────

PROJECT         = "meli-bi-data"
DATE_FROM       = "2026-01-01"
OUTPUT_FILE     = "dashboard_estoque.html"

INVENTORY_IDS = [
    'QCGO40352', 'NEFY38719', 'BHEI39907', 'BHZW22909', 'VLKF40709',
    'JVUG38431', 'NGMH40323', 'KEKJ53768', 'WDUM53222', 'MXPN53308',
    'EPVM97846',
]
ULTRAPASSE_IDS = {'EPVM97846'}

PRODUCT_NAMES = {
    'QCGO40352': 'Smart 2 Claro',
    'NEFY38719': 'Smart 2 Vivo',
    'BHEI39907': 'Smart 2 Tim',
    'BHZW22909': 'Mini BT',
    'VLKF40709': 'Pro3 Claro',
    'JVUG38431': 'Pro3 Vivo',
    'NGMH40323': 'Pro3 Tim',
    'KEKJ53768': 'Air 2 Claro',
    'WDUM53222': 'Air 2 Vivo',
    'MXPN53308': 'Air 2 Tim',
    'EPVM97846': 'Ultrapasse',
}

_IDS = ", ".join(f"'{i}'" for i in INVENTORY_IDS)

# ── QUERIES ───────────────────────────────────────────────────────────────────

QUERY_SALES_MONTHLY = f"""
SELECT
  DATE_TRUNC(CALENDAR_DATE, MONTH)                            AS month,
  CASE WHEN INVENTORY_ID = 'EPVM97846' THEN 'Ultrapasse'
       ELSE 'Point' END                                       AS product_type,
  SUM(OUTBOUND)                                               AS total_outbound
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
WHERE CALENDAR_DATE >= '{DATE_FROM}'
  AND INVENTORY_ID IN ({_IDS})
  AND SIT_SITE_ID = 'MLB'
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_STOCK_DETAIL = f"""
SELECT
  INVENTORY_ID,
  CASE WHEN INVENTORY_ID = 'EPVM97846' THEN 'Ultrapasse'
       ELSE 'Point' END                                       AS product_type,
  SUM(AVAILABLE_STOCK)                                        AS available,
  SUM(QUARENTINE_STOCK)                                       AS quarantine,
  SUM(DAMAGED_STOCK)                                          AS damaged,
  SUM(TRANSFER_STOCK)                                         AS transfer,
  SUM(INVOICE_PENDING_STOCK)                                  AS invoice_pending,
  SUM(OUTBOUND_PENDING_STOCK)                                 AS outbound_pending,
  SUM(WAITING_REMOVAL_STOCK)                                  AS waiting_removal,
  SUM(TOTAL_STOCK)                                            AS total_stock
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
WHERE CALENDAR_DATE = (
    SELECT MAX(CALENDAR_DATE)
    FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
    WHERE INVENTORY_ID IN ({_IDS}) AND SIT_SITE_ID = 'MLB'
  )
  AND INVENTORY_ID IN ({_IDS})
  AND SIT_SITE_ID = 'MLB'
GROUP BY 1, 2
ORDER BY product_type, INVENTORY_ID
"""

QUERY_SAFETY_STOCK = f"""
WITH latest AS (
  SELECT MAX(CALENDAR_DATE) AS max_date
  FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
  WHERE INVENTORY_ID IN ({_IDS}) AND SIT_SITE_ID = 'MLB'
),
sales_60d AS (
  SELECT
    INVENTORY_ID,
    SUM(OUTBOUND)                                               AS total_outbound_60d,
    COUNT(DISTINCT CALENDAR_DATE)                               AS days_with_data,
    SAFE_DIVIDE(SUM(OUTBOUND), COUNT(DISTINCT CALENDAR_DATE))   AS avg_daily_sales
  FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`, latest
  WHERE CALENDAR_DATE > DATE_SUB(max_date, INTERVAL 60 DAY)
    AND CALENDAR_DATE <= max_date
    AND INVENTORY_ID IN ({_IDS})
    AND SIT_SITE_ID = 'MLB'
  GROUP BY 1
),
current_stock AS (
  SELECT INVENTORY_ID, SUM(AVAILABLE_STOCK) AS available_stock
  FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`, latest
  WHERE CALENDAR_DATE = max_date
    AND INVENTORY_ID IN ({_IDS})
    AND SIT_SITE_ID = 'MLB'
  GROUP BY 1
)
SELECT
  s.INVENTORY_ID,
  CASE WHEN s.INVENTORY_ID = 'EPVM97846' THEN 'Ultrapasse' ELSE 'Point' END AS product_type,
  ROUND(s.avg_daily_sales, 1)                                 AS avg_daily_sales,
  s.total_outbound_60d,
  s.days_with_data,
  ROUND(c.available_stock, 0)                                 AS available_stock,
  ROUND(SAFE_DIVIDE(c.available_stock, s.avg_daily_sales), 1) AS days_of_stock
FROM sales_60d s
JOIN current_stock c USING (INVENTORY_ID)
ORDER BY days_of_stock ASC
"""

QUERY_WAREHOUSE_DIST = f"""
SELECT
  WAREHOUSE_ID,
  SUM(TOTAL_STOCK)                                            AS total_stock,
  SUM(AVAILABLE_STOCK)                                        AS available_stock
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
WHERE CALENDAR_DATE = (
    SELECT MAX(CALENDAR_DATE)
    FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
    WHERE INVENTORY_ID IN ({_IDS}) AND SIT_SITE_ID = 'MLB'
  )
  AND INVENTORY_ID IN ({_IDS})
  AND SIT_SITE_ID = 'MLB'
  AND TOTAL_STOCK > 0
GROUP BY 1
ORDER BY 2 DESC
"""

# ── HELPERS ───────────────────────────────────────────────────────────────────

def run_query(client, sql, label):
    print(f"  [{label}] executando...")
    df = client.query(sql).to_dataframe()
    for col in df.select_dtypes(include=["object"]).columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    print(f"  [{label}] {len(df)} linhas")
    return df


def safe_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


# ── MONTAGEM DE DADOS ─────────────────────────────────────────────────────────

def build_sales_data(df):
    df = df.copy()
    df["month"] = df["month"].astype(str)
    all_months = sorted(df["month"].unique())

    point_data = []
    ultra_data = []
    for m in all_months:
        sub = df[df["month"] == m]
        p = sub[sub["product_type"] == "Point"]["total_outbound"].sum()
        u = sub[sub["product_type"] == "Ultrapasse"]["total_outbound"].sum()
        point_data.append(safe_int(p))
        ultra_data.append(safe_int(u))

    return {
        "labels": all_months,
        "point":  point_data,
        "ultra":  ultra_data,
        "kpis": {
            "total_point": safe_int(df[df["product_type"] == "Point"]["total_outbound"].sum()),
            "total_ultra": safe_int(df[df["product_type"] == "Ultrapasse"]["total_outbound"].sum()),
        }
    }


def build_stock_data(df):
    df = df.copy()
    skus      = [PRODUCT_NAMES.get(i, i) for i in df["INVENTORY_ID"].tolist()]
    types     = df["product_type"].tolist()
    fields    = ["available", "quarantine", "damaged", "transfer",
                 "invoice_pending", "outbound_pending", "waiting_removal"]
    field_labels = ["Disponivel", "Quarentena", "Danificado", "Transferencia",
                    "Pend. Fiscal", "Pend. Saida", "Aguard. Remocao"]

    series = {}
    for f, lbl in zip(fields, field_labels):
        series[lbl] = [safe_int(row[f]) for _, row in df.iterrows()]

    total_by_sku = [safe_int(row["total_stock"]) for _, row in df.iterrows()]

    return {
        "skus":       skus,
        "types":      types,
        "series":     series,
        "totals":     total_by_sku,
        "kpis": {
            "total_stock":     safe_int(df["total_stock"].sum()),
            "total_available": safe_int(df["available"].sum()),
            "total_blocked":   safe_int((df["total_stock"] - df["available"]).sum()),
        }
    }


def build_safety_data(df):
    df = df.copy()
    products   = [PRODUCT_NAMES.get(i, i) for i in df["INVENTORY_ID"].tolist()]
    avail      = [safe_int(v) for v in df["available_stock"]]
    avg_daily  = [round(float(v), 1) if pd.notna(v) else 0 for v in df["avg_daily_sales"]]
    days       = [round(float(v), 1) if pd.notna(v) else 0 for v in df["days_of_stock"]]
    outbound60 = [safe_int(v) for v in df["total_outbound_60d"]]

    min_idx = days.index(min(days))
    max_idx = days.index(max(days))

    return {
        "products":    products,
        "available":   avail,
        "avg_daily":   avg_daily,
        "days":        days,
        "outbound_60d": outbound60,
        "kpis": {
            "critical_product": products[min_idx],
            "critical_days":    days[min_idx],
            "best_product":     products[max_idx],
            "best_days":        days[max_idx],
            "avg_days":         round(sum(days) / len(days), 1) if days else 0,
        }
    }


def build_warehouse_data(df):
    df = df.copy()
    total = float(df["total_stock"].sum())
    warehouses = df["WAREHOUSE_ID"].tolist()
    stocks     = [safe_int(v) for v in df["total_stock"]]
    pcts       = [round(s / total * 100, 1) if total > 0 else 0 for s in stocks]

    return {
        "warehouses": warehouses,
        "stocks":     stocks,
        "pcts":       pcts,
        "kpis": {
            "total_warehouses": len(warehouses),
            "top_warehouse":    warehouses[0] if warehouses else "N/A",
            "top_pct":          pcts[0] if pcts else 0,
        }
    }


# ── HTML TEMPLATE ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Dashboard Estoque MLB — FBM</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {{
  --blue:#2D73F5; --orange:#FF7A00; --green:#00C48C; --red:#FF4D4F;
  --purple:#9747FF; --teal:#00BCD4; --yellow:#FFC107; --lime:#8BC34A;
  --gray:#607D8B;
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
.content{{ padding:22px 28px; max-width:1600px; margin:0 auto; }}
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
.kpi.c-red    .val{{ color:var(--red); }}
.section-title{{ font-size:.82rem; font-weight:700; color:var(--muted);
                 text-transform:uppercase; letter-spacing:.06em;
                 margin:28px 0 12px; padding-bottom:6px;
                 border-bottom:1px solid var(--border); }}
.chart-card{{ background:var(--card); border-radius:10px; padding:18px 20px;
              box-shadow:0 1px 4px rgba(0,0,0,.07); margin-bottom:16px; }}
.chart-card h3{{ font-size:.88rem; font-weight:600; margin-bottom:12px; }}
.chart-card canvas{{ max-height:320px; }}
.chart-card.tall canvas{{ max-height:480px; }}
.chart-card.xtall canvas{{ max-height:640px; }}
.footer{{ text-align:center; padding:16px; font-size:.71rem; color:var(--muted); margin-top:8px; }}
</style>
</head>
<body>

<div class="header">
  <h1>Dashboard Estoque MLB — FBM</h1>
  <span class="badge">Atualizado: <span id="updated"></span></span>
</div>

<div class="content">

  <!-- KPIs globais -->
  <div style="margin-top:20px">
  <div class="kpi-grid">
    <div class="kpi c-blue">  <div class="lbl">Total Vendido Point (2026)</div>   <div class="val" id="k-point-sales"></div>  <div class="sub">Outbound acumulado</div></div>
    <div class="kpi c-purple"><div class="lbl">Total Vendido Ultrapasse (2026)</div><div class="val" id="k-ultra-sales"></div>  <div class="sub">Outbound acumulado</div></div>
    <div class="kpi c-green"> <div class="lbl">Estoque Total (snapshot)</div>      <div class="val" id="k-stock-total"></div> <div class="sub">Todos os SKUs</div></div>
    <div class="kpi c-orange"><div class="lbl">Estoque Disponivel</div>            <div class="val" id="k-stock-avail"></div> <div class="sub">Available stock</div></div>
    <div class="kpi c-red">   <div class="lbl">Estoque Bloqueado</div>             <div class="val" id="k-stock-block"></div> <div class="sub">Quarentena + outros</div></div>
    <div class="kpi">         <div class="lbl">Warehouses Ativos</div>             <div class="val" id="k-wh-count"></div>   <div class="sub">Com estoque</div></div>
    <div class="kpi c-blue">  <div class="lbl">Top Warehouse</div>                 <div class="val" style="font-size:1.1rem" id="k-wh-top"></div><div class="sub" id="k-wh-top-pct"></div></div>
  </div>
  <div style="margin-top:12px">
  <div class="kpi-grid">
    <div class="kpi c-red">   <div class="lbl">Menor Cobertura</div>   <div class="val" id="k-ss-critical-days"></div><div class="sub" id="k-ss-critical-prod"></div></div>
    <div class="kpi c-green"> <div class="lbl">Maior Cobertura</div>   <div class="val" id="k-ss-best-days"></div>   <div class="sub" id="k-ss-best-prod"></div></div>
    <div class="kpi c-orange"><div class="lbl">Cobertura Media</div>   <div class="val" id="k-ss-avg-days"></div>    <div class="sub">Media entre produtos</div></div>
  </div>
  </div>
  </div>

  <!-- Grafico 1 -->
  <div class="section-title">Grafico 1 — Vendas por Mes (Point vs Ultrapasse)</div>
  <div class="chart-card">
    <h3>Total de Vendas (Outbound) por Mes — MLB</h3>
    <canvas id="c1"></canvas>
  </div>

  <!-- Grafico 2 -->
  <div class="section-title">Grafico 2 — Detalhamento de Estoque por SKU (snapshot mais recente)</div>
  <div class="chart-card tall">
    <h3>Composicao do Estoque por INVENTORY_ID</h3>
    <canvas id="c2"></canvas>
  </div>

  <!-- Grafico 3 -->
  <div class="section-title">Grafico 3 — Distribuicao por Warehouse (% do total)</div>
  <div class="chart-card xtall">
    <h3>Estoque Total por Warehouse — % representatividade</h3>
    <canvas id="c3"></canvas>
  </div>

  <!-- Grafico 4 -->
  <div class="section-title">Grafico 4 — Estoque de Seguranca (Dias de Cobertura)</div>
  <div class="chart-card tall">
    <h3>Dias de Estoque Disponivel vs Media de Vendas Diarias (ultimos 60 dias)</h3>
    <canvas id="c4"></canvas>
  </div>

</div>

<div class="footer">
  Fonte: meli-bi-data · WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC · MLB · a partir de {date_from} · <span id="footer-dt"></span>
</div>

<script>
const D = {data_json};

// ── Utils ─────────────────────────────────────────────────────────────────────
function fmtN(n){{
  if(n==null||n===undefined) return "—";
  if(n>=1e6) return (n/1e6).toFixed(1)+"M";
  if(n>=1e3) return (n/1e3).toFixed(1)+"K";
  return String(n);
}}
function ax(c,a){{ return c+Math.round(a*255).toString(16).padStart(2,"0"); }}

const STOCK_COLORS = {{
  "Disponivel":     "#00C48C",
  "Quarentena":     "#FF7A00",
  "Danificado":     "#FF4D4F",
  "Transferencia":  "#2D73F5",
  "Pend. Fiscal":   "#9747FF",
  "Pend. Saida":    "#FFC107",
  "Aguard. Remocao":"#607D8B",
}};

// ── KPIs ──────────────────────────────────────────────────────────────────────
document.getElementById("updated")       .textContent = D.updated_at;
document.getElementById("footer-dt")     .textContent = D.updated_at;
document.getElementById("k-point-sales") .textContent = fmtN(D.sales.kpis.total_point);
document.getElementById("k-ultra-sales") .textContent = fmtN(D.sales.kpis.total_ultra);
document.getElementById("k-stock-total") .textContent = fmtN(D.stock.kpis.total_stock);
document.getElementById("k-stock-avail") .textContent = fmtN(D.stock.kpis.total_available);
document.getElementById("k-stock-block") .textContent = fmtN(D.stock.kpis.total_blocked);
document.getElementById("k-wh-count")       .textContent = D.warehouse.kpis.total_warehouses;
document.getElementById("k-wh-top")         .textContent = D.warehouse.kpis.top_warehouse;
document.getElementById("k-wh-top-pct")     .textContent = D.warehouse.kpis.top_pct+"% do total";
document.getElementById("k-ss-critical-days").textContent = D.safety.kpis.critical_days+"d";
document.getElementById("k-ss-critical-prod").textContent = D.safety.kpis.critical_product;
document.getElementById("k-ss-best-days")   .textContent = D.safety.kpis.best_days+"d";
document.getElementById("k-ss-best-prod")   .textContent = D.safety.kpis.best_product;
document.getElementById("k-ss-avg-days")    .textContent = D.safety.kpis.avg_days+"d";

// ── Grafico 1 — Vendas mensais ────────────────────────────────────────────────
new Chart(document.getElementById("c1"), {{
  type: "bar",
  data: {{
    labels: D.sales.labels,
    datasets: [
      {{
        label: "Point",
        data: D.sales.point,
        backgroundColor: ax("#2D73F5", 0.8),
        borderColor: "#2D73F5",
        borderWidth: 1,
        order: 2,
      }},
      {{
        label: "Ultrapasse",
        data: D.sales.ultra,
        backgroundColor: ax("#9747FF", 0.8),
        borderColor: "#9747FF",
        borderWidth: 1,
        order: 1,
        type: "line",
        tension: 0.35,
        pointRadius: 4,
        fill: false,
      }}
    ]
  }},
  options: {{
    responsive: true,
    interaction: {{ mode: "index", intersect: false }},
    plugins: {{ legend: {{ position: "top" }} }},
    scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: "Unidades vendidas" }} }} }}
  }}
}});

// ── Grafico 2 — Estoque por SKU (stacked bar) ─────────────────────────────────
const stockLabels  = D.stock.types.map((t,i) => D.stock.skus[i]+" ("+t+")");
const stockSeries  = Object.entries(D.stock.series);
new Chart(document.getElementById("c2"), {{
  type: "bar",
  data: {{
    labels: stockLabels,
    datasets: stockSeries.map(([name, vals]) => ({{
      label: name,
      data: vals,
      backgroundColor: ax(STOCK_COLORS[name]||"#607D8B", 0.8),
      borderColor: STOCK_COLORS[name]||"#607D8B",
      borderWidth: 1,
    }}))
  }},
  options: {{
    responsive: true,
    interaction: {{ mode: "index", intersect: false }},
    plugins: {{ legend: {{ position: "top" }} }},
    scales: {{
      x: {{ stacked: true }},
      y: {{ stacked: true, beginAtZero: true, title: {{ display: true, text: "Unidades" }} }}
    }}
  }}
}});

// ── Grafico 4 — Dias de cobertura (safety stock) ─────────────────────────────
new Chart(document.getElementById("c4"), {{
  type: "bar",
  data: {{
    labels: D.safety.products,
    datasets: [
      {{
        label: "Dias de estoque disponivel",
        data: D.safety.days,
        backgroundColor: D.safety.days.map(d =>
          d < 15  ? ax("#FF4D4F", 0.85) :
          d < 30  ? ax("#FF7A00", 0.85) :
                    ax("#00C48C", 0.85)),
        borderColor: D.safety.days.map(d =>
          d < 15 ? "#FF4D4F" : d < 30 ? "#FF7A00" : "#00C48C"),
        borderWidth: 1,
      }},
      {{
        label: "Media vendas/dia (60d)",
        data: D.safety.avg_daily,
        type: "line",
        yAxisID: "y2",
        borderColor: "#2D73F5",
        backgroundColor: "transparent",
        borderWidth: 2,
        pointRadius: 5,
        pointBackgroundColor: "#2D73F5",
        tension: 0.3,
      }}
    ]
  }},
  options: {{
    responsive: true,
    interaction: {{ mode: "index", intersect: false }},
    plugins: {{
      legend: {{ position: "top" }},
      tooltip: {{
        callbacks: {{
          afterBody: ctx => {{
            const i = ctx[0].dataIndex;
            return ["Estoque disponivel: "+D.safety.available[i].toLocaleString("pt-BR")+" un",
                    "Outbound 60d: "+D.safety.outbound_60d[i].toLocaleString("pt-BR")+" un"];
          }}
        }}
      }}
    }},
    scales: {{
      x: {{ title: {{ display: true, text: "Produto" }} }},
      y: {{
        beginAtZero: true,
        title: {{ display: true, text: "Dias de cobertura" }},
        grid: {{
          color: ctx => ctx.tick.value === 15 ? "rgba(255,77,79,0.4)" :
                        ctx.tick.value === 30 ? "rgba(255,122,0,0.4)" : "rgba(0,0,0,0.05)"
        }}
      }},
      y2: {{
        position: "right",
        beginAtZero: true,
        title: {{ display: true, text: "Vendas/dia (unid)" }},
        grid: {{ drawOnChartArea: false }}
      }}
    }}
  }}
}});

// ── Grafico 3 — Warehouse % (horizontal bar) ──────────────────────────────────
const whLabels = D.warehouse.warehouses.map(
  (w,i) => w+" ("+D.warehouse.pcts[i]+"%)");
new Chart(document.getElementById("c3"), {{
  type: "bar",
  data: {{
    labels: whLabels,
    datasets: [{{
      label: "Estoque total",
      data: D.warehouse.stocks,
      backgroundColor: D.warehouse.pcts.map(p =>
        p >= 10 ? ax("#2D73F5",0.8) : p >= 5 ? ax("#00C48C",0.8) : ax("#607D8B",0.6)),
      borderWidth: 1,
    }}]
  }},
  options: {{
    indexAxis: "y",
    responsive: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          label: ctx => " "+ctx.parsed.x.toLocaleString("pt-BR")+" unid. ("+D.warehouse.pcts[ctx.dataIndex]+"%)"
        }}
      }}
    }},
    scales: {{ x: {{ beginAtZero: true, title: {{ display: true, text: "Unidades" }} }} }}
  }}
}});
</script>
</body>
</html>
"""


# ── GERAR HTML ────────────────────────────────────────────────────────────────

def generate_html(sales_data, stock_data, warehouse_data, safety_data):
    payload = {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "sales":      sales_data,
        "stock":      stock_data,
        "warehouse":  warehouse_data,
        "safety":     safety_data,
    }
    data_json = json.dumps(payload, ensure_ascii=False, default=str)
    html = HTML_TEMPLATE.format(data_json=data_json, date_from=DATE_FROM)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nDashboard gerado: {OUTPUT_FILE}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Conectando ao BigQuery ({PROJECT})...")
    client = bigquery.Client(project=PROJECT)

    print("\nConsultando dados...")
    df_sales     = run_query(client, QUERY_SALES_MONTHLY,  "vendas-mensais")
    df_stock     = run_query(client, QUERY_STOCK_DETAIL,   "estoque-detalhe")
    df_warehouse = run_query(client, QUERY_WAREHOUSE_DIST, "warehouse-dist")
    df_safety    = run_query(client, QUERY_SAFETY_STOCK,   "safety-stock")

    print("\nMontando dados...")
    sales_data     = build_sales_data(df_sales)
    stock_data     = build_stock_data(df_stock)
    warehouse_data = build_warehouse_data(df_warehouse)
    safety_data    = build_safety_data(df_safety)

    print("Gerando HTML...")
    generate_html(sales_data, stock_data, warehouse_data, safety_data)

    print("\nPara publicar:")
    print("  git add dashboard_estoque.html && git commit -m 'dashboard estoque MLB' && git push")


if __name__ == "__main__":
    main()
