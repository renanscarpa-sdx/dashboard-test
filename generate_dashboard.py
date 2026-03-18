"""
generate_dashboard.py
---------------------
Consulta BigQuery e gera um dashboard HTML estático pronto para GitHub Pages.

USO:
    python generate_dashboard.py

SAÍDA:
    index.html  (abrir no browser ou subir para o GitHub Pages)

DEPENDÊNCIAS:
    pip install google-cloud-bigquery pandas db-dtypes
"""

from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

PROJECT = "meli-bi-data"
DASHBOARD_TITLE = "Dashboard Logística FBM"

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

# ─── QUERIES ─────────────────────────────────────────────────────────────────

# Ajuste as queries conforme sua análise. Estas são exemplos funcionais.

QUERY_WEEKLY = f"""
SELECT
    DATE_TRUNC(CALENDAR_DATE, WEEK(MONDAY))        AS week,
    SUM(SI_FBM)                                     AS fbm_units,
    SUM(SI_ME2)                                     AS me2_units,
    SUM(SI_FLEX)                                    AS flex_units,
    SUM(SI_XD)                                      AS xd_units,
    SUM(GMV_FBM_USD)                                AS gmv_fbm_usd,
    SUM(GMV_ME2_USD)                                AS gmv_me2_usd
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_GROWTH_DETAIL`
WHERE
    CALENDAR_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
    AND CUS_CUST_ID IN ({', '.join(str(c) for c in MAIN_CUSTS)})
    AND SIT_SITE_ID = 'MLB'
GROUP BY 1
ORDER BY 1
"""

QUERY_TOP_SELLERS = f"""
SELECT
    CUS_CUST_ID                                     AS cust_id,
    CUS_NICKNAME                                    AS nickname,
    SUM(SI_FBM)                                     AS fbm_units,
    SUM(SKU_TOTAL_STOCK)                            AS total_stock,
    SUM(SKU_SALEABLE_STOCK)                         AS healthy_stock,
    SUM(GMV_FBM_USD)                                AS gmv_fbm_usd
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_GROWTH_DETAIL`
WHERE
    CALENDAR_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
    AND CUS_CUST_ID IN ({', '.join(str(c) for c in MAIN_CUSTS)})
    AND SIT_SITE_ID = 'MLB'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 15
"""

QUERY_KPIS = f"""
SELECT
    SUM(SI_FBM)                                     AS total_fbm_units,
    SUM(SI_ME2)                                     AS total_me2_units,
    SUM(SI_FLEX)                                    AS total_flex_units,
    SUM(SKU_TOTAL_STOCK)                            AS total_stock,
    SUM(SKU_SALEABLE_STOCK)                         AS total_healthy_stock,
    SUM(GMV_FBM_USD)                                AS total_gmv_fbm_usd,
    COUNT(DISTINCT CUS_CUST_ID)                     AS active_sellers
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_GROWTH_DETAIL`
WHERE
    CALENDAR_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
    AND CUS_CUST_ID IN ({', '.join(str(c) for c in MAIN_CUSTS)})
    AND SIT_SITE_ID = 'MLB'
"""

# ─── FUNÇÕES DE DADOS ────────────────────────────────────────────────────────

def run_query(client: bigquery.Client, sql: str) -> pd.DataFrame:
    print(f"  Rodando query ({sql[:60].strip()}...)")
    return client.query(sql).to_dataframe()


def fmt_number(n) -> str:
    """Formata número para exibição (ex: 1.2M, 34K)."""
    if n is None:
        return "—"
    n = float(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:.0f}"


def build_data_object(df_weekly: pd.DataFrame, df_sellers: pd.DataFrame, df_kpis: pd.DataFrame) -> dict:
    """Monta o objeto JS com todos os dados para o dashboard."""

    weekly_labels   = df_weekly["week"].astype(str).tolist()
    fbm_series      = df_weekly["fbm_units"].fillna(0).astype(int).tolist()
    me2_series      = df_weekly["me2_units"].fillna(0).astype(int).tolist()
    flex_series     = df_weekly["flex_units"].fillna(0).astype(int).tolist()
    xd_series       = df_weekly["xd_units"].fillna(0).astype(int).tolist()
    gmv_fbm_series  = df_weekly["gmv_fbm_usd"].fillna(0).astype(float).round(0).astype(int).tolist()
    gmv_me2_series  = df_weekly["gmv_me2_usd"].fillna(0).astype(float).round(0).astype(int).tolist()

    seller_labels   = df_sellers["nickname"].fillna(df_sellers["cust_id"].astype(str)).tolist()
    seller_fbm      = df_sellers["fbm_units"].fillna(0).astype(int).tolist()
    seller_stock    = df_sellers["healthy_stock"].fillna(0).astype(int).tolist()
    seller_gmv      = df_sellers["gmv_fbm_usd"].fillna(0).astype(float).round(0).astype(int).tolist()

    kpi = df_kpis.iloc[0]

    return {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "kpis": {
            "fbm_units":      int(kpi.get("total_fbm_units", 0) or 0),
            "me2_units":      int(kpi.get("total_me2_units", 0) or 0),
            "flex_units":     int(kpi.get("total_flex_units", 0) or 0),
            "healthy_stock":  int(kpi.get("total_healthy_stock", 0) or 0),
            "total_stock":    int(kpi.get("total_stock", 0) or 0),
            "gmv_fbm_usd":    int(float(kpi.get("total_gmv_fbm_usd", 0) or 0)),
            "active_sellers": int(kpi.get("active_sellers", 0) or 0),
        },
        "weekly": {
            "labels":    weekly_labels,
            "fbm":       fbm_series,
            "me2":       me2_series,
            "flex":      flex_series,
            "xd":        xd_series,
            "gmv_fbm":   gmv_fbm_series,
            "gmv_me2":   gmv_me2_series,
        },
        "top_sellers": {
            "labels":        seller_labels,
            "fbm_units":     seller_fbm,
            "healthy_stock": seller_stock,
            "gmv_fbm_usd":   seller_gmv,
        },
    }


# ─── GERAÇÃO DO HTML ─────────────────────────────────────────────────────────

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
    --blue:   #2D73F5;
    --orange: #FF7A00;
    --green:  #00C48C;
    --red:    #FF4D4F;
    --bg:     #F4F6FA;
    --card:   #FFFFFF;
    --text:   #1A1A2E;
    --muted:  #7A8099;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          background: var(--bg); color: var(--text); }}

  /* ── Header ── */
  .header {{
    background: var(--blue); color: #fff;
    padding: 18px 32px; display: flex; align-items: center; gap: 16px;
  }}
  .header h1 {{ font-size: 1.3rem; font-weight: 700; }}
  .header .badge {{
    background: rgba(255,255,255,.2); border-radius: 20px;
    padding: 3px 12px; font-size: .75rem; margin-left: auto;
  }}

  /* ── Tabs ── */
  .tabs {{ background: #fff; border-bottom: 2px solid #E8EAEE;
           display: flex; padding: 0 32px; gap: 4px; }}
  .tab {{
    padding: 14px 20px; cursor: pointer; font-size: .9rem;
    color: var(--muted); border-bottom: 3px solid transparent;
    margin-bottom: -2px; transition: all .2s;
  }}
  .tab.active {{ color: var(--blue); border-bottom-color: var(--blue); font-weight: 600; }}
  .tab:hover:not(.active) {{ color: var(--text); }}

  /* ── Content ── */
  .content {{ padding: 28px 32px; }}
  .pane {{ display: none; }}
  .pane.active {{ display: block; }}

  /* ── KPI Cards ── */
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
               gap: 16px; margin-bottom: 28px; }}
  .kpi-card {{
    background: var(--card); border-radius: 12px;
    padding: 20px 22px; box-shadow: 0 1px 4px rgba(0,0,0,.08);
  }}
  .kpi-card .label {{ font-size: .78rem; color: var(--muted); margin-bottom: 6px; }}
  .kpi-card .value {{ font-size: 1.8rem; font-weight: 700; color: var(--text); }}
  .kpi-card .sub   {{ font-size: .75rem; color: var(--muted); margin-top: 4px; }}
  .kpi-card.blue   .value {{ color: var(--blue);   }}
  .kpi-card.orange .value {{ color: var(--orange); }}
  .kpi-card.green  .value {{ color: var(--green);  }}

  /* ── Charts Grid ── */
  .chart-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(460px, 1fr));
                 gap: 20px; }}
  .chart-card {{
    background: var(--card); border-radius: 12px;
    padding: 22px; box-shadow: 0 1px 4px rgba(0,0,0,.08);
  }}
  .chart-card h3 {{ font-size: .95rem; font-weight: 600; margin-bottom: 16px; }}
  .chart-card canvas {{ max-height: 280px; }}

  /* ── Footer ── */
  .footer {{ text-align: center; padding: 20px; font-size: .75rem; color: var(--muted); }}
</style>
</head>
<body>

<div class="header">
  <h1>📦 {title}</h1>
  <span class="badge">Atualizado: <span id="updated"></span></span>
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('overview')">Visão Geral</div>
  <div class="tab" onclick="showTab('semanal')">Semanal</div>
  <div class="tab" onclick="showTab('sellers')">Top Sellers</div>
</div>

<div class="content">

  <!-- ── TAB: Visão Geral ── -->
  <div id="pane-overview" class="pane active">
    <div class="kpi-grid">
      <div class="kpi-card blue">
        <div class="label">Unidades FBM (4 sem)</div>
        <div class="value" id="kpi-fbm">—</div>
        <div class="sub">Fulfillment by Mercado Livre</div>
      </div>
      <div class="kpi-card">
        <div class="label">Unidades ME2 (4 sem)</div>
        <div class="value" id="kpi-me2">—</div>
        <div class="sub">Mercado Envios 2</div>
      </div>
      <div class="kpi-card orange">
        <div class="label">Unidades Flex (4 sem)</div>
        <div class="value" id="kpi-flex">—</div>
        <div class="sub">Entrega própria</div>
      </div>
      <div class="kpi-card green">
        <div class="label">Estoque Saudável</div>
        <div class="value" id="kpi-healthy">—</div>
        <div class="sub">vs <span id="kpi-total-stock">—</span> total</div>
      </div>
      <div class="kpi-card blue">
        <div class="label">GMV FBM USD (4 sem)</div>
        <div class="value" id="kpi-gmv">—</div>
        <div class="sub">Receita FBM em dólares</div>
      </div>
      <div class="kpi-card">
        <div class="label">Sellers Ativos</div>
        <div class="value" id="kpi-sellers">—</div>
        <div class="sub">Últimas 4 semanas</div>
      </div>
    </div>

    <div class="chart-grid">
      <div class="chart-card">
        <h3>Evolução Semanal — Unidades por Modal</h3>
        <canvas id="chart-weekly-overview"></canvas>
      </div>
      <div class="chart-card">
        <h3>Top 10 Sellers — Unidades FBM (4 sem)</h3>
        <canvas id="chart-top-sellers-overview"></canvas>
      </div>
    </div>
  </div>

  <!-- ── TAB: Semanal ── -->
  <div id="pane-semanal" class="pane">
    <div class="chart-grid">
      <div class="chart-card">
        <h3>FBM vs ME2 — Unidades Semanais</h3>
        <canvas id="chart-fbm-me2"></canvas>
      </div>
      <div class="chart-card">
        <h3>Flex + XD — Unidades Semanais</h3>
        <canvas id="chart-flex-xd"></canvas>
      </div>
      <div class="chart-card" style="grid-column: 1 / -1">
        <h3>Todos os Modais — Empilhado</h3>
        <canvas id="chart-stacked"></canvas>
      </div>
      <div class="chart-card" style="grid-column: 1 / -1">
        <h3>GMV Semanal — FBM vs ME2 (USD)</h3>
        <canvas id="chart-gmv-weekly"></canvas>
      </div>
    </div>
  </div>

  <!-- ── TAB: Top Sellers ── -->
  <div id="pane-sellers" class="pane">
    <div class="chart-grid">
      <div class="chart-card">
        <h3>Top 15 Sellers — Unidades FBM</h3>
        <canvas id="chart-sellers-fbm"></canvas>
      </div>
      <div class="chart-card">
        <h3>Top 15 Sellers — Estoque Saudável</h3>
        <canvas id="chart-sellers-stock"></canvas>
      </div>
    </div>
  </div>

</div>

<div class="footer">
  Fonte: meli-bi-data · WHOWNER.DM_SHP_FBM_GROWTH_DETAIL · Gerado em <span id="footer-date"></span>
</div>

<script>
const D = {data_json};

// ── Utilitários ──────────────────────────────────────────────────────────────

function fmtNum(n) {{
  if (n == null) return "—";
  if (n >= 1e6) return (n/1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n/1e3).toFixed(1) + "K";
  return n.toString();
}}

function showTab(name) {{
  document.querySelectorAll(".pane").forEach(p => p.classList.remove("active"));
  document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  document.getElementById("pane-" + name).classList.add("active");
  event.target.classList.add("active");
}}

// ── KPIs ──────────────────────────────────────────────────────────────────────

document.getElementById("updated").textContent       = D.updated_at;
document.getElementById("footer-date").textContent   = D.updated_at;
document.getElementById("kpi-fbm").textContent       = fmtNum(D.kpis.fbm_units);
document.getElementById("kpi-me2").textContent       = fmtNum(D.kpis.me2_units);
document.getElementById("kpi-flex").textContent      = fmtNum(D.kpis.flex_units);
document.getElementById("kpi-healthy").textContent   = fmtNum(D.kpis.healthy_stock);
document.getElementById("kpi-total-stock").textContent = fmtNum(D.kpis.total_stock);
document.getElementById("kpi-gmv").textContent       = fmtNum(D.kpis.gmv_fbm_usd);
document.getElementById("kpi-sellers").textContent   = D.kpis.active_sellers;

// ── Paleta ───────────────────────────────────────────────────────────────────

const BLUE   = "#2D73F5";
const ORANGE = "#FF7A00";
const GREEN  = "#00C48C";
const PURPLE = "#9747FF";
const alpha  = (c, a) => c + Math.round(a*255).toString(16).padStart(2,"0");

// ── Helpers de criação de chart ───────────────────────────────────────────────

function lineChart(id, labels, datasets) {{
  return new Chart(document.getElementById(id), {{
    type: "line",
    data: {{ labels, datasets }},
    options: {{
      responsive: true,
      interaction: {{ mode: "index", intersect: false }},
      plugins: {{ legend: {{ position: "top" }} }},
      scales: {{ y: {{ beginAtZero: true }} }},
    }},
  }});
}}

function barChart(id, labels, datasets, stacked=false) {{
  return new Chart(document.getElementById(id), {{
    type: "bar",
    data: {{ labels, datasets }},
    options: {{
      responsive: true,
      interaction: {{ mode: "index", intersect: false }},
      plugins: {{ legend: {{ position: "top" }} }},
      scales: {{
        x: {{ stacked }},
        y: {{ stacked, beginAtZero: true }},
      }},
    }},
  }});
}}

function mkLine(label, data, color) {{
  return {{
    label, data,
    borderColor: color,
    backgroundColor: alpha(color, 0.15),
    borderWidth: 2,
    pointRadius: 3,
    tension: 0.3,
    fill: true,
  }};
}}

function mkBar(label, data, color) {{
  return {{ label, data, backgroundColor: alpha(color, 0.85), borderRadius: 4 }};
}}

// ── Charts ────────────────────────────────────────────────────────────────────

const wl = D.weekly.labels;
const wf = D.weekly;

// Visão geral — weekly overview
lineChart("chart-weekly-overview", wl, [
  mkLine("FBM",  wf.fbm,  BLUE),
  mkLine("ME2",  wf.me2,  ORANGE),
  mkLine("Flex", wf.flex, GREEN),
]);

// Visão geral — top sellers
barChart("chart-top-sellers-overview",
  D.top_sellers.labels.slice(0,10),
  [mkBar("FBM Units", D.top_sellers.fbm_units.slice(0,10), BLUE)]
);

// Semanal — FBM vs ME2
lineChart("chart-fbm-me2", wl, [
  mkLine("FBM", wf.fbm, BLUE),
  mkLine("ME2", wf.me2, ORANGE),
]);

// Semanal — Flex + XD
lineChart("chart-flex-xd", wl, [
  mkLine("Flex", wf.flex, GREEN),
  mkLine("XD",   wf.xd,  PURPLE),
]);

// Semanal — empilhado
barChart("chart-stacked", wl, [
  mkBar("FBM",  wf.fbm,  BLUE),
  mkBar("ME2",  wf.me2,  ORANGE),
  mkBar("Flex", wf.flex, GREEN),
  mkBar("XD",   wf.xd,  PURPLE),
], true);

// Sellers — FBM
barChart("chart-sellers-fbm", D.top_sellers.labels, [
  mkBar("FBM Units", D.top_sellers.fbm_units, BLUE)
]);

// Sellers — Stock
barChart("chart-sellers-stock", D.top_sellers.labels, [
  mkBar("Estoque Saudável", D.top_sellers.healthy_stock, GREEN)
]);

// Semanal — GMV
lineChart("chart-gmv-weekly", wl, [
  mkLine("GMV FBM USD", wf.gmv_fbm, BLUE),
  mkLine("GMV ME2 USD", wf.gmv_me2, ORANGE),
]);

</script>
</body>
</html>
"""


def generate_html(data: dict, title: str, output_path: str = "index.html"):
    data_json = json.dumps(data, ensure_ascii=False, default=str)
    html = HTML_TEMPLATE.format(title=title, data_json=data_json)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nDashboard gerado: {output_path}")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    print(f"Conectando ao BigQuery ({PROJECT})...")
    client = bigquery.Client(project=PROJECT)

    print("Buscando dados...")
    df_weekly  = run_query(client, QUERY_WEEKLY)
    df_sellers = run_query(client, QUERY_TOP_SELLERS)
    df_kpis    = run_query(client, QUERY_KPIS)

    print("Montando objeto de dados...")
    data = build_data_object(df_weekly, df_sellers, df_kpis)

    print("Gerando HTML...")
    generate_html(data, DASHBOARD_TITLE, output_path="index.html")

    print("\nProximos passos:")
    print("  1. Abra index.html no browser para conferir")
    print("  2. git init && git add . && git commit -m 'dashboard'")
    print("  3. git remote add origin https://github.com/SEU_USER/SEU_REPO.git")
    print("  4. git push -u origin main")
    print("  5. No GitHub: Settings > Pages > Branch: main > Save")
    print("  6. Acesse: https://SEU_USER.github.io/SEU_REPO/")


if __name__ == "__main__":
    main()
