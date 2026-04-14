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
DASHBOARD_TITLE = "Dashboard Iniciativas SDX"
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
    1440191315, 1584758630,   # Cards MLM
]
_CUSTS              = ", ".join(str(c) for c in MAIN_CUSTS)
_SITES_GERAL        = "'MLA', 'MLC', 'MLM', 'MLU'"   # Points & Others
_SITES_GERAL_CARDS  = "'MLA', 'MLC', 'MLM', 'MLU'"    # Cards (inclui MLM)

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
  SIT_SITE_ID                                                           AS site,
  COUNT(*)                                                              AS total_orders,
  SUM(items)                                                            AS total_items,
  COUNTIF(delivered_flag = 1)                                           AS delivered_orders,
  SUM(CASE WHEN delivered_flag = 1 THEN items ELSE 0 END)               AS delivered_items,
  COUNTIF(delivered_flag = 0)                                           AS not_delivered_orders,
  SUM(CASE WHEN delivered_flag = 0 THEN items ELSE 0 END)               AS not_delivered_items
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

# ── QUERIES — ABA 3: Qualidade & SLA (Points & Others) ───────────────────────

QUERY_POINTS_SLA = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH)                       AS month,
    SIT_SITE_ID,
    FLAG_DELIVERED                                            AS is_del,
    COALESCE(ENTREGA_SAMEDAY, 0)                              AS sameday,
    COALESCE(ENTREGA_UP_TO_NEXT_DAY, 0)                       AS next_day,
    COALESCE(ENTREGA_UP_TO_3_DAYS, 0)                         AS up_3d,
    COALESCE(ENTREGA_UP_TO_4_DAYS, 0)                         AS up_4d,
    NULLIF(SLA, '')                                           AS sla,
    NULLIF(SLA_FIRST_VISIT, '')                               AS sla_fv
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
  UNION ALL
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH),
    SIT_SITE_ID,
    FLAG_DELIVERED,
    COALESCE(ENTREGA_SAMEDAY, 0),
    COALESCE(ENTREGA_UP_TO_NEXT_DAY, 0),
    COALESCE(ENTREGA_UP_TO_3_DAYS, 0),
    COALESCE(ENTREGA_UP_TO_4_DAYS, 0),
    NULLIF(SLA, ''),
    NULLIF(SLA_FIRST_VISIT, '')
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL})
)
SELECT
  month,
  SIT_SITE_ID                                                 AS site,
  COUNTIF(is_del=1 AND sameday=1)                            AS speed_sd,
  COUNTIF(is_del=1 AND next_day=1 AND sameday=0)             AS speed_nd,
  COUNTIF(is_del=1 AND up_3d=1  AND next_day=0)              AS speed_2_3d,
  COUNTIF(is_del=1 AND up_4d=1  AND up_3d=0)                 AS speed_4d,
  COUNTIF(is_del=1 AND up_4d=0)                              AS speed_more4d,
  COUNTIF(sla IS NOT NULL)                                   AS sla_total,
  COUNTIF(sla = 'ON TIME')                                   AS sla_on_time,
  COUNTIF(sla = 'EARLY')                                     AS sla_early,
  COUNTIF(sla = 'DELAY')                                     AS sla_delay,
  COUNTIF(sla_fv IS NOT NULL)                                AS sla_fv_total,
  COUNTIF(sla_fv = 'ON TIME')                                AS sla_fv_on_time,
  COUNTIF(sla_fv = 'EARLY')                                  AS sla_fv_early,
  COUNTIF(sla_fv = 'DELAY')                                  AS sla_fv_delay
FROM base
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_POINTS_MOTIVOS = f"""
WITH base AS (
  SELECT SIT_SITE_ID,
    TRIM(UPPER(MOTIVO_NO_ENTREGA_NAME_1)) AS motivo
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND MOTIVO_NO_ENTREGA_NAME_1 IS NOT NULL
    AND MOTIVO_NO_ENTREGA_NAME_1 != ''
  UNION ALL
  SELECT SIT_SITE_ID,
    TRIM(UPPER(MOTIVO_NO_ENTREGA_NAME_1))
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL})
    AND MOTIVO_NO_ENTREGA_NAME_1 IS NOT NULL
    AND MOTIVO_NO_ENTREGA_NAME_1 != ''
)
SELECT SIT_SITE_ID AS site, motivo, COUNT(*) AS cnt
FROM base
GROUP BY 1, 2
ORDER BY 1, 3 DESC
"""

# ── QUERIES — ABA 4: Qualidade & SLA (Cards) ─────────────────────────────────

QUERY_CARDS_SLA = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH)                    AS month,
    SIT_SITE_ID,
    IF(SHP_STATUS_ID='delivered',1,0)                         AS is_del,
    COALESCE(ENTREGA_SAMEDAY, 0)                              AS sameday,
    COALESCE(ENTREGA_UP_TO_NEXT_DAY, 0)                       AS next_day,
    COALESCE(ENTREGA_UP_TO_3_DAYS, 0)                         AS up_3d,
    COALESCE(ENTREGA_UP_TO_4_DAYS, 0)                         AS up_4d,
    NULLIF(SLA, '')                                           AS sla,
    NULLIF(SLA_FIRST_VISIT, '')                               AS sla_fv
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID_MLB`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
  UNION ALL
  SELECT
    DATE_TRUNC(SHP_DATE_CREATED_ID, MONTH),
    SIT_SITE_ID,
    IF(SHP_STATUS_ID='delivered',1,0),
    COALESCE(ENTREGA_SAMEDAY, 0),
    COALESCE(ENTREGA_UP_TO_NEXT_DAY, 0),
    COALESCE(ENTREGA_UP_TO_3_DAYS, 0),
    COALESCE(ENTREGA_UP_TO_4_DAYS, 0),
    NULLIF(SLA, ''),
    NULLIF(SLA_FIRST_VISIT, '')
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL_CARDS})
)
SELECT
  month,
  SIT_SITE_ID                                                 AS site,
  COUNTIF(is_del=1 AND sameday=1)                            AS speed_sd,
  COUNTIF(is_del=1 AND next_day=1 AND sameday=0)             AS speed_nd,
  COUNTIF(is_del=1 AND up_3d=1  AND next_day=0)              AS speed_2_3d,
  COUNTIF(is_del=1 AND up_4d=1  AND up_3d=0)                 AS speed_4d,
  COUNTIF(is_del=1 AND up_4d=0)                              AS speed_more4d,
  COUNTIF(sla IS NOT NULL)                                   AS sla_total,
  COUNTIF(sla = 'ON TIME')                                   AS sla_on_time,
  COUNTIF(sla = 'EARLY')                                     AS sla_early,
  COUNTIF(sla = 'DELAY')                                     AS sla_delay,
  COUNTIF(sla_fv IS NOT NULL)                                AS sla_fv_total,
  COUNTIF(sla_fv = 'ON TIME')                                AS sla_fv_on_time,
  COUNTIF(sla_fv = 'EARLY')                                  AS sla_fv_early,
  COUNTIF(sla_fv = 'DELAY')                                  AS sla_fv_delay
FROM base
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_CARDS_MOTIVOS = f"""
WITH base AS (
  SELECT SIT_SITE_ID,
    TRIM(UPPER(MOTIVO_NO_ENTREGA_NAME_1)) AS motivo
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID_MLB`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND MOTIVO_NO_ENTREGA_NAME_1 IS NOT NULL
    AND MOTIVO_NO_ENTREGA_NAME_1 != ''
  UNION ALL
  SELECT SIT_SITE_ID,
    TRIM(UPPER(MOTIVO_NO_ENTREGA_NAME_1))
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_PREPAID`
  WHERE SHP_DATE_CREATED_ID >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL_CARDS})
    AND MOTIVO_NO_ENTREGA_NAME_1 IS NOT NULL
    AND MOTIVO_NO_ENTREGA_NAME_1 != ''
)
SELECT SIT_SITE_ID AS site, motivo, COUNT(*) AS cnt
FROM base
GROUP BY 1, 2
ORDER BY 1, 3 DESC
"""

# ── QUERIES — ABA 5: FULL (Fulfillment) ──────────────────────────────────────

QUERY_FULL_SALES = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH)                       AS month,
    SIT_SITE_ID,
    FLAG_DELIVERED                                            AS delivered_flag,
    COALESCE(Q_DEVICES, 1)                                    AS items
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SHP_PICKING_TYPE_ID = 'fulfillment'
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
    AND SHP_PICKING_TYPE_ID = 'fulfillment'
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

QUERY_FULL_LT = f"""
WITH base AS (
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH)                       AS month,
    SIT_SITE_ID,
    LEAD_TIME_DIAS_HABILES                                    AS lead_time_days
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SHP_PICKING_TYPE_ID = 'fulfillment'
    AND FLAG_DELIVERED = 1
    AND LEAD_TIME_DIAS_HABILES > 0
    AND LEAD_TIME_DIAS_HABILES < 30
  UNION ALL
  SELECT
    DATE_TRUNC(DATE(ORDER_DATE), MONTH),
    SIT_SITE_ID,
    LEAD_TIME_DIAS_HABILES
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL})
    AND SHP_PICKING_TYPE_ID = 'fulfillment'
    AND FLAG_DELIVERED = 1
    AND LEAD_TIME_DIAS_HABILES > 0
    AND LEAD_TIME_DIAS_HABILES < 30
)
SELECT
  month,
  SIT_SITE_ID                                                 AS site,
  ROUND(AVG(lead_time_days), 2)                               AS avg_lead_time,
  COUNT(*)                                                    AS cnt
FROM base
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_FULL_LT_STATE = f"""
WITH base AS (
  SELECT
    SIT_SITE_ID,
    COALESCE(NULLIF(TRIM(SHP_ADD_STATE_NAME), ''), 'unknown') AS state,
    LEAD_TIME_DIAS_HABILES                                    AS lead_time_days
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS_MLB`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SHP_PICKING_TYPE_ID = 'fulfillment'
    AND FLAG_DELIVERED = 1
    AND LEAD_TIME_DIAS_HABILES > 0
    AND LEAD_TIME_DIAS_HABILES < 30
  UNION ALL
  SELECT
    SIT_SITE_ID,
    COALESCE(NULLIF(TRIM(SHP_ADD_STATE_NAME), ''), 'unknown'),
    LEAD_TIME_DIAS_HABILES
  FROM `meli-bi-data.SBOX_OPER_MP.TBL_LK_SDX_BASE_ORDERS`
  WHERE DATE(ORDER_DATE) >= '{DATE_FROM}'
    AND SHP_SENDER_ID IN ({_CUSTS})
    AND SIT_SITE_ID IN ({_SITES_GERAL})
    AND SHP_PICKING_TYPE_ID = 'fulfillment'
    AND FLAG_DELIVERED = 1
    AND LEAD_TIME_DIAS_HABILES > 0
    AND LEAD_TIME_DIAS_HABILES < 30
)
SELECT
  SIT_SITE_ID                                                 AS site,
  state,
  ROUND(AVG(lead_time_days), 2)                               AS avg_lead_time,
  COUNT(*)                                                    AS cnt
FROM base
GROUP BY 1, 2
ORDER BY 1, 4 DESC
"""

QUERY_FULL_NODO = f"""
SELECT
  SIT_SITE_ID                                                 AS site,
  COALESCE(NULLIF(TRIM(TIPO_NODO_DESAG), ''), 'unknown')      AS tipo_nodo,
  SUM(OUTBOUND)                                               AS total_outbound,
  COUNT(DISTINCT INVENTORY_ID)                                AS skus
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
WHERE CALENDAR_DATE >= '{DATE_FROM}'
  AND CUS_CUST_ID IN ({_CUSTS})
GROUP BY 1, 2
ORDER BY 1, 3 DESC
"""

QUERY_FULL_WAREHOUSE = f"""
SELECT
  SIT_SITE_ID                                                 AS site,
  COALESCE(NULLIF(TRIM(WAREHOUSE_ID), ''), 'unknown')         AS warehouse_id,
  SUM(OUTBOUND)                                               AS total_outbound,
  COUNT(DISTINCT INVENTORY_ID)                                AS skus
FROM `meli-bi-data.WHOWNER.DM_SHP_FBM_STOCK_QUALITY_FC`
WHERE CALENDAR_DATE >= '{DATE_FROM}'
  AND CUS_CUST_ID IN ({_CUSTS})
GROUP BY 1, 2
ORDER BY 1, 3 DESC
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
        rate         = [round(d / t * 100, 1) if t > 0 else 0 for d, t in zip(delivered, sold)]
        not_del      = [int(s.loc[m, "not_delivered_items"]) if m in s.index and "not_delivered_items" in s.columns else 0 for m in all_months]
        failure_rate = [round(n / t * 100, 1) if t > 0 else 0 for n, t in zip(not_del, sold)]

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
            "failure_rate":  failure_rate,
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


# ── MONTAGEM — Qualidade & SLA ────────────────────────────────────────────────

def build_sla_data(df_sla: pd.DataFrame, df_motivos: pd.DataFrame) -> dict:
    df_sla = df_sla.copy()
    df_sla["month"] = df_sla["month"].astype(str)

    all_months = sorted(df_sla["month"].unique())
    all_sites  = sorted(df_sla["site"].unique())

    def get(row, col):
        v = row.get(col, 0)
        return int(v) if pd.notna(v) else 0

    by_site = {}
    for site in all_sites:
        s = df_sla[df_sla["site"] == site].set_index("month")

        def mg(col, m):
            if m not in s.index: return 0
            return int(s.loc[m, col]) if pd.notna(s.loc[m, col]) else 0

        speed, sla_on, sla_ea, sla_dl, fv_on, fv_dl = [], [], [], [], [], []
        for m in all_months:
            sd  = mg("speed_sd", m);  nd  = mg("speed_nd", m)
            d23 = mg("speed_2_3d", m); d4 = mg("speed_4d", m); dm = mg("speed_more4d", m)
            t  = mg("sla_total", m);   tf = mg("sla_fv_total", m)
            speed.append({"sd":sd,"nd":nd,"d23":d23,"d4":d4,"dm":dm})
            sla_on.append(round(mg("sla_on_time",m)/max(t,1)*100, 1))
            sla_ea.append(round(mg("sla_early",m)  /max(t,1)*100, 1))
            sla_dl.append(round(mg("sla_delay",m)  /max(t,1)*100, 1))
            fv_on.append( round(mg("sla_fv_on_time",m)/max(tf,1)*100, 1))
            fv_dl.append( round(mg("sla_fv_delay",m)  /max(tf,1)*100, 1))

        motivos = []
        if df_motivos is not None:
            sub = df_motivos[df_motivos["site"] == site].sort_values("cnt", ascending=False).head(10)
            motivos = [{"label": str(r["motivo"]), "cnt": int(r["cnt"])} for _, r in sub.iterrows()]

        by_site[site] = {
            "speed":            speed,
            "sla_on_time_pct":  sla_on,
            "sla_early_pct":    sla_ea,
            "sla_delay_pct":    sla_dl,
            "sla_fv_on_time_pct": fv_on,
            "sla_fv_delay_pct":   fv_dl,
            "motivos":          motivos,
        }

    total_t  = int(df_sla["sla_total"].sum())
    total_on = int(df_sla["sla_on_time"].sum())
    total_dl = int(df_sla["sla_delay"].sum())
    total_fv = int(df_sla["sla_fv_total"].sum())
    total_fv_on = int(df_sla["sla_fv_on_time"].sum())

    top_motivo = "N/A"
    if df_motivos is not None and len(df_motivos) > 0:
        top = df_motivos.groupby("motivo")["cnt"].sum().sort_values(ascending=False)
        if len(top) > 0:
            top_motivo = str(top.index[0])[:35]

    return {
        "labels":  all_months,
        "sites":   all_sites,
        "by_site": by_site,
        "kpis": {
            "on_time_pct":    round(total_on / max(total_t, 1) * 100, 1),
            "delay_pct":      round(total_dl / max(total_t, 1) * 100, 1),
            "fv_on_time_pct": round(total_fv_on / max(total_fv, 1) * 100, 1),
            "top_motivo":     top_motivo,
        },
    }


# ── MONTAGEM — FULL (Fulfillment) ────────────────────────────────────────────

def build_full_data(df_sales, df_lt, df_lt_state, df_nodo, df_warehouse):
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

        sent      = [int(s.loc[m, "total_items"])    if m in s.index else 0 for m in all_months]
        delivered = [int(s.loc[m, "delivered_items"]) if m in s.index else 0 for m in all_months]

        lt_avg = []
        for m in all_months:
            sub = lt[lt["month"] == m]
            lt_avg.append(wavg(sub, "avg_lead_time", "cnt") if len(sub) > 0 else None)

        lt_by_state = []
        if df_lt_state is not None:
            sub = df_lt_state[df_lt_state["site"] == site].sort_values("cnt", ascending=False).head(5)
            lt_by_state = [{"state": str(r["state"]), "avg_lt": float(r["avg_lead_time"]), "cnt": int(r["cnt"])}
                           for _, r in sub.iterrows()]

        tipo_nodo = []
        if df_nodo is not None:
            sub = df_nodo[df_nodo["site"] == site].sort_values("total_outbound", ascending=False)
            tipo_nodo = [{"label": str(r["tipo_nodo"]), "val": int(r["total_outbound"]), "skus": int(r["skus"])}
                         for _, r in sub.iterrows()]

        warehouse = []
        if df_warehouse is not None:
            sub = df_warehouse[df_warehouse["site"] == site].sort_values("total_outbound", ascending=False).head(20)
            warehouse = [{"label": str(r["warehouse_id"]), "val": int(r["total_outbound"]), "skus": int(r["skus"])}
                         for _, r in sub.iterrows()]

        by_site[site] = {
            "sent":         sent,
            "delivered":    delivered,
            "lead_time":    lt_avg,
            "lt_by_state":  lt_by_state,
            "tipo_nodo":    tipo_nodo,
            "warehouse":    warehouse,
        }

    total_sent = int(df_sales["total_items"].sum())
    total_del  = int(df_sales["delivered_items"].sum())
    overall_lt = wavg(df_lt, "avg_lead_time", "cnt") or 0

    return {
        "labels":  all_months,
        "sites":   all_sites,
        "by_site": by_site,
        "kpis": {
            "total_sent":      total_sent,
            "total_delivered": total_del,
            "delivery_rate":   round(total_del / max(total_sent, 1) * 100, 1),
            "avg_lead_time":   overall_lt,
            "active_sites":    len(all_sites),
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
.kpi.c-red    .val{{ color:var(--red); }}
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
  <h1>Dashboard Iniciativas SDX</h1>
  <span class="badge">Atualizado: <span id="updated"></span></span>
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('points', this)">Visao Geral Points &amp; Others</div>
  <div class="tab"        onclick="showTab('cards',  this)">Visao Geral Cards</div>
  <div class="tab"        onclick="showTab('sla3',   this)">Qualidade &amp; SLA — Points</div>
  <div class="tab"        onclick="showTab('sla4',   this)">Qualidade &amp; SLA — Cards</div>
  <div class="tab"        onclick="showTab('full',   this)">FULL</div>
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
  <div class="section-title">Grafico 2 — Taxa de Not Delivered por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Taxa de Not Delivered (%) por Mes e Pais — status = not_delivered</h3><canvas id="p-c2-rate"></canvas></div>
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

<!-- ── ABA 3: Qualidade & SLA — Points ── -->
<div id="pane-sla3" class="pane">
  <div class="kpi-grid">
    <div class="kpi c-green"> <div class="lbl">% No Prazo (SLA)</div>      <div class="val" id="s3-k-ontime"></div> <div class="sub">ON TIME sobre total</div></div>
    <div class="kpi c-red">   <div class="lbl">% Com Atraso</div>           <div class="val" id="s3-k-delay"></div>  <div class="sub">DELAY sobre total</div></div>
    <div class="kpi c-blue">  <div class="lbl">% No Prazo (1a Visita)</div> <div class="val" id="s3-k-fv"></div>    <div class="sub">SLA_FIRST_VISIT ON TIME</div></div>
    <div class="kpi c-orange"><div class="lbl">Top Motivo Nao Entrega</div> <div class="val" style="font-size:1rem" id="s3-k-mot"></div><div class="sub">Mais frequente em 2026</div></div>
  </div>
  <div class="section-title">Grafico 1 — Mix de Velocidade de Entrega por Pais</div>
  <div class="chart-grid" id="s3-speed"></div>
  <div class="section-title">Grafico 2 — Performance de SLA por Mes e Pais</div>
  <div class="chart-grid">
    <div class="chart-card"><h3>% No Prazo (ON TIME) por Pais</h3><canvas id="s3-sla-ontime"></canvas></div>
    <div class="chart-card"><h3>% Atraso (DELAY) por Pais</h3>    <canvas id="s3-sla-delay"></canvas></div>
  </div>
  <div class="section-title">Grafico 3 — Principais Motivos de Nao Entrega (por Pais)</div>
  <div class="chart-grid" id="s3-motivos"></div>
  <div class="section-title">Grafico 4 — SLA Entrega vs 1a Visita (por Pais)</div>
  <div class="chart-grid" id="s3-fv"></div>
</div>

<!-- ── ABA 4: Qualidade & SLA — Cards ── -->
<div id="pane-sla4" class="pane">
  <div class="kpi-grid">
    <div class="kpi c-green"> <div class="lbl">% No Prazo (SLA)</div>      <div class="val" id="s4-k-ontime"></div> <div class="sub">ON TIME sobre total</div></div>
    <div class="kpi c-red">   <div class="lbl">% Com Atraso</div>           <div class="val" id="s4-k-delay"></div>  <div class="sub">DELAY sobre total</div></div>
    <div class="kpi c-blue">  <div class="lbl">% No Prazo (1a Visita)</div> <div class="val" id="s4-k-fv"></div>    <div class="sub">SLA_FIRST_VISIT ON TIME</div></div>
    <div class="kpi c-orange"><div class="lbl">Top Motivo Nao Entrega</div> <div class="val" style="font-size:1rem" id="s4-k-mot"></div><div class="sub">Mais frequente em 2026</div></div>
  </div>
  <div class="section-title">Grafico 1 — Mix de Velocidade de Entrega por Pais</div>
  <div class="chart-grid" id="s4-speed"></div>
  <div class="section-title">Grafico 2 — Performance de SLA por Mes e Pais</div>
  <div class="chart-grid">
    <div class="chart-card"><h3>% No Prazo (ON TIME) por Pais</h3><canvas id="s4-sla-ontime"></canvas></div>
    <div class="chart-card"><h3>% Atraso (DELAY) por Pais</h3>    <canvas id="s4-sla-delay"></canvas></div>
  </div>
  <div class="section-title">Grafico 3 — Principais Motivos de Nao Entrega (por Pais)</div>
  <div class="chart-grid" id="s4-motivos"></div>
  <div class="section-title">Grafico 4 — SLA Entrega vs 1a Visita (por Pais)</div>
  <div class="chart-grid" id="s4-fv"></div>
</div>

<!-- ── ABA 5: FULL (Fulfillment) ── -->
<div id="pane-full" class="pane">
  <div class="kpi-grid">
    <div class="kpi c-blue">  <div class="lbl">Itens Enviados (2026)</div>    <div class="val" id="f-k-sent"></div>  <div class="sub">SHP_PICKING_TYPE = fulfillment</div></div>
    <div class="kpi c-green"> <div class="lbl">Itens Entregues (2026)</div>   <div class="val" id="f-k-del"></div>   <div class="sub">FLAG_DELIVERED = 1</div></div>
    <div class="kpi c-orange"><div class="lbl">Taxa de Entrega</div>           <div class="val" id="f-k-rate"></div>  <div class="sub">Entregues / Enviados</div></div>
    <div class="kpi c-purple"><div class="lbl">Lead Time Medio</div>           <div class="val" id="f-k-lt"></div>    <div class="sub">Dias habeis (entregues)</div></div>
    <div class="kpi">         <div class="lbl">Paises Ativos</div>             <div class="val" id="f-k-sites"></div> <div class="sub">Sites com dados 2026</div></div>
  </div>
  <div class="section-title">Grafico 1 — Itens Enviados por Fulfillment por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Itens Enviados — Fulfillment</h3><canvas id="f-c1"></canvas></div>
  </div>
  <div class="section-title">Grafico 2 — Itens Entregues por Fulfillment por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Itens Entregues — Fulfillment</h3><canvas id="f-c2"></canvas></div>
  </div>
  <div class="section-title">Grafico 3 — Lead Time Medio por Mes (por Pais)</div>
  <div class="chart-grid cols-1">
    <div class="chart-card"><h3>Lead Time Medio — Fulfillment (Dias Habeis)</h3><canvas id="f-c3"></canvas></div>
  </div>
  <div class="section-title">Grafico 4 — Lead Time por Estado de Destino (por Pais)</div>
  <div class="chart-grid" id="f-c4"></div>
  <div class="section-title">Grafico 5 — Tipo de Armazem (TIPO_NODO_DESAG) por Pais</div>
  <div class="chart-grid" id="f-c5"></div>
  <div class="section-title">Grafico 6 — Origem do Armazem (WAREHOUSE_ID) por Pais</div>
  <div class="chart-grid" id="f-c6"></div>
</div>

</div><!-- /content -->
<div class="footer">Fonte: meli-bi-data · SBOX_OPER_MP · WHOWNER · a partir de {date_from} · <span id="footer-dt"></span></div>

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
const COLORS      = ["#2D73F5","#FF7A00","#00C48C","#9747FF","#FF4D4F","#607D8B","#FFC107","#8BC34A"];
function sc(site){{ return SITE_COLORS[site]||"#607D8B"; }}
function ax(c,a){{ return c+Math.round(a*255).toString(16).padStart(2,"0"); }}

// ── Builders de dataset ───────────────────────────────────────────────────────
function mkLine(label,data,c,fill=false){{
  return{{label,data,borderColor:c,backgroundColor:fill?ax(c,0.12):"transparent",
    borderWidth:2,pointRadius:3,tension:0.35,spanGaps:true,fill}};
}}
function mkBar(label,data,c){{
  return{{label,data,backgroundColor:ax(c,0.75),borderColor:c,borderWidth:1}};
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
function renderTab(prefix, tabData, rateKey="delivery_rate"){{
  const ml    = tabData.labels;
  const bs    = tabData.by_site;
  const sites = tabData.sites;

  // Grafico 1
  lineChart(prefix+"-c1-sold", ml, sites.map(s=>mkLine(s, bs[s].sold,      sc(s))), "Itens");
  lineChart(prefix+"-c1-del",  ml, sites.map(s=>mkLine(s, bs[s].delivered, sc(s))), "Itens");

  // Grafico 2
  lineChart(prefix+"-c2-rate", ml, sites.map(s=>mkLine(s, bs[s][rateKey], sc(s), true)), "%");

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

// ── SLA helpers ───────────────────────────────────────────────────────────────
function mkCard(container, title){{
  const d = document.createElement("div");
  d.className = "chart-card";
  d.innerHTML = `<h3>${{title}}</h3><canvas></canvas>`;
  container.appendChild(d);
  return d;
}}
function barChartEl(el, labels, datasets, stacked=false){{
  return new Chart(el,{{
    type:"bar", data:{{labels,datasets}},
    options:{{responsive:true,interaction:{{mode:"index",intersect:false}},
      plugins:{{legend:{{position:"top"}}}},
      scales:{{x:{{stacked}},y:{{stacked,beginAtZero:true}}}}
    }}
  }});
}}
function hbarChartEl(el, labels, datasets){{
  return new Chart(el,{{
    type:"bar", data:{{labels,datasets}},
    options:{{responsive:true,indexAxis:"y",
      plugins:{{legend:{{display:false}}}},
      scales:{{x:{{beginAtZero:true}}}}
    }}
  }});
}}

const SPEED_LABELS = ["Mesmo Dia","Dia Seguinte","2-3 Dias","4 Dias","+4 Dias"];
const SPEED_COLS   = ["sd","nd","d23","d4","dm"];
const SPEED_COLORS = [COLORS[2],COLORS[0],COLORS[1],COLORS[3],COLORS[4]];

function renderSlaTab(P, D){{
  const ml = D.labels, sites = D.sites, bs = D.by_site;

  // KPIs
  document.getElementById(P+"-k-ontime").textContent = D.kpis.on_time_pct+"%";
  document.getElementById(P+"-k-delay") .textContent = D.kpis.delay_pct+"%";
  document.getElementById(P+"-k-fv")    .textContent = D.kpis.fv_on_time_pct+"%";
  document.getElementById(P+"-k-mot")   .textContent = D.kpis.top_motivo;

  // Grafico 1 — speed mix (stacked bar por pais)
  const cSpd = document.getElementById(P+"-speed");
  sites.forEach(site => {{
    const card = mkCard(cSpd, "Mix Velocidade — "+site);
    const datasets = SPEED_COLS.map((k,i) =>
      mkBar(SPEED_LABELS[i], bs[site].speed.map(s=>s[k]), SPEED_COLORS[i]));
    barChartEl(card.querySelector("canvas"), ml, datasets, true);
  }});

  // Grafico 2 — SLA % linhas
  lineChart(P+"-sla-ontime", ml,
    sites.map(s => mkLine(s, bs[s].sla_on_time_pct, sc(s), true)), "%");
  lineChart(P+"-sla-delay",  ml,
    sites.map(s => mkLine(s, bs[s].sla_delay_pct,   sc(s))), "%");

  // Grafico 3 — motivos horizontal bar por pais
  const cMot = document.getElementById(P+"-motivos");
  sites.forEach(site => {{
    const mot = bs[site].motivos;
    if(!mot.length) return;
    const card = mkCard(cMot, "Motivos Nao Entrega — "+site);
    card.querySelector("canvas").style.maxHeight = "300px";
    hbarChartEl(card.querySelector("canvas"),
      mot.map(m=>m.label).reverse(),
      [mkBar("Ocorrencias", mot.map(m=>m.cnt).reverse(), sc(site))]);
  }});

  // Grafico 4 — SLA vs 1a visita por pais
  const cFv = document.getElementById(P+"-fv");
  sites.forEach(site => {{
    const card = mkCard(cFv, "SLA Entrega vs 1a Visita — "+site);
    lineChartEl(card.querySelector("canvas"), ml, [
      mkLine("No Prazo — Entrega",  bs[site].sla_on_time_pct,    COLORS[0], true),
      mkLine("No Prazo — 1a Visita",bs[site].sla_fv_on_time_pct, COLORS[2]),
      mkLine("Atraso — Entrega",    bs[site].sla_delay_pct,      COLORS[4]),
    ], "%");
  }});
}}

// ── FULL tab ──────────────────────────────────────────────────────────────────
function renderFullTab(D){{
  const ml = D.labels, sites = D.sites, bs = D.by_site;

  // KPIs
  document.getElementById("f-k-sent") .textContent = fmtN(D.kpis.total_sent);
  document.getElementById("f-k-del")  .textContent = fmtN(D.kpis.total_delivered);
  document.getElementById("f-k-rate") .textContent = D.kpis.delivery_rate+"%";
  document.getElementById("f-k-lt")   .textContent = D.kpis.avg_lead_time+"d";
  document.getElementById("f-k-sites").textContent = D.kpis.active_sites;

  // Grafico 1 — enviados
  lineChart("f-c1", ml, sites.map(s=>mkLine(s, bs[s].sent,      sc(s))), "Itens");
  // Grafico 2 — entregues
  lineChart("f-c2", ml, sites.map(s=>mkLine(s, bs[s].delivered, sc(s))), "Itens");
  // Grafico 3 — lead time
  lineChart("f-c3", ml, sites.map(s=>mkLine(s, bs[s].lead_time, sc(s))), "Dias habeis");

  // Grafico 4 — LT por estado (hbar por pais)
  const c4 = document.getElementById("f-c4");
  sites.forEach(site => {{
    const data = bs[site].lt_by_state;
    if(!data.length) return;
    const card = mkCard(c4, "Lead Time por Estado — "+site);
    card.querySelector("canvas").style.maxHeight = "400px";
    hbarChartEl(card.querySelector("canvas"),
      data.map(d=>d.state).reverse(),
      [mkBar("Dias habeis", data.map(d=>d.avg_lt).reverse(), sc(site))]);
  }});

  // Grafico 5 — tipo_nodo (hbar por pais)
  const c5 = document.getElementById("f-c5");
  sites.forEach(site => {{
    const data = bs[site].tipo_nodo;
    if(!data.length) return;
    const card = mkCard(c5, "Tipo Armazem — "+site);
    hbarChartEl(card.querySelector("canvas"),
      data.map(d=>d.label).reverse(),
      [mkBar("Outbound", data.map(d=>d.val).reverse(), sc(site))]);
  }});

  // Grafico 6 — warehouse (hbar por pais)
  const c6 = document.getElementById("f-c6");
  sites.forEach(site => {{
    const data = bs[site].warehouse;
    if(!data.length) return;
    const card = mkCard(c6, "Warehouse ID — "+site);
    card.querySelector("canvas").style.maxHeight = "400px";
    hbarChartEl(card.querySelector("canvas"),
      data.map(d=>d.label).reverse(),
      [mkBar("Outbound", data.map(d=>d.val).reverse(), sc(site))]);
  }});
}}

// ── Init ──────────────────────────────────────────────────────────────────────
document.getElementById("updated")  .textContent = D.updated_at;
document.getElementById("footer-dt").textContent = D.updated_at;

setKpis("p", D.points.kpis);
setKpis("c", D.cards.kpis);
renderTab("p", D.points, "failure_rate");
renderTab("c", D.cards);
renderSlaTab("s3", D.sla_points);
renderSlaTab("s4", D.sla_cards);
renderFullTab(D.full);
</script>
</body>
</html>
"""


# ── GERAR HTML ────────────────────────────────────────────────────────────────

def generate_html(points_data, cards_data, sla_points, sla_cards, full_data, title, output="index.html"):
    payload = {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "points":     points_data,
        "cards":      cards_data,
        "sla_points": sla_points,
        "sla_cards":  sla_cards,
        "full":       full_data,
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

    print("\n[ABA 1] Points & Others — visao geral...")
    df_p_sales = run_query(client, QUERY_POINTS_SALES, "points-vendas")
    df_p_lt    = run_query(client, QUERY_POINTS_LT,    "points-leadtime")

    print("\n[ABA 2] Cards — visao geral...")
    df_c_sales = run_query(client, QUERY_CARDS_SALES,  "cards-vendas")
    df_c_lt    = run_query(client, QUERY_CARDS_LT,     "cards-leadtime")

    print("\n[ABA 3] Points — qualidade & SLA...")
    df_p_sla    = run_query(client, QUERY_POINTS_SLA,    "points-sla")
    df_p_motivos= run_query(client, QUERY_POINTS_MOTIVOS,"points-motivos")

    print("\n[ABA 4] Cards — qualidade & SLA...")
    df_c_sla    = run_query(client, QUERY_CARDS_SLA,     "cards-sla")
    df_c_motivos= run_query(client, QUERY_CARDS_MOTIVOS, "cards-motivos")

    print("\n[ABA 5] FULL (Fulfillment)...")
    df_f_sales    = run_query(client, QUERY_FULL_SALES,    "full-vendas")
    df_f_lt       = run_query(client, QUERY_FULL_LT,       "full-leadtime")
    df_f_lt_state = run_query(client, QUERY_FULL_LT_STATE, "full-lt-estado")
    df_f_nodo     = run_query(client, QUERY_FULL_NODO,     "full-nodo")
    df_f_warehouse= run_query(client, QUERY_FULL_WAREHOUSE,"full-warehouse")

    print("\nMontando dados...")
    points_data = build_tab_data(df_p_sales, df_p_lt)
    cards_data  = build_tab_data(df_c_sales, df_c_lt)
    sla_points  = build_sla_data(df_p_sla, df_p_motivos)
    sla_cards   = build_sla_data(df_c_sla, df_c_motivos)
    full_data   = build_full_data(df_f_sales, df_f_lt, df_f_lt_state, df_f_nodo, df_f_warehouse)

    print("Gerando HTML...")
    generate_html(points_data, cards_data, sla_points, sla_cards, full_data, DASHBOARD_TITLE)

    print("\nPara publicar:")
    print("  git add index.html && git commit -m 'abas 3 e 4 SLA' && git push")


if __name__ == "__main__":
    main()
