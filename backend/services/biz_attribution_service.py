"""biz_attribution_service.py — 归因数据双源拼接 + 字段对齐

为 routes/biz.py 的 /overview /top-campaigns /campaign-daily 三个 endpoint 提供
"返回结构与 biz_daily_report_repository 一致" 的归因表视图，让原视图通过 ?source=
切换数据源，response 结构完全不变（前端零改动）。

数据源切换：
- 历史日（< today_la）：biz_attribution_ad_daily       (T+1 cohort)
- 当天日（>= today_la）：biz_attribution_ad_intraday   (D0 实时)

混合窗口逻辑：
- end_date < today_la         → 全 daily
- start_date >= today_la      → 全 intraday
- 跨边界                      → daily [start, today-1]  ∪  intraday [today, end]

关键字段映射（normalized → attribution）：
- spend                       → SUM(spend)
- impressions / clicks        → SUM(同名)
- installs (normalized)       → SUM(activation)            (dwd 真实激活事件)
- conversions (normalized)    → SUM(purchase)              (dwd 媒体首充事件)
- revenue (normalized)        → SUM(total_recharge_amount) (数仓口径真实充值)

ROAS / CTR / CPC / CPM / CPI / CPA 公式与 normalized 一致，只是分母 / 分子换源。

注意：top-campaigns / campaign-daily 仅使用 daily 表，因为 intraday 表无 campaign_id
维度（intraday 只到 ad_id）。campaign 级排行 / 趋势对 D0 实时性需求很低，使用
daily-only 也是合理选择。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from db import get_biz_conn

LA_TZ = ZoneInfo("America/Los_Angeles")


# ─────────────────────────────────────────────────────────────
#  helpers
# ─────────────────────────────────────────────────────────────

def _today_la_str() -> str:
    return datetime.now(LA_TZ).strftime("%Y-%m-%d")


def _safe_div(a, b) -> Optional[float]:
    if not b:
        return None
    return round(float(a) / float(b), 6)


def _split_window(start_date: str, end_date: str) -> tuple[Optional[tuple[str, str]], Optional[tuple[str, str]]]:
    """根据 today_la 把 [start, end] 拆成 (daily_window, intraday_window)。

    - end < today_la           → daily=[start, end],          intraday=None
    - start >= today_la        → daily=None,                   intraday=[start, end]
    - 跨边界                   → daily=[start, today-1天],     intraday=[today, end]
    """
    today_la = datetime.now(LA_TZ).date()
    today_str = today_la.strftime("%Y-%m-%d")
    yesterday_str = (today_la - timedelta(days=1)).strftime("%Y-%m-%d")

    if end_date < today_str:
        return (start_date, end_date), None
    if start_date >= today_str:
        return None, (start_date, end_date)
    return (start_date, yesterday_str), (today_str, end_date)


# 前端传入的 platform/account_id 命名风格 → 归因表里实际存的命名风格
# - normalized 的 platform: meta / tiktok
# - attribution 的 platform: facebook / tiktok
# - normalized 的 Meta account_id 带 'act_' 前缀，attribution 不带
_PLATFORM_NORMALIZED_TO_ATTRIBUTION = {
    "meta": "facebook",
    "facebook": "facebook",
    "tiktok": "tiktok",
}


def _normalize_platform(platform: Optional[str]) -> Optional[str]:
    if not platform:
        return None
    return _PLATFORM_NORMALIZED_TO_ATTRIBUTION.get(platform.lower(), platform)


def _normalize_account_id(account_id: Optional[str]) -> Optional[str]:
    """attribution 表存的是无前缀的数字 account_id；前端可能传 'act_xxx'（Meta 风格）"""
    if not account_id:
        return None
    if account_id.startswith("act_"):
        return account_id[4:]
    return account_id


def _build_filter_extra(platform: Optional[str],
                        account_id: Optional[str]) -> tuple[str, list]:
    parts: list[str] = []
    args: list = []
    p = _normalize_platform(platform)
    a = _normalize_account_id(account_id)
    if p:
        parts.append("platform = %s")
        args.append(p)
    if a:
        parts.append("account_id = %s")
        args.append(a)
    sql_part = (" AND " + " AND ".join(parts)) if parts else ""
    return sql_part, args


def _query_one(sql: str, args: list) -> dict:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, args)
        return cur.fetchone() or {}


def _query_all(sql: str, args: list) -> list[dict]:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, args)
        return cur.fetchall() or []


# attribution.platform=facebook → 返回前端时改回 meta（前端把 platform 当筛选 key 用）
_PLATFORM_OUT_EXPR = "CASE WHEN platform = 'facebook' THEN 'meta' ELSE platform END"


# ─────────────────────────────────────────────────────────────
#  /overview  (双源拼接)
# ─────────────────────────────────────────────────────────────

# 注意 stat_date / installs / conversions / revenue 字段名要与 normalized 完全一致
_OVERVIEW_DAILY_SQL = """
SELECT
    COALESCE(SUM(spend), 0)                  AS total_spend,
    COALESCE(SUM(impressions), 0)            AS total_impressions,
    COALESCE(SUM(clicks), 0)                 AS total_clicks,
    COALESCE(SUM(activation), 0)             AS total_installs,
    COALESCE(SUM(purchase), 0)               AS total_conversions,
    COALESCE(SUM(total_recharge_amount), 0)  AS total_revenue
FROM biz_attribution_ad_daily
WHERE ds_account_local BETWEEN %s AND %s
"""

_OVERVIEW_INTRADAY_SQL = """
SELECT
    COALESCE(SUM(spend), 0)                  AS total_spend,
    COALESCE(SUM(impressions), 0)            AS total_impressions,
    COALESCE(SUM(clicks), 0)                 AS total_clicks,
    COALESCE(SUM(activation), 0)             AS total_installs,
    COALESCE(SUM(purchase), 0)               AS total_conversions,
    COALESCE(SUM(total_recharge_amount), 0)  AS total_revenue
FROM biz_attribution_ad_intraday
WHERE ds_account_local BETWEEN %s AND %s
"""


def get_overview(start_date: str, end_date: str, *,
                 platform: Optional[str] = None,
                 account_id: Optional[str] = None) -> dict:
    daily_w, intra_w = _split_window(start_date, end_date)
    extra_sql, extra_args = _build_filter_extra(platform, account_id)

    agg = {
        "total_spend": 0.0,
        "total_impressions": 0,
        "total_clicks": 0,
        "total_installs": 0,
        "total_conversions": 0,
        "total_revenue": 0.0,
    }

    if daily_w:
        row = _query_one(
            _OVERVIEW_DAILY_SQL + extra_sql,
            [daily_w[0], daily_w[1]] + extra_args,
        )
        agg["total_spend"]       += float(row.get("total_spend") or 0)
        agg["total_impressions"] += int(row.get("total_impressions") or 0)
        agg["total_clicks"]      += int(row.get("total_clicks") or 0)
        agg["total_installs"]    += int(row.get("total_installs") or 0)
        agg["total_conversions"] += int(row.get("total_conversions") or 0)
        agg["total_revenue"]     += float(row.get("total_revenue") or 0)

    if intra_w:
        row = _query_one(
            _OVERVIEW_INTRADAY_SQL + extra_sql,
            [intra_w[0], intra_w[1]] + extra_args,
        )
        agg["total_spend"]       += float(row.get("total_spend") or 0)
        agg["total_impressions"] += int(row.get("total_impressions") or 0)
        agg["total_clicks"]      += int(row.get("total_clicks") or 0)
        agg["total_installs"]    += int(row.get("total_installs") or 0)
        agg["total_conversions"] += int(row.get("total_conversions") or 0)
        agg["total_revenue"]     += float(row.get("total_revenue") or 0)

    s   = agg["total_spend"]
    im  = agg["total_impressions"]
    cl  = agg["total_clicks"]
    ins = agg["total_installs"]
    cv  = agg["total_conversions"]
    rv  = agg["total_revenue"]

    agg["avg_ctr"]  = _safe_div(cl, im)
    agg["avg_cpc"]  = _safe_div(s, cl)
    agg["avg_cpm"]  = round(s / im * 1000, 4) if im else None
    agg["avg_cpi"]  = _safe_div(s, ins)
    agg["avg_cpa"]  = _safe_div(s, cv)
    agg["avg_roas"] = _safe_div(rv, s)
    return agg


# ─────────────────────────────────────────────────────────────
#  /top-campaigns  (仅 daily，campaign 维度无 intraday)
# ─────────────────────────────────────────────────────────────

_TOP_CAMPAIGN_METRIC_EXPR = {
    "spend":       "SUM(spend)",
    "revenue":     "SUM(total_recharge_amount)",
    "clicks":      "SUM(clicks)",
    "installs":    "SUM(activation)",
    "conversions": "SUM(purchase)",
    "roas":        "CASE WHEN SUM(spend) > 0 "
                   "THEN SUM(total_recharge_amount) / SUM(spend) ELSE 0 END",
}


def get_top_campaigns(start_date: str, end_date: str, *,
                      platform: Optional[str] = None,
                      account_id: Optional[str] = None,
                      metric: str = "spend",
                      limit: int = 20) -> list[dict]:
    extra_sql, extra_args = _build_filter_extra(platform, account_id)
    order_expr = _TOP_CAMPAIGN_METRIC_EXPR.get(metric, "SUM(spend)")
    limit = min(max(int(limit), 1), 100)

    sql = f"""
    SELECT
        {_PLATFORM_OUT_EXPR}                      AS platform,
        account_id,
        campaign_id,
        MAX(campaign_name)                        AS campaign_name,
        COALESCE(SUM(spend), 0)                   AS total_spend,
        COALESCE(SUM(impressions), 0)             AS total_impressions,
        COALESCE(SUM(clicks), 0)                  AS total_clicks,
        COALESCE(SUM(activation), 0)              AS total_installs,
        COALESCE(SUM(purchase), 0)                AS total_conversions,
        COALESCE(SUM(total_recharge_amount), 0)   AS total_revenue,
        CASE WHEN SUM(spend) > 0
             THEN ROUND(SUM(total_recharge_amount) / SUM(spend), 4)
             ELSE NULL END                         AS avg_roas
    FROM biz_attribution_ad_daily
    WHERE ds_account_local BETWEEN %s AND %s {extra_sql}
    GROUP BY platform, account_id, campaign_id
    ORDER BY {order_expr} DESC
    LIMIT %s
    """
    args = [start_date, end_date] + extra_args + [limit]
    return _query_all(sql, args)


# ─────────────────────────────────────────────────────────────
#  /creative-analysis  /ad-agg  -- 按 ad_id 维度聚合 (仅 daily)
# ─────────────────────────────────────────────────────────────

_AD_AGG_ORDER_MAP = {
    "total_spend":       "total_spend",
    "total_revenue":     "total_revenue",
    "total_impressions": "total_impressions",
    "total_clicks":      "total_clicks",
    "total_installs":    "total_installs",
    "total_conversions": "total_conversions",
    "ctr":               "ctr",
    "cpc":               "cpc",
    "cpm":               "cpm",
    "cpi":               "cpi",
    "cpa":               "cpa",
    "roas":              "roas",
    "ad_name":           "ad_name",
}


def get_ad_aggregated(start_date: str, end_date: str, *,
                      platform: Optional[str] = None,
                      account_id: Optional[str] = None,
                      campaign_id: Optional[str] = None,
                      adgroup_id: Optional[str] = None,
                      name_filter: Optional[str] = None,
                      order_by: str = "total_spend",
                      order_dir: str = "desc") -> list[dict]:
    """ad_id 维度聚合，返回结构与 biz_ad_daily_repository.get_ad_aggregated 兼容"""
    extra_sql, extra_args = _build_filter_extra(platform, account_id)
    order_col = _AD_AGG_ORDER_MAP.get(order_by, "total_spend")
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"

    extra_clauses: list[str] = []
    extra_args2: list = []
    if campaign_id:
        extra_clauses.append("campaign_id = %s")
        extra_args2.append(campaign_id)
    if adgroup_id:
        extra_clauses.append("adgroup_id = %s")
        extra_args2.append(adgroup_id)
    if name_filter:
        extra_clauses.append("ad_name LIKE %s")
        extra_args2.append(f"%{name_filter}%")
    extra2_sql = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    sql = f"""
        SELECT
            {_PLATFORM_OUT_EXPR}                 AS platform,
            account_id,
            campaign_id,
            MAX(campaign_name)                   AS campaign_name,
            adgroup_id,
            MAX(adgroup_name)                    AS adgroup_name,
            ad_id,
            MAX(ad_name)                         AS ad_name,
            SUM(spend)                           AS total_spend,
            SUM(total_recharge_amount)           AS total_revenue,
            SUM(impressions)                     AS total_impressions,
            SUM(clicks)                          AS total_clicks,
            SUM(activation)                      AS total_installs,
            SUM(purchase)                        AS total_conversions,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) / SUM(impressions), 6)
                 ELSE NULL END                   AS ctr,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(spend) / SUM(clicks), 4)
                 ELSE NULL END                   AS cpc,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(spend) / SUM(impressions) * 1000, 4)
                 ELSE NULL END                   AS cpm,
            CASE WHEN SUM(activation) > 0
                 THEN ROUND(SUM(spend) / SUM(activation), 4)
                 ELSE NULL END                   AS cpi,
            CASE WHEN SUM(purchase) > 0
                 THEN ROUND(SUM(spend) / SUM(purchase), 4)
                 ELSE NULL END                   AS cpa,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(total_recharge_amount) / SUM(spend), 4)
                 ELSE NULL END                   AS roas
        FROM biz_attribution_ad_daily
        WHERE ds_account_local BETWEEN %s AND %s {extra_sql} {extra2_sql}
        GROUP BY platform, account_id, campaign_id, adgroup_id, ad_id
        ORDER BY {order_col} {order_dir_l}
    """
    args = [start_date, end_date] + extra_args + extra_args2
    return _query_all(sql, args)


# ─────────────────────────────────────────────────────────────
#  /campaign-agg  /adgroup-agg  -- 操作台聚合视图（仅 daily）
# ─────────────────────────────────────────────────────────────

_CAMPAIGN_AGG_ORDER = {
    "total_spend":       "total_spend",
    "total_revenue":     "total_revenue",
    "total_impressions": "total_impressions",
    "total_clicks":      "total_clicks",
    "total_installs":    "total_installs",
    "total_conversions": "total_conversions",
    "ctr":  "ctr", "cpc":  "cpc", "cpm":  "cpm",
    "cpi":  "cpi", "cpa":  "cpa", "roas": "roas",
    "campaign_name": "campaign_name",
}


def get_campaign_aggregated(start_date: str, end_date: str, *,
                            platform: Optional[str] = None,
                            account_id: Optional[str] = None,
                            campaign_name: Optional[str] = None,
                            order_by: str = "total_spend",
                            order_dir: str = "desc") -> list[dict]:
    """campaign 维度聚合，对齐 biz_daily_report_repository.get_campaign_aggregated 字段"""
    extra_sql, extra_args = _build_filter_extra(platform, account_id)
    order_col = _CAMPAIGN_AGG_ORDER.get(order_by, "total_spend")
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"

    name_sql = ""
    name_args: list = []
    if campaign_name:
        name_sql = " AND campaign_name LIKE %s"
        name_args = [f"%{campaign_name}%"]

    sql = f"""
        SELECT
            {_PLATFORM_OUT_EXPR}                  AS platform,
            account_id,
            campaign_id,
            MAX(campaign_name)                    AS campaign_name,
            SUM(spend)                            AS total_spend,
            SUM(total_recharge_amount)            AS total_revenue,
            SUM(impressions)                      AS total_impressions,
            SUM(clicks)                           AS total_clicks,
            SUM(activation)                       AS total_installs,
            SUM(purchase)                         AS total_conversions,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) / SUM(impressions), 6)
                 ELSE NULL END                    AS ctr,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(spend) / SUM(clicks), 4)
                 ELSE NULL END                    AS cpc,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(spend) / SUM(impressions) * 1000, 4)
                 ELSE NULL END                    AS cpm,
            CASE WHEN SUM(activation) > 0
                 THEN ROUND(SUM(spend) / SUM(activation), 4)
                 ELSE NULL END                    AS cpi,
            CASE WHEN SUM(purchase) > 0
                 THEN ROUND(SUM(spend) / SUM(purchase), 4)
                 ELSE NULL END                    AS cpa,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(total_recharge_amount) / SUM(spend), 4)
                 ELSE NULL END                    AS roas
        FROM biz_attribution_ad_daily
        WHERE ds_account_local BETWEEN %s AND %s {extra_sql} {name_sql}
        GROUP BY platform, account_id, campaign_id
        ORDER BY {order_col} {order_dir_l}
    """
    args = [start_date, end_date] + extra_args + name_args
    return _query_all(sql, args)


def get_adgroup_aggregated(start_date: str, end_date: str, *,
                           platform: Optional[str] = None,
                           account_id: Optional[str] = None,
                           campaign_id: Optional[str] = None,
                           order_by: str = "total_spend",
                           order_dir: str = "desc") -> list[dict]:
    """adgroup 维度聚合，对齐 biz_adgroup_daily_repository.get_adgroup_aggregated 字段"""
    extra_sql, extra_args = _build_filter_extra(platform, account_id)
    order_col = _CAMPAIGN_AGG_ORDER.get(order_by, "total_spend")
    if order_col == "campaign_name":
        order_col = "adgroup_name"
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"

    cid_sql = ""
    cid_args: list = []
    if campaign_id:
        cid_sql = " AND campaign_id = %s"
        cid_args = [campaign_id]

    sql = f"""
        SELECT
            {_PLATFORM_OUT_EXPR}                  AS platform,
            account_id,
            campaign_id,
            MAX(campaign_name)                    AS campaign_name,
            adgroup_id,
            MAX(adgroup_name)                     AS adgroup_name,
            SUM(spend)                            AS total_spend,
            SUM(total_recharge_amount)            AS total_revenue,
            SUM(impressions)                      AS total_impressions,
            SUM(clicks)                           AS total_clicks,
            SUM(activation)                       AS total_installs,
            SUM(purchase)                         AS total_conversions,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) / SUM(impressions), 6)
                 ELSE NULL END                    AS ctr,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(spend) / SUM(clicks), 4)
                 ELSE NULL END                    AS cpc,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(spend) / SUM(impressions) * 1000, 4)
                 ELSE NULL END                    AS cpm,
            CASE WHEN SUM(activation) > 0
                 THEN ROUND(SUM(spend) / SUM(activation), 4)
                 ELSE NULL END                    AS cpi,
            CASE WHEN SUM(purchase) > 0
                 THEN ROUND(SUM(spend) / SUM(purchase), 4)
                 ELSE NULL END                    AS cpa,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(total_recharge_amount) / SUM(spend), 4)
                 ELSE NULL END                    AS roas
        FROM biz_attribution_ad_daily
        WHERE ds_account_local BETWEEN %s AND %s {extra_sql} {cid_sql}
        GROUP BY platform, account_id, campaign_id, adgroup_id
        ORDER BY {order_col} {order_dir_l}
    """
    args = [start_date, end_date] + extra_args + cid_args
    return _query_all(sql, args)


# ─────────────────────────────────────────────────────────────
#  /campaign-daily  (仅 daily)
# ─────────────────────────────────────────────────────────────

# 排序列白名单：兼容前端原有 ?order_by= 取值
_CAMPAIGN_DAILY_ORDER_MAP = {
    "stat_date":     "stat_date",
    "platform":      "platform",
    "campaign_name": "campaign_name",
    "spend":         "spend",
    "impressions":   "impressions",
    "clicks":        "clicks",
    "installs":      "installs",
    "conversions":   "conversions",
    "revenue":       "revenue",
    "ctr":           "ctr",
    "cpc":           "cpc",
    "cpm":           "cpm",
    "cpi":           "cpi",
    "cpa":           "cpa",
    "roas":          "roas",
}


def get_campaign_daily_list(start_date: str, end_date: str, *,
                            platform: Optional[str] = None,
                            account_id: Optional[str] = None,
                            campaign_name: Optional[str] = None,
                            page: int = 1, page_size: int = 20,
                            order_by: str = "stat_date",
                            order_dir: str = "desc") -> dict:
    extra_sql, extra_args = _build_filter_extra(platform, account_id)
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"
    order_col = _CAMPAIGN_DAILY_ORDER_MAP.get(order_by, "stat_date")
    page = max(1, int(page))
    page_size = min(max(1, int(page_size)), 200)

    name_sql = ""
    name_args: list = []
    if campaign_name:
        name_sql = " AND campaign_name LIKE %s"
        name_args = [f"%{campaign_name}%"]

    base_where_sql = (
        "WHERE ds_account_local BETWEEN %s AND %s" + extra_sql + name_sql
    )
    base_args = [start_date, end_date] + extra_args + name_args

    count_sql = f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT 1
            FROM biz_attribution_ad_daily
            {base_where_sql}
            GROUP BY platform, account_id, campaign_id, ds_account_local
        ) t
    """

    list_sql = f"""
        SELECT
            {_PLATFORM_OUT_EXPR}                     AS platform,
            account_id,
            campaign_id,
            MAX(campaign_name)                       AS campaign_name,
            ds_account_local                         AS stat_date,
            COALESCE(SUM(spend), 0)                  AS spend,
            COALESCE(SUM(impressions), 0)            AS impressions,
            COALESCE(SUM(clicks), 0)                 AS clicks,
            COALESCE(SUM(activation), 0)             AS installs,
            COALESCE(SUM(purchase), 0)               AS conversions,
            COALESCE(SUM(total_recharge_amount), 0)  AS revenue,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks) / SUM(impressions), 6)
                 ELSE NULL END                       AS ctr,
            CASE WHEN SUM(clicks) > 0
                 THEN ROUND(SUM(spend) / SUM(clicks), 4)
                 ELSE NULL END                       AS cpc,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(spend) / SUM(impressions) * 1000, 4)
                 ELSE NULL END                       AS cpm,
            CASE WHEN SUM(activation) > 0
                 THEN ROUND(SUM(spend) / SUM(activation), 4)
                 ELSE NULL END                       AS cpi,
            CASE WHEN SUM(purchase) > 0
                 THEN ROUND(SUM(spend) / SUM(purchase), 4)
                 ELSE NULL END                       AS cpa,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(total_recharge_amount) / SUM(spend), 4)
                 ELSE NULL END                       AS roas
        FROM biz_attribution_ad_daily
        {base_where_sql}
        GROUP BY platform, account_id, campaign_id, ds_account_local
        ORDER BY {order_col} {order_dir_l}
        LIMIT %s OFFSET %s
    """

    offset = (page - 1) * page_size

    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(count_sql, base_args)
        total_row = cur.fetchone() or {}
        total = int(total_row.get("cnt") or 0)
        cur.execute(list_sql, base_args + [page_size, offset])
        rows = cur.fetchall() or []

    for r in rows:
        if r.get("stat_date"):
            r["stat_date"] = str(r["stat_date"])

    return {"total": total, "list": rows, "page": page, "page_size": page_size}


# ─────────────────────────────────────────────────────────────
#  data range（最早/最晚 ds，前端默认日期使用）
# ─────────────────────────────────────────────────────────────

def get_data_range_combined() -> dict:
    """daily 表 + intraday 表的合并窗口"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT MIN(ds_la) AS d_min, MAX(ds_la) AS d_max, COUNT(*) AS d_cnt "
            "FROM biz_attribution_ad_daily"
        )
        d = cur.fetchone() or {}
        cur.execute(
            "SELECT MIN(ds_la) AS i_min, MAX(ds_la) AS i_max, COUNT(*) AS i_cnt, "
            "MAX(synced_at) AS i_synced FROM biz_attribution_ad_intraday"
        )
        i = cur.fetchone() or {}
    mins = [v for v in [d.get("d_min"), i.get("i_min")] if v]
    maxs = [v for v in [d.get("d_max"), i.get("i_max")] if v]
    return {
        "min_ds_la":  str(min(mins)) if mins else None,
        "max_ds_la":  str(max(maxs)) if maxs else None,
        "daily_rows":    int(d.get("d_cnt") or 0),
        "intraday_rows": int(i.get("i_cnt") or 0),
        "intraday_last_synced_at": str(i.get("i_synced")) if i.get("i_synced") else None,
    }
