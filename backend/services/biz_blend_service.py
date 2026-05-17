"""biz_blend_service.py — Legacy 投放指标 + Attribution 真值业务结果

为 routes/biz.py 的 7 个 endpoint 提供 "投放指标用 normalized + 业务结果用 attribution"
的混合视图，让前端通过 ?source=blend 切换。response 结构与 normalized 完全一致。

字段拼接规则：
- spend / impressions / clicks / installs / ctr / cpc / cpm / cpi → normalized（保留 TikTok / Meta 全量覆盖）
- conversions     → attribution.purchase                  (数仓真实付费用户数)
- revenue         → attribution.total_recharge_amount     (数仓真实充值)
- registrations   → attribution.registration              (数仓媒体口径注册事件数)
- 衍生 cpa / roas → 用 blend 后分子分母重算

attribution 双源策略（daily + intraday，daily 优先）：
- daily 表：MaxCompute T+1 cohort（粒度 campaign/adgroup/ad 全有），数据稳定但延迟 1 天起
- intraday 表：ClickHouse 实时（粒度只到 ad_id，每 30 分钟同步），D0 当天就能看
- 同一 (date, ad_id) 同时存在时取 daily；daily 缺失才用 intraday → 当 daily 上游 SLA 滞后
  时（如 metis_dw.ads_ad_delivery_di 分区延迟），intraday 自动兜底，前端不再显示 0

JOIN 策略：
- normalized 是主表（LEFT），attribution 子查询作为副表
- 关联键：stat_date / platform_norm / account_id_norm / [campaign_id|adgroup_id|ad_id]
- platform 归一：attribution.facebook → normalized.meta
- account_id 归一：attribution 不带 act_ 前缀 → normalized.act_xxx (Meta)
- intraday 没有 campaign_id/adgroup_id 列，需通过 biz_ad_daily_normalized 反推上推聚合
- attribution 都没匹配上的行：conversions / revenue 为 0（前端可视化为"未归因"）
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from db import get_biz_conn

LA_TZ = ZoneInfo("America/Los_Angeles")


# ─────────────────────────────────────────────────────────────
#  helpers
# ─────────────────────────────────────────────────────────────

def _safe_div(a, b) -> Optional[float]:
    if not b:
        return None
    return round(float(a) / float(b), 6)


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


def _build_filter_n(platform: Optional[str],
                    account_id: Optional[str]) -> tuple[str, list]:
    """构造 normalized 主表的 WHERE 过滤片段（前端口径直接用）"""
    parts: list[str] = []
    args: list = []
    if platform:
        parts.append("n.platform = %s")
        args.append(platform.lower())
    if account_id:
        parts.append("n.account_id = %s")
        args.append(account_id)
    sql_part = (" AND " + " AND ".join(parts)) if parts else ""
    return sql_part, args


# attribution 子查询的 platform / account_id 归一化映射列
# attribution.platform = facebook → normalized.platform = meta
# attribution.account_id（无前缀）+ Meta → 加 'act_' 前缀对齐 normalized
_ATTR_PLAT_EXPR = "CASE WHEN platform = 'facebook' THEN 'meta' ELSE platform END"
_ATTR_ACC_EXPR  = (
    "CASE WHEN platform = 'facebook' AND account_id NOT LIKE 'act\\_%%' "
    "THEN CONCAT('act_', account_id) ELSE account_id END"
)


def _plat_expr(alias: str) -> str:
    """带表别名的 platform 归一表达式（attribution.facebook → meta）"""
    p = f"{alias}." if alias else ""
    return f"CASE WHEN {p}platform = 'facebook' THEN 'meta' ELSE {p}platform END"


def _acc_expr(alias: str) -> str:
    """带表别名的 account_id 归一表达式（Meta 加 act_ 前缀）"""
    p = f"{alias}." if alias else ""
    return (
        f"CASE WHEN {p}platform = 'facebook' AND {p}account_id NOT LIKE 'act\\_%%' "
        f"THEN CONCAT('act_', {p}account_id) ELSE {p}account_id END"
    )


def _attr_filter_args(platform: Optional[str],
                      account_id: Optional[str],
                      alias: str = "") -> tuple[str, list]:
    """构造 attribution 子查询内的 WHERE 过滤片段，
    把前端口径转换为 attribution 表存储口径，减小 JOIN 集合"""
    p = f"{alias}." if alias else ""
    parts: list[str] = []
    args: list = []
    if platform:
        pl = platform.lower()
        attr_p = "facebook" if pl == "meta" else pl
        parts.append(f"{p}platform = %s")
        args.append(attr_p)
    if account_id:
        attr_a = account_id[4:] if account_id.startswith("act_") else account_id
        parts.append(f"{p}account_id = %s")
        args.append(attr_a)
    sql_part = (" AND " + " AND ".join(parts)) if parts else ""
    return sql_part, args


# ─────────────────────────────────────────────────────────────
#  attribution 双源子查询 helper（daily 优先 + intraday 兜底）
# ─────────────────────────────────────────────────────────────

def _attr_subquery(grain: str,
                   start_date: str,
                   end_date: str,
                   platform: Optional[str],
                   account_id: Optional[str]) -> tuple[str, list]:
    """生成 attribution 子查询：daily 优先，intraday 兜底（按 (date, ad_id) 去重）。

    输出列：
        d, plat, acc, key_id,
        attr_conversions, attr_registrations, attr_revenue,
        attr_spend,  -- attribution 数仓侧自报的 spend，用于"无归因覆盖"判定
        attr_cum_d0, attr_cum_d7, attr_cum_d30
            -- cohort 窗口累计充值（来自 daily 行；intraday 兜底行仅 attr_cum_d0≈实时充值，D7/D30 为 NULL）

    Args:
        grain: "campaign" | "adgroup" | "ad"
        start_date / end_date: 查询窗口
        platform / account_id: 前端口径过滤条件

    Returns:
        (sql_fragment, args_list) — sql 已包含 daily + intraday 两段日期 placeholder，
        args 顺序为：[start, end] + daily_filter_args + [start, end] + intraday_filter_args
    """
    if grain == "campaign":
        daily_grain_col = "campaign_id"
    elif grain == "adgroup":
        daily_grain_col = "adgroup_id"
    elif grain == "ad":
        daily_grain_col = "ad_id"
    else:
        raise ValueError(f"unknown grain: {grain}")

    a_filter_d, a_args_d = _attr_filter_args(platform, account_id, alias="")
    a_filter_i, a_args_i = _attr_filter_args(platform, account_id, alias="i")

    # daily 部分：直接按粒度取行（外层 GROUP BY 再 SUM）
    daily_part = f"""
        SELECT
            ds_account_local        AS d,
            {_ATTR_PLAT_EXPR}       AS plat,
            {_ATTR_ACC_EXPR}        AS acc,
            {daily_grain_col}       AS key_id,
            purchase                AS attr_conversions,
            registration            AS attr_registrations,
            total_recharge_amount   AS attr_revenue,
            spend                   AS attr_spend,
            cum_recharge_1d         AS attr_cum_d0,
            cum_recharge_7d         AS attr_cum_d7,
            cum_recharge_30d        AS attr_cum_d30
        FROM biz_attribution_ad_daily
        WHERE ds_account_local BETWEEN %s AND %s
          AND {daily_grain_col} <> '' {a_filter_d}
    """

    if grain == "ad":
        # ad 粒度：直接按 intraday.ad_id（不需要反推 normalized）
        intraday_part = f"""
            SELECT
                i.ds_account_local      AS d,
                {_plat_expr("i")}       AS plat,
                {_acc_expr("i")}        AS acc,
                i.ad_id                 AS key_id,
                i.purchase              AS attr_conversions,
                i.registration          AS attr_registrations,
                i.total_recharge_amount AS attr_revenue,
                i.spend                 AS attr_spend,
                i.total_recharge_amount AS attr_cum_d0,
                CAST(NULL AS DECIMAL(18,4)) AS attr_cum_d7,
                CAST(NULL AS DECIMAL(18,4)) AS attr_cum_d30
            FROM biz_attribution_ad_intraday i
            LEFT JOIN biz_attribution_ad_daily d
                ON d.ds_account_local = i.ds_account_local
               AND d.platform         = i.platform
               AND d.account_id       = i.account_id
               AND d.ad_id            = i.ad_id
            WHERE i.ds_account_local BETWEEN %s AND %s
              AND d.id IS NULL
              AND i.ad_id <> '' {a_filter_i}
        """
    else:
        # campaign/adgroup 粒度：通过 biz_ad_daily_normalized 反推
        norm_grain_col = f"n.{daily_grain_col}"
        intraday_part = f"""
            SELECT
                i.ds_account_local      AS d,
                n.platform              AS plat,
                n.account_id            AS acc,
                {norm_grain_col}        AS key_id,
                i.purchase              AS attr_conversions,
                i.registration          AS attr_registrations,
                i.total_recharge_amount AS attr_revenue,
                i.spend                 AS attr_spend,
                i.total_recharge_amount AS attr_cum_d0,
                CAST(NULL AS DECIMAL(14,4)) AS attr_cum_d7,
                CAST(NULL AS DECIMAL(14,4)) AS attr_cum_d30
            FROM biz_attribution_ad_intraday i
            INNER JOIN biz_ad_daily_normalized n
                ON n.stat_date  = i.ds_account_local
               AND n.platform   = {_plat_expr("i")}
               AND n.account_id = {_acc_expr("i")}
               AND n.ad_id      = i.ad_id
            LEFT JOIN biz_attribution_ad_daily d
                ON d.ds_account_local = i.ds_account_local
               AND d.platform         = i.platform
               AND d.account_id       = i.account_id
               AND d.ad_id            = i.ad_id
            WHERE i.ds_account_local BETWEEN %s AND %s
              AND d.id IS NULL
              AND {norm_grain_col} <> '' {a_filter_i}
        """

    sql = f"""
        SELECT
            d, plat, acc, key_id,
            SUM(attr_conversions)   AS attr_conversions,
            SUM(attr_registrations) AS attr_registrations,
            SUM(attr_revenue)       AS attr_revenue,
            SUM(attr_spend)         AS attr_spend,
            SUM(attr_cum_d0)        AS attr_cum_d0,
            SUM(attr_cum_d7)        AS attr_cum_d7,
            SUM(attr_cum_d30)       AS attr_cum_d30
        FROM (
            {daily_part}
            UNION ALL
            {intraday_part}
        ) u
        GROUP BY d, plat, acc, key_id
    """
    args = [start_date, end_date] + a_args_d + [start_date, end_date] + a_args_i
    return sql, args


# ─────────────────────────────────────────────────────────────
#  /overview
# ─────────────────────────────────────────────────────────────

def get_overview(start_date: str, end_date: str, *,
                 platform: Optional[str] = None,
                 account_id: Optional[str] = None) -> dict:
    """整站汇总指标（campaign 粒度聚合）。

    spend/impressions/clicks/installs 来自 normalized；
    conversions/revenue 来自 attribution（按 campaign 维度对齐）。
    """
    n_filter, n_args = _build_filter_n(platform, account_id)
    attr_sql, attr_args = _attr_subquery("campaign", start_date, end_date, platform, account_id)

    sql = f"""
    SELECT
        COALESCE(SUM(n.spend), 0)               AS total_spend,
        COALESCE(SUM(n.impressions), 0)         AS total_impressions,
        COALESCE(SUM(n.clicks), 0)              AS total_clicks,
        COALESCE(SUM(n.installs), 0)            AS total_installs,
        COALESCE(SUM(a.attr_conversions), 0)    AS total_conversions,
        COALESCE(SUM(a.attr_registrations), 0)  AS total_registrations,
        COALESCE(SUM(a.attr_revenue), 0)        AS total_revenue,
        COALESCE(SUM(a.attr_spend), 0)          AS attribution_spend,
        COALESCE(SUM(a.attr_cum_d0), 0)         AS total_cum_d0,
        COALESCE(SUM(a.attr_cum_d7), 0)         AS total_cum_d7,
        COALESCE(SUM(a.attr_cum_d30), 0)        AS total_cum_d30
    FROM biz_campaign_daily_normalized n
    LEFT JOIN ({attr_sql}) a
        ON a.d      = n.stat_date
       AND a.plat   = n.platform
       AND a.acc    = n.account_id
       AND a.key_id = n.campaign_id
    WHERE n.stat_date BETWEEN %s AND %s {n_filter}
    """
    args = attr_args + [start_date, end_date] + n_args
    row = _query_one(sql, args)

    s   = float(row.get("total_spend") or 0)
    im  = int(row.get("total_impressions") or 0)
    cl  = int(row.get("total_clicks") or 0)
    ins = int(row.get("total_installs") or 0)
    cv  = int(row.get("total_conversions") or 0)
    reg = int(row.get("total_registrations") or 0)
    rv  = float(row.get("total_revenue") or 0)
    attr_s = float(row.get("attribution_spend") or 0)
    c0 = float(row.get("total_cum_d0") or 0)
    c7 = float(row.get("total_cum_d7") or 0)
    c30 = float(row.get("total_cum_d30") or 0)

    return {
        "total_spend":         s,
        "total_impressions":   im,
        "total_clicks":        cl,
        "total_installs":      ins,
        "total_conversions":   cv,
        "total_registrations": reg,
        "total_revenue":       rv,
        "attribution_spend":   attr_s,
        "avg_ctr":  _safe_div(cl, im),
        "avg_cpc":  _safe_div(s, cl),
        "avg_cpm":  round(s / im * 1000, 4) if im else None,
        "avg_cpi":  _safe_div(s, ins),
        "avg_cpa":  _safe_div(s, cv),
        "avg_roas": _safe_div(rv, s),
        "avg_roi_d0":  _safe_div(c0, s),
        "avg_roi_d7":  _safe_div(c7, s),
        "avg_roi_d30": _safe_div(c30, s),
    }


# ─────────────────────────────────────────────────────────────
#  /top-campaigns
# ─────────────────────────────────────────────────────────────

_TOP_CAMPAIGN_METRIC_EXPR = {
    "spend":       "SUM(n.spend)",
    "revenue":     "COALESCE(SUM(a.attr_revenue), 0)",
    "clicks":      "SUM(n.clicks)",
    "installs":    "SUM(n.installs)",
    "conversions": "COALESCE(SUM(a.attr_conversions), 0)",
    "roas":        "CASE WHEN SUM(n.spend) > 0 "
                   "THEN COALESCE(SUM(a.attr_revenue), 0) / SUM(n.spend) ELSE 0 END",
    "roi_d0":      "CASE WHEN SUM(n.spend) > 0 "
                   "THEN COALESCE(SUM(a.attr_cum_d0), 0) / SUM(n.spend) ELSE 0 END",
    "roi_d7":      "CASE WHEN SUM(n.spend) > 0 "
                   "THEN COALESCE(SUM(a.attr_cum_d7), 0) / SUM(n.spend) ELSE 0 END",
    "roi_d30":     "CASE WHEN SUM(n.spend) > 0 "
                   "THEN COALESCE(SUM(a.attr_cum_d30), 0) / SUM(n.spend) ELSE 0 END",
}


def get_top_campaigns(start_date: str, end_date: str, *,
                      platform: Optional[str] = None,
                      account_id: Optional[str] = None,
                      metric: str = "spend",
                      limit: int = 20) -> list[dict]:
    n_filter, n_args = _build_filter_n(platform, account_id)
    attr_sql, attr_args = _attr_subquery("campaign", start_date, end_date, platform, account_id)
    order_expr = _TOP_CAMPAIGN_METRIC_EXPR.get(metric, "SUM(n.spend)")
    limit = min(max(int(limit), 1), 100)

    sql = f"""
    SELECT
        n.platform                                 AS platform,
        n.account_id                               AS account_id,
        n.campaign_id                              AS campaign_id,
        MAX(n.campaign_name)                       AS campaign_name,
        COALESCE(SUM(n.spend), 0)                  AS total_spend,
        COALESCE(SUM(n.impressions), 0)            AS total_impressions,
        COALESCE(SUM(n.clicks), 0)                 AS total_clicks,
        COALESCE(SUM(n.installs), 0)               AS total_installs,
        COALESCE(SUM(a.attr_conversions), 0)       AS total_conversions,
        COALESCE(SUM(a.attr_registrations), 0)     AS total_registrations,
        COALESCE(SUM(a.attr_revenue), 0)           AS total_revenue,
        COALESCE(SUM(a.attr_spend), 0)             AS attribution_spend,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_revenue), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS avg_roas,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d0), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d0,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d7), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d7,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d30), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d30
    FROM biz_campaign_daily_normalized n
    LEFT JOIN ({attr_sql}) a
        ON a.d      = n.stat_date
       AND a.plat   = n.platform
       AND a.acc    = n.account_id
       AND a.key_id = n.campaign_id
    WHERE n.stat_date BETWEEN %s AND %s {n_filter}
    GROUP BY n.platform, n.account_id, n.campaign_id
    ORDER BY {order_expr} DESC
    LIMIT %s
    """
    args = attr_args + [start_date, end_date] + n_args + [limit]
    return _query_all(sql, args)


# ─────────────────────────────────────────────────────────────
#  /campaign-agg
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
    "roi_d0": "roi_d0", "roi_d7": "roi_d7", "roi_d30": "roi_d30",
    "campaign_name": "campaign_name",
}


def get_campaign_aggregated(start_date: str, end_date: str, *,
                            platform: Optional[str] = None,
                            account_id: Optional[str] = None,
                            campaign_name: Optional[str] = None,
                            order_by: str = "total_spend",
                            order_dir: str = "desc") -> list[dict]:
    n_filter, n_args = _build_filter_n(platform, account_id)
    attr_sql, attr_args = _attr_subquery("campaign", start_date, end_date, platform, account_id)
    order_col = _CAMPAIGN_AGG_ORDER.get(order_by, "total_spend")
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"

    name_sql = ""
    name_args: list = []
    if campaign_name:
        name_sql = " AND n.campaign_name LIKE %s"
        name_args = [f"%{campaign_name}%"]

    sql = f"""
    SELECT
        n.platform                                 AS platform,
        n.account_id                               AS account_id,
        n.campaign_id                              AS campaign_id,
        MAX(n.campaign_name)                       AS campaign_name,
        SUM(n.spend)                               AS total_spend,
        COALESCE(SUM(a.attr_revenue), 0)           AS total_revenue,
        COALESCE(SUM(a.attr_spend), 0)             AS attribution_spend,
        SUM(n.impressions)                         AS total_impressions,
        SUM(n.clicks)                              AS total_clicks,
        SUM(n.installs)                            AS total_installs,
        COALESCE(SUM(a.attr_conversions), 0)       AS total_conversions,
        COALESCE(SUM(a.attr_registrations), 0)     AS total_registrations,
        CASE WHEN SUM(n.impressions) > 0
             THEN ROUND(SUM(n.clicks) / SUM(n.impressions), 6)
             ELSE NULL END                          AS ctr,
        CASE WHEN SUM(n.clicks) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.clicks), 4)
             ELSE NULL END                          AS cpc,
        CASE WHEN SUM(n.impressions) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.impressions) * 1000, 4)
             ELSE NULL END                          AS cpm,
        CASE WHEN SUM(n.installs) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.installs), 4)
             ELSE NULL END                          AS cpi,
        CASE WHEN COALESCE(SUM(a.attr_conversions), 0) > 0
             THEN ROUND(SUM(n.spend) / SUM(a.attr_conversions), 4)
             ELSE NULL END                          AS cpa,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_revenue), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roas,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d0), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d0,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d7), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d7,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d30), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d30
    FROM biz_campaign_daily_normalized n
    LEFT JOIN ({attr_sql}) a
        ON a.d      = n.stat_date
       AND a.plat   = n.platform
       AND a.acc    = n.account_id
       AND a.key_id = n.campaign_id
    WHERE n.stat_date BETWEEN %s AND %s {n_filter} {name_sql}
    GROUP BY n.platform, n.account_id, n.campaign_id
    ORDER BY {order_col} {order_dir_l}
    """
    args = attr_args + [start_date, end_date] + n_args + name_args
    return _query_all(sql, args)


# ─────────────────────────────────────────────────────────────
#  /adgroup-agg
# ─────────────────────────────────────────────────────────────

def get_adgroup_aggregated(start_date: str, end_date: str, *,
                           platform: Optional[str] = None,
                           account_id: Optional[str] = None,
                           campaign_id: Optional[str] = None,
                           order_by: str = "total_spend",
                           order_dir: str = "desc") -> list[dict]:
    n_filter, n_args = _build_filter_n(platform, account_id)
    attr_sql, attr_args = _attr_subquery("adgroup", start_date, end_date, platform, account_id)
    order_col = _CAMPAIGN_AGG_ORDER.get(order_by, "total_spend")
    if order_col == "campaign_name":
        order_col = "adgroup_name"
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"

    cid_sql = ""
    cid_args: list = []
    if campaign_id:
        cid_sql = " AND n.campaign_id = %s"
        cid_args = [campaign_id]

    sql = f"""
    SELECT
        n.platform                                 AS platform,
        n.account_id                               AS account_id,
        n.campaign_id                              AS campaign_id,
        MAX(n.campaign_name)                       AS campaign_name,
        n.adgroup_id                               AS adgroup_id,
        MAX(n.adgroup_name)                        AS adgroup_name,
        SUM(n.spend)                               AS total_spend,
        COALESCE(SUM(a.attr_revenue), 0)           AS total_revenue,
        COALESCE(SUM(a.attr_spend), 0)             AS attribution_spend,
        SUM(n.impressions)                         AS total_impressions,
        SUM(n.clicks)                              AS total_clicks,
        SUM(n.installs)                            AS total_installs,
        COALESCE(SUM(a.attr_conversions), 0)       AS total_conversions,
        COALESCE(SUM(a.attr_registrations), 0)     AS total_registrations,
        CASE WHEN SUM(n.impressions) > 0
             THEN ROUND(SUM(n.clicks) / SUM(n.impressions), 6)
             ELSE NULL END                          AS ctr,
        CASE WHEN SUM(n.clicks) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.clicks), 4)
             ELSE NULL END                          AS cpc,
        CASE WHEN SUM(n.impressions) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.impressions) * 1000, 4)
             ELSE NULL END                          AS cpm,
        CASE WHEN SUM(n.installs) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.installs), 4)
             ELSE NULL END                          AS cpi,
        CASE WHEN COALESCE(SUM(a.attr_conversions), 0) > 0
             THEN ROUND(SUM(n.spend) / SUM(a.attr_conversions), 4)
             ELSE NULL END                          AS cpa,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_revenue), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roas,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d0), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d0,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d7), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d7,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d30), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d30
    FROM biz_adgroup_daily_normalized n
    LEFT JOIN ({attr_sql}) a
        ON a.d      = n.stat_date
       AND a.plat   = n.platform
       AND a.acc    = n.account_id
       AND a.key_id = n.adgroup_id
    WHERE n.stat_date BETWEEN %s AND %s {n_filter} {cid_sql}
    GROUP BY n.platform, n.account_id, n.campaign_id, n.adgroup_id
    ORDER BY {order_col} {order_dir_l}
    """
    args = attr_args + [start_date, end_date] + n_args + cid_args
    return _query_all(sql, args)


# ─────────────────────────────────────────────────────────────
#  /ad-agg / /creative-analysis
# ─────────────────────────────────────────────────────────────

_AD_AGG_ORDER_MAP = {
    "total_spend":       "total_spend",
    "total_revenue":     "total_revenue",
    "total_impressions": "total_impressions",
    "total_clicks":      "total_clicks",
    "total_installs":    "total_installs",
    "total_conversions": "total_conversions",
    "ctr":   "ctr",   "cpc": "cpc",  "cpm": "cpm",
    "cpi":   "cpi",   "cpa": "cpa",  "roas": "roas",
    "roi_d0": "roi_d0", "roi_d7": "roi_d7", "roi_d30": "roi_d30",
    "ad_name": "ad_name",
}


def get_ad_aggregated(start_date: str, end_date: str, *,
                      platform: Optional[str] = None,
                      account_id: Optional[str] = None,
                      campaign_id: Optional[str] = None,
                      adgroup_id: Optional[str] = None,
                      name_filter: Optional[str] = None,
                      content_key: Optional[str] = None,
                      drama_keyword: Optional[str] = None,
                      language_code: Optional[str] = None,
                      order_by: str = "total_spend",
                      order_dir: str = "desc") -> list[dict]:
    from repositories._drama_filter import (
        drama_filter_where,
        drama_join_for_normalized,
        drama_select_fields,
    )

    n_filter, n_args = _build_filter_n(platform, account_id)
    attr_sql, attr_args = _attr_subquery("ad", start_date, end_date, platform, account_id)
    order_col = _AD_AGG_ORDER_MAP.get(order_by, "total_spend")
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"

    extra_clauses: list[str] = []
    extra_args: list = []
    if campaign_id:
        extra_clauses.append("n.campaign_id = %s")
        extra_args.append(campaign_id)
    if adgroup_id:
        extra_clauses.append("n.adgroup_id = %s")
        extra_args.append(adgroup_id)
    if name_filter:
        extra_clauses.append("n.ad_name LIKE %s")
        extra_args.append(f"%{name_filter}%")
    extra_sql = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    drama_join_sql = drama_join_for_normalized(main_alias="n", mapping_alias="m")
    drama_where_sql, drama_where_args = drama_filter_where(
        content_key=content_key,
        drama_keyword=drama_keyword,
        language_code=language_code,
        mapping_alias="m",
    )
    drama_select_sql = drama_select_fields(mapping_alias="m", aggregate=True)

    sql = f"""
    SELECT
        n.platform                                 AS platform,
        n.account_id                               AS account_id,
        n.campaign_id                              AS campaign_id,
        MAX(n.campaign_name)                       AS campaign_name,
        n.adgroup_id                               AS adgroup_id,
        MAX(n.adgroup_name)                        AS adgroup_name,
        n.ad_id                                    AS ad_id,
        MAX(n.ad_name)                             AS ad_name,
        SUM(n.spend)                               AS total_spend,
        COALESCE(SUM(a.attr_revenue), 0)           AS total_revenue,
        COALESCE(SUM(a.attr_spend), 0)             AS attribution_spend,
        SUM(n.impressions)                         AS total_impressions,
        SUM(n.clicks)                              AS total_clicks,
        SUM(n.installs)                            AS total_installs,
        COALESCE(SUM(a.attr_conversions), 0)       AS total_conversions,
        COALESCE(SUM(a.attr_registrations), 0)     AS total_registrations,
        CASE WHEN SUM(n.impressions) > 0
             THEN ROUND(SUM(n.clicks) / SUM(n.impressions), 6)
             ELSE NULL END                          AS ctr,
        CASE WHEN SUM(n.clicks) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.clicks), 4)
             ELSE NULL END                          AS cpc,
        CASE WHEN SUM(n.impressions) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.impressions) * 1000, 4)
             ELSE NULL END                          AS cpm,
        CASE WHEN SUM(n.installs) > 0
             THEN ROUND(SUM(n.spend) / SUM(n.installs), 4)
             ELSE NULL END                          AS cpi,
        CASE WHEN COALESCE(SUM(a.attr_conversions), 0) > 0
             THEN ROUND(SUM(n.spend) / SUM(a.attr_conversions), 4)
             ELSE NULL END                          AS cpa,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_revenue), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roas,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d0), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d0,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d7), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d7,
        CASE WHEN SUM(n.spend) > 0
             THEN ROUND(COALESCE(SUM(a.attr_cum_d30), 0) / SUM(n.spend), 4)
             ELSE NULL END                          AS roi_d30,
        {drama_select_sql}
    FROM biz_ad_daily_normalized n
    LEFT JOIN ({attr_sql}) a
        ON a.d      = n.stat_date
       AND a.plat   = n.platform
       AND a.acc    = n.account_id
       AND a.key_id = n.ad_id
    {drama_join_sql}
    WHERE n.stat_date BETWEEN %s AND %s {n_filter} {extra_sql} {drama_where_sql}
    GROUP BY n.platform, n.account_id, n.campaign_id, n.adgroup_id, n.ad_id
    ORDER BY {order_col} {order_dir_l}
    """
    args = attr_args + [start_date, end_date] + n_args + extra_args + drama_where_args
    return _query_all(sql, args)


# ─────────────────────────────────────────────────────────────
#  /campaign-daily（带分页）
# ─────────────────────────────────────────────────────────────

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
    "ctr":  "ctr", "cpc": "cpc", "cpm": "cpm",
    "cpi":  "cpi", "cpa": "cpa", "roas": "roas",
    "roi_d0": "roi_d0", "roi_d7": "roi_d7", "roi_d30": "roi_d30",
}


def get_campaign_daily_list(start_date: str, end_date: str, *,
                            platform: Optional[str] = None,
                            account_id: Optional[str] = None,
                            campaign_name: Optional[str] = None,
                            page: int = 1, page_size: int = 20,
                            order_by: str = "stat_date",
                            order_dir: str = "desc") -> dict:
    n_filter, n_args = _build_filter_n(platform, account_id)
    attr_sql, attr_args = _attr_subquery("campaign", start_date, end_date, platform, account_id)
    order_dir_l = order_dir.lower() if order_dir.lower() in ("asc", "desc") else "desc"
    order_col = _CAMPAIGN_DAILY_ORDER_MAP.get(order_by, "stat_date")
    page = max(1, int(page))
    page_size = min(max(1, int(page_size)), 200)

    name_sql = ""
    name_args: list = []
    if campaign_name:
        name_sql = " AND n.campaign_name LIKE %s"
        name_args = [f"%{campaign_name}%"]

    base_where = (
        "WHERE n.stat_date BETWEEN %s AND %s"
        + n_filter + name_sql
    )

    count_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM biz_campaign_daily_normalized n
        {base_where}
    """
    count_args = [start_date, end_date] + n_args + name_args
    total = (_query_one(count_sql, count_args).get("cnt") or 0)

    list_sql = f"""
        SELECT
            n.id                                       AS id,
            n.stat_date                                AS stat_date,
            n.platform                                 AS platform,
            n.account_id                               AS account_id,
            n.campaign_id                              AS campaign_id,
            n.campaign_name                            AS campaign_name,
            n.spend                                    AS spend,
            n.impressions                              AS impressions,
            n.clicks                                   AS clicks,
            n.installs                                 AS installs,
            COALESCE(a.attr_conversions, 0)            AS conversions,
            COALESCE(a.attr_registrations, 0)          AS registrations,
            COALESCE(a.attr_revenue, 0)                AS revenue,
            n.ctr                                      AS ctr,
            n.cpc                                      AS cpc,
            n.cpm                                      AS cpm,
            n.cpi                                      AS cpi,
            CASE WHEN COALESCE(a.attr_conversions, 0) > 0
                 THEN ROUND(n.spend / a.attr_conversions, 4)
                 ELSE NULL END                          AS cpa,
            CASE WHEN n.spend > 0
                 THEN ROUND(COALESCE(a.attr_revenue, 0) / n.spend, 4)
                 ELSE NULL END                          AS roas,
            CASE WHEN n.spend > 0
                 THEN ROUND(COALESCE(a.attr_cum_d0, 0) / n.spend, 4)
                 ELSE NULL END                          AS roi_d0,
            CASE WHEN n.spend > 0
                 THEN ROUND(COALESCE(a.attr_cum_d7, 0) / n.spend, 4)
                 ELSE NULL END                          AS roi_d7,
            CASE WHEN n.spend > 0
                 THEN ROUND(COALESCE(a.attr_cum_d30, 0) / n.spend, 4)
                 ELSE NULL END                          AS roi_d30
        FROM biz_campaign_daily_normalized n
        LEFT JOIN ({attr_sql}) a
            ON a.d      = n.stat_date
           AND a.plat   = n.platform
           AND a.acc    = n.account_id
           AND a.key_id = n.campaign_id
        {base_where}
        ORDER BY {order_col} {order_dir_l}
        LIMIT %s OFFSET %s
    """
    list_args = (
        attr_args
        + [start_date, end_date] + n_args + name_args
        + [page_size, (page - 1) * page_size]
    )
    rows = _query_all(list_sql, list_args)

    return {
        "total":     int(total),
        "page":      page,
        "page_size": page_size,
        "list":      rows,
    }


# ─────────────────────────────────────────────────────────────
#  /data-range（前端默认日期）
# ─────────────────────────────────────────────────────────────

def get_data_range_combined() -> dict:
    """blend 模式下取 normalized 主表的 stat_date min/max 即可，
    与 legacy 行为一致（保持前端默认日期不变）"""
    sql = """
    SELECT MIN(stat_date) AS min_date, MAX(stat_date) AS max_date
    FROM biz_campaign_daily_normalized
    """
    row = _query_one(sql, [])
    return {
        "min_date": row.get("min_date"),
        "max_date": row.get("max_date"),
    }
