"""广告回传转化数据访问层 — ad_returned_conversion_daily (adpilot_biz)

本模块所有读写操作均使用 get_biz_conn()，连接业务库（adpilot_biz）。
数据口径说明：
  - data_label = 'returned'，全部为广告平台归因回传数据
  - 不等同于后端订单真值，不用于财务核算
  - 仅用于投放优化分析
"""
from __future__ import annotations

import json
from typing import Optional

from db import get_biz_conn

# ══════════════════════════════════════════════════════════
#  平台字段支持矩阵（静态知识，基于平台 API 能力）
#
#  True  = 平台明确支持该字段（有对应 API 字段可提取）
#  False = 平台不支持或当前未接入，写入时降级为 0
#
#  字段说明：
#    registrations_returned:    广告平台归因注册数
#    purchase_value_returned:   广告平台归因充值/购买价值
#    subscribe_value_returned:  广告平台归因订阅价值（无平台明确支持）
#    d1_value_returned:         D1 Cohort 回传价值（无平台支持 D1 拆分）
# ══════════════════════════════════════════════════════════

PLATFORM_FIELD_SUPPORT: dict[str, dict[str, bool]] = {
    "meta": {
        # Meta actions 数组中有 complete_registration action_type
        "registrations_returned":       True,
        # Meta action_values 数组中有 purchase 类型可提取金额
        "purchase_value_returned":      True,
        # subscribe_value_returned 字段已接入，DB 列存在，支持展示
        "subscribe_value_returned":     True,
        # Meta Insights 无 D1 cohort 拆分，降级为 0
        "d1_value_returned":            False,
        # D0 Cohort 字段：DB 列已存在，展示入库值（当前平台 API 无 D0 cohort 拆分故值为 0）
        "d0_registrations_returned":    True,
        "d0_purchase_value_returned":   True,
        "d0_subscribe_value_returned":  True,
    },
    "tiktok": {
        # TikTok registration + on_web_register 字段已接入，注册优化目标下有值
        "registrations_returned":       True,
        # TikTok complete_payment 仅为次数，无金额字段，降级为 0
        "purchase_value_returned":      False,
        # subscribe_value_returned 字段已接入，DB 列存在，值当前为 0
        "subscribe_value_returned":     True,
        "d1_value_returned":            False,
        # D0 Cohort 字段：DB 列已存在，展示入库值（当前值为 0）
        "d0_registrations_returned":    True,
        "d0_purchase_value_returned":   True,
        "d0_subscribe_value_returned":  True,
    },
    "google": {
        # Google Ads 当前未接入，所有字段降级为 0
        "registrations_returned":       False,
        "purchase_value_returned":      False,
        # subscribe_value_returned 字段已接入，DB 列存在，值当前为 0
        "subscribe_value_returned":     True,
        "d1_value_returned":            False,
        # D0 Cohort 字段：DB 列已存在，展示入库值（当前值为 0）
        "d0_registrations_returned":    True,
        "d0_purchase_value_returned":   True,
        "d0_subscribe_value_returned":  True,
    },
}

# 未知 media_source 时，取所有平台 OR 并集（只要有一个平台支持则 True）
_ALL_PLATFORMS_OR: dict[str, bool] = {
    field: any(pf[field] for pf in PLATFORM_FIELD_SUPPORT.values())
    for field in next(iter(PLATFORM_FIELD_SUPPORT.values()))
}

_AVAILABILITY_FIELDS = [
    "registrations_returned",
    "purchase_value_returned",
    "subscribe_value_returned",
    "d1_value_returned",
    "d0_registrations_returned",
    "d0_purchase_value_returned",
    "d0_subscribe_value_returned",
]


def get_static_availability(media_source: str | None) -> dict[str, bool]:
    """
    根据 media_source 返回字段支持标识（静态，基于平台能力）。
    - media_source 已知 → 精确返回该平台矩阵
    - media_source 为空（混合） → 取所有平台 OR 并集
    """
    if media_source and media_source in PLATFORM_FIELD_SUPPORT:
        return dict(PLATFORM_FIELD_SUPPORT[media_source])
    return dict(_ALL_PLATFORMS_OR)


# ── 枚举 ──────────────────────────────────────────────────

# group_by 合法值与对应 SQL 表达式
_GROUP_BY_MAP: dict[str, dict] = {
    "date":     {"select": "stat_date AS dimension_key, stat_date AS dimension_label",
                 "group":  "stat_date",
                 "order":  "stat_date"},
    "media":    {"select": "media_source AS dimension_key, media_source AS dimension_label",
                 "group":  "media_source",
                 "order":  "total_spend"},
    "campaign": {"select": "campaign_id AS dimension_key, COALESCE(NULLIF(ANY_VALUE(campaign_name),''), campaign_id) AS dimension_label",
                 "group":  "campaign_id",
                 "order":  "total_spend"},
    "adset":    {"select": "adset_id AS dimension_key, COALESCE(NULLIF(ANY_VALUE(adset_name),''), adset_id) AS dimension_label",
                 "group":  "adset_id",
                 "order":  "total_spend"},
    "ad":       {"select": "ad_id AS dimension_key, COALESCE(NULLIF(ANY_VALUE(ad_name),''), ad_id) AS dimension_label",
                 "group":  "ad_id",
                 "order":  "total_spend"},
    "country":  {"select": "country AS dimension_key, COALESCE(country, 'unknown') AS dimension_label",
                 "group":  "country",
                 "order":  "total_spend"},
    "platform": {"select": "platform AS dimension_key, COALESCE(platform, 'mixed') AS dimension_label",
                 "group":  "platform",
                 "order":  "total_spend"},
}

# 允许排序的安全列名白名单
_ALLOWED_ORDER = {
    "stat_date", "spend", "impressions", "clicks", "installs",
    "registrations_returned", "purchase_value_returned", "subscribe_value_returned",
    "total_value_returned", "d0_roi_returned", "d1_value_returned", "d1_roi_returned",
    "d0_registrations_returned", "d0_purchase_value_returned", "d0_subscribe_value_returned",
    "total_spend", "total_impressions", "total_clicks", "total_installs",
    "total_registrations_returned", "total_purchase_value_returned",
    "total_subscribe_value_returned", "total_total_value_returned",
    "total_d0_registrations_returned", "total_d0_purchase_value_returned",
    "total_d0_subscribe_value_returned",
}


# ── Filter Builder ─────────────────────────────────────────

def _build_filter(
    start_date: str,
    end_date: str,
    media_source: Optional[str] = None,
    account_id: Optional[str] = None,
    country: Optional[str] = None,
    platform: Optional[str] = None,
    campaign_id: Optional[str] = None,
    adset_id: Optional[str] = None,
    ad_id: Optional[str] = None,
    search_keyword: Optional[str] = None,
) -> tuple[str, list]:
    """统一 WHERE 条件构建器，所有查询必须经由此函数，不得手写 WHERE 子句。"""
    clauses: list[str] = ["stat_date BETWEEN %s AND %s"]
    params: list = [start_date, end_date]

    if media_source:
        clauses.append("media_source = %s")
        params.append(media_source)
    if account_id:
        clauses.append("account_id = %s")
        params.append(account_id)
    if country:
        clauses.append("country = %s")
        params.append(country)
    if platform:
        clauses.append("platform = %s")
        params.append(platform)
    if campaign_id:
        clauses.append("campaign_id = %s")
        params.append(campaign_id)
    if adset_id:
        clauses.append("adset_id = %s")
        params.append(adset_id)
    if ad_id:
        clauses.append("ad_id = %s")
        params.append(ad_id)
    if search_keyword:
        # 对 campaign/adset/ad 名称做模糊搜索
        kw = f"%{search_keyword}%"
        clauses.append(
            "(campaign_name LIKE %s OR adset_name LIKE %s OR ad_name LIKE %s)"
        )
        params.extend([kw, kw, kw])

    return " AND ".join(clauses), params


# ── 指标汇总 SQL 片段 ──────────────────────────────────────

_METRICS_SELECT = """
    COALESCE(SUM(spend), 0)                        AS total_spend,
    COALESCE(SUM(impressions), 0)                  AS total_impressions,
    COALESCE(SUM(clicks), 0)                       AS total_clicks,
    COALESCE(SUM(installs), 0)                     AS total_installs,
    COALESCE(SUM(registrations_returned), 0)       AS total_registrations_returned,
    COALESCE(SUM(purchase_value_returned), 0)      AS total_purchase_value_returned,
    COALESCE(SUM(subscribe_value_returned), 0)     AS total_subscribe_value_returned,
    COALESCE(SUM(total_value_returned), 0)         AS total_total_value_returned,
    COALESCE(SUM(d1_value_returned), 0)            AS total_d1_value_returned,
    COALESCE(SUM(d0_registrations_returned), 0)    AS total_d0_registrations_returned,
    COALESCE(SUM(d0_purchase_value_returned), 0)   AS total_d0_purchase_value_returned,
    COALESCE(SUM(d0_subscribe_value_returned), 0)  AS total_d0_subscribe_value_returned
"""


def _calc_roi(total_value: float, spend: float) -> float:
    """ROI = total_value / spend，spend 为 0 时返回 0"""
    if spend <= 0:
        return 0.0
    return round(float(total_value) / float(spend), 4)


def _row_to_summary(row: dict) -> dict:
    spend = float(row.get("total_spend") or 0)
    total_value = float(row.get("total_total_value_returned") or 0)
    d1_value = float(row.get("total_d1_value_returned") or 0)
    d0_purchase = float(row.get("total_d0_purchase_value_returned") or 0)
    d0_subscribe = float(row.get("total_d0_subscribe_value_returned") or 0)
    d0_value = d0_purchase + d0_subscribe
    return {
        "spend":                        round(spend, 4),
        "impressions":                  int(row.get("total_impressions") or 0),
        "clicks":                       int(row.get("total_clicks") or 0),
        "installs":                     int(row.get("total_installs") or 0),
        "registrations_returned":       int(row.get("total_registrations_returned") or 0),
        "purchase_value_returned":      round(float(row.get("total_purchase_value_returned") or 0), 4),
        "subscribe_value_returned":     round(float(row.get("total_subscribe_value_returned") or 0), 4),
        "total_value_returned":         round(total_value, 4),
        "cumulative_roi_returned":      _calc_roi(total_value, spend),
        "d0_roi_returned":              _calc_roi(d0_value, spend) if d0_value > 0 else _calc_roi(total_value, spend),
        "d0_roi_is_fallback":           d0_value <= 0,
        "d1_value_returned":            round(d1_value, 4),
        "d1_roi_returned":              _calc_roi(d1_value, spend),
        "d0_registrations_returned":    int(row.get("total_d0_registrations_returned") or 0),
        "d0_purchase_value_returned":   round(d0_purchase, 4),
        "d0_subscribe_value_returned":  round(d0_subscribe, 4),
    }


# ── 公开查询接口 ───────────────────────────────────────────

def query_summary(
    start_date: str,
    end_date: str,
    **filter_kwargs,
) -> dict:
    """返回筛选条件下的全量汇总指标（summary 卡片用）。"""
    where, params = _build_filter(start_date, end_date, **filter_kwargs)
    sql = f"""
        SELECT {_METRICS_SELECT}
        FROM ad_returned_conversion_daily
        WHERE {where}
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone() or {}
    return _row_to_summary(row)


def query_data_availability(
    start_date: str,
    end_date: str,
    **filter_kwargs,
) -> dict[str, bool]:
    """
    动态检查当前筛选范围内各 returned 字段是否存在非零数据。

    与 get_static_availability() 的区别：
    - 静态：基于平台能力（meta 理论上支持 purchase_value_returned）
    - 动态：基于实际入库数据（如果 meta 数据未同步，动态结果也为 False）

    两者结合使用：
    - static=True && dynamic=False → 字段平台支持，但本次查询范围无数据（可能未同步）
    - static=False                 → 平台不支持，固定显示"暂不支持"提示
    """
    where, params = _build_filter(start_date, end_date, **filter_kwargs)
    sql = f"""
        SELECT
            MAX(registrations_returned)      > 0 AS reg_avail,
            MAX(purchase_value_returned)     > 0 AS purchase_avail,
            MAX(subscribe_value_returned)    > 0 AS subscribe_avail,
            MAX(d1_value_returned)           > 0 AS d1_avail,
            MAX(d0_registrations_returned)   > 0 AS d0_reg_avail,
            MAX(d0_purchase_value_returned)  > 0 AS d0_purchase_avail,
            MAX(d0_subscribe_value_returned) > 0 AS d0_subscribe_avail
        FROM ad_returned_conversion_daily
        WHERE {where}
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone() or {}
    return {
        "registrations_returned":      bool(row.get("reg_avail")),
        "purchase_value_returned":     bool(row.get("purchase_avail")),
        "subscribe_value_returned":    bool(row.get("subscribe_avail")),
        "d1_value_returned":           bool(row.get("d1_avail")),
        "d0_registrations_returned":   bool(row.get("d0_reg_avail")),
        "d0_purchase_value_returned":  bool(row.get("d0_purchase_avail")),
        "d0_subscribe_value_returned": bool(row.get("d0_subscribe_avail")),
    }


def query_rows(
    start_date: str,
    end_date: str,
    group_by: str = "date",
    order_dir: str = "desc",
    **filter_kwargs,
) -> list[dict]:
    """
    按 group_by 聚合，返回带 dimension_key / dimension_label 的数据行列表。

    group_by 合法值: date | media | campaign | adset | ad | country | platform
    """
    if group_by not in _GROUP_BY_MAP:
        group_by = "date"
    gb = _GROUP_BY_MAP[group_by]

    where, params = _build_filter(start_date, end_date, **filter_kwargs)

    order_dir_safe = "DESC" if order_dir.lower() == "desc" else "ASC"
    order_col = gb["order"]
    # 聚合列名前缀 total_ 对应
    if order_col not in ("stat_date", "dimension_key"):
        order_col = f"total_{order_col}" if not order_col.startswith("total_") else order_col

    sql = f"""
        SELECT
            {gb['select']},
            {_METRICS_SELECT}
        FROM ad_returned_conversion_daily
        WHERE {where}
        GROUP BY {gb['group']}
        ORDER BY {gb['order']} {order_dir_safe}
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        raw_rows = cur.fetchall()

    result = []
    for r in raw_rows:
        spend = float(r.get("total_spend") or 0)
        total_value = float(r.get("total_total_value_returned") or 0)
        d1_value = float(r.get("total_d1_value_returned") or 0)
        d0_purchase = float(r.get("total_d0_purchase_value_returned") or 0)
        d0_subscribe = float(r.get("total_d0_subscribe_value_returned") or 0)
        d0_value = d0_purchase + d0_subscribe

        dim_key = str(r.get("dimension_key") or "")
        dim_label = str(r.get("dimension_label") or dim_key or "")

        result.append({
            "dimension_key":               dim_key,
            "dimension_label":             dim_label,
            "spend":                       round(spend, 4),
            "impressions":                 int(r.get("total_impressions") or 0),
            "clicks":                      int(r.get("total_clicks") or 0),
            "installs":                    int(r.get("total_installs") or 0),
            "registrations_returned":      int(r.get("total_registrations_returned") or 0),
            "purchase_value_returned":     round(d0_purchase if d0_purchase else float(r.get("total_purchase_value_returned") or 0), 4),
            "subscribe_value_returned":    round(d0_subscribe if d0_subscribe else float(r.get("total_subscribe_value_returned") or 0), 4),
            "total_value_returned":        round(total_value, 4),
            "cumulative_roi_returned":     _calc_roi(total_value, spend),
            "d0_roi_returned":             _calc_roi(d0_value, spend) if d0_value > 0 else _calc_roi(total_value, spend),
            "d1_value_returned":           round(d1_value, 4),
            "d1_roi_returned":             _calc_roi(d1_value, spend),
            "d0_registrations_returned":   int(r.get("total_d0_registrations_returned") or 0),
            "d0_purchase_value_returned":  round(d0_purchase, 4),
            "d0_subscribe_value_returned": round(d0_subscribe, 4),
        })
    return result


# ── 树形层级查询 ───────────────────────────────────────────

def query_hierarchy_rows(
    start_date: str,
    end_date: str,
    **filter_kwargs,
) -> list[dict]:
    """
    按 (campaign_id, adset_id, ad_id) 三维 GROUP BY，返回完整层级明细行。

    每行包含 campaign/adset/ad 各层的 id 与 name，供前端自下而上聚合构建树。
    与三次独立聚合不同，此方案保证同一批数据来源，消除父子数据守恒问题。
    """
    where, params = _build_filter(start_date, end_date, **filter_kwargs)
    sql = f"""
        SELECT
            COALESCE(campaign_id, '')              AS campaign_id,
            COALESCE(ANY_VALUE(campaign_name), '') AS campaign_name,
            COALESCE(adset_id, '')                 AS adset_id,
            COALESCE(ANY_VALUE(adset_name), '')    AS adset_name,
            COALESCE(ad_id, '')                    AS ad_id,
            COALESCE(ANY_VALUE(ad_name), '')       AS ad_name,
            {_METRICS_SELECT}
        FROM ad_returned_conversion_daily
        WHERE {where}
        GROUP BY campaign_id, adset_id, ad_id
        ORDER BY total_spend DESC
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        raw_rows = cur.fetchall()

    result = []
    for r in raw_rows:
        spend = float(r.get("total_spend") or 0)
        total_value = float(r.get("total_total_value_returned") or 0)
        d1_value = float(r.get("total_d1_value_returned") or 0)
        result.append({
            "campaign_id":   str(r.get("campaign_id") or ""),
            "campaign_name": str(r.get("campaign_name") or ""),
            "adset_id":      str(r.get("adset_id") or ""),
            "adset_name":    str(r.get("adset_name") or ""),
            "ad_id":         str(r.get("ad_id") or ""),
            "ad_name":       str(r.get("ad_name") or ""),
            "spend":                       round(spend, 4),
            "impressions":                 int(r.get("total_impressions") or 0),
            "clicks":                      int(r.get("total_clicks") or 0),
            "installs":                    int(r.get("total_installs") or 0),
            "registrations_returned":      int(r.get("total_registrations_returned") or 0),
            "purchase_value_returned":     round(float(r.get("total_purchase_value_returned") or 0), 4),
            "subscribe_value_returned":    round(float(r.get("total_subscribe_value_returned") or 0), 4),
            "total_value_returned":        round(total_value, 4),
            "d1_value_returned":           round(d1_value, 4),
            "d0_registrations_returned":   int(r.get("total_d0_registrations_returned") or 0),
            "d0_purchase_value_returned":  round(float(r.get("total_d0_purchase_value_returned") or 0), 4),
            "d0_subscribe_value_returned": round(float(r.get("total_d0_subscribe_value_returned") or 0), 4),
        })
    return result


# ── 写入接口 ───────────────────────────────────────────────

def upsert(
    *,
    stat_date: str,
    media_source: str,
    account_id: str = "",
    campaign_id: str = "",
    campaign_name: str = "",
    adset_id: str = "",
    adset_name: str = "",
    ad_id: str = "",
    ad_name: str = "",
    country: str = "",
    platform: str = "",
    impressions: int = 0,
    clicks: int = 0,
    installs: int = 0,
    spend: float = 0.0,
    registrations_returned: int = 0,
    purchase_value_returned: float = 0.0,
    subscribe_value_returned: float = 0.0,
    d1_value_returned: float = 0.0,
    d0_registrations_returned: int = 0,
    d0_purchase_value_returned: float = 0.0,
    d0_subscribe_value_returned: float = 0.0,
    raw_payload: dict | None = None,
) -> int:
    """
    写入一条回传口径日报数据（upsert）。

    本函数使用 get_biz_conn()，仅写入 adpilot_biz 业务库，
    不会触碰 app database 或 prd 产研库。

    平台字段映射规则（与 PLATFORM_FIELD_SUPPORT 对应）：

    Meta:
      - registrations_returned:   来自 actions 数组中 action_type=complete_registration 的 value
      - purchase_value_returned:  来自 action_values 数组中 action_type=purchase 的 value（金额）
      - subscribe_value_returned: 降级为 0（Meta 无独立订阅金额字段）
      - d1_value_returned:        降级为 0（Meta Insights 无 D1 cohort 拆分）

    TikTok:
      - registrations_returned:   固定为 0（安装 ≠ 注册，无 complete_registration 回传事件）
                                  注意：complete_payment 是购买次数，绝不可用作注册数的降级值
      - purchase_value_returned:  固定为 0（TikTok complete_payment 为次数，无金额字段）
      - subscribe_value_returned: 固定为 0
      - d1_value_returned:        固定为 0

    Google:
      - 当前未接入，所有 returned 字段固定为 0

    - total_value_returned 由此函数自动计算 = purchase + subscribe
    - d0_roi_returned / d1_roi_returned 由此函数自动计算
    """
    total_value = purchase_value_returned + subscribe_value_returned
    d0_roi = round(total_value / spend, 6) if spend > 0 else 0.0
    d1_roi = round(d1_value_returned / spend, 6) if spend > 0 else 0.0

    sql = """
        INSERT INTO ad_returned_conversion_daily
            (stat_date, media_source, account_id, campaign_id, campaign_name,
             adset_id, adset_name, ad_id, ad_name, country, platform,
             impressions, clicks, installs, spend,
             registrations_returned, purchase_value_returned,
             subscribe_value_returned, total_value_returned,
             d0_roi_returned, d1_value_returned, d1_roi_returned,
             d0_registrations_returned, d0_purchase_value_returned, d0_subscribe_value_returned,
             data_label, raw_payload)
        VALUES
            (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,
             %s,%s,%s,%s,
             %s,%s,%s,%s, %s,%s,%s,
             %s,%s,%s,
             'returned', %s)
        ON DUPLICATE KEY UPDATE
            impressions                 = VALUES(impressions),
            clicks                      = VALUES(clicks),
            installs                    = VALUES(installs),
            spend                       = VALUES(spend),
            registrations_returned      = VALUES(registrations_returned),
            purchase_value_returned     = VALUES(purchase_value_returned),
            subscribe_value_returned    = VALUES(subscribe_value_returned),
            total_value_returned        = VALUES(total_value_returned),
            d0_roi_returned             = VALUES(d0_roi_returned),
            d1_value_returned           = VALUES(d1_value_returned),
            d1_roi_returned             = VALUES(d1_roi_returned),
            d0_registrations_returned   = VALUES(d0_registrations_returned),
            d0_purchase_value_returned  = VALUES(d0_purchase_value_returned),
            d0_subscribe_value_returned = VALUES(d0_subscribe_value_returned),
            raw_payload                 = VALUES(raw_payload)
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (
            stat_date, media_source,
            account_id or "", campaign_id or "", campaign_name or None,
            adset_id or "", adset_name or None,
            ad_id or "", ad_name or None,
            country or "", platform or "",
            impressions, clicks, installs, spend,
            registrations_returned, purchase_value_returned,
            subscribe_value_returned, total_value,
            d0_roi, d1_value_returned, d1_roi,
            d0_registrations_returned, d0_purchase_value_returned, d0_subscribe_value_returned,
            json.dumps(raw_payload, ensure_ascii=False) if raw_payload else None,
        ))
        conn.commit()
        return cur.lastrowid
