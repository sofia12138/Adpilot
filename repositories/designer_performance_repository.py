"""设计师人效报表数据访问层

设计师解析规则：
  ad_name 按 '-' 分割，取第一个字段作为 designer_name。
  若 ad_name 为空 / 不含 '-'，则 designer_name = '未识别'。
"""
from __future__ import annotations

from typing import Optional

from db import get_biz_conn


# ---------------------------------------------------------------------------
# SQL 级设计师名称解析表达式
# ---------------------------------------------------------------------------

def _designer_expr() -> str:
    """返回从 ad_name 提取 designer_name 的 SQL CASE 表达式"""
    return (
        "CASE "
        "WHEN ad_name IS NULL OR TRIM(ad_name) = '' "
        "    THEN '未识别' "
        "WHEN LOCATE('-', TRIM(ad_name)) = 0 "
        "    THEN '未识别' "
        "ELSE TRIM(SUBSTRING_INDEX(TRIM(ad_name), '-', 1)) "
        "END"
    )


# ---------------------------------------------------------------------------
# 设计师汇总
# ---------------------------------------------------------------------------

def get_designer_summary(
    start_date: str,
    end_date: str,
    *,
    platform: Optional[str] = None,
    keyword: Optional[str] = None,
) -> list[dict]:
    """按设计师维度聚合，返回各设计师的消耗/展示/点击等汇总数据"""
    clauses = ["stat_date BETWEEN %s AND %s"]
    params: list = [start_date, end_date]

    if platform:
        clauses.append("platform = %s")
        params.append(platform)

    where = " AND ".join(clauses)
    expr = _designer_expr()

    sql = f"""
        SELECT
            ({expr})                    AS designer_name,
            COUNT(DISTINCT ad_id)       AS material_count,
            SUM(spend)                  AS total_spend,
            SUM(impressions)            AS impressions,
            SUM(clicks)                 AS clicks,
            SUM(installs)               AS installs,
            SUM(conversions)            AS conversions,
            SUM(revenue)                AS purchase_value,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks)   / SUM(impressions), 6) ELSE NULL END AS ctr,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(revenue)  / SUM(spend),       4) ELSE NULL END AS roas
        FROM biz_ad_daily_normalized
        WHERE {where}
        GROUP BY designer_name
        ORDER BY total_spend DESC
    """

    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    # keyword 过滤在 Python 侧完成（避免在 HAVING 中重复 CASE 表达式）
    if keyword:
        kw = keyword.strip().lower()
        rows = [r for r in rows if kw in (r.get("designer_name") or "").lower()]

    return rows


# ---------------------------------------------------------------------------
# 设计师素材明细
# ---------------------------------------------------------------------------

def get_designer_materials(
    start_date: str,
    end_date: str,
    designer_name: str,
    *,
    platform: Optional[str] = None,
) -> list[dict]:
    """返回某设计师在指定时间范围内的素材明细（按 ad 维度聚合）"""
    expr = _designer_expr()

    clauses = [
        "stat_date BETWEEN %s AND %s",
        f"({expr}) = %s",
    ]
    params: list = [start_date, end_date, designer_name]

    if platform:
        clauses.append("platform = %s")
        params.append(platform)

    where = " AND ".join(clauses)

    sql = f"""
        SELECT
            ad_id,
            MAX(ad_name)            AS ad_name,
            platform,
            MAX(campaign_name)      AS campaign_name,
            SUM(spend)              AS spend,
            SUM(impressions)        AS impressions,
            SUM(clicks)             AS clicks,
            SUM(installs)           AS installs,
            SUM(conversions)        AS registrations,
            SUM(revenue)            AS purchase_value,
            CASE WHEN SUM(impressions) > 0
                 THEN ROUND(SUM(clicks)  / SUM(impressions), 6) ELSE NULL END AS ctr,
            CASE WHEN SUM(spend) > 0
                 THEN ROUND(SUM(revenue) / SUM(spend),       4) ELSE NULL END AS roas
        FROM biz_ad_daily_normalized
        WHERE {where}
        GROUP BY ad_id, platform
        ORDER BY spend DESC
    """

    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()
