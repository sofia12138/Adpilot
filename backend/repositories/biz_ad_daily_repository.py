"""Ad 归一化日报数据访问层 — biz_ad_daily_normalized"""
from __future__ import annotations

import json
from typing import Optional

from db import get_biz_conn


def _safe_div(a, b) -> Optional[float]:
    if not b:
        return None
    return round(float(a) / float(b), 6)


def _calc_derived(row: dict) -> dict:
    spend = float(row.get("spend", 0) or 0)
    impressions = int(row.get("impressions", 0) or 0)
    clicks = int(row.get("clicks", 0) or 0)
    installs = int(row.get("installs", 0) or 0)
    conversions = int(row.get("conversions", 0) or 0)
    revenue = float(row.get("revenue", 0) or 0)
    return {
        "ctr": _safe_div(clicks, impressions),
        "cpc": _safe_div(spend, clicks),
        "cpm": round(spend / impressions * 1000, 4) if impressions else None,
        "cpi": _safe_div(spend, installs),
        "cpa": _safe_div(spend, conversions),
        "roas": _safe_div(revenue, spend),
    }


def upsert_batch(rows: list[dict]) -> int:
    if not rows:
        return 0
    with get_biz_conn() as conn:
        cur = conn.cursor()
        sql = """INSERT INTO biz_ad_daily_normalized
                 (platform, account_id, campaign_id, campaign_name,
                  adgroup_id, adgroup_name, ad_id, ad_name, stat_date,
                  spend, impressions, clicks, installs, conversions, revenue,
                  ctr, cpc, cpm, cpi, cpa, roas, raw_json)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s)
                 ON DUPLICATE KEY UPDATE
                   account_id    = VALUES(account_id),
                   campaign_id   = VALUES(campaign_id),
                   campaign_name = VALUES(campaign_name),
                   adgroup_id    = VALUES(adgroup_id),
                   adgroup_name  = VALUES(adgroup_name),
                   ad_name       = VALUES(ad_name),
                   spend         = VALUES(spend),
                   impressions   = VALUES(impressions),
                   clicks        = VALUES(clicks),
                   installs      = VALUES(installs),
                   conversions   = VALUES(conversions),
                   revenue       = VALUES(revenue),
                   ctr           = VALUES(ctr),
                   cpc           = VALUES(cpc),
                   cpm           = VALUES(cpm),
                   cpi           = VALUES(cpi),
                   cpa           = VALUES(cpa),
                   roas          = VALUES(roas),
                   raw_json      = VALUES(raw_json)"""
        params = []
        for r in rows:
            d = _calc_derived(r)
            params.append((
                r["platform"], r["account_id"],
                r.get("campaign_id", ""), r.get("campaign_name", ""),
                r.get("adgroup_id", ""), r.get("adgroup_name", ""),
                r["ad_id"], r.get("ad_name", ""),
                r["stat_date"],
                r.get("spend", 0), r.get("impressions", 0), r.get("clicks", 0),
                r.get("installs", 0), r.get("conversions", 0), r.get("revenue", 0),
                d["ctr"], d["cpc"], d["cpm"], d["cpi"], d["cpa"], d["roas"],
                json.dumps(r["raw_json"], ensure_ascii=False) if r.get("raw_json") else None,
            ))
        cur.executemany(sql, params)
        conn.commit()
        return cur.rowcount


def _build_where(platform: str | None, account_id: str | None,
                 start_date: str, end_date: str,
                 name_filter: str | None = None,
                 campaign_id: str | None = None,
                 adgroup_id: str | None = None,
                 table_alias: str = "") -> tuple[str, list]:
    prefix = f"{table_alias}." if table_alias else ""
    clauses = [f"{prefix}stat_date BETWEEN %s AND %s"]
    params: list = [start_date, end_date]
    if platform:
        clauses.append(f"{prefix}platform = %s")
        params.append(platform)
    if account_id:
        clauses.append(f"{prefix}account_id = %s")
        params.append(account_id)
    if campaign_id:
        clauses.append(f"{prefix}campaign_id = %s")
        params.append(campaign_id)
    if adgroup_id:
        clauses.append(f"{prefix}adgroup_id = %s")
        params.append(adgroup_id)
    if name_filter:
        clauses.append(f"{prefix}ad_name LIKE %s")
        params.append(f"%{name_filter}%")
    return " AND ".join(clauses), params


_ALLOWED_ORDER = {
    "stat_date", "spend", "impressions", "clicks", "installs",
    "conversions", "revenue", "ctr", "cpc", "cpm", "cpi", "cpa", "roas",
    "ad_name", "adgroup_name", "campaign_name",
}


def get_daily_list(start_date: str, end_date: str, *,
                   platform: str | None = None,
                   account_id: str | None = None,
                   name_filter: str | None = None,
                   page: int = 1, page_size: int = 20,
                   order_by: str = "stat_date",
                   order_dir: str = "desc") -> dict:
    where, params = _build_where(platform, account_id, start_date, end_date, name_filter)
    if order_by not in _ALLOWED_ORDER:
        order_by = "stat_date"
    if order_dir.lower() not in ("asc", "desc"):
        order_dir = "desc"

    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) AS cnt FROM biz_ad_daily_normalized WHERE {where}", params)
        total = cur.fetchone()["cnt"]

        offset = (page - 1) * page_size
        sql = f"""
            SELECT platform, account_id,
                   campaign_id, campaign_name,
                   adgroup_id, adgroup_name,
                   ad_id, ad_name,
                   stat_date, spend, impressions, clicks, installs,
                   conversions, revenue, ctr, cpc, cpm, cpi, cpa, roas
            FROM biz_ad_daily_normalized
            WHERE {where}
            ORDER BY {order_by} {order_dir}
            LIMIT %s OFFSET %s
        """
        cur.execute(sql, params + [page_size, offset])
        rows = cur.fetchall()

    for r in rows:
        if r.get("stat_date"):
            r["stat_date"] = str(r["stat_date"])
    return {"total": total, "list": rows, "page": page, "page_size": page_size}


def get_ad_aggregated(start_date: str, end_date: str, *,
                      platform: str | None = None,
                      account_id: str | None = None,
                      campaign_id: str | None = None,
                      adgroup_id: str | None = None,
                      content_key: str | None = None,
                      drama_keyword: str | None = None,
                      language_code: str | None = None,
                      order_by: str = "total_spend",
                      order_dir: str = "desc") -> list[dict]:
    from repositories._drama_filter import (
        drama_filter_where,
        drama_join_for_normalized,
        drama_select_fields,
    )

    where, params = _build_where(platform, account_id, start_date, end_date,
                                 campaign_id=campaign_id, adgroup_id=adgroup_id,
                                 table_alias="n")
    _AGG_ORDER = {
        "total_spend", "total_revenue", "total_impressions", "total_clicks",
        "total_installs", "total_conversions", "ctr", "cpc", "cpm", "cpi", "cpa", "roas",
        "ad_name",
    }
    order_col = order_by if order_by in _AGG_ORDER else "total_spend"
    if order_dir.lower() not in ("asc", "desc"):
        order_dir = "desc"

    drama_join_sql = drama_join_for_normalized(main_alias="n", mapping_alias="m")
    drama_where_sql, drama_where_args = drama_filter_where(
        content_key=content_key,
        drama_keyword=drama_keyword,
        language_code=language_code,
        mapping_alias="m",
    )
    drama_select_sql = drama_select_fields(mapping_alias="m", aggregate=True)

    sql = f"""
        SELECT n.platform                        AS platform,
               n.account_id                      AS account_id,
               n.campaign_id                     AS campaign_id,
               MAX(n.campaign_name)              AS campaign_name,
               n.adgroup_id                      AS adgroup_id,
               MAX(n.adgroup_name)               AS adgroup_name,
               n.ad_id                           AS ad_id,
               MAX(n.ad_name)                    AS ad_name,
               SUM(n.spend)                      AS total_spend,
               SUM(n.revenue)                    AS total_revenue,
               SUM(n.impressions)                AS total_impressions,
               SUM(n.clicks)                     AS total_clicks,
               SUM(n.installs)                   AS total_installs,
               SUM(n.conversions)                AS total_conversions,
               CASE WHEN SUM(n.impressions)>0 THEN ROUND(SUM(n.clicks)/SUM(n.impressions),6) ELSE NULL END AS ctr,
               CASE WHEN SUM(n.clicks)>0      THEN ROUND(SUM(n.spend)/SUM(n.clicks),4)      ELSE NULL END AS cpc,
               CASE WHEN SUM(n.impressions)>0 THEN ROUND(SUM(n.spend)/SUM(n.impressions)*1000,4) ELSE NULL END AS cpm,
               CASE WHEN SUM(n.installs)>0    THEN ROUND(SUM(n.spend)/SUM(n.installs),4)    ELSE NULL END AS cpi,
               CASE WHEN SUM(n.conversions)>0 THEN ROUND(SUM(n.spend)/SUM(n.conversions),4) ELSE NULL END AS cpa,
               CASE WHEN SUM(n.spend)>0       THEN ROUND(SUM(n.revenue)/SUM(n.spend),4)     ELSE NULL END AS roas,
               {drama_select_sql}
        FROM biz_ad_daily_normalized n
        {drama_join_sql}
        WHERE {where} {drama_where_sql}
        GROUP BY n.platform, n.account_id, n.campaign_id, n.adgroup_id, n.ad_id
        ORDER BY {order_col} {order_dir}
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params + drama_where_args)
        return cur.fetchall()
