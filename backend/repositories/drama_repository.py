"""剧级数据访问层 — ad_drama_mapping + fact_drama_daily (adpilot_biz)

所有读写操作均使用 get_biz_conn()，连接业务库（adpilot_biz）。

核心约束（与解析器一致）：
  - drama_name_raw 只来自活动名称第 10 字段
  - remark_raw 不参与任何聚合、搜索、content_key 生成
  - content_key: 小程序 = drama_id；APP 无 drama_id 时 = normalized(localized_drama_name)

【归因切换决策（Tier 3）】
fact_drama_daily 拥有 country / language_code / drama_id 等剧级独有维度，
归因表 biz_attribution_ad_daily 只到 ad_id 粒度，没有 language_code / country
（账户级 country ≠ 充值用户 country）。

因此本视图（剧级分析）不直接从归因表读取，而是保留 fact_drama_daily 作为聚合源；
若未来需要把 fact_drama_daily.purchase_value 替换为数仓真实充值，需要在
tasks/sync_drama.py 的 ETL 里增加：
    biz_attribution_ad_daily.ad_id → ad_drama_mapping → drama_id 聚合 total_recharge_amount
然后回写 fact_drama_daily.purchase_value（保留 locale 拆分能力需重新设计 country / language_code 来源）。

当前阶段建议：保留旧实现不动，归因切换以单独工作项跟踪。
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

from db import get_biz_conn


# ─────────────────────────────────────────────────────────────
# content_key 标准化（与前端 normalizeContentKey 保持一致）
# ─────────────────────────────────────────────────────────────

def normalize_content_key(localized_drama_name: str) -> str:
    """
    对 localized_drama_name 做标准化，生成 content_key。
    转小写、去首尾空格、压缩内部空白为单个空格。
    remark_raw 绝对不能传入此函数。
    """
    s = localized_drama_name.strip().lower()
    return re.sub(r'\s+', ' ', s)


def build_content_key(source_type: str, drama_id: str, localized_drama_name: str) -> str:
    """
    内容键生成规则：
      - 小程序：优先使用 drama_id（稳定）
      - APP：若有 drama_id 用 drama_id，否则 normalized(localized_drama_name)
    """
    if drama_id and drama_id.strip():
        return drama_id.strip()
    return normalize_content_key(localized_drama_name)


# ─────────────────────────────────────────────────────────────
# ad_drama_mapping — 写入 / 查询
# ─────────────────────────────────────────────────────────────

def upsert_mapping(row: dict) -> None:
    """
    Upsert 一条广告活动的剧名解析映射记录。
    唯一键：(platform, account_id, campaign_id)

    row 必须包含字段：
      platform, account_id, campaign_id, campaign_name,
      source_type, channel, adset_id, adset_name, ad_id, ad_name,
      drama_id, drama_type, country,
      drama_name_raw, localized_drama_name, language_code, language_tag_raw,
      buyer_name, buyer_short_name, optimization_type, bid_type, publish_date,
      remark_raw, content_key, match_source, is_confirmed, parse_status, parse_error
    """
    sql = """
        INSERT INTO ad_drama_mapping (
            source_type, platform, channel, account_id,
            campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
            drama_id, drama_type, country,
            drama_name_raw, localized_drama_name, language_code, language_tag_raw,
            buyer_name, buyer_short_name, optimization_type, bid_type, publish_date,
            remark_raw, content_key, match_source, is_confirmed, parse_status, parse_error
        ) VALUES (
            %(source_type)s, %(platform)s, %(channel)s, %(account_id)s,
            %(campaign_id)s, %(campaign_name)s, %(adset_id)s, %(adset_name)s,
            %(ad_id)s, %(ad_name)s,
            %(drama_id)s, %(drama_type)s, %(country)s,
            %(drama_name_raw)s, %(localized_drama_name)s, %(language_code)s, %(language_tag_raw)s,
            %(buyer_name)s, %(buyer_short_name)s, %(optimization_type)s, %(bid_type)s, %(publish_date)s,
            %(remark_raw)s, %(content_key)s, %(match_source)s, %(is_confirmed)s,
            %(parse_status)s, %(parse_error)s
        )
        ON DUPLICATE KEY UPDATE
            campaign_name        = VALUES(campaign_name),
            source_type          = VALUES(source_type),
            channel              = VALUES(channel),
            drama_id             = VALUES(drama_id),
            drama_type           = VALUES(drama_type),
            country              = VALUES(country),
            drama_name_raw       = VALUES(drama_name_raw),
            localized_drama_name = VALUES(localized_drama_name),
            language_code        = VALUES(language_code),
            language_tag_raw     = VALUES(language_tag_raw),
            buyer_name           = VALUES(buyer_name),
            buyer_short_name     = VALUES(buyer_short_name),
            optimization_type    = VALUES(optimization_type),
            bid_type             = VALUES(bid_type),
            publish_date         = VALUES(publish_date),
            remark_raw           = VALUES(remark_raw),
            content_key          = VALUES(content_key),
            parse_status         = VALUES(parse_status),
            parse_error          = VALUES(parse_error),
            updated_at           = CURRENT_TIMESTAMP
    """
    with get_biz_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, row)


def upsert_mappings_bulk(rows: list[dict]) -> int:
    """批量 upsert，返回受影响行数。"""
    if not rows:
        return 0
    affected = 0
    with get_biz_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("""
                    INSERT INTO ad_drama_mapping (
                        source_type, platform, channel, account_id,
                        campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
                        drama_id, drama_type, country,
                        drama_name_raw, localized_drama_name, language_code, language_tag_raw,
                        buyer_name, buyer_short_name, optimization_type, bid_type, publish_date,
                        remark_raw, content_key, match_source, is_confirmed, parse_status, parse_error
                    ) VALUES (
                        %(source_type)s, %(platform)s, %(channel)s, %(account_id)s,
                        %(campaign_id)s, %(campaign_name)s, %(adset_id)s, %(adset_name)s,
                        %(ad_id)s, %(ad_name)s,
                        %(drama_id)s, %(drama_type)s, %(country)s,
                        %(drama_name_raw)s, %(localized_drama_name)s, %(language_code)s, %(language_tag_raw)s,
                        %(buyer_name)s, %(buyer_short_name)s, %(optimization_type)s, %(bid_type)s, %(publish_date)s,
                        %(remark_raw)s, %(content_key)s, %(match_source)s, %(is_confirmed)s,
                        %(parse_status)s, %(parse_error)s
                    )
                    ON DUPLICATE KEY UPDATE
                        campaign_name        = VALUES(campaign_name),
                        source_type          = VALUES(source_type),
                        channel              = VALUES(channel),
                        drama_id             = VALUES(drama_id),
                        drama_type           = VALUES(drama_type),
                        country              = VALUES(country),
                        drama_name_raw       = VALUES(drama_name_raw),
                        localized_drama_name = VALUES(localized_drama_name),
                        language_code        = VALUES(language_code),
                        language_tag_raw     = VALUES(language_tag_raw),
                        buyer_name           = VALUES(buyer_name),
                        buyer_short_name     = VALUES(buyer_short_name),
                        optimization_type    = VALUES(optimization_type),
                        bid_type             = VALUES(bid_type),
                        publish_date         = VALUES(publish_date),
                        remark_raw           = VALUES(remark_raw),
                        content_key          = VALUES(content_key),
                        parse_status         = VALUES(parse_status),
                        parse_error          = VALUES(parse_error),
                        updated_at           = CURRENT_TIMESTAMP
                """, row)
                affected += cur.rowcount
    return affected


# ─────────────────────────────────────────────────────────────
# fact_drama_daily — 写入
# ─────────────────────────────────────────────────────────────

def upsert_fact_daily_bulk(rows: list[dict]) -> int:
    """
    批量 upsert fact_drama_daily。
    唯一键：(stat_date, source_type, platform, channel, account_id, country, content_key, language_code)
    """
    if not rows:
        return 0
    affected = 0
    sql = """
        INSERT INTO fact_drama_daily (
            stat_date, source_type, platform, channel, account_id, country,
            drama_id, drama_type, localized_drama_name, language_code, content_key,
            spend, impressions, clicks, installs, registrations, purchase_value
        ) VALUES (
            %(stat_date)s, %(source_type)s, %(platform)s, %(channel)s,
            %(account_id)s, %(country)s,
            %(drama_id)s, %(drama_type)s, %(localized_drama_name)s,
            %(language_code)s, %(content_key)s,
            %(spend)s, %(impressions)s, %(clicks)s,
            %(installs)s, %(registrations)s, %(purchase_value)s
        )
        ON DUPLICATE KEY UPDATE
            drama_id             = VALUES(drama_id),
            drama_type           = VALUES(drama_type),
            localized_drama_name = VALUES(localized_drama_name),
            spend                = VALUES(spend),
            impressions          = VALUES(impressions),
            clicks               = VALUES(clicks),
            installs             = VALUES(installs),
            registrations        = VALUES(registrations),
            purchase_value       = VALUES(purchase_value),
            updated_at           = CURRENT_TIMESTAMP
    """
    with get_biz_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
                affected += cur.rowcount
    return affected


# ─────────────────────────────────────────────────────────────
# 查询：剧级总览
# ─────────────────────────────────────────────────────────────

def query_drama_summary(
    start_date: str,
    end_date: str,
    source_type: Optional[str] = None,
    platform: Optional[str] = None,
    channel: Optional[str] = None,
    country: Optional[str] = None,
    keyword: Optional[str] = None,
    language_code: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """
    按 content_key 聚合，返回剧级总览数据。
    keyword 匹配 localized_drama_name（不匹配 remark_raw）。
    remark_raw 不参与任何分组或筛选。
    """
    conditions = ["stat_date BETWEEN %s AND %s"]
    params: list = [start_date, end_date]

    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)
    if platform:
        conditions.append("platform = %s")
        params.append(platform)
    if channel:
        conditions.append("channel = %s")
        params.append(channel)
    if country:
        conditions.append("country = %s")
        params.append(country)
    if language_code:
        conditions.append("language_code = %s")
        params.append(language_code)
    if keyword:
        conditions.append("localized_drama_name LIKE %s")
        params.append(f"%{keyword}%")

    where = " AND ".join(conditions)

    count_sql = f"""
        SELECT COUNT(DISTINCT content_key) AS total
        FROM fact_drama_daily
        WHERE {where}
    """
    data_sql = f"""
        SELECT
            content_key,
            ANY_VALUE(drama_id)             AS drama_id,
            ANY_VALUE(drama_type)           AS drama_type,
            ANY_VALUE(localized_drama_name) AS localized_drama_name,
            SUM(spend)                      AS spend,
            SUM(impressions)                AS impressions,
            SUM(clicks)                     AS clicks,
            SUM(installs)                   AS installs,
            SUM(registrations)              AS registrations,
            SUM(purchase_value)             AS purchase_value,
            COUNT(DISTINCT language_code)   AS language_count
        FROM fact_drama_daily
        WHERE {where}
        GROUP BY content_key
        ORDER BY spend DESC
        LIMIT %s OFFSET %s
    """
    offset = (page - 1) * page_size

    with get_biz_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = cur.fetchone()["total"]

            cur.execute(data_sql, params + [page_size, offset])
            rows = cur.fetchall()

    result = []
    for r in rows:
        spend = float(r["spend"] or 0)
        purchase = float(r["purchase_value"] or 0)
        clicks = int(r["clicks"] or 0)
        impressions = int(r["impressions"] or 0)
        result.append({
            "content_key": r["content_key"],
            "drama_id": r["drama_id"] or "",
            "drama_type": r["drama_type"] or "",
            "localized_drama_name": r["localized_drama_name"] or "",
            "spend": round(spend, 4),
            "impressions": impressions,
            "clicks": clicks,
            "installs": int(r["installs"] or 0),
            "registrations": int(r["registrations"] or 0),
            "purchase_value": round(purchase, 4),
            "ctr": round(clicks / impressions * 100, 4) if impressions else 0,
            "cpc": round(spend / clicks, 4) if clicks else 0,
            "roas": round(purchase / spend, 4) if spend else 0,
            "language_count": int(r["language_count"] or 0),
        })

    return {"total": total, "page": page, "page_size": page_size, "rows": result}


# ─────────────────────────────────────────────────────────────
# 查询：语言版本明细
# ─────────────────────────────────────────────────────────────

def query_locale_breakdown(
    start_date: str,
    end_date: str,
    content_key: Optional[str] = None,
    drama_id: Optional[str] = None,
    source_type: Optional[str] = None,
    platform: Optional[str] = None,
    channel: Optional[str] = None,
    country: Optional[str] = None,
) -> list[dict]:
    """
    按 language_code 聚合，返回某剧的语言版本明细。
    content_key 或 drama_id 至少提供一个。
    """
    conditions = ["stat_date BETWEEN %s AND %s"]
    params: list = [start_date, end_date]

    if content_key:
        conditions.append("content_key = %s")
        params.append(content_key)
    elif drama_id:
        conditions.append("drama_id = %s")
        params.append(drama_id)

    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)
    if platform:
        conditions.append("platform = %s")
        params.append(platform)
    if channel:
        conditions.append("channel = %s")
        params.append(channel)
    if country:
        conditions.append("country = %s")
        params.append(country)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            language_code,
            ANY_VALUE(localized_drama_name) AS localized_drama_name,
            SUM(spend)                      AS spend,
            SUM(clicks)                     AS clicks,
            SUM(installs)                   AS installs,
            SUM(registrations)              AS registrations,
            SUM(purchase_value)             AS purchase_value
        FROM fact_drama_daily
        WHERE {where}
        GROUP BY language_code
        ORDER BY spend DESC
    """
    with get_biz_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    result = []
    for r in rows:
        spend = float(r["spend"] or 0)
        purchase = float(r["purchase_value"] or 0)
        result.append({
            "language_code": r["language_code"],
            "localized_drama_name": r["localized_drama_name"] or "",
            "spend": round(spend, 4),
            "clicks": int(r["clicks"] or 0),
            "installs": int(r["installs"] or 0),
            "registrations": int(r["registrations"] or 0),
            "purchase_value": round(purchase, 4),
            "roas": round(purchase / spend, 4) if spend else 0,
        })
    return result


# ─────────────────────────────────────────────────────────────
# 查询：按天趋势
# ─────────────────────────────────────────────────────────────

def query_drama_trend(
    start_date: str,
    end_date: str,
    content_key: Optional[str] = None,
    language_code: Optional[str] = None,
    source_type: Optional[str] = None,
    platform: Optional[str] = None,
    channel: Optional[str] = None,
    country: Optional[str] = None,
) -> list[dict]:
    """
    按天聚合趋势数据。
    可按 content_key 和可选 language_code 筛选。
    """
    conditions = ["stat_date BETWEEN %s AND %s"]
    params: list = [start_date, end_date]

    if content_key:
        conditions.append("content_key = %s")
        params.append(content_key)
    if language_code:
        conditions.append("language_code = %s")
        params.append(language_code)
    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)
    if platform:
        conditions.append("platform = %s")
        params.append(platform)
    if channel:
        conditions.append("channel = %s")
        params.append(channel)
    if country:
        conditions.append("country = %s")
        params.append(country)

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            stat_date,
            SUM(spend)         AS spend,
            SUM(clicks)        AS clicks,
            SUM(installs)      AS installs,
            SUM(registrations) AS registrations,
            SUM(purchase_value) AS purchase_value
        FROM fact_drama_daily
        WHERE {where}
        GROUP BY stat_date
        ORDER BY stat_date ASC
    """
    with get_biz_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        {
            "stat_date": str(r["stat_date"]),
            "spend": round(float(r["spend"] or 0), 4),
            "clicks": int(r["clicks"] or 0),
            "installs": int(r["installs"] or 0),
            "registrations": int(r["registrations"] or 0),
            "purchase_value": round(float(r["purchase_value"] or 0), 4),
        }
        for r in rows
    ]
