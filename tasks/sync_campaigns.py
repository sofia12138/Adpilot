"""
Campaign / Adgroup / Ad 数据同步任务 — 从 TikTok / Meta API 拉取 **日级** 数据写入 adpilot_biz

用法:
    python -m tasks.sync_campaigns                           # 同步昨天数据
    python -m tasks.sync_campaigns --start 2025-03-01 --end 2025-03-31
    python -m tasks.sync_campaigns --platform tiktok
    python -m tasks.sync_campaigns --platform meta
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger
from config import get_settings
from repositories import (
    biz_campaign_repository,
    biz_daily_report_repository,
    biz_sync_log_repository,
)
from repositories import biz_adgroup_daily_repository, biz_ad_daily_repository
from repositories import biz_adgroup_repository, biz_ad_repository


def _date_chunks(start: str, end: str, max_days: int = 30):
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    while s <= e:
        chunk_end = min(s + timedelta(days=max_days - 1), e)
        yield s.isoformat(), chunk_end.isoformat()
        s = chunk_end + timedelta(days=1)


# ═══════════════════════════════════════════════════════════
#  TikTok 通用日报拉取器
# ═══════════════════════════════════════════════════════════

async def _sync_tiktok_report_level(
    client, advertiser_id: str,
    data_level: str, id_dim: str,
    start_date: str, end_date: str,
    name_lookup: dict[str, str],
    parent_lookup: dict[str, dict] | None = None,
    campaign_name_lookup: dict[str, str] | None = None,
    adgroup_name_lookup: dict[str, str] | None = None,
):
    """通用 TikTok 报表拉取：按 30 天分段、带 stat_time_day 维度
    
    parent_lookup: {entity_id: {"campaign_id": ..., "adgroup_id": ...}}
    campaign_name_lookup: {campaign_id: campaign_name}
    adgroup_name_lookup: {adgroup_id: adgroup_name}
    """
    metrics = ["spend", "impressions", "clicks", "conversion",
               "complete_payment", "total_complete_payment_rate",
               "registration"]       # App 内注册数（注册优化目标 / 次级转化事件）
    all_rows: list[dict] = []
    parent_lookup = parent_lookup or {}
    campaign_name_lookup = campaign_name_lookup or {}
    adgroup_name_lookup = adgroup_name_lookup or {}

    for chunk_start, chunk_end in _date_chunks(start_date, end_date, max_days=30):
        page = 1
        while True:
            params = {
                "advertiser_id": advertiser_id,
                "report_type": "BASIC",
                "data_level": data_level,
                "dimensions": json.dumps([id_dim, "stat_time_day"]),
                "metrics": json.dumps(metrics),
                "start_date": chunk_start,
                "end_date": chunk_end,
                "page": page,
                "page_size": 200,
            }
            resp = await client.get("report/integrated/get/", params)
            items = resp.get("list", [])
            if not items:
                break

            for item in items:
                dims = item.get("dimensions", {})
                m = item.get("metrics", {})
                entity_id = str(dims.get(id_dim, ""))
                stat_day = (dims.get("stat_time_day", "") or "")[:10] or chunk_start

                row = {
                    "platform": "tiktok",
                    "account_id": advertiser_id,
                    "stat_date": stat_day,
                    "spend": float(m.get("spend", 0) or 0),
                    "impressions": int(m.get("impressions", 0) or 0),
                    "clicks": int(m.get("clicks", 0) or 0),
                    "installs": 0,
                    "conversions": int(m.get("conversion", 0) or 0),
                    "revenue": float(m.get("complete_payment", 0) or 0),
                    "registrations": int(m.get("registration", 0) or 0),  # App 内注册数
                    "raw_json": item,
                }

                if data_level == "AUCTION_CAMPAIGN":
                    row["campaign_id"] = entity_id
                    row["campaign_name"] = name_lookup.get(entity_id, "")
                elif data_level == "AUCTION_ADGROUP":
                    row["adgroup_id"] = entity_id
                    row["adgroup_name"] = name_lookup.get(entity_id, "")
                    parent = parent_lookup.get(entity_id, {})
                    cid = parent.get("campaign_id", "")
                    row["campaign_id"] = cid
                    row["campaign_name"] = campaign_name_lookup.get(cid, "")
                elif data_level == "AUCTION_AD":
                    row["ad_id"] = entity_id
                    row["ad_name"] = name_lookup.get(entity_id, "")
                    parent = parent_lookup.get(entity_id, {})
                    cid = parent.get("campaign_id", "")
                    agid = parent.get("adgroup_id", "")
                    row["campaign_id"] = cid
                    row["campaign_name"] = campaign_name_lookup.get(cid, "")
                    row["adgroup_id"] = agid
                    row["adgroup_name"] = adgroup_name_lookup.get(agid, "")

                all_rows.append(row)

            page_total = resp.get("page_info", {}).get("total_number", 0)
            if page * 200 >= page_total:
                break
            page += 1

    return all_rows


# ═══════════════════════════════════════════════════════════
#  TikTok 同步
# ═══════════════════════════════════════════════════════════

async def sync_tiktok_campaigns(advertiser_id: str,
                                start_date: str, end_date: str,
                                access_token: str | None = None):
    from tiktok_ads.api.campaign import CampaignService
    from tiktok_ads.api.client import TikTokClient

    client = TikTokClient(access_token=access_token) if access_token else TikTokClient()
    svc = CampaignService(advertiser_id, client=client)

    # 1) Campaign 列表
    log_id = biz_sync_log_repository.create(task_name="sync_tiktok_campaigns", platform="tiktok", account_id=advertiser_id)
    try:
        page = 1
        all_campaigns: list[dict] = []
        while True:
            resp = await svc.list(page=page, page_size=100)
            items = resp.get("list", [])
            if not items:
                break
            all_campaigns.extend(items)
            total = resp.get("page_info", {}).get("total_number", 0)
            if len(all_campaigns) >= total:
                break
            page += 1

        rows = [{
            "platform": "tiktok", "account_id": advertiser_id,
            "campaign_id": str(c.get("campaign_id", "")),
            "campaign_name": c.get("campaign_name", ""),
            "objective": c.get("objective_type", ""), "buying_type": "",
            "status": c.get("operation_status", c.get("status", "")),
            "is_active": c.get("operation_status") == "ENABLE",
            "raw_json": c,
        } for c in all_campaigns]
        affected = biz_campaign_repository.upsert_batch(rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"同步 {len(rows)} 个 campaign")
        logger.info(f"[TikTok] campaign 列表同步完成: {len(rows)} 个")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[TikTok] campaign 列表同步失败: {e}")
        raise

    campaign_map = {str(c.get("campaign_id")): c.get("campaign_name", "") for c in all_campaigns}

    # 2) Campaign 日报
    from repositories import returned_conversion_repository
    log_id = biz_sync_log_repository.create(task_name="sync_tiktok_campaign_daily", platform="tiktok", account_id=advertiser_id, sync_date=start_date)
    try:
        report_rows = await _sync_tiktok_report_level(
            client, advertiser_id, "AUCTION_CAMPAIGN", "campaign_id",
            start_date, end_date, campaign_map)
        affected = biz_daily_report_repository.upsert_batch(report_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"campaign 日报 {len(report_rows)} 条")
        logger.info(f"[TikTok] campaign 日报同步完成: {len(report_rows)} 条")
        # 同步写入回传口径表（registrations_returned 固定为 0，TikTok 无 registration 字段）
        for r in report_rows:
            ret = _tiktok_row_to_returned_row(r, advertiser_id)
            try:
                returned_conversion_repository.upsert(**ret)
            except Exception as e_ret:
                logger.warning(f"[TikTok] returned_conversion upsert(campaign) 失败: {e_ret}")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[TikTok] campaign 日报同步失败: {e}")

    # 3) Adgroup 列表 → 入库 + 名称映射
    from tiktok_ads.api.adgroup import AdGroupService
    ag_svc = AdGroupService(advertiser_id, client=client)

    log_id = biz_sync_log_repository.create(task_name="sync_tiktok_adgroups", platform="tiktok", account_id=advertiser_id)
    all_adgroups: list[dict] = []
    try:
        page = 1
        while True:
            resp = await ag_svc.list(page=page, page_size=100)
            items = resp.get("list", [])
            if not items:
                break
            all_adgroups.extend(items)
            total = resp.get("page_info", {}).get("total_number", 0)
            if len(all_adgroups) >= total:
                break
            page += 1

        ag_rows = [{
            "platform": "tiktok", "account_id": advertiser_id,
            "campaign_id": str(ag.get("campaign_id", "")),
            "adgroup_id": str(ag.get("adgroup_id", "")),
            "adgroup_name": ag.get("adgroup_name", ""),
            "status": ag.get("operation_status", ag.get("status", "")),
            "is_active": ag.get("operation_status") == "ENABLE",
            "raw_json": ag,
        } for ag in all_adgroups]
        affected = biz_adgroup_repository.upsert_batch(ag_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"同步 {len(ag_rows)} 个 adgroup")
        logger.info(f"[TikTok] adgroup 列表同步完成: {len(ag_rows)} 个")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[TikTok] adgroup 列表同步失败: {e}")

    adgroup_name_map = {str(ag.get("adgroup_id")): ag.get("adgroup_name", "") for ag in all_adgroups}
    adgroup_parent_map = {str(ag.get("adgroup_id")): {"campaign_id": str(ag.get("campaign_id", ""))} for ag in all_adgroups}

    # 4) Adgroup 日报
    log_id = biz_sync_log_repository.create(task_name="sync_tiktok_adgroup_daily", platform="tiktok", account_id=advertiser_id, sync_date=start_date)
    try:
        adgroup_rows = await _sync_tiktok_report_level(
            client, advertiser_id, "AUCTION_ADGROUP", "adgroup_id",
            start_date, end_date, adgroup_name_map,
            parent_lookup=adgroup_parent_map,
            campaign_name_lookup=campaign_map)
        affected = biz_adgroup_daily_repository.upsert_batch(adgroup_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"adgroup 日报 {len(adgroup_rows)} 条")
        logger.info(f"[TikTok] adgroup 日报同步完成: {len(adgroup_rows)} 条")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[TikTok] adgroup 日报同步失败: {e}")

    # 5) Ad 列表 → 入库 + 名称映射
    from tiktok_ads.api.ad import AdService
    ad_svc = AdService(advertiser_id, client=client)

    log_id = biz_sync_log_repository.create(task_name="sync_tiktok_ads", platform="tiktok", account_id=advertiser_id)
    all_ads: list[dict] = []
    try:
        page = 1
        while True:
            resp = await ad_svc.list(page=page, page_size=100)
            items = resp.get("list", [])
            if not items:
                break
            all_ads.extend(items)
            total = resp.get("page_info", {}).get("total_number", 0)
            if len(all_ads) >= total:
                break
            page += 1

        ad_db_rows = [{
            "platform": "tiktok", "account_id": advertiser_id,
            "campaign_id": str(a.get("campaign_id", "")),
            "adgroup_id": str(a.get("adgroup_id", "")),
            "ad_id": str(a.get("ad_id", "")),
            "ad_name": a.get("ad_name", ""),
            "status": a.get("operation_status", a.get("status", "")),
            "is_active": a.get("operation_status") == "ENABLE",
            "raw_json": a,
        } for a in all_ads]
        affected = biz_ad_repository.upsert_batch(ad_db_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"同步 {len(ad_db_rows)} 个 ad")
        logger.info(f"[TikTok] ad 列表同步完成: {len(ad_db_rows)} 个")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[TikTok] ad 列表同步失败: {e}")

    ad_name_map = {str(a.get("ad_id")): a.get("ad_name", "") for a in all_ads}
    ad_parent_map = {str(a.get("ad_id")): {
        "campaign_id": str(a.get("campaign_id", "")),
        "adgroup_id": str(a.get("adgroup_id", "")),
    } for a in all_ads}

    # 6) Ad 日报
    log_id = biz_sync_log_repository.create(task_name="sync_tiktok_ad_daily", platform="tiktok", account_id=advertiser_id, sync_date=start_date)
    try:
        ad_rows = await _sync_tiktok_report_level(
            client, advertiser_id, "AUCTION_AD", "ad_id",
            start_date, end_date, ad_name_map,
            parent_lookup=ad_parent_map,
            campaign_name_lookup=campaign_map,
            adgroup_name_lookup=adgroup_name_map)
        affected = biz_ad_daily_repository.upsert_batch(ad_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"ad 日报 {len(ad_rows)} 条")
        logger.info(f"[TikTok] ad 日报同步完成: {len(ad_rows)} 条")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[TikTok] ad 日报同步失败: {e}")


# ═══════════════════════════════════════════════════════════
#  Meta 通用 Insights 拉取器
# ═══════════════════════════════════════════════════════════

async def _fetch_meta_insights_paged(client, ad_account_id: str,
                                     start_date: str, end_date: str,
                                     level: str, fields: str) -> list[dict]:
    import httpx
    params = {
        "fields": fields,
        "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
        "time_increment": "1",
        "level": level,
        "limit": 500,
    }
    all_data: list[dict] = []
    resp = await client.get(f"{ad_account_id}/insights", params)
    all_data.extend(resp.get("data", []))

    while resp.get("paging", {}).get("next"):
        next_url = resp["paging"]["next"]
        async with httpx.AsyncClient(timeout=30.0) as http:
            r = await http.get(next_url)
            resp = r.json()
            if "error" in resp:
                break
            all_data.extend(resp.get("data", []))
    return all_data


def _parse_meta_actions(raw_list: list | None) -> dict[str, int]:
    """
    Meta actions / action_values 字段为数组结构，需按 action_type 精确提取。
    返回 {action_type: int(value)} 字典。不要直接按 key 读取，避免漏值或解析错误。
    """
    if not raw_list:
        return {}
    result: dict[str, int] = {}
    for entry in raw_list:
        if isinstance(entry, dict):
            atype = entry.get("action_type", "")
            if atype:
                result[atype] = int(float(entry.get("value", 0) or 0))
    return result


def _parse_meta_action_values(raw_list: list | None) -> dict[str, float]:
    """
    Meta action_values 字段为数组结构，按 action_type 精确提取金额（float）。
    用于提取 purchase 金额（purchase_value_returned）。
    """
    if not raw_list:
        return {}
    result: dict[str, float] = {}
    for entry in raw_list:
        if isinstance(entry, dict):
            atype = entry.get("action_type", "")
            if atype:
                result[atype] = float(entry.get("value", 0.0) or 0.0)
    return result


def _meta_insight_to_row(item: dict, ad_account_id: str, level: str) -> dict:
    # actions / action_values 均为数组，必须通过 _parse_meta_* 函数提取
    actions = _parse_meta_actions(item.get("actions"))
    action_values = _parse_meta_action_values(item.get("action_values"))

    row = {
        "platform": "meta",
        "account_id": ad_account_id,
        "campaign_id": str(item.get("campaign_id", "")),
        "campaign_name": item.get("campaign_name", ""),
        "stat_date": item.get("date_start", ""),
        "spend": float(item.get("spend", 0) or 0),
        "impressions": int(item.get("impressions", 0) or 0),
        "clicks": int(item.get("clicks", 0) or 0),
        "installs": actions.get("mobile_app_install", 0),
        "conversions": actions.get("offsite_conversion", 0) + actions.get("purchase", 0),
        "revenue": 0,
        "raw_json": item,
    }
    if level in ("adset", "ad"):
        row["adgroup_id"] = str(item.get("adset_id", ""))
        row["adgroup_name"] = item.get("adset_name", "")
    if level == "ad":
        row["ad_id"] = str(item.get("ad_id", ""))
        row["ad_name"] = item.get("ad_name", "")
    return row


def _meta_insight_to_returned_row(item: dict, ad_account_id: str, level: str) -> dict:
    """
    将 Meta Insights 数据映射到 ad_returned_conversion_daily 回传口径行。

    字段映射说明（对应 PLATFORM_FIELD_SUPPORT["meta"]）：
    - registrations_returned:   actions 数组中 action_type=complete_registration 的 value
    - purchase_value_returned:  action_values 数组中 action_type=purchase 的 value（金额，非次数）
    - subscribe_value_returned: 0（Meta 无独立订阅价值字段）
    - d1_value_returned:        0（Meta Insights 无 D1 cohort 拆分）
    - installs:                 actions 数组中 action_type=mobile_app_install 的 value
    """
    actions = _parse_meta_actions(item.get("actions"))
    action_values = _parse_meta_action_values(item.get("action_values"))

    returned: dict = {
        "stat_date":                item.get("date_start", ""),
        "media_source":             "meta",
        "account_id":               ad_account_id,
        "campaign_id":              str(item.get("campaign_id", "")),
        "campaign_name":            item.get("campaign_name", ""),
        "adset_id":                 "",
        "adset_name":               "",
        "ad_id":                    "",
        "ad_name":                  "",
        "country":                  "",
        "platform":                 "",
        "impressions":              int(item.get("impressions", 0) or 0),
        "clicks":                   int(item.get("clicks", 0) or 0),
        "installs":                 actions.get("mobile_app_install", 0),
        "spend":                    float(item.get("spend", 0) or 0),
        # Meta 支持：从 actions 数组精确匹配 complete_registration
        "registrations_returned":   actions.get("complete_registration", 0),
        # Meta 支持：从 action_values 数组精确匹配 purchase（金额，非次数）
        "purchase_value_returned":  action_values.get("purchase", 0.0),
        # Meta 不支持：无独立订阅价值字段，固定为 0
        "subscribe_value_returned":    0.0,
        # Meta 不支持：无 D1 cohort 拆分，固定为 0
        "d1_value_returned":           0.0,
        # D0 Cohort：Meta 标准 Insights API 无法拆分 D0 cohort，固定为 0
        "d0_registrations_returned":   0,
        "d0_purchase_value_returned":  0.0,
        "d0_subscribe_value_returned": 0.0,
        "raw_payload":                 item,
    }
    if level in ("adset", "ad"):
        returned["adset_id"]   = str(item.get("adset_id", ""))
        returned["adset_name"] = item.get("adset_name", "")
    if level == "ad":
        returned["ad_id"]   = str(item.get("ad_id", ""))
        returned["ad_name"] = item.get("ad_name", "")
    return returned


def _tiktok_row_to_returned_row(row: dict, advertiser_id: str) -> dict:
    """
    将 TikTok 日报行映射到 ad_returned_conversion_daily 回传口径行。

    字段映射说明（对应 PLATFORM_FIELD_SUPPORT["tiktok"]）：
    - registrations_returned:   固定为 0
                                TikTok conversion 是安装类事件，安装 ≠ 注册，
                                complete_payment 是购买次数而非注册，均不可用作降级值。
    - purchase_value_returned:  固定为 0（TikTok complete_payment 为次数，无金额字段）
    - subscribe_value_returned: 固定为 0
    - d1_value_returned:        固定为 0
    """
    return {
        "stat_date":                row.get("stat_date", ""),
        "media_source":             "tiktok",
        "account_id":               advertiser_id,
        "campaign_id":              row.get("campaign_id", ""),
        "campaign_name":            row.get("campaign_name", ""),
        "adset_id":                 row.get("adgroup_id", ""),
        "adset_name":               row.get("adgroup_name", ""),
        "ad_id":                    row.get("ad_id", ""),
        "ad_name":                  row.get("ad_name", ""),
        "country":                  "",
        "platform":                 "",
        "impressions":              int(row.get("impressions", 0) or 0),
        "clicks":                   int(row.get("clicks", 0) or 0),
        "installs":                 int(row.get("installs", 0) or 0),
        "spend":                    float(row.get("spend", 0) or 0),
        # TikTok 支持：registration（App 内注册）+ on_web_register（落地页注册）之和
        # 在注册优化目标 / 次级转化配置下有值，安装优化目标下通常为 0
        "registrations_returned":   int(row.get("registrations", 0) or 0),
        # TikTok 不支持：无购买金额字段，固定 0
        "purchase_value_returned":     0.0,
        "subscribe_value_returned":    0.0,
        "d1_value_returned":           0.0,
        # D0 Cohort：TikTok 当前不支持，固定 0
        "d0_registrations_returned":   0,
        "d0_purchase_value_returned":  0.0,
        "d0_subscribe_value_returned": 0.0,
        "raw_payload":                 row.get("raw_json"),
    }


# ═══════════════════════════════════════════════════════════
#  Meta 同步
# ═══════════════════════════════════════════════════════════

async def sync_meta_campaigns(ad_account_id: str,
                              start_date: str, end_date: str,
                              access_token: str | None = None):
    from meta_ads.api.campaigns import MetaCampaignService
    from meta_ads.api.client import MetaClient

    client = MetaClient(access_token=access_token) if access_token else MetaClient()
    svc = MetaCampaignService(ad_account_id, client=client)

    # 1) Campaign 列表
    log_id = biz_sync_log_repository.create(task_name="sync_meta_campaigns", platform="meta", account_id=ad_account_id)
    try:
        resp = await svc.list(limit=200)
        items = resp.get("data", [])
        rows = [{
            "platform": "meta", "account_id": ad_account_id,
            "campaign_id": str(c.get("id", "")),
            "campaign_name": c.get("name", ""),
            "objective": c.get("objective", ""),
            "buying_type": c.get("buying_type", ""),
            "status": c.get("effective_status", c.get("status", "")),
            "is_active": c.get("effective_status") == "ACTIVE",
            "raw_json": c,
        } for c in items]
        affected = biz_campaign_repository.upsert_batch(rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"同步 {len(rows)} 个 campaign")
        logger.info(f"[Meta] campaign 列表同步完成: {len(rows)} 个")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[Meta] campaign 列表同步失败: {e}")
        raise

    # 2) Adset 列表 → 入库
    from meta_ads.api.adsets import MetaAdSetService
    adset_svc = MetaAdSetService(ad_account_id, client=client)

    log_id = biz_sync_log_repository.create(task_name="sync_meta_adsets", platform="meta", account_id=ad_account_id)
    all_adsets: list[dict] = []
    try:
        resp = await adset_svc.list(limit=200)
        all_adsets = resp.get("data", [])
        ag_rows = [{
            "platform": "meta", "account_id": ad_account_id,
            "campaign_id": str(a.get("campaign_id", "")),
            "adgroup_id": str(a.get("id", "")),
            "adgroup_name": a.get("name", ""),
            "status": a.get("effective_status", a.get("status", "")),
            "is_active": a.get("effective_status") == "ACTIVE",
            "raw_json": a,
        } for a in all_adsets]
        affected = biz_adgroup_repository.upsert_batch(ag_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"同步 {len(ag_rows)} 个 adset")
        logger.info(f"[Meta] adset 列表同步完成: {len(ag_rows)} 个")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[Meta] adset 列表同步失败: {e}")

    # 3) Ad 列表 → 入库
    from meta_ads.api.ads import MetaAdService
    ad_svc = MetaAdService(ad_account_id, client=client)

    log_id = biz_sync_log_repository.create(task_name="sync_meta_ads", platform="meta", account_id=ad_account_id)
    all_meta_ads: list[dict] = []
    try:
        resp = await ad_svc.list(limit=200)
        all_meta_ads = resp.get("data", [])
        ad_db_rows = [{
            "platform": "meta", "account_id": ad_account_id,
            "campaign_id": str(a.get("campaign_id", "")),
            "adgroup_id": str(a.get("adset_id", "")),
            "ad_id": str(a.get("id", "")),
            "ad_name": a.get("name", ""),
            "status": a.get("effective_status", a.get("status", "")),
            "is_active": a.get("effective_status") == "ACTIVE",
            "raw_json": a,
        } for a in all_meta_ads]
        affected = biz_ad_repository.upsert_batch(ad_db_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"同步 {len(ad_db_rows)} 个 ad")
        logger.info(f"[Meta] ad 列表同步完成: {len(ad_db_rows)} 个")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[Meta] ad 列表同步失败: {e}")

    # action_values 字段必须加入，用于提取 purchase 金额（purchase_value_returned）
    campaign_fields = "campaign_id,campaign_name,spend,impressions,clicks,actions,action_values"
    adset_fields = "campaign_id,campaign_name,adset_id,adset_name,spend,impressions,clicks,actions,action_values"
    ad_fields = "campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,actions,action_values"

    from repositories import returned_conversion_repository

    # 4) Campaign 日报
    log_id = biz_sync_log_repository.create(task_name="sync_meta_campaign_daily", platform="meta", account_id=ad_account_id, sync_date=start_date)
    try:
        data = await _fetch_meta_insights_paged(client, ad_account_id, start_date, end_date, "campaign", campaign_fields)
        report_rows = [_meta_insight_to_row(item, ad_account_id, "campaign") for item in data]
        affected = biz_daily_report_repository.upsert_batch(report_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"campaign 日报 {len(report_rows)} 条")
        logger.info(f"[Meta] campaign 日报同步完成: {len(report_rows)} 条")
        # 同步写入回传口径表（ad_returned_conversion_daily，adpilot_biz）
        for item in data:
            ret = _meta_insight_to_returned_row(item, ad_account_id, "campaign")
            try:
                returned_conversion_repository.upsert(**ret)
            except Exception as e_ret:
                logger.warning(f"[Meta] returned_conversion upsert(campaign) 失败: {e_ret}")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[Meta] campaign 日报同步失败: {e}")

    # 5) Adset 日报
    log_id = biz_sync_log_repository.create(task_name="sync_meta_adset_daily", platform="meta", account_id=ad_account_id, sync_date=start_date)
    try:
        data = await _fetch_meta_insights_paged(client, ad_account_id, start_date, end_date, "adset", adset_fields)
        adset_rows = [_meta_insight_to_row(item, ad_account_id, "adset") for item in data]
        affected = biz_adgroup_daily_repository.upsert_batch(adset_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"adset 日报 {len(adset_rows)} 条")
        logger.info(f"[Meta] adset 日报同步完成: {len(adset_rows)} 条")
        # 同步写入回传口径表（adset 级别）
        for item in data:
            ret = _meta_insight_to_returned_row(item, ad_account_id, "adset")
            try:
                returned_conversion_repository.upsert(**ret)
            except Exception as e_ret:
                logger.warning(f"[Meta] returned_conversion upsert(adset) 失败: {e_ret}")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[Meta] adset 日报同步失败: {e}")

    # 6) Ad 日报
    log_id = biz_sync_log_repository.create(task_name="sync_meta_ad_daily", platform="meta", account_id=ad_account_id, sync_date=start_date)
    try:
        data = await _fetch_meta_insights_paged(client, ad_account_id, start_date, end_date, "ad", ad_fields)
        ad_rows = [_meta_insight_to_row(item, ad_account_id, "ad") for item in data]
        affected = biz_ad_daily_repository.upsert_batch(ad_rows)
        biz_sync_log_repository.finish(log_id, status="success", rows_affected=affected, message=f"ad 日报 {len(ad_rows)} 条")
        logger.info(f"[Meta] ad 日报同步完成: {len(ad_rows)} 条")
        # 同步写入回传口径表（ad 级别，最细粒度）
        for item in data:
            ret = _meta_insight_to_returned_row(item, ad_account_id, "ad")
            try:
                returned_conversion_repository.upsert(**ret)
            except Exception as e_ret:
                logger.warning(f"[Meta] returned_conversion upsert(ad) 失败: {e_ret}")
    except Exception as e:
        biz_sync_log_repository.finish(log_id, status="failed", message=str(e))
        logger.error(f"[Meta] ad 日报同步失败: {e}")


# ═══════════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════════

async def run(platform: str | None = None,
              start_date: str | None = None, end_date: str | None = None):
    settings = get_settings()

    if not start_date:
        yesterday = date.today() - timedelta(days=1)
        start_date = yesterday.strftime("%Y-%m-%d")
    if not end_date:
        end_date = start_date

    logger.info(f"开始同步数据: platform={platform or 'all'}, range={start_date}~{end_date}")

    try:
        from repositories import biz_account_repository
        db_accounts = biz_account_repository.list_active()
    except Exception:
        db_accounts = []

    synced_any = False

    if db_accounts:
        for acct in db_accounts:
            p = acct["platform"]
            if platform and p != platform:
                continue
            aid = acct["account_id"]
            token = acct.get("access_token")
            try:
                if p == "tiktok":
                    await sync_tiktok_campaigns(aid, start_date, end_date, access_token=token)
                elif p == "meta":
                    await sync_meta_campaigns(aid, start_date, end_date, access_token=token)
                biz_account_repository.update_last_synced(acct["id"])
                synced_any = True
            except Exception as e:
                logger.error(f"同步账户 {p}/{aid} 失败: {e}")

    if not synced_any:
        if platform in (None, "tiktok"):
            advertiser_id = settings.tiktok_advertiser_id
            if advertiser_id:
                await sync_tiktok_campaigns(advertiser_id, start_date, end_date)
            else:
                logger.warning("TIKTOK_ADVERTISER_ID 未配置，跳过 TikTok 同步")
        if platform in (None, "meta"):
            ad_account_id = settings.meta_ad_account_id
            if ad_account_id:
                await sync_meta_campaigns(ad_account_id, start_date, end_date)
            else:
                logger.warning("META_AD_ACCOUNT_ID 未配置，跳过 Meta 同步")

    logger.info("同步完成")


def main():
    parser = argparse.ArgumentParser(description="AdPilot 全量数据同步")
    parser.add_argument("--platform", choices=["tiktok", "meta"], default=None)
    parser.add_argument("--start", default=None, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD")
    args = parser.parse_args()
    asyncio.run(run(platform=args.platform, start_date=args.start, end_date=args.end))


if __name__ == "__main__":
    main()
