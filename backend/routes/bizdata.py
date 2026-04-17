"""业务数据路由 - 对接 MySQL 中的 channel_day_report 等表（异步化）"""
from __future__ import annotations

import asyncio
from fastapi import APIRouter, Query
from typing import Optional

from db import query_channel_report, query_channel_report_summary, query_campaign_business_map, query_media_campaign_day

router = APIRouter(prefix="/bizdata", tags=["业务数据"])


def _serialize_dates(rows: list[dict]) -> list[dict]:
    for r in rows:
        for key in ("report_date", "stat_time_day", "created_at", "updated_at"):
            if r.get(key):
                r[key] = str(r[key])
    return rows


@router.get("/channel_report")
async def channel_report(
    start_date: str = Query(...),
    end_date: str = Query(...),
    campaign_id: Optional[str] = None,
    advertiser_id: Optional[str] = None,
    ad_platform: Optional[int] = None,
):
    rows = await asyncio.to_thread(
        query_channel_report, start_date, end_date, campaign_id, advertiser_id, ad_platform
    )
    return {"data": _serialize_dates(rows)}


@router.get("/channel_report_summary")
async def channel_report_summary(
    start_date: str = Query(...),
    end_date: str = Query(...),
    campaign_id: Optional[str] = None,
    advertiser_id: Optional[str] = None,
    ad_platform: Optional[int] = None,
):
    data = await asyncio.to_thread(
        query_channel_report_summary, start_date, end_date, campaign_id, advertiser_id, ad_platform
    )
    return {"data": data}


@router.get("/campaign_business_map")
async def campaign_business_map(
    start_date: str = Query(...),
    end_date: str = Query(...),
    ad_platform: Optional[int] = None,
):
    data = await asyncio.to_thread(
        query_campaign_business_map, start_date, end_date, ad_platform
    )
    return {"data": data}


@router.get("/media_campaign_day")
async def media_campaign_day(
    start_date: str = Query(...),
    end_date: str = Query(...),
    platform: str = "tiktok",
    advertiser_id: Optional[str] = None,
):
    rows = await asyncio.to_thread(
        query_media_campaign_day, start_date, end_date, platform, advertiser_id
    )
    return {"data": _serialize_dates(rows)}
