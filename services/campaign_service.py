"""统一 Campaign 业务编排层 — 屏蔽平台差异"""
from __future__ import annotations

from integrations.base import get_ads_adapter
from schemas.campaign import CampaignDTO
from services.oplog_service import log_operation


async def list_campaigns(platform: str, **kwargs) -> tuple[list[dict], list[CampaignDTO]]:
    adapter = get_ads_adapter(platform)
    return await adapter.list_campaigns(**kwargs)


async def update_campaign_status(
    platform: str,
    campaign_ids: list[str],
    status: str,
    operator: str = "",
    advertiser_id: str | None = None,
    **kwargs,
) -> dict:
    adapter = get_ads_adapter(platform, advertiser_id=advertiser_id)
    result = await adapter.update_campaign_status(campaign_ids, status, **kwargs)
    for cid in campaign_ids:
        log_operation(
            username=operator,
            action=status,
            target_type="campaign",
            target_id=cid,
            platform=platform,
        )
    return result
