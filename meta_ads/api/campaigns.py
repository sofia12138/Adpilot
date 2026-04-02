"""Meta 广告系列管理"""

from __future__ import annotations
from meta_ads.api.client import MetaClient
from config import get_settings


class MetaCampaignService:
    def __init__(self, ad_account_id: str | None = None, client: MetaClient | None = None):
        self.client = client or MetaClient()
        self.ad_account_id = ad_account_id or get_settings().meta_ad_account_id

    async def list(self, limit: int = 50, filtering: str | None = None) -> dict:
        params = {
            "fields": "id,name,objective,status,effective_status,daily_budget,lifetime_budget,budget_remaining,created_time,updated_time",
            "limit": limit,
        }
        if filtering:
            params["filtering"] = filtering
        return await self.client.get(f"{self.ad_account_id}/campaigns", params)

    async def create(self, data: dict) -> dict:
        return await self.client.post(f"{self.ad_account_id}/campaigns", data)

    async def update(self, campaign_id: str, data: dict) -> dict:
        return await self.client.post(campaign_id, data)

    async def update_status(self, campaign_id: str, status: str) -> dict:
        return await self.client.post(campaign_id, {"status": status})
