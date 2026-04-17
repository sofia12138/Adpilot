"""平台适配器抽象基类 + 工厂函数"""
from __future__ import annotations

from abc import ABC, abstractmethod

from schemas.campaign import CampaignDTO


class BaseAdsAdapter(ABC):
    """所有广告平台适配器的基类。本轮只覆盖 campaign list + status。"""

    @abstractmethod
    async def list_campaigns(self, **kwargs) -> tuple[list[dict], list[CampaignDTO]]:
        """返回 (原始平台数据列表, 标准化 DTO 列表)"""
        ...

    @abstractmethod
    async def update_campaign_status(self, campaign_ids: list[str], status: str, **kwargs) -> dict:
        """更新 campaign 状态，返回平台原始响应"""
        ...


def get_ads_adapter(platform: str, advertiser_id: str | None = None) -> BaseAdsAdapter:
    """根据平台标识返回对应的适配器实例"""
    if platform == "tiktok":
        from integrations.tiktok.adapter import TikTokAdsAdapter
        return TikTokAdsAdapter(advertiser_id=advertiser_id)
    elif platform == "meta":
        from integrations.meta.adapter import MetaAdsAdapter
        return MetaAdsAdapter(ad_account_id=advertiser_id)
    else:
        raise ValueError(f"不支持的广告平台: {platform}")
