"""统一 Campaign DTO — 跨平台标准化字段"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class CampaignDTO(BaseModel):
    id: str
    name: str = ""
    status: str = ""
    platform: str = ""
    spend: Optional[float] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    conversions: Optional[int] = None
    revenue: Optional[float] = None
    roi: Optional[float] = None
