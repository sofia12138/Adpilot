"""TikTok 广告系列路由 — list/status 走统一 service，create/update 保持原样"""
from fastapi import APIRouter, Depends, Query
from tiktok_ads.api.campaign import CampaignService
from tiktok_ads.models.campaign import CampaignCreate, CampaignUpdate
from services import campaign_service
from auth import get_current_user, User

router = APIRouter(prefix="/campaigns", tags=["广告系列"])


@router.post("/")
async def create_campaign(data: CampaignCreate, advertiser_id: str | None = Query(None),
                          _user: User = Depends(get_current_user)):
    return {"message": "ok", "data": await CampaignService(advertiser_id).create(data)}


@router.get("/")
async def list_campaigns(advertiser_id: str | None = Query(None),
                         page: int = Query(1), page_size: int = Query(20)):
    raw_items, normalized = await campaign_service.list_campaigns(
        "tiktok", page=page, page_size=page_size,
    )
    return {
        "data": {"list": raw_items},
        "normalized": [dto.model_dump() for dto in normalized],
    }


@router.put("/")
async def update_campaign(data: CampaignUpdate, advertiser_id: str | None = Query(None),
                          _user: User = Depends(get_current_user)):
    return {"message": "ok", "data": await CampaignService(advertiser_id).update(data)}


@router.post("/status")
async def update_status(campaign_ids: list[str], status: str = Query(...),
                        advertiser_id: str | None = Query(None),
                        user: User = Depends(get_current_user)):
    result = await campaign_service.update_campaign_status(
        "tiktok", campaign_ids, status,
        operator=user.username, advertiser_id=advertiser_id,
    )
    return {"message": "ok", "data": result}
