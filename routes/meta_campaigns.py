"""Meta 广告系列路由 — list/status 走统一 service，create 保持原样"""
from fastapi import APIRouter, Body, Depends, Query
from meta_ads.api.campaigns import MetaCampaignService
from services import campaign_service
from auth import get_current_user, User

router = APIRouter(prefix="/meta/campaigns", tags=["Meta 广告系列"])


@router.get("/")
async def list_campaigns(ad_account_id: str | None = Query(None),
                         limit: int = Query(50)):
    raw_items, normalized = await campaign_service.list_campaigns(
        "meta", limit=limit,
    )
    return {
        "data": raw_items,
        "normalized": [dto.model_dump() for dto in normalized],
    }


@router.post("/")
async def create_campaign(data: dict = Body(...), ad_account_id: str | None = Query(None),
                          _user: User = Depends(get_current_user)):
    result = await MetaCampaignService(ad_account_id).create(data)
    return {"message": "ok", "data": result}


@router.post("/{campaign_id}/status")
async def update_status(campaign_id: str, status: str = Query(...),
                        user: User = Depends(get_current_user)):
    result = await campaign_service.update_campaign_status(
        "meta", [campaign_id], status, operator=user.username,
    )
    return {"message": "ok", "data": result}
