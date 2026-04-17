from fastapi import APIRouter, Depends, Query
from tiktok_ads.api.adgroup import AdGroupService
from tiktok_ads.models.adgroup import AdGroupCreate, AdGroupUpdate
from auth import get_current_user

router = APIRouter(prefix="/adgroups", tags=["广告组"])


@router.post("/")
async def create_adgroup(data: AdGroupCreate, advertiser_id: str | None = Query(None),
                         _user=Depends(get_current_user)):
    return {"message": "ok", "data": await AdGroupService(advertiser_id).create(data)}


@router.get("/")
async def list_adgroups(advertiser_id: str | None = Query(None), campaign_id: str | None = Query(None), page: int = Query(1), page_size: int = Query(20)):
    campaign_ids = [campaign_id] if campaign_id else None
    return {"data": await AdGroupService(advertiser_id).list(campaign_ids=campaign_ids, page=page, page_size=page_size)}


@router.put("/")
async def update_adgroup(data: AdGroupUpdate, advertiser_id: str | None = Query(None),
                         _user=Depends(get_current_user)):
    return {"message": "ok", "data": await AdGroupService(advertiser_id).update(data)}


@router.post("/status")
async def update_status(adgroup_ids: list[str], status: str = Query(...), advertiser_id: str | None = Query(None),
                        _user=Depends(get_current_user)):
    return {"message": "ok", "data": await AdGroupService(advertiser_id).update_status(adgroup_ids, status)}
