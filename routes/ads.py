from fastapi import APIRouter, Depends, Query
from tiktok_ads.api.ad import AdService
from tiktok_ads.models.ad import AdCreate, AdUpdate
from auth import get_current_user

router = APIRouter(prefix="/ads", tags=["广告"])


@router.post("/")
async def create_ad(data: AdCreate, advertiser_id: str | None = Query(None),
                    _user=Depends(get_current_user)):
    return {"message": "ok", "data": await AdService(advertiser_id).create(data)}


@router.get("/")
async def list_ads(advertiser_id: str | None = Query(None), adgroup_id: str | None = Query(None), page: int = Query(1), page_size: int = Query(20)):
    adgroup_ids = [adgroup_id] if adgroup_id else None
    return {"data": await AdService(advertiser_id).list(adgroup_ids=adgroup_ids, page=page, page_size=page_size)}


@router.put("/")
async def update_ad(data: AdUpdate, advertiser_id: str | None = Query(None),
                    _user=Depends(get_current_user)):
    return {"message": "ok", "data": await AdService(advertiser_id).update(data)}


@router.post("/status")
async def update_status(ad_ids: list[str], status: str = Query(...), advertiser_id: str | None = Query(None),
                        _user=Depends(get_current_user)):
    return {"message": "ok", "data": await AdService(advertiser_id).update_status(ad_ids, status)}
