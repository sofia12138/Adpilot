from fastapi import APIRouter, Body, Depends, Query
from meta_ads.api.ads import MetaAdService
from auth import get_current_user

router = APIRouter(prefix="/meta/ads", tags=["Meta 广告"])


@router.get("/")
async def list_ads(ad_account_id: str | None = Query(None), adset_id: str | None = Query(None), limit: int = Query(50)):
    data = await MetaAdService(ad_account_id).list(adset_id=adset_id, limit=limit)
    return {"data": data.get("data", [])}


@router.post("/")
async def create_ad(data: dict = Body(...), ad_account_id: str | None = Query(None),
                    _user=Depends(get_current_user)):
    result = await MetaAdService(ad_account_id).create(data)
    return {"message": "ok", "data": result}


@router.post("/{ad_id}/status")
async def update_status(ad_id: str, status: str = Query(...),
                        _user=Depends(get_current_user)):
    result = await MetaAdService().update_status(ad_id, status)
    return {"message": "ok", "data": result}
