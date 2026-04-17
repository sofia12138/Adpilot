from fastapi import APIRouter, Body, Depends, Query
from meta_ads.api.adsets import MetaAdSetService
from auth import get_current_user

router = APIRouter(prefix="/meta/adsets", tags=["Meta 广告组"])


@router.get("/")
async def list_adsets(ad_account_id: str | None = Query(None), campaign_id: str | None = Query(None), limit: int = Query(50)):
    data = await MetaAdSetService(ad_account_id).list(campaign_id=campaign_id, limit=limit)
    return {"data": data.get("data", [])}


@router.post("/")
async def create_adset(data: dict = Body(...), ad_account_id: str | None = Query(None),
                       _user=Depends(get_current_user)):
    result = await MetaAdSetService(ad_account_id).create(data)
    return {"message": "ok", "data": result}


@router.post("/{adset_id}/status")
async def update_status(adset_id: str, status: str = Query(...),
                        _user=Depends(get_current_user)):
    result = await MetaAdSetService().update_status(adset_id, status)
    return {"message": "ok", "data": result}
