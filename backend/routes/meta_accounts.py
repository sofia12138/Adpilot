from fastapi import APIRouter, Query
from meta_ads.api.accounts import MetaAccountService

router = APIRouter(prefix="/meta/accounts", tags=["Meta 广告账户"])


@router.get("/")
async def list_accounts():
    try:
        data = await MetaAccountService().list_accounts()
        return {"data": data.get("data", [])}
    except Exception as e:
        return {"data": [], "error": str(e)}


@router.get("/info")
async def get_account_info(ad_account_id: str | None = Query(None)):
    data = await MetaAccountService().get_account_info(ad_account_id)
    return {"data": data}
