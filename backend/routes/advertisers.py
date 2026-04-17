"""广告主账户路由 — 复用全局 httpx 连接池"""

from fastapi import APIRouter
from config import get_settings
from tiktok_ads.api.client import _get_shared_client

router = APIRouter(prefix="/advertisers", tags=["广告主"])


@router.get("/")
async def list_advertisers():
    """获取当前 Token 关联的所有广告主账户"""
    settings = get_settings()
    url = f"{settings.tiktok_api_base_url}/oauth2/advertiser/get/"
    headers = {"Access-Token": settings.tiktok_access_token, "Content-Type": "application/json"}
    params = {"app_id": settings.tiktok_app_id, "secret": settings.tiktok_app_secret}

    client = _get_shared_client()
    r = await client.get(url, headers=headers, params=params)
    data = r.json()

    if data.get("code") != 0:
        return {"data": [], "error": data.get("message")}

    return {"data": data.get("data", {}).get("list", [])}
