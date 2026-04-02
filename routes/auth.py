from fastapi import APIRouter, Query
from tiktok_ads.auth.oauth import TikTokAuth

router = APIRouter(prefix="/auth", tags=["认证"])


@router.get("/authorize")
async def get_auth_url(redirect_uri: str = Query(...)):
    auth = TikTokAuth()
    return {"authorization_url": auth.get_authorization_url(redirect_uri)}


@router.get("/callback")
async def auth_callback(auth_code: str = Query(...)):
    auth = TikTokAuth()
    token_data = await auth.get_access_token(auth_code)
    return {"access_token": token_data.get("access_token"), "advertiser_ids": token_data.get("advertiser_ids")}
