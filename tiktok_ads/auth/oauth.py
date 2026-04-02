"""TikTok OAuth 2.0 认证流程"""

import httpx
from loguru import logger
from config import get_settings


class TikTokAuth:
    AUTH_URL = "https://business-api.tiktok.com/portal/auth"
    TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"

    def __init__(self):
        settings = get_settings()
        self.app_id = settings.tiktok_app_id
        self.app_secret = settings.tiktok_app_secret

    def get_authorization_url(self, redirect_uri: str, state: str = "") -> str:
        params = {"app_id": self.app_id, "redirect_uri": redirect_uri, "state": state}
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.AUTH_URL}?{query}"

    async def get_access_token(self, auth_code: str) -> dict:
        payload = {"app_id": self.app_id, "secret": self.app_secret, "auth_code": auth_code}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(self.TOKEN_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"获取 access_token 失败: {data.get('message')}")
        token_data = data["data"]
        logger.info(f"获取 token 成功, advertiser_ids={token_data.get('advertiser_ids')}")
        return token_data
