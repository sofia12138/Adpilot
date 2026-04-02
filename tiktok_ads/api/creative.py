"""素材管理（视频、图片）"""

from __future__ import annotations

import json

from tiktok_ads.api.client import TikTokClient
from config import get_settings


class CreativeService:
    def __init__(self, client: TikTokClient | None = None):
        self.client = client or TikTokClient()
        self.advertiser_id = get_settings().tiktok_advertiser_id

    async def list_videos(self, page: int = 1, page_size: int = 20) -> dict:
        params = {
            "advertiser_id": self.advertiser_id,
            "filtering": json.dumps({}),
            "page": page,
            "page_size": page_size,
        }
        return await self.client.get("file/video/ad/search/", params)

    async def get_video_info(self, video_ids: list[str]) -> dict:
        params = {
            "advertiser_id": self.advertiser_id,
            "video_ids": json.dumps(video_ids),
        }
        return await self.client.get("file/video/ad/info/", params)

    async def list_images(self, page: int = 1, page_size: int = 20) -> dict:
        params = {
            "advertiser_id": self.advertiser_id,
            "filtering": json.dumps({}),
            "page": page,
            "page_size": page_size,
        }
        return await self.client.get("file/image/ad/search/", params)

    async def upload_image_by_url(self, image_url: str, file_name: str) -> dict:
        payload = {
            "advertiser_id": self.advertiser_id,
            "upload_type": "UPLOAD_BY_URL",
            "image_url": image_url,
            "file_name": file_name,
        }
        return await self.client.post("file/image/ad/upload/", payload)

    async def list_apps(self) -> dict:
        params = {"advertiser_id": self.advertiser_id}
        return await self.client.get("app/list/", params)

    async def list_identities(self, identity_type: str = "BC_AUTH_TT") -> dict:
        params = {
            "advertiser_id": self.advertiser_id,
            "identity_type": identity_type,
        }
        return await self.client.get("identity/get/", params)
