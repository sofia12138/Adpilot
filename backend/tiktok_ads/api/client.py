"""TikTok Marketing API 基础 HTTP 客户端"""

import httpx
from loguru import logger
from config import get_settings

_shared_http: httpx.AsyncClient | None = None


def _get_shared_client() -> httpx.AsyncClient:
    """进程内复用的 AsyncClient，供直连完整 URL 的请求（如广告主列表）使用。"""
    global _shared_http
    if _shared_http is None:
        _shared_http = httpx.AsyncClient(timeout=30.0)
    return _shared_http


class TikTokApiError(Exception):
    def __init__(self, code: int, message: str, request_id: str = ""):
        self.code = code
        self.message = message
        self.request_id = request_id
        super().__init__(f"[{code}] {message} (request_id={request_id})")


class TikTokClient:
    def __init__(self, access_token: str | None = None):
        settings = get_settings()
        self.base_url = settings.tiktok_api_base_url
        self.access_token = access_token or settings.tiktok_access_token
        self._client = httpx.AsyncClient(timeout=30.0)

    @property
    def _headers(self) -> dict:
        return {
            "Access-Token": self.access_token,
            "Content-Type": "application/json",
        }

    async def _handle_response(self, response: httpx.Response) -> dict:
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise TikTokApiError(
                code=data.get("code", -1),
                message=data.get("message", "Unknown error"),
                request_id=data.get("request_id", ""),
            )
        logger.debug(f"API OK: request_id={data.get('request_id')}")
        return data.get("data", {})

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"GET {url}")
        resp = await self._client.get(url, headers=self._headers, params=params)
        return await self._handle_response(resp)

    async def post(self, endpoint: str, payload: dict | None = None) -> dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"POST {url}")
        resp = await self._client.post(url, headers=self._headers, json=payload or {})
        return await self._handle_response(resp)

    async def close(self):
        await self._client.aclose()
