"""Meta (Facebook) Marketing API HTTP 客户端 — 全局连接池复用"""

from __future__ import annotations
import httpx
from loguru import logger
from config import get_settings

_shared_client: httpx.AsyncClient | None = None


def _get_shared_client() -> httpx.AsyncClient:
    global _shared_client
    if _shared_client is None or _shared_client.is_closed:
        _shared_client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
    return _shared_client


class MetaApiError(Exception):
    def __init__(self, code: int, message: str, fbtrace_id: str = ""):
        self.code = code
        self.message = message
        self.fbtrace_id = fbtrace_id
        super().__init__(f"[{code}] {message} (fbtrace_id={fbtrace_id})")


class MetaClient:
    def __init__(self, access_token: str | None = None):
        settings = get_settings()
        self.base_url = settings.meta_api_base_url
        self.access_token = access_token or settings.meta_access_token

    @property
    def _client(self) -> httpx.AsyncClient:
        return _get_shared_client()

    def _add_token(self, params: dict | None) -> dict:
        params = params or {}
        params["access_token"] = self.access_token
        return params

    async def _handle_response(self, response: httpx.Response) -> dict:
        data = response.json()
        if "error" in data:
            err = data["error"]
            raise MetaApiError(
                code=err.get("code", -1),
                message=err.get("message", "Unknown error"),
                fbtrace_id=err.get("fbtrace_id", ""),
            )
        response.raise_for_status()
        return data

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"META GET {url}")
        resp = await self._client.get(url, params=self._add_token(params))
        return await self._handle_response(resp)

    async def post(self, endpoint: str, payload: dict | None = None) -> dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"META POST {url}")
        payload = payload or {}
        payload["access_token"] = self.access_token
        resp = await self._client.post(url, data=payload)
        return await self._handle_response(resp)

    async def close(self):
        pass
