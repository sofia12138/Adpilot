"""素材管理路由"""

from fastapi import APIRouter, Query
from tiktok_ads.api.creative import CreativeService

router = APIRouter(prefix="/creatives", tags=["素材管理"])


@router.get("/videos")
async def list_videos(page: int = Query(1), page_size: int = Query(20)):
    service = CreativeService()
    return {"data": await service.list_videos(page, page_size)}


@router.get("/videos/info")
async def get_video_info(video_ids: str = Query(..., description="逗号分隔的视频ID")):
    service = CreativeService()
    ids = [v.strip() for v in video_ids.split(",")]
    return {"data": await service.get_video_info(ids)}


@router.get("/images")
async def list_images(page: int = Query(1), page_size: int = Query(20)):
    service = CreativeService()
    return {"data": await service.list_images(page, page_size)}


@router.get("/apps")
async def list_apps():
    service = CreativeService()
    return {"data": await service.list_apps()}


@router.get("/identities")
async def list_identities(identity_type: str = Query("BC_AUTH_TT")):
    service = CreativeService()
    return {"data": await service.list_identities(identity_type)}
