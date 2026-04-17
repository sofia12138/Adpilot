"""操作日志路由 — 读写走 MySQL，返回兼容旧字段"""
from __future__ import annotations

from fastapi import APIRouter, Query, Depends
from auth import get_current_user
from services.oplog_service import log_operation, list_logs

router = APIRouter(prefix="/oplog", tags=["操作日志"])


@router.get("/")
async def get_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=100),
    _user=Depends(get_current_user),
):
    logs, total = list_logs(page=page, page_size=page_size)
    return {"data": logs, "total": total}


@router.post("/")
async def create_log(
    body: dict,
    user=Depends(get_current_user),
):
    log_operation(
        username=user.username,
        action=body.get("action", ""),
        target_type=body.get("target_type", ""),
        target_id=body.get("target_id", ""),
        platform=body.get("platform", ""),
    )
    return {"ok": True}
