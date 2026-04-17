"""Insight 配置路由"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from auth import User, get_current_user
from services import insight_config_service
from services.oplog_service import log_operation

router = APIRouter(prefix="/insight", tags=["Insight 配置"])


class RoiThresholds(BaseModel):
    min: float
    low: float
    target: float
    high: float


class InsightConfigBody(BaseModel):
    roi: RoiThresholds


def _require_super_admin(user: User = Depends(get_current_user)) -> User:
    if user.role not in ("admin", "super_admin"):
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


@router.get("/config")
async def get_config(_: User = Depends(get_current_user)):
    return insight_config_service.get_insight_config()


@router.put("/config")
async def update_config(body: InsightConfigBody, admin: User = Depends(_require_super_admin)):
    insight_config_service.update_insight_config(body.model_dump())
    log_operation(
        username=admin.username,
        action="更新Insight配置",
        target_type="insight_config",
        target_id="roi_thresholds",
    )
    return {"ok": True, **insight_config_service.get_insight_config()}
