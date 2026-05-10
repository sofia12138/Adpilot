"""运营数据面板路由

GET /api/ops/daily-stats?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
    - 仅 ops_dashboard 面板权限的用户可访问（默认即 super_admin）
    - 返回升序日期数组，区间内每天一行（无数据补零行）
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import User, get_current_user
from services import ops_service, panel_service

router = APIRouter(prefix="/ops", tags=["运营数据"])

_OPS_PANEL_KEY = "ops_dashboard"
_MAX_RANGE_DAYS = 90


def _require_ops_panel(user: User = Depends(get_current_user)) -> User:
    """要求当前用户在 allowed_panels 中含 ops_dashboard"""
    allowed = panel_service.resolve_allowed_panels(user.username, user.role)
    if _OPS_PANEL_KEY not in allowed:
        raise HTTPException(status_code=403, detail="无运营数据面板访问权限")
    return user


def _parse_date(s: str, name: str) -> str:
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{name} 格式错误，应为 YYYY-MM-DD")
    return s


@router.get("/daily-stats")
async def daily_stats(
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD（含）"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD（含）"),
    _: User = Depends(_require_ops_panel),
):
    s = _parse_date(start_date, "start_date")
    e = _parse_date(end_date, "end_date")

    if s > e:
        raise HTTPException(status_code=400, detail="start_date 不能晚于 end_date")

    days = (datetime.strptime(e, "%Y-%m-%d") - datetime.strptime(s, "%Y-%m-%d")).days + 1
    if days > _MAX_RANGE_DAYS:
        raise HTTPException(status_code=400, detail=f"区间过大，最多 {_MAX_RANGE_DAYS} 天")

    rows = ops_service.query_daily_ops(s, e)
    return {"rows": rows}
