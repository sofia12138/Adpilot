"""数据同步路由 — /api/sync/*

提供手动触发同步和查询同步状态的接口。
定时同步由 app.py 中的 APScheduler 负责，本路由仅处理手动请求。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from services import sync_state

router = APIRouter(prefix="/sync", tags=["数据同步"])


async def _do_sync(start_date: str, end_date: str):
    """实际执行同步的异步任务（在 BackgroundTasks 中运行）"""
    from tasks.sync_campaigns import run as sync_run

    sync_state.set_running(True, date_range=f"{start_date} ~ {end_date}")
    try:
        await sync_run(start_date=start_date, end_date=end_date)
        sync_state.set_done()
    except Exception as e:
        sync_state.set_error(str(e))


@router.get("/status", summary="查询同步状态")
async def get_sync_status():
    """返回当前同步任务的运行状态、上次完成时间和最近一次错误。"""
    return {"code": 0, "data": sync_state.get_state()}


@router.post("/trigger", summary="手动触发数据同步")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    days: int = Query(default=2, ge=1, le=90, description="向前同步天数，默认 2 天"),
):
    """
    手动触发后台数据同步任务。

    - 若同步任务已在运行，返回 409 避免重复触发。
    - 同步在后台异步执行，接口立即返回。
    - 通过 GET /api/sync/status 轮询结果。
    """
    if sync_state.get_state()["is_running"]:
        raise HTTPException(status_code=409, detail="同步任务正在运行，请稍后再试")

    end = date.today()
    start = end - timedelta(days=days - 1)
    background_tasks.add_task(_do_sync, str(start), str(end))

    return {
        "code": 0,
        "message": f"已触发同步，范围: {start} ~ {end}",
        "data": {"start_date": str(start), "end_date": str(end)},
    }
