"""数据同步路由 — /api/sync/*

提供手动触发同步和查询同步状态的接口。
定时同步由 app.py 中的 APScheduler 负责，本路由仅处理手动请求。

多模块状态说明：
  structure — Campaign / Adset / Ad 结构列表（名称、状态、预算）
  reports   — 各层级日报（花费 / 展示 / 点击 / 安装）
  returned  — 广告回传转化口径数据
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from services import sync_state

router = APIRouter(prefix="/sync", tags=["数据同步"])


async def _do_sync(start_date: str, end_date: str):
    """实际执行全量同步的异步任务（在 BackgroundTasks 中运行）"""
    from tasks.sync_campaigns import run as sync_run
    try:
        await sync_run(start_date=start_date, end_date=end_date)
    except Exception as e:
        for mod in ("structure", "reports", "returned"):
            sync_state.set_error(mod, str(e))


@router.get("/status", summary="查询同步状态（全模块）")
async def get_sync_status():
    """
    返回所有同步模块的状态快照：
      structure — Campaign/Adset/Ad 结构列表
      reports   — 日报数据（花费/展示/点击/安装）
      returned  — 回传转化数据
    """
    return {"code": 0, "data": sync_state.get_all_state()}


@router.get("/status/{module}", summary="查询单个模块同步状态")
async def get_module_status(module: str):
    """
    查询指定模块的同步状态。
    module 合法值：structure / reports / returned
    """
    if module not in sync_state.MODULES:
        raise HTTPException(
            status_code=400,
            detail=f"未知模块: {module}，合法值: {', '.join(sync_state.MODULES)}"
        )
    return {"code": 0, "data": sync_state.get_module_state(module)}


@router.post("/trigger", summary="手动触发全量数据同步")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    days: int = Query(default=2, ge=1, le=90, description="向前同步天数，默认 2 天"),
):
    """
    手动触发后台全量数据同步任务（Campaign/Adset/Ad 结构 + 所有层级日报 + 回传转化）。

    - 若任何模块正在同步，返回 409 避免重复触发。
    - 同步在后台异步执行，接口立即返回。
    - 通过 GET /api/sync/status 轮询结果。
    """
    if sync_state.is_any_running():
        raise HTTPException(status_code=409, detail="同步任务正在运行，请稍后再试")

    end   = date.today()
    start = end - timedelta(days=days - 1)

    background_tasks.add_task(_do_sync, str(start), str(end))

    return {
        "code": 0,
        "message": f"已触发全量同步，范围: {start} ~ {end}",
        "data": {"start_date": str(start), "end_date": str(end)},
    }


@router.post("/trigger/{module}", summary="手动触发指定模块同步")
async def trigger_module_sync(
    module: str,
    background_tasks: BackgroundTasks,
    days: int = Query(default=2, ge=1, le=90, description="向前同步天数，默认 2 天"),
):
    """
    手动触发指定模块的同步（目前所有模块复用同一 run() 函数，按日期范围同步）。
    module 合法值：structure / reports / returned / all
    """
    if module not in (*sync_state.MODULES, "all"):
        raise HTTPException(
            status_code=400,
            detail=f"未知模块: {module}，合法值: {', '.join(sync_state.MODULES)} / all"
        )
    if sync_state.is_any_running():
        raise HTTPException(status_code=409, detail="同步任务正在运行，请稍后再试")

    end   = date.today()
    start = end - timedelta(days=days - 1)

    background_tasks.add_task(_do_sync, str(start), str(end))

    return {
        "code": 0,
        "message": f"已触发 [{module}] 同步，范围: {start} ~ {end}",
        "data": {"module": module, "start_date": str(start), "end_date": str(end)},
    }
