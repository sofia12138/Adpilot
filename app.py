"""AdPilot — 广告投放管理系统 FastAPI 入口"""

import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from config import get_settings
from db import DatabaseUnavailableError, init_app_tables, init_biz_tables, migrate_json_data, validate_db_config
from tiktok_ads.api.client import TikTokApiError
from meta_ads.api.client import MetaApiError

# ── 定时同步任务 ───────────────────────────────────────────

_scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")


async def _scheduled_sync():
    """每 20 分钟自动同步昨天和今天的数据"""
    from services import sync_state
    from tasks.sync_campaigns import run as sync_run

    if sync_state.get_state()["is_running"]:
        logger.info("定时同步：任务已在运行，跳过本次")
        return

    end = date.today()
    start = end - timedelta(days=1)
    date_range = f"{start} ~ {end}"
    logger.info(f"定时同步开始: {date_range}")
    sync_state.set_running(True, date_range=date_range)
    try:
        await sync_run(start_date=str(start), end_date=str(end))
        sync_state.set_done()
        logger.info(f"定时同步完成: {date_range}")
    except Exception as e:
        sync_state.set_error(str(e))
        logger.error(f"定时同步失败: {e}")


# ── 生命周期：建表 + 数据迁移 + 启动调度器 ────────────────

@asynccontextmanager
async def lifespan(application: FastAPI):
    validate_db_config()
    logger.info("正在初始化应用元数据表...")
    init_app_tables()
    init_biz_tables()
    migrate_json_data()
    from services.user_service import ensure_default_admin
    ensure_default_admin()
    from services.account_service import seed_from_env
    seed_from_env()

    # 启动定时同步调度器（每 20 分钟）
    _scheduler.add_job(
        _scheduled_sync,
        trigger="interval",
        minutes=20,
        id="sync_ad_data",
        replace_existing=True,
        next_run_time=datetime.now() + timedelta(minutes=20),  # 启动后 20 分钟首次执行
    )
    _scheduler.start()
    logger.info("定时同步调度器已启动（每 20 分钟）")

    logger.info("应用启动完成")
    yield

    _scheduler.shutdown(wait=False)
    logger.info("定时同步调度器已停止")


# ── 应用实例 ──────────────────────────────────────────────

app = FastAPI(
    title="AdPilot - 广告投放管理系统",
    description="对接 TikTok & Meta Marketing API，支持广告系列/广告组/广告的创建、管理和数据报表",
    version="1.0.0",
    lifespan=lifespan,
)


# ── 中间件 ────────────────────────────────────────────────

def _setup_middleware(application: FastAPI):
    settings = get_settings()
    origins = getattr(settings, "cors_origins", None) or ["*"]
    if isinstance(origins, str):
        origins = [o.strip() for o in origins.split(",")]
    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )


_setup_middleware(app)


# ── 路由注册 ──────────────────────────────────────────────

def _register_routers(application: FastAPI):
    from routes.auth import router as auth_router
    from routes.users import router as users_router
    from routes.campaigns import router as campaign_router
    from routes.adgroups import router as adgroup_router
    from routes.ads import router as ad_router
    from routes.reports import router as report_router
    from routes.creatives import router as creative_router
    from routes.advertisers import router as advertiser_router
    from routes.meta_accounts import router as meta_account_router
    from routes.meta_campaigns import router as meta_campaign_router
    from routes.meta_adsets import router as meta_adset_router
    from routes.meta_ads import router as meta_ad_router
    from routes.meta_reports import router as meta_report_router
    from routes.bizdata import router as bizdata_router
    from routes.oplog import router as oplog_router
    from routes.templates import router as template_router
    from routes.biz import router as biz_router
    from routes.panels import router as panels_router
    from routes.insight import router as insight_router
    from routes.accounts import router as accounts_router
    from routes.analysis import router as analysis_router
    from routes.sync import router as sync_router

    for r in [
        auth_router, users_router, campaign_router, adgroup_router,
        ad_router, report_router, creative_router, advertiser_router,
        meta_account_router, meta_campaign_router, meta_adset_router,
        meta_ad_router, meta_report_router, bizdata_router, oplog_router,
        template_router, biz_router, panels_router, insight_router,
        accounts_router, analysis_router, sync_router,
    ]:
        application.include_router(r, prefix="/api")


_register_routers(app)


# ── 静态文件 ──────────────────────────────────────────────

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── 异常处理 ──────────────────────────────────────────────

@app.exception_handler(TikTokApiError)
async def tiktok_api_error_handler(request: Request, exc: TikTokApiError):
    logger.error(f"TikTok API 错误: {exc}")
    return JSONResponse(
        status_code=400,
        content={
            "error": "tiktok_api_error",
            "code": exc.code,
            "message": exc.message,
            "request_id": exc.request_id,
        },
    )


@app.exception_handler(MetaApiError)
async def meta_api_error_handler(request: Request, exc: MetaApiError):
    logger.error(f"Meta API 错误: {exc}")
    return JSONResponse(
        status_code=400,
        content={
            "error": "meta_api_error",
            "code": exc.code,
            "message": exc.message,
            "fbtrace_id": exc.fbtrace_id,
        },
    )


@app.exception_handler(DatabaseUnavailableError)
async def db_unavailable_handler(request: Request, exc: DatabaseUnavailableError):
    logger.error(f"数据库不可用: {exc}")
    return JSONResponse(
        status_code=503,
        content={"error": "database_unavailable", "message": "数据库连接不可用，请稍后重试"},
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    logger.warning(f"参数错误: {exc}")
    return JSONResponse(
        status_code=422,
        content={"error": "validation_error", "message": str(exc)},
    )


@app.exception_handler(PermissionError)
async def permission_error_handler(request: Request, exc: PermissionError):
    logger.warning(f"权限不足: {exc}")
    return JSONResponse(
        status_code=403,
        content={"error": "forbidden", "message": str(exc) or "权限不足"},
    )


@app.exception_handler(KeyError)
async def key_error_handler(request: Request, exc: KeyError):
    logger.warning(f"资源未找到: {exc}")
    return JSONResponse(
        status_code=404,
        content={"error": "not_found", "message": f"资源未找到: {exc}"},
    )


@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    logger.exception(f"未捕获异常: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "internal_error", "message": "服务器内部错误"},
    )


# ── 基础端点 ──────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def index():
    index_file = static_dir / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    return {"message": "AdPilot API is running"}


@app.get("/health", tags=["系统"])
async def health():
    settings = get_settings()
    return {
        "status": "ok",
        "tiktok_configured": bool(settings.tiktok_access_token),
        "meta_configured": bool(settings.meta_access_token),
        "tiktok_advertiser_id": settings.tiktok_advertiser_id,
        "meta_ad_account_id": settings.meta_ad_account_id,
    }


# ── 开发入口 ──────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "app:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=True,
    )
