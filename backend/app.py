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
#
# 两个错开的调度任务：
#   Job 1  sync_all_job    — 全量同步（结构 + 日报 + 回传），每 20 分钟，启动后 20 分钟首次执行
#   Job 2  sync_reports_job — 仅日报 + 回传（不含结构），每 20 分钟，启动后 30 分钟首次执行（错开 10 分钟）
#
# 之所以错开而非合并：避免同时向平台 API 发起大量请求，降低触发限流风险。

_scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")


async def _sync_all_job():
    """Job 1：全量同步（Campaign/Adset/Ad 结构 + 所有层级日报 + 回传转化）"""
    from services import sync_state
    from tasks.sync_campaigns import run as sync_run

    if sync_state.is_any_running():
        logger.info("定时同步(全量)：有任务正在运行，跳过本次")
        return

    end   = date.today()
    start = end - timedelta(days=1)
    date_range = f"{start} ~ {end}"
    logger.info(f"全量同步开始: {date_range}")
    try:
        await sync_run(start_date=str(start), end_date=str(end))
        logger.info(f"全量同步完成: {date_range}")
    except Exception as e:
        for mod in ("structure", "reports", "returned"):
            sync_state.set_error(mod, str(e))
        logger.error(f"全量同步失败: {e}")

    # 全量同步完成后，附带同步优化师映射和事实表
    try:
        from tasks.sync_optimizer import run as sync_optimizer_run
        opt_start = end - timedelta(days=7)
        sync_optimizer_run(str(opt_start), str(end))
        logger.info("优化师映射同步完成")
    except Exception as e:
        logger.error(f"优化师映射同步失败: {e}")


async def _sync_reports_job():
    """Job 2：仅同步日报 + 回传（错开 10 分钟，弥补高频数据实时性）"""
    from services import sync_state
    from tasks.sync_campaigns import run as sync_run

    if sync_state.is_any_running():
        logger.info("定时同步(日报)：有任务正在运行，跳过本次")
        return

    end   = date.today()
    start = end  # 仅今天
    date_range = f"{start}"
    logger.info(f"日报补充同步开始: {date_range}")
    # 通过 platform=None 触发所有平台今天的日报同步
    # run() 内部会同时更新 sync_state
    try:
        await sync_run(start_date=str(start), end_date=str(end))
        logger.info(f"日报补充同步完成: {date_range}")
    except Exception as e:
        for mod in ("reports", "returned"):
            sync_state.set_error(mod, str(e))
        logger.error(f"日报补充同步失败: {e}")


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

    now = datetime.now()

    # Job 1：全量同步（每 20 分钟，启动后 20 分钟首次）
    _scheduler.add_job(
        _sync_all_job,
        trigger="interval",
        minutes=20,
        id="sync_all",
        replace_existing=True,
        next_run_time=now + timedelta(minutes=20),
    )

    # Job 2：日报补充同步（每 20 分钟，启动后 30 分钟首次，与 Job1 错开 10 分钟）
    _scheduler.add_job(
        _sync_reports_job,
        trigger="interval",
        minutes=20,
        id="sync_reports",
        replace_existing=True,
        next_run_time=now + timedelta(minutes=30),
    )

    _scheduler.start()
    logger.info("定时同步调度器已启动（全量每 20 分钟 | 日报补充错开 10 分钟）")

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
    from routes.drama import router as drama_router
    from routes.designer_performance import router as designer_performance_router
    from routes.optimizer_performance import router as optimizer_performance_router
    from routes.optimizer_directory import router as optimizer_directory_router
    from routes.meta_assets import router as meta_assets_router
    from routes.ad_assets import router as ad_assets_router

    for r in [
        auth_router, users_router, campaign_router, adgroup_router,
        ad_router, report_router, creative_router, advertiser_router,
        meta_account_router, meta_campaign_router, meta_adset_router,
        meta_ad_router, meta_report_router, bizdata_router, oplog_router,
        template_router, biz_router, panels_router, insight_router,
        accounts_router, analysis_router, sync_router, drama_router,
        designer_performance_router, optimizer_performance_router,
        optimizer_directory_router, meta_assets_router, ad_assets_router,
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
