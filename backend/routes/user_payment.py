"""用户付费面板路由（/api/ops/users/*）

权限：
- 复用 ops_dashboard panelKey
- 申请白名单：任何 ops_dashboard 用户可提交（含 admin/super_admin）
- 审批白名单：仅 super_admin（且 ≠ 申请人）

接口（10 个）：
- GET    /api/ops/users/summary              T+1 用户聚合列表
- GET    /api/ops/users/{user_id}/orders     单用户订单明细
- GET    /api/ops/users/orders               订单全量明细（分页）
- GET    /api/ops/users/today                实时：今日 LA 用户聚合（直查 PolarDB + 60s cache）
- GET    /api/ops/users/kpi                  顶栏 KPI（双口径）
- GET    /api/ops/users/anomaly/whitelist                 白名单列表
- GET    /api/ops/users/anomaly/applications             工单列表
- POST   /api/ops/users/anomaly/applications             提交申请
- POST   /api/ops/users/anomaly/applications/{id}/approve  通过
- POST   /api/ops/users/anomaly/applications/{id}/reject   拒绝
- POST   /api/ops/users/anomaly/applications/{id}/withdraw  撤回
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query

from auth import User, get_current_user
from services import panel_service
from services import user_anomaly_approval_service as approval_service
from services import user_payment_realtime_service as realtime_service
from services import user_payment_service
from repositories import user_anomaly_whitelist_repository as whitelist_repo

router = APIRouter(prefix="/ops/users", tags=["用户付费面板"])

_OPS_PANEL_KEY = "ops_dashboard"
_SUPER_ADMIN_ROLES = {"super_admin"}


def _require_panel(user: User = Depends(get_current_user)) -> User:
    allowed = panel_service.resolve_allowed_panels(user.username, user.role)
    if _OPS_PANEL_KEY not in allowed:
        raise HTTPException(status_code=403, detail="无运营数据面板访问权限")
    return user


def _require_super_admin(user: User = Depends(_require_panel)) -> User:
    if user.role not in _SUPER_ADMIN_ROLES:
        raise HTTPException(status_code=403, detail="仅 super_admin 可执行审批操作")
    return user


def _handle_app_error(fn):
    """把 ApplicationError 转 HTTPException 400/403。"""
    from functools import wraps

    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except approval_service.ApplicationError as e:
            status = 403 if e.code in ("self_review_forbidden", "not_applicant") else 400
            raise HTTPException(status_code=status, detail={"code": e.code, "message": e.message})
    return wrapper


# ─────────────────────────────────────────────────────────────
#  KPI / 列表查询
# ─────────────────────────────────────────────────────────────

@router.get("/kpi")
async def get_kpi(
    start_ds: Optional[str] = Query(None, description="LA 起始日 YYYY-MM-DD，与 end_ds 同时提供时启用窗口聚合"),
    end_ds: Optional[str] = Query(None),
    _: User = Depends(_require_panel),
):
    """双口径 KPI。

    - 不传 start_ds/end_ds：走 90 天累计快照（biz_user_payment_summary）
    - 同时传 start_ds & end_ds：基于 biz_user_payment_order 在该窗口内重新聚合
    """
    if start_ds and end_ds:
        return user_payment_service.get_kpi_dual_by_window(start_ds, end_ds)
    return user_payment_service.get_kpi_dual()


@router.get("/summary")
async def list_users(
    start_ds: Optional[str] = Query(None, description="LA 起始日 YYYY-MM-DD，与 end_ds 同时提供时启用窗口聚合"),
    end_ds: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    oauth_platform: Optional[int] = Query(None, description="-1 游客 / 1 google / 2 facebook / 3 apple"),
    first_channel_id: Optional[str] = Query(None, description="精确匹配单个 channel_id（保留兼容）"),
    channel_kind: Optional[str] = Query(None, description="按平台分类：organic / tiktok / meta / other"),
    first_os_type: Optional[int] = Query(None, ge=0, le=2),
    anomaly_tag: Optional[str] = Query(None),
    user_id: Optional[int] = Query(None),
    min_total_orders: Optional[int] = Query(None, ge=0),
    min_paid_orders: Optional[int] = Query(None, ge=0, description="只看成功付费用户时传 1"),
    order_by: str = Query("last_action_time_utc"),
    order_desc: bool = Query(True),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    _: User = Depends(_require_panel),
):
    if start_ds and end_ds:
        return user_payment_service.list_users_by_window(
            start_ds=start_ds, end_ds=end_ds,
            region=region, oauth_platform=oauth_platform,
            first_channel_id=first_channel_id, channel_kind=channel_kind,
            first_os_type=first_os_type,
            anomaly_tag=anomaly_tag, user_id=user_id,
            min_total_orders=min_total_orders,
            min_paid_orders=min_paid_orders,
            order_by=order_by, order_desc=order_desc,
            page=page, page_size=page_size,
        )
    return user_payment_service.list_users(
        region=region,
        oauth_platform=oauth_platform,
        first_channel_id=first_channel_id,
        channel_kind=channel_kind,
        first_os_type=first_os_type,
        anomaly_tag=anomaly_tag,
        user_id=user_id,
        min_total_orders=min_total_orders,
        min_paid_orders=min_paid_orders,
        order_by=order_by,
        order_desc=order_desc,
        page=page,
        page_size=page_size,
    )


@router.get("/orders")
async def list_orders(
    start_ds: Optional[str] = Query(None),
    end_ds: Optional[str] = Query(None),
    user_id: Optional[int] = Query(None),
    order_status: Optional[int] = Query(None, ge=0, le=6),
    os_type: Optional[int] = Query(None, ge=0, le=2),
    channel_id: Optional[str] = Query(None),
    is_subscribe: Optional[int] = Query(None, ge=-1, le=1),
    order_by: str = Query("created_at_la"),
    order_desc: bool = Query(True),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    _: User = Depends(_require_panel),
):
    return user_payment_service.list_orders(
        start_ds=start_ds, end_ds=end_ds, user_id=user_id,
        order_status=order_status, os_type=os_type, channel_id=channel_id,
        is_subscribe=is_subscribe,
        order_by=order_by, order_desc=order_desc,
        page=page, page_size=page_size,
    )


@router.get("/today")
async def list_today(
    la_ds: Optional[str] = Query(None, description="LA 日 YYYY-MM-DD，留空 = 今日 LA"),
    refresh: bool = Query(False, description="强制刷新缓存"),
    _: User = Depends(_require_panel),
):
    try:
        return realtime_service.list_today(la_ds, force_refresh=refresh)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/channel-dict")
async def get_channel_dict_route(_: User = Depends(_require_panel)):
    """返回 channel_id → 渠道元信息字典（含可读 label）。前端缓存使用。"""
    from db import get_channel_dict
    import asyncio
    return {"items": await asyncio.to_thread(get_channel_dict)}


@router.get("/{user_id}/orders")
async def list_orders_of_user(
    user_id: int = Path(..., ge=1),
    limit: int = Query(500, ge=1, le=2000),
    _: User = Depends(_require_panel),
):
    return {"items": user_payment_service.list_orders_for_user(user_id, limit=limit)}


# ─────────────────────────────────────────────────────────────
#  白名单（只读）
# ─────────────────────────────────────────────────────────────

@router.get("/anomaly/whitelist")
async def get_whitelist(_: User = Depends(_require_panel)):
    return {"items": whitelist_repo.list_whitelist()}


# ─────────────────────────────────────────────────────────────
#  审批工单
# ─────────────────────────────────────────────────────────────

@router.get("/anomaly/applications")
async def list_applications(
    status: Optional[str] = Query(None, description="pending / approved / rejected / withdrawn"),
    target_user_id: Optional[int] = Query(None),
    applicant_user: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    _: User = Depends(_require_panel),
):
    return {
        **approval_service.list_applications(
            status=status,
            target_user_id=target_user_id,
            applicant_user=applicant_user,
            page=page, page_size=page_size,
        ),
        "pending_count": approval_service.count_pending(),
    }


@router.post("/anomaly/applications")
@_handle_app_error
def create_application(
    body: dict = Body(...),
    user: User = Depends(_require_panel),
):
    target_user_id = int(body.get("target_user_id") or 0)
    if target_user_id <= 0:
        raise HTTPException(status_code=400, detail="target_user_id 必填且 > 0")
    return approval_service.submit_application(
        target_user_id=target_user_id,
        requested_tag=str(body.get("requested_tag") or "whitelist"),
        action=str(body.get("action") or "add"),
        reason=str(body.get("reason") or ""),
        applicant_user=user.username,
    )


@router.post("/anomaly/applications/{application_id}/approve")
@_handle_app_error
def approve_application(
    application_id: int = Path(..., ge=1),
    body: dict = Body(default_factory=dict),
    user: User = Depends(_require_super_admin),
):
    return approval_service.approve(
        application_id=application_id,
        reviewer_user=user.username,
        review_note=str(body.get("review_note") or ""),
    )


@router.post("/anomaly/applications/{application_id}/reject")
@_handle_app_error
def reject_application(
    application_id: int = Path(..., ge=1),
    body: dict = Body(default_factory=dict),
    user: User = Depends(_require_super_admin),
):
    return approval_service.reject(
        application_id=application_id,
        reviewer_user=user.username,
        review_note=str(body.get("review_note") or ""),
    )


@router.post("/anomaly/applications/{application_id}/withdraw")
@_handle_app_error
def withdraw_application(
    application_id: int = Path(..., ge=1),
    user: User = Depends(_require_panel),
):
    return approval_service.withdraw(
        application_id=application_id,
        applicant_user=user.username,
    )
