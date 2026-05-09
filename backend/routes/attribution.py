"""归因日报查询路由 — biz_attribution_ad_daily

数据来源：metis_dw.ads_ad_delivery_di（MaxCompute）→ ClickHouse → BIZ MySQL

核心场景：
- /api/attribution/overview      大盘卡片：注册/内购/订阅/充值/N 日 ROI
- /api/attribution/daily         日趋势（按 ds 聚合，含 N 日 ROI）
- /api/attribution/by-ad         按 ad / adgroup / campaign / account 维度聚合 + 分页
- /api/attribution/top           Top N 排行榜（按指定指标）
- /api/attribution/cohort        指定 cohort 日的 D1/D7/D30/D120 ROI 曲线
- /api/attribution/data-range    返回 BIZ 表中的 ds_la 覆盖范围（前端默认时间区间用）

时区：
- 默认按 ds_account_local（账户时区近似日，LA + Phoenix 当前等价于 LA）
- 调用方可显式传 tz_basis=la 切换到上游原始 LA cohort 日
"""
from __future__ import annotations

import asyncio
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from repositories import biz_attribution_ad_daily_repository as repo

router = APIRouter(prefix="/attribution", tags=["归因日报"])

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TZ_BASIS = {"account_local", "la"}


def _check_dates(start_date: str, end_date: str):
    if not _DATE_RE.match(start_date):
        raise HTTPException(400, f"start_date 格式错误，应为 YYYY-MM-DD，实际: {start_date}")
    if not _DATE_RE.match(end_date):
        raise HTTPException(400, f"end_date 格式错误，应为 YYYY-MM-DD，实际: {end_date}")
    if start_date > end_date:
        raise HTTPException(400, f"start_date({start_date}) 不能大于 end_date({end_date})")


def _check_tz_basis(tz_basis: str):
    if tz_basis not in _TZ_BASIS:
        raise HTTPException(400, f"tz_basis 必须是 'account_local' 或 'la'，实际: {tz_basis}")


@router.get("/overview")
async def attribution_overview(
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end_date:   str = Query(..., description="结束日期 YYYY-MM-DD"),
    tz_basis:   str = Query("account_local", description="日期口径: account_local / la"),
    platform:   Optional[str] = Query(None, description="媒体过滤: tiktok / facebook / google ..."),
    account_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    adgroup_id: Optional[str] = Query(None),
    ad_id:      Optional[str] = Query(None),
    content_id: Optional[int] = Query(None, description="剧 ID 过滤"),
):
    """大盘总览：注册/内购/订阅/充值 + N 日 ROI 一锅端"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    data = await asyncio.to_thread(
        repo.get_overview,
        start_date, end_date,
        tz_basis=tz_basis, platform=platform, account_id=account_id,
        campaign_id=campaign_id, adgroup_id=adgroup_id,
        ad_id=ad_id, content_id=content_id,
    )
    return {"code": 0, "message": "ok", "data": data}


@router.get("/daily")
async def attribution_daily(
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end_date:   str = Query(..., description="结束日期 YYYY-MM-DD"),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    adgroup_id: Optional[str] = Query(None),
    ad_id:      Optional[str] = Query(None),
    content_id: Optional[int] = Query(None),
):
    """日趋势：按 ds 聚合，含 N 日 ROI 曲线"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    rows = await asyncio.to_thread(
        repo.get_daily_trend,
        start_date, end_date,
        tz_basis=tz_basis, platform=platform, account_id=account_id,
        campaign_id=campaign_id, adgroup_id=adgroup_id,
        ad_id=ad_id, content_id=content_id,
    )
    return {"code": 0, "message": "ok", "data": rows}


@router.get("/by-ad")
async def attribution_by_ad(
    start_date: str = Query(...),
    end_date:   str = Query(...),
    group_by:   str = Query("ad", description="ad / adgroup / campaign / account"),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    adgroup_id: Optional[str] = Query(None),
    ad_id:      Optional[str] = Query(None),
    content_id: Optional[int] = Query(None),
    name_filter: Optional[str] = Query(None, description="ad/adgroup/campaign 名称模糊过滤"),
    order_by:   str = Query("total_spend", description="排序字段；ROI 类传 roi_7d / roi_30d 等"),
    order_dir:  str = Query("desc"),
    page:       int = Query(1, ge=1),
    page_size:  int = Query(50, ge=1, le=200),
):
    """按 ad / adgroup / campaign / account 维度聚合，支持分页 + 排序"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    data = await asyncio.to_thread(
        repo.get_aggregated,
        start_date, end_date,
        group_by=group_by, tz_basis=tz_basis,
        platform=platform, account_id=account_id,
        campaign_id=campaign_id, adgroup_id=adgroup_id,
        ad_id=ad_id, content_id=content_id, name_filter=name_filter,
        order_by=order_by, order_dir=order_dir,
        page=page, page_size=page_size,
    )
    return {"code": 0, "message": "ok", "data": data}


@router.get("/top")
async def attribution_top(
    start_date: str = Query(...),
    end_date:   str = Query(...),
    metric:     str = Query("roi_7d", description="排行指标：roi_7d / roi_30d / total_recharge_amount / first_iap_amount / spend ..."),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
    limit:      int = Query(20, ge=1, le=100),
):
    """Top N 广告：按指定指标排行（默认 roi_7d）"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    rows = await asyncio.to_thread(
        repo.get_top_ads,
        start_date, end_date,
        metric=metric, tz_basis=tz_basis,
        platform=platform, account_id=account_id, limit=limit,
    )
    return {"code": 0, "message": "ok", "data": rows}


@router.get("/cohort")
async def attribution_cohort(
    ds:         str = Query(..., description="cohort 日 YYYY-MM-DD（默认按 ds_account_local 解读）"),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    adgroup_id: Optional[str] = Query(None),
    ad_id:      Optional[str] = Query(None),
):
    """指定 cohort 日的 D1/D3/D7/D14/D30/D90/D120 ROI 曲线"""
    if not _DATE_RE.match(ds):
        raise HTTPException(400, f"ds 格式错误，应为 YYYY-MM-DD，实际: {ds}")
    _check_tz_basis(tz_basis)
    data = await asyncio.to_thread(
        repo.get_cohort_curve,
        ds=ds, tz_basis=tz_basis,
        platform=platform, account_id=account_id,
        campaign_id=campaign_id, adgroup_id=adgroup_id, ad_id=ad_id,
    )
    return {"code": 0, "message": "ok", "data": data}


@router.get("/data-range")
async def attribution_data_range():
    """返回 BIZ 表当前覆盖的 ds_la 区间（前端默认时间区间用）"""
    data = await asyncio.to_thread(repo.get_data_range)
    return {"code": 0, "message": "ok", "data": data}


# ─────────────────────────────────────────────────────────────
#  当日实时归因（biz_attribution_ad_intraday）
#
#  与上面的 cohort daily 接口分开：
#   - daily:    T+1 cohort 完整数据（ads_ad_delivery_di）
#   - intraday: 当日小时级实时数据（ods_media_report_data_hi + dwd_invest_recharge_df）
#  前端按 tab 选择，不在后端做"今天/历史"自动路由（语义更清晰，调试也容易）
# ─────────────────────────────────────────────────────────────

from repositories import biz_attribution_ad_intraday_repository as intraday_repo  # noqa: E402

_ALLOWED_INTRADAY_METRICS = {
    "spend", "impressions", "clicks", "registration", "purchase",
    "first_iap_amount", "first_iap_count",
    "first_sub_amount", "first_sub_count",
    "total_recharge_amount",
}
_ALLOWED_INTRADAY_GROUP_BY = {"ad", "account"}


@router.get("/intraday/data-range")
async def intraday_data_range():
    """返回 intraday 表当前覆盖的 ds_account_local 区间 + 上游最新版本时间戳"""
    data = await asyncio.to_thread(intraday_repo.get_data_range)
    return {"code": 0, "message": "ok", "data": data}


@router.get("/intraday/overview")
async def intraday_overview(
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end_date:   str = Query(..., description="结束日期 YYYY-MM-DD"),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
    ad_id:      Optional[str] = Query(None),
):
    """当日实时大盘：spend / 注册 / 首充 / 总充值 / intraday ROI"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    data = await asyncio.to_thread(
        intraday_repo.get_overview,
        start_date, end_date,
        tz_basis=tz_basis, platform=platform,
        account_id=account_id, ad_id=ad_id,
    )
    return {"code": 0, "message": "ok", "data": data}


@router.get("/intraday/daily")
async def intraday_daily(
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end_date:   str = Query(..., description="结束日期 YYYY-MM-DD"),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
    ad_id:      Optional[str] = Query(None),
):
    """按 ds_account_local / ds_la 聚合的日级趋势"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    rows = await asyncio.to_thread(
        intraday_repo.get_daily_trend,
        start_date, end_date,
        tz_basis=tz_basis, platform=platform,
        account_id=account_id, ad_id=ad_id,
    )
    return {"code": 0, "message": "ok", "data": rows}


@router.get("/intraday/aggregated")
async def intraday_aggregated(
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end_date:   str = Query(..., description="结束日期 YYYY-MM-DD"),
    group_by:   str = Query("ad", description="ad / account"),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
    limit:      int = Query(200, ge=1, le=1000),
    offset:     int = Query(0,   ge=0),
    order_by:   str = Query("spend"),
    order_dir:  str = Query("DESC"),
):
    """按 ad / account 维度聚合 + 分页"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    if group_by not in _ALLOWED_INTRADAY_GROUP_BY:
        raise HTTPException(400, f"group_by 必须是 {sorted(_ALLOWED_INTRADAY_GROUP_BY)}")
    if order_by not in _ALLOWED_INTRADAY_METRICS:
        raise HTTPException(400, f"order_by 必须是 {sorted(_ALLOWED_INTRADAY_METRICS)}")
    if order_dir.upper() not in ("ASC", "DESC"):
        raise HTTPException(400, "order_dir 必须是 ASC / DESC")
    rows = await asyncio.to_thread(
        intraday_repo.get_aggregated,
        start_date, end_date,
        group_by=group_by, tz_basis=tz_basis,
        platform=platform, account_id=account_id,
        limit=limit, offset=offset,
        order_by=order_by, order_dir=order_dir,
    )
    return {"code": 0, "message": "ok", "data": rows}


@router.get("/intraday/top-ads")
async def intraday_top_ads(
    start_date: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end_date:   str = Query(..., description="结束日期 YYYY-MM-DD"),
    metric:     str = Query("spend", description=f"按何指标排序 {sorted(_ALLOWED_INTRADAY_METRICS)}"),
    limit:      int = Query(10, ge=1, le=200),
    tz_basis:   str = Query("account_local"),
    platform:   Optional[str] = Query(None),
    account_id: Optional[str] = Query(None),
):
    """按指定指标 Top N 广告"""
    _check_dates(start_date, end_date)
    _check_tz_basis(tz_basis)
    if metric not in _ALLOWED_INTRADAY_METRICS:
        raise HTTPException(400, f"metric 必须是 {sorted(_ALLOWED_INTRADAY_METRICS)}")
    rows = await asyncio.to_thread(
        intraday_repo.get_top_ads,
        start_date, end_date,
        metric=metric, limit=limit, tz_basis=tz_basis,
        platform=platform, account_id=account_id,
    )
    return {"code": 0, "message": "ok", "data": rows}
