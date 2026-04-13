"""广告回传分析路由 — /api/analysis/*

本模块所有查询均基于 ad_returned_conversion_daily 表（adpilot_biz 业务库）。
数据口径为「广告平台归因回传」，非后端订单真值。
命名统一使用 returned 前缀区分，不与真实 revenue / ROI 混用。
"""
from __future__ import annotations

import asyncio
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from repositories import returned_conversion_repository

router = APIRouter(prefix="/analysis", tags=["广告回传分析（回传口径）"])

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_VALID_GROUP_BY = {"date", "media", "campaign", "adset", "ad", "country", "platform"}

# 层级视图公共筛选参数（不含 group_by / campaign_id / adset_id / ad_id）
_HIERARCHY_FILTER_PARAMS = dict(
    media_source=(Optional[str], Query(None, description="媒体来源: meta / tiktok / google")),
    account_id=(Optional[str], Query(None, description="广告账户ID")),
    country=(Optional[str], Query(None, description="国家/地区")),
    platform=(Optional[str], Query(None, description="操作系统平台: ios / android / mixed")),
    search_keyword=(Optional[str], Query(None, description="名称模糊搜索")),
)


def _validate_dates(start_date: str, end_date: str):
    if not _DATE_RE.match(start_date):
        raise HTTPException(400, f"start_date 格式错误，应为 YYYY-MM-DD: {start_date}")
    if not _DATE_RE.match(end_date):
        raise HTTPException(400, f"end_date 格式错误，应为 YYYY-MM-DD: {end_date}")
    if start_date > end_date:
        raise HTTPException(400, f"start_date ({start_date}) 不能晚于 end_date ({end_date})")


def _build_availability(
    static: dict[str, bool],
    dynamic: dict[str, bool],
) -> dict[str, dict]:
    """
    构建分离式 availability 结构。

    规则：
    - supported:       只取静态平台能力，与当前数值是否为 0 无关
                       supported=False → 平台根本不支持该字段，前端显示"暂不支持"提示
                       supported=True  → 平台支持，无论当前值是否为 0，前端正常展示数值
    - has_nonzero_data: 辅助信息，表示当前筛选范围内该字段是否存在过 >0 的值
                       不用于判断"是否支持"，仅可用于辅助提示（如"当前筛选无数据"）
    """
    return {
        field: {
            "supported":        static[field],
            "has_nonzero_data": dynamic[field],
        }
        for field in static
    }


@router.get(
    "/returned-conversion",
    summary="广告回传转化分析",
    description=(
        "基于广告平台归因回传数据聚合分析。"
        "返回 summary（汇总指标卡）+ availability（字段可用性标识）+ rows（按 group_by 分组的明细行）。"
        "所有指标为回传口径（returned），不等同于后端订单真值。"
        "数据存储于 adpilot_biz 业务库的 ad_returned_conversion_daily 表。"
    ),
)
async def returned_conversion(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    media_source: Optional[str] = Query(None, description="媒体来源: meta / tiktok / google"),
    account_id: Optional[str] = Query(None, description="广告账户ID"),
    country: Optional[str] = Query(None, description="国家/地区"),
    platform: Optional[str] = Query(None, description="操作系统平台: ios / android / mixed"),
    campaign_id: Optional[str] = Query(None, description="广告系列ID"),
    adset_id: Optional[str] = Query(None, description="广告组ID"),
    ad_id: Optional[str] = Query(None, description="广告ID"),
    search_keyword: Optional[str] = Query(None, description="名称模糊搜索（campaign/adset/ad name）"),
    group_by: str = Query(
        "date",
        description="聚合维度: date | media | campaign | adset | ad | country | platform",
    ),
    order_dir: str = Query("desc", description="排序方向: asc / desc"),
):
    _validate_dates(start_date, end_date)

    if group_by not in _VALID_GROUP_BY:
        raise HTTPException(
            400,
            f"group_by 值非法: {group_by}，合法值: {', '.join(sorted(_VALID_GROUP_BY))}",
        )

    filter_kwargs = dict(
        media_source=media_source,
        account_id=account_id,
        country=country,
        platform=platform,
        campaign_id=campaign_id,
        adset_id=adset_id,
        ad_id=ad_id,
        search_keyword=search_keyword,
    )

    # 三路并发：summary + 动态可用性 + rows
    summary, dynamic_avail, rows = await asyncio.gather(
        asyncio.to_thread(
            returned_conversion_repository.query_summary,
            start_date, end_date,
            **filter_kwargs,
        ),
        asyncio.to_thread(
            returned_conversion_repository.query_data_availability,
            start_date, end_date,
            **filter_kwargs,
        ),
        asyncio.to_thread(
            returned_conversion_repository.query_rows,
            start_date, end_date,
            group_by=group_by,
            order_dir=order_dir,
            **filter_kwargs,
        ),
    )

    # 静态：基于平台能力声明（与当前数值是否为 0 完全无关）
    static_avail = returned_conversion_repository.get_static_availability(media_source)

    # availability 结构说明：
    #   supported:        仅取自静态矩阵，表示平台是否具备该字段能力
    #                     supported=True + 值=0 → 正常展示 0
    #                     supported=False       → 显示"暂不支持"，绝不展示数字
    #   has_nonzero_data: 动态查询，当前筛选范围是否有 >0 的实际值（辅助信息）
    availability = _build_availability(static_avail, dynamic_avail)

    return {
        "code": 0,
        "message": "ok",
        "meta": {
            "data_label":  "returned",
            "disclaimer":  "基于广告平台归因回传，仅用于投放优化分析，不等同于后端订单真值。",
            "group_by":    group_by,
            "db":          "adpilot_biz / ad_returned_conversion_daily",
        },
        "summary": summary,
        "availability": availability,
        "rows": rows,
    }


@router.get(
    "/returned-conversion/hierarchy",
    summary="广告回传转化层级视图",
    description=(
        "按 (campaign_id, adset_id, ad_id) 三维 GROUP BY 一次性返回完整层级明细行，"
        "每行包含 campaign/adset/ad 的 id 与 name 以及各指标聚合值。"
        "前端从同一批数据自下而上构建 Campaign → Adset → Ad 树，确保父子数据守恒。"
    ),
)
async def returned_conversion_hierarchy(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    media_source: Optional[str] = Query(None, description="媒体来源: meta / tiktok / google"),
    account_id: Optional[str] = Query(None, description="广告账户ID"),
    country: Optional[str] = Query(None, description="国家/地区"),
    platform: Optional[str] = Query(None, description="操作系统平台: ios / android / mixed"),
    search_keyword: Optional[str] = Query(None, description="名称模糊搜索（campaign/adset/ad name）"),
):
    _validate_dates(start_date, end_date)

    filter_kwargs = dict(
        media_source=media_source,
        account_id=account_id,
        country=country,
        platform=platform,
        search_keyword=search_keyword,
    )

    summary, dynamic_avail, rows = await asyncio.gather(
        asyncio.to_thread(
            returned_conversion_repository.query_summary,
            start_date, end_date, **filter_kwargs,
        ),
        asyncio.to_thread(
            returned_conversion_repository.query_data_availability,
            start_date, end_date, **filter_kwargs,
        ),
        asyncio.to_thread(
            returned_conversion_repository.query_hierarchy_rows,
            start_date, end_date, **filter_kwargs,
        ),
    )

    static_avail = returned_conversion_repository.get_static_availability(media_source)
    availability = _build_availability(static_avail, dynamic_avail)

    return {
        "code": 0,
        "message": "ok",
        "summary": summary,
        "availability": availability,
        "rows": rows,
    }
