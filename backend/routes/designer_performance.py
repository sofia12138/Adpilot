"""设计师人效报表路由"""
from __future__ import annotations

import asyncio
import re
from typing import Optional

from fastapi import APIRouter, Query, HTTPException

from config import get_settings
from repositories import designer_performance_repository

router = APIRouter(prefix="/designer-performance", tags=["设计师人效报表"])

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ALLOWED_SOURCE = {"auto", "attribution", "legacy", "blend"}


def _check_dates(start_date: str, end_date: str):
    if not _DATE_RE.match(start_date):
        raise HTTPException(400, f"start_date 格式错误，应为 YYYY-MM-DD，实际: {start_date}")
    if not _DATE_RE.match(end_date):
        raise HTTPException(400, f"end_date 格式错误，应为 YYYY-MM-DD，实际: {end_date}")
    if start_date > end_date:
        raise HTTPException(400, f"start_date({start_date}) 不能大于 end_date({end_date})")


def _resolve_source(source: str) -> str:
    if source not in _ALLOWED_SOURCE:
        raise HTTPException(400, f"source 必须是 {_ALLOWED_SOURCE}, 实际: {source}")
    if source != "auto":
        return source
    settings = get_settings()
    default = (settings.data_source_default or "").lower()
    if default in {"blend", "attribution", "legacy"}:
        return default
    return "attribution" if settings.attribution_primary else "blend"


def _float(v) -> Optional[float]:
    return float(v) if v is not None else None


@router.get("/summary")
async def designer_summary(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    platform: Optional[str] = Query(None, description="平台过滤: tiktok / meta"),
    keyword: Optional[str] = Query(None, description="设计师关键词搜索"),
    source: str = Query("auto", description="数据源: auto/attribution/legacy"),
    content_key: Optional[str] = Query(None, description="剧内容唯一键"),
    drama_keyword: Optional[str] = Query(None, description="剧名关键词（模糊匹配）"),
    language_code: Optional[str] = Query(None, description="语言代码筛选"),
):
    """按设计师维度聚合的人效汇总，默认按总消耗降序

    支持按剧筛选：content_key / drama_keyword / language_code 任意组合。
    """
    _check_dates(start_date, end_date)
    src = _resolve_source(source)
    drama_kwargs = {
        "content_key": content_key,
        "drama_keyword": drama_keyword,
        "language_code": language_code,
    }

    if src == "blend":
        rows = await asyncio.to_thread(
            designer_performance_repository.get_designer_summary_blend,
            start_date, end_date,
            platform=platform,
            keyword=keyword,
            **drama_kwargs,
        )
    elif src == "attribution":
        rows = await asyncio.to_thread(
            designer_performance_repository.get_designer_summary_attribution,
            start_date, end_date,
            platform=platform,
            keyword=keyword,
            **drama_kwargs,
        )
    else:
        rows = await asyncio.to_thread(
            designer_performance_repository.get_designer_summary,
            start_date, end_date,
            platform=platform,
            keyword=keyword,
            **drama_kwargs,
        )

    result = []
    for r in rows:
        result.append({
            "designer_name":  r.get("designer_name") or "未识别",
            "material_count": int(r.get("material_count") or 0),
            "total_spend":    round(float(r.get("total_spend") or 0), 2),
            "impressions":    int(r.get("impressions") or 0),
            "clicks":         int(r.get("clicks") or 0),
            "installs":       int(r.get("installs") or 0),
            "conversions":    int(r.get("conversions") or 0),
            "purchase_value": round(float(r.get("purchase_value") or 0), 2),
            "ctr":            _float(r.get("ctr")),
            "roas":           _float(r.get("roas")),
        })

    return {"code": 0, "message": "ok", "data": result, "_source": src}


@router.get("/materials")
async def designer_materials(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    designer_name: str = Query(..., description="设计师名称"),
    platform: Optional[str] = Query(None, description="平台过滤: tiktok / meta"),
    source: str = Query("auto", description="数据源: auto/attribution/legacy"),
    content_key: Optional[str] = Query(None, description="剧内容唯一键"),
    drama_keyword: Optional[str] = Query(None, description="剧名关键词（模糊匹配）"),
    language_code: Optional[str] = Query(None, description="语言代码筛选"),
):
    """返回指定设计师在时间范围内的素材明细列表（支持按剧筛选）"""
    _check_dates(start_date, end_date)
    src = _resolve_source(source)
    drama_kwargs = {
        "content_key": content_key,
        "drama_keyword": drama_keyword,
        "language_code": language_code,
    }

    if src == "blend":
        rows = await asyncio.to_thread(
            designer_performance_repository.get_designer_materials_blend,
            start_date, end_date, designer_name,
            platform=platform,
            **drama_kwargs,
        )
    elif src == "attribution":
        rows = await asyncio.to_thread(
            designer_performance_repository.get_designer_materials_attribution,
            start_date, end_date, designer_name,
            platform=platform,
            **drama_kwargs,
        )
    else:
        rows = await asyncio.to_thread(
            designer_performance_repository.get_designer_materials,
            start_date, end_date, designer_name,
            platform=platform,
            **drama_kwargs,
        )

    result = []
    for r in rows:
        result.append({
            "ad_id":          r.get("ad_id", ""),
            "ad_name":        r.get("ad_name", ""),
            "platform":       r.get("platform", ""),
            "campaign_name":  r.get("campaign_name", ""),
            "localized_drama_name": r.get("localized_drama_name") or "",
            "language_code":  r.get("language_code") or "",
            "content_key":    r.get("content_key") or "",
            "spend":          round(float(r.get("spend") or 0), 2),
            "impressions":    int(r.get("impressions") or 0),
            "clicks":         int(r.get("clicks") or 0),
            "installs":       int(r.get("installs") or 0),
            "registrations":  int(r.get("registrations") or 0),
            "purchase_value": round(float(r.get("purchase_value") or 0), 2),
            "ctr":            _float(r.get("ctr")),
            "roas":           _float(r.get("roas")),
        })

    return {"code": 0, "message": "ok", "data": result, "_source": src}


# ═══════════════════════ 剧名筛选选项 ═══════════════════════

@router.get("/drama-options")
async def designer_drama_options(
    start_date: str = Query(..., description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(..., description="结束日期 YYYY-MM-DD"),
    platform: Optional[str] = Query(None, description="平台过滤: tiktok / meta"),
):
    """返回有数据的剧列表 + 语言代码列表，供前端筛选下拉框使用"""
    _check_dates(start_date, end_date)

    def _query():
        from db import get_biz_conn
        sql_dramas = """
            SELECT
                m.content_key                AS content_key,
                MAX(m.localized_drama_name)  AS localized_drama_name,
                MAX(m.language_code)         AS language_code,
                SUM(n.spend)                 AS total_spend
            FROM biz_ad_daily_normalized n
            JOIN ad_drama_mapping m
                ON m.platform    = n.platform
               AND m.account_id  = n.account_id
               AND m.campaign_id = n.campaign_id
            WHERE n.stat_date BETWEEN %s AND %s
              AND m.content_key <> ''
        """
        params: list = [start_date, end_date]
        if platform:
            sql_dramas += " AND n.platform = %s"
            params.append(platform)
        sql_dramas += " GROUP BY m.content_key ORDER BY total_spend DESC LIMIT 500"

        with get_biz_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql_dramas, params)
            dramas = cur.fetchall()
            cur.execute(
                """
                SELECT DISTINCT language_code
                FROM ad_drama_mapping
                WHERE language_code <> ''
                ORDER BY language_code ASC
                """
            )
            langs = cur.fetchall()
        return dramas, langs

    dramas, langs = await asyncio.to_thread(_query)
    drama_list = [
        {
            "content_key": d.get("content_key", ""),
            "localized_drama_name": d.get("localized_drama_name", ""),
            "language_code": d.get("language_code", ""),
            "total_spend": float(d.get("total_spend") or 0),
        }
        for d in dramas
    ]
    language_list = [l.get("language_code", "") for l in langs if l.get("language_code")]
    return {
        "code": 0,
        "message": "ok",
        "data": {"dramas": drama_list, "languages": language_list},
    }
