"""区域渠道分析 service 层

职责：
- 把两张聚合表 (register / revenue) 的行从 cents → USD，channel_kind int → str 标签
- 智能数据源路由：
    auto    → 今/昨日 LA 走 _intraday 表，其余走 _daily 表
    daily   → 全部走 _daily（老行为）
    intraday→ 全部走 _intraday（仅今/昨日 LA 有数据）

返回结构面向前端，前端在内存自行 reshape 成 KPI / 趋势 / 国家表三种视图。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from repositories import biz_ops_region_repository as repo

logger = logging.getLogger(__name__)
_LA_TZ = ZoneInfo("America/Los_Angeles")

# channel_kind int → str 字面量（前端友好）
KIND_INT_TO_STR = {0: "organic", 1: "tiktok", 2: "meta", 3: "other"}


def _ds_str(ds) -> str:
    if isinstance(ds, (date, datetime)):
        return ds.strftime("%Y-%m-%d")
    return str(ds)[:10]


def _kind_str(kind) -> str:
    try:
        return KIND_INT_TO_STR.get(int(kind), "other")
    except Exception:
        return "other"


def _format_register_row(r: dict, *, source: str) -> dict:
    return {
        "ds": _ds_str(r.get("ds")),
        "region": (r.get("region") or "UNK"),
        "channel_kind": _kind_str(r.get("channel_kind")),
        "register_uv": int(r.get("register_uv") or 0),
        "data_source": source,
    }


def _format_revenue_row(r: dict, *, source: str) -> dict:
    return {
        "ds": _ds_str(r.get("ds")),
        "region": (r.get("region") or "UNK"),
        "channel_kind": _kind_str(r.get("channel_kind")),
        "os_type": int(r.get("os_type") or 0),
        "payer_uv": int(r.get("payer_uv") or 0),
        "order_cnt": int(r.get("order_cnt") or 0),
        "revenue_usd": round(int(r.get("revenue_cents") or 0) / 100.0, 2),
        "sub_revenue_usd": round(int(r.get("sub_revenue_cents") or 0) / 100.0, 2),
        "iap_revenue_usd": round(int(r.get("iap_revenue_cents") or 0) / 100.0, 2),
        "data_source": source,
    }


def _resolve_realtime_dates() -> set[str]:
    today = datetime.now(_LA_TZ).date()
    yesterday = today - timedelta(days=1)
    return {today.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")}


def _safe_register_range(start_ds: str, end_ds: str, *, table: str) -> list[dict]:
    try:
        return repo.query_register_range(start_ds, end_ds, table=table)
    except Exception as e:
        logger.warning("biz_ops_region 注册表查询失败 table=%s: %s", table, e)
        return []


def _safe_revenue_range(start_ds: str, end_ds: str, *, table: str) -> list[dict]:
    try:
        return repo.query_revenue_range(start_ds, end_ds, table=table)
    except Exception as e:
        logger.warning("biz_ops_region 充值表查询失败 table=%s: %s", table, e)
        return []


def query_daily_stats(start_date: str, end_date: str, *, source: str = "auto") -> dict:
    """读取区域渠道分析数据。

    返回:
        {
          "register_rows": [...],   # 注册侧 (ds × region × channel_kind)
          "revenue_rows":  [...],   # 充值侧 (ds × region × channel_kind × os_type)
          "data_source":   "auto" | "daily" | "intraday"
        }

    auto 模式下：
      - 今/昨日 LA 行从 _intraday 取（标记 data_source='intraday'）
      - 其余日期从 _daily 取（标记 data_source='daily'）
    """
    register_rows: list[dict] = []
    revenue_rows: list[dict] = []

    if source == "intraday":
        # 仅 _intraday（窗口被自动限制为今/昨日 LA）
        for r in _safe_register_range(start_date, end_date, table=repo.REGISTER_INTRADAY):
            register_rows.append(_format_register_row(r, source="intraday"))
        for r in _safe_revenue_range(start_date, end_date, table=repo.REVENUE_INTRADAY):
            revenue_rows.append(_format_revenue_row(r, source="intraday"))
        return {
            "register_rows": register_rows,
            "revenue_rows": revenue_rows,
            "data_source": "intraday",
        }

    if source == "daily":
        for r in _safe_register_range(start_date, end_date, table=repo.REGISTER_DAILY):
            register_rows.append(_format_register_row(r, source="daily"))
        for r in _safe_revenue_range(start_date, end_date, table=repo.REVENUE_DAILY):
            revenue_rows.append(_format_revenue_row(r, source="daily"))
        return {
            "register_rows": register_rows,
            "revenue_rows": revenue_rows,
            "data_source": "daily",
        }

    # auto: 拼接 _daily（历史） + _intraday（今/昨日 LA），实时层覆盖
    realtime_dates = _resolve_realtime_dates()

    daily_register = _safe_register_range(start_date, end_date, table=repo.REGISTER_DAILY)
    daily_revenue = _safe_revenue_range(start_date, end_date, table=repo.REVENUE_DAILY)

    intraday_register = _safe_register_range(
        start_date, end_date, table=repo.REGISTER_INTRADAY,
    )
    intraday_revenue = _safe_revenue_range(
        start_date, end_date, table=repo.REVENUE_INTRADAY,
    )

    # 注册侧合并：实时表覆盖今/昨日的 daily 行
    for r in daily_register:
        ds = _ds_str(r.get("ds"))
        if ds in realtime_dates:
            continue  # 让位给 intraday
        register_rows.append(_format_register_row(r, source="daily"))
    for r in intraday_register:
        ds = _ds_str(r.get("ds"))
        if ds in realtime_dates:
            register_rows.append(_format_register_row(r, source="intraday"))

    # 充值侧合并
    for r in daily_revenue:
        ds = _ds_str(r.get("ds"))
        if ds in realtime_dates:
            continue
        revenue_rows.append(_format_revenue_row(r, source="daily"))
    for r in intraday_revenue:
        ds = _ds_str(r.get("ds"))
        if ds in realtime_dates:
            revenue_rows.append(_format_revenue_row(r, source="intraday"))

    return {
        "register_rows": register_rows,
        "revenue_rows": revenue_rows,
        "data_source": "auto",
    }
