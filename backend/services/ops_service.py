"""运营数据面板 service 层

把 biz_ops_daily 的多行（os_type=0/1/2）合并成前端 DailyOpsRow 一行的扁平结构。

约定：
- 输入 [start_date, end_date] 闭区间，区间内每个 ds 都返回一行
  （即使该 ds 在库里没有任何记录，也补全零行 — 让前端图表 X 轴对齐）
- 金额字段单位 USD（已在同步层从美分换算，DECIMAL → float）
- 日期字段统一 'YYYY-MM-DD' 字符串
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from repositories import biz_attribution_ad_intraday_repository as intraday_repo
from repositories import biz_ops_daily_repository as repo


def _ds_to_str(ds) -> str:
    if isinstance(ds, (date, datetime)):
        return ds.strftime("%Y-%m-%d")
    return str(ds)[:10]


def _empty_row(ds: str) -> dict:
    return {
        "date": ds,
        # 用户侧
        "new_register_uv": 0,
        "new_active_uv": 0,
        "active_uv": 0,
        "d1_retained_uv": 0,
        "d7_retained_uv": 0,
        "d30_retained_uv": 0,
        "total_payer_uv": 0,
        # 投放侧（全量平台合计，单位 USD）
        "ad_spend": 0.0,
        # iOS
        "ios_subscribe_revenue": 0.0,
        "ios_onetime_revenue": 0.0,
        "ios_first_sub_orders": 0,
        "ios_repeat_sub_orders": 0,
        "ios_first_iap_orders": 0,
        "ios_repeat_iap_orders": 0,
        "ios_payer_uv": 0,
        # Android
        "android_subscribe_revenue": 0.0,
        "android_onetime_revenue": 0.0,
        "android_first_sub_orders": 0,
        "android_repeat_sub_orders": 0,
        "android_first_iap_orders": 0,
        "android_repeat_iap_orders": 0,
        "android_payer_uv": 0,
    }


def _merge_user_row(out: dict, raw: dict) -> None:
    """把 os_type=0 的用户侧行（+投放）合并到目标 dict"""
    out["new_register_uv"] = int(raw.get("new_register_uv") or 0)
    out["new_active_uv"]   = int(raw.get("new_active_uv") or 0)
    out["active_uv"]       = int(raw.get("active_uv") or 0)
    out["d1_retained_uv"]  = int(raw.get("d1_retained_uv") or 0)
    out["d7_retained_uv"]  = int(raw.get("d7_retained_uv") or 0)
    out["d30_retained_uv"] = int(raw.get("d30_retained_uv") or 0)
    out["total_payer_uv"]  = int(raw.get("total_payer_uv") or 0)
    out["ad_spend"]        = float(raw.get("ad_spend_usd") or 0)


def _merge_pay_row(out: dict, raw: dict, prefix: str) -> None:
    """把 os_type=1/2 的付费行合并到目标 dict（prefix 'ios_' 或 'android_'）"""
    out[f"{prefix}subscribe_revenue"]   = float(raw.get("subscribe_revenue_usd") or 0)
    out[f"{prefix}onetime_revenue"]     = float(raw.get("onetime_revenue_usd") or 0)
    out[f"{prefix}first_sub_orders"]    = int(raw.get("first_sub_orders") or 0)
    out[f"{prefix}repeat_sub_orders"]   = int(raw.get("repeat_sub_orders") or 0)
    out[f"{prefix}first_iap_orders"]    = int(raw.get("first_iap_orders") or 0)
    out[f"{prefix}repeat_iap_orders"]   = int(raw.get("repeat_iap_orders") or 0)
    out[f"{prefix}payer_uv"]            = int(raw.get("payer_uv") or 0)


def query_daily_ops(start_date: str, end_date: str) -> list[dict]:
    """读取运营面板日报。

    返回升序日期数组，区间内每天一行。
    某天如果在库里没记录，会补全零行（图表 X 轴需要对齐）。
    """
    raw_rows = repo.query_range(start_date, end_date)

    # 先按 (ds_str → empty_row) 建桩，保证每个 ds 都有一行
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    by_ds: dict[str, dict] = {}
    cur = start
    while cur <= end:
        ds_str = cur.strftime("%Y-%m-%d")
        by_ds[ds_str] = _empty_row(ds_str)
        cur += timedelta(days=1)

    # 合并各行
    for raw in raw_rows:
        ds_str = _ds_to_str(raw.get("ds"))
        if ds_str not in by_ds:
            continue
        os_type = int(raw.get("os_type") or 0)
        if os_type == 0:
            _merge_user_row(by_ds[ds_str], raw)
        elif os_type == 1:  # Android
            _merge_pay_row(by_ds[ds_str], raw, "android_")
        elif os_type == 2:  # iOS
            _merge_pay_row(by_ds[ds_str], raw, "ios_")

    # ad_spend 兜底：T+1 cohort 表 biz_attribution_ad_daily 通常昨天/今天还没分区，
    # sync_ops_daily 也只在每天 LA 03:00 跑一次，导致面板上昨天/今天的 ad_spend 为 0。
    # 这里对 ad_spend == 0 的日期，从实时归因表 biz_attribution_ad_intraday 取 SUM(spend) 兜底。
    # 已经被 daily 同步过来的非零值不会被覆盖，避免历史数据被实时口径污染。
    _fill_ad_spend_from_intraday(by_ds, start_date, end_date)

    return [by_ds[k] for k in sorted(by_ds.keys())]


def _fill_ad_spend_from_intraday(by_ds: dict[str, dict],
                                 start_date: str, end_date: str) -> None:
    """对 by_ds 里 ad_spend == 0 的日期，用 biz_attribution_ad_intraday 实时 spend 兜底。"""
    zero_days = [ds for ds, row in by_ds.items() if not row.get("ad_spend")]
    if not zero_days:
        return
    try:
        intraday_spend = intraday_repo.sum_spend_by_ds_la(start_date, end_date)
    except Exception:
        # 兜底失败不影响主流程，保持现有 0 值返回
        return
    for ds in zero_days:
        v = intraday_spend.get(ds)
        if v and v > 0:
            by_ds[ds]["ad_spend"] = float(v)
