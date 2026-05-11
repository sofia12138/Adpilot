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
from repositories import biz_daily_report_repository as norm_repo
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
        # 数据来源标记 (前端可据此提示)
        "revenue_source": "platform",  # 'platform' = MC 全量真值；'intraday_fallback' = CK 归因兜底（偏低）
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

    # ad_spend 数据源切换说明：
    #   主源：biz_campaign_daily_normalized — 直接来自 Meta/TikTok 平台 API，
    #         跟 AdPilot Meta 操作台 / 平台后台账单 1:1 同源（口径最准）。
    #   fallback：biz_attribution_ad_intraday — 仅在主源对某天没有任何分区时兜底，
    #         避免主源同步任务中断导致面板完全空白。
    # 不再使用 biz_ops_daily.ad_spend_usd（其上游 biz_attribution_ad_daily 是归因
    # cohort 口径，TikTok 归因数据缺失会导致系统性偏低，与平台后台不符）。
    _override_ad_spend_from_normalized(by_ds, start_date, end_date)

    # 收入兜底：sync_ops_daily / sync_ops_pay_intraday 都没拉到付费侧时（如 DMS
    # AccessKey 失效），从 biz_attribution_ad_intraday 取 first_sub/first_iap 实时
    # 归因金额作为最后兜底。注意这是「投放归因流水」口径，比 dwd_recharge_order_df
    # 全量真值偏低（漏自然流量、漏未归因、漏二次充值），仅作为应急展示。
    _fill_revenue_from_intraday(by_ds, start_date, end_date)

    return [by_ds[k] for k in sorted(by_ds.keys())]


def _override_ad_spend_from_normalized(by_ds: dict[str, dict],
                                       start_date: str, end_date: str) -> None:
    """用 biz_campaign_daily_normalized 的 SUM(spend) 覆盖每天的 ad_spend。

    主源命中的日期：直接覆盖（无论原值是 0 还是非 0），保证跟操作台口径一致。
    主源未命中的日期：兜底到 biz_attribution_ad_intraday；都没有则保持原值（通常为 0）。
    """
    try:
        norm_spend = norm_repo.sum_spend_by_stat_date(start_date, end_date)
    except Exception:
        norm_spend = {}

    missing_days: list[str] = []
    for ds in by_ds.keys():
        v = norm_spend.get(ds)
        if v is not None:
            by_ds[ds]["ad_spend"] = float(v)
        elif not by_ds[ds].get("ad_spend"):
            missing_days.append(ds)

    if not missing_days:
        return
    try:
        intraday_spend = intraday_repo.sum_spend_by_ds_la(start_date, end_date)
    except Exception:
        return
    for ds in missing_days:
        v = intraday_spend.get(ds)
        if v and v > 0:
            by_ds[ds]["ad_spend"] = float(v)


def _fill_revenue_from_intraday(by_ds: dict[str, dict],
                                start_date: str, end_date: str) -> None:
    """对 daily 付费侧完全为空的日期，从 biz_attribution_ad_intraday 取归因流水兜底。

    判定条件：iOS / Android 的订阅 + 内购 4 项金额全为 0 且 4 项订单数全为 0。
    满足时，该天有可能是 sync_ops_daily / sync_ops_pay_intraday 都没拉到的应急情况。
    口径警示：CK intraday 是「按 ad_id 归因」的投放流水，比 MC 全量订单流水偏低，
    仅作为兜底展示，待 MC 同步任务恢复后会被覆盖回真值。
    """
    def _is_pay_empty(row: dict) -> bool:
        for prefix in ("ios_", "android_"):
            for k in ("subscribe_revenue", "onetime_revenue",
                      "first_sub_orders", "first_iap_orders",
                      "repeat_sub_orders", "repeat_iap_orders"):
                if row.get(f"{prefix}{k}"):
                    return False
        return True

    empty_days = [ds for ds, row in by_ds.items() if _is_pay_empty(row)]
    if not empty_days:
        return
    try:
        from db import get_biz_conn
        intraday_rev: dict[str, dict] = {}
        with get_biz_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT ds_la, "
                "       SUM(first_sub_amount)  AS first_sub_amount, "
                "       SUM(first_iap_amount)  AS first_iap_amount, "
                "       SUM(first_sub_count)   AS first_sub_count, "
                "       SUM(first_iap_count)   AS first_iap_count "
                "FROM biz_attribution_ad_intraday "
                "WHERE ds_la BETWEEN %s AND %s "
                "GROUP BY ds_la",
                (start_date, end_date),
            )
            for r in cur.fetchall():
                ds = r.get("ds_la")
                ds_str = ds.strftime("%Y-%m-%d") if hasattr(ds, "strftime") else str(ds)[:10]
                intraday_rev[ds_str] = r
    except Exception:
        return

    for ds in empty_days:
        rec = intraday_rev.get(ds)
        if not rec:
            continue
        sub_amt = float(rec.get("first_sub_amount") or 0)
        iap_amt = float(rec.get("first_iap_amount") or 0)
        sub_cnt = int(rec.get("first_sub_count") or 0)
        iap_cnt = int(rec.get("first_iap_count") or 0)
        if sub_amt + iap_amt <= 0 and sub_cnt + iap_cnt <= 0:
            continue
        # CK intraday 表无 os_type 维度，兜底时把总量统一塞到 iOS 字段（不拆平台）。
        # KPI 卡总收入 = iOS+Android 仍然正确；按平台拆分会显示在 iOS 一侧偏高，
        # 这是有意为之的「警示性失真」 — 配合 revenue_source 标记前端会提示。
        # 真值口径恢复后会被 MC 全量数据覆盖。
        by_ds[ds]["ios_subscribe_revenue"]    = round(sub_amt, 4)
        by_ds[ds]["ios_onetime_revenue"]      = round(iap_amt, 4)
        by_ds[ds]["ios_first_sub_orders"]     = sub_cnt
        by_ds[ds]["ios_first_iap_orders"]     = iap_cnt
        by_ds[ds]["revenue_source"]           = "intraday_fallback"
