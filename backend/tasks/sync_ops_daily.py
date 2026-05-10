"""sync_ops_daily.py — 运营数据面板日报同步任务（DMS OpenAPI 版）

数据流：
    metis_dw.ads_app_di            (period_type=day)   ─┐
    metis_dw.dwd_recharge_order_df                       ─→ DMS ExecuteScript ─→ adpilot_biz.biz_ops_daily

为什么走 DMS：与 sync_attribution_daily 同因 — metis_dw 项目级 ACL 没把当前 RAM
用户加入项目成员，pyodps 直连不通；DMS Enterprise 已开通 SELECT 权限。

时区对齐：
- ads_app_di.ds 是 LA 日，直接用作 ds
- dwd_recharge_order_df.ds 是 UTC 日，订单要按 LA 日聚合 →
    使用 FROM_UTC_TIMESTAMP(created_at_utc, 'America/Los_Angeles') 转 LA 日
- 拉 dwd 时把 UTC 分区窗口扩 [la_lo - 1, la_hi + 1] 防止 LA 日订单跨 UTC 日丢失

同步频率：每日 03:00 LA（夏令时 UTC 10:00 / 冬令时 UTC 11:00），调度由 app.py 注册到 apscheduler
默认回填窗口：30 天（运营面板最大区间）

CLI:
    python -m tasks.sync_ops_daily                          # 默认回填 30 天
    python -m tasks.sync_ops_daily --backfill 60
    python -m tasks.sync_ops_daily --start-ds 2026-04-10 --end-ds 2026-05-09
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from config import get_settings
from db import get_biz_conn
from integrations.dms_client import (
    DmsAuthError,
    DmsError,
    DmsSqlError,
    get_default_client,
)
from repositories import biz_ops_daily_repository as repo
from repositories import biz_sync_log_repository as sync_log_repo

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_ops_daily"


# ─────────────────────────────────────────────────────────────
#  SQL 模板
# ─────────────────────────────────────────────────────────────

# ads_app_di 的 ds 已经是 LA 日（period_type=day），直接 GROUP BY ds
_USER_SIDE_SQL = """
SELECT
    ds,
    SUM(new_register_uv)   AS new_register_uv,
    SUM(new_active_uv)     AS new_active_uv,
    SUM(active_uv)         AS active_uv,
    SUM(d1_retained_uv)    AS d1_retained_uv,
    SUM(d7_retained_uv)    AS d7_retained_uv,
    SUM(d30_retained_uv)   AS d30_retained_uv,
    SUM(recharge_pay_uv)   AS total_payer_uv
FROM metis_dw.ads_app_di
WHERE period_type='day' AND ds BETWEEN '{la_lo}' AND '{la_hi}'
GROUP BY ds
"""

# dwd_recharge_order_df 的 ds 是 UTC 日，订单要按 LA 日重切
# 嵌套查询：内层先把 created_at_utc 转 LA 日，外层再按 LA 日 + os_type 聚合
_PAY_SIDE_SQL = """
SELECT
    t.ds_la                                                                     AS ds,
    t.os_type                                                                   AS os_type,
    SUM(CASE WHEN t.is_subscribe = 1                       THEN t.pay_amount ELSE 0 END) / 100.0  AS subscribe_revenue_usd,
    SUM(CASE WHEN t.is_subscribe IN (0, -1)                THEN t.pay_amount ELSE 0 END) / 100.0  AS onetime_revenue_usd,
    SUM(CASE WHEN t.first_subscribe = 1                    THEN 1 ELSE 0 END)   AS first_sub_orders,
    SUM(CASE WHEN t.is_subscribe = 1 AND t.first_subscribe = 0 THEN 1 ELSE 0 END) AS repeat_sub_orders,
    SUM(CASE WHEN t.first_inapp = 1                        THEN 1 ELSE 0 END)   AS first_iap_orders,
    SUM(CASE WHEN t.is_subscribe IN (0, -1) AND t.first_inapp = 0 THEN 1 ELSE 0 END) AS repeat_iap_orders,
    COUNT(DISTINCT t.user_id)                                                   AS payer_uv
FROM (
    SELECT
        TO_CHAR(FROM_UTC_TIMESTAMP(CAST(created_at_utc AS TIMESTAMP), 'America/Los_Angeles'), 'yyyy-MM-dd') AS ds_la,
        os_type,
        is_subscribe,
        first_subscribe,
        first_inapp,
        pay_amount,
        user_id
    FROM metis_dw.dwd_recharge_order_df
    WHERE ds BETWEEN '{utc_lo}' AND '{utc_hi}'
) t
WHERE t.ds_la BETWEEN '{la_lo}' AND '{la_hi}'
  AND t.os_type IN (1, 2)
GROUP BY t.ds_la, t.os_type
"""


# ─────────────────────────────────────────────────────────────
#  字段转换
# ─────────────────────────────────────────────────────────────

def _to_int(v) -> int:
    if v is None or v == "":
        return 0
    try:
        return int(float(v))
    except Exception:
        return 0


def _to_float(v) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return round(float(v), 4)
    except Exception:
        return 0.0


def _to_str(v) -> str:
    return "" if v is None else str(v)


# ─────────────────────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────────────────────

def _today_la():
    return datetime.now(LA_TZ).date()


def _fetch_user_side(la_lo: str, la_hi: str) -> list[dict]:
    """从 MC 拉用户侧聚合（ads_app_di）"""
    settings = get_settings()
    client = get_default_client()
    sql = _USER_SIDE_SQL.format(la_lo=la_lo, la_hi=la_hi)
    logger.info("MC 用户侧 SQL 区间 %s ~ %s", la_lo, la_hi)
    try:
        result = client.execute(sql, settings.dms_mc_db_id)
    except DmsAuthError as e:
        raise RuntimeError(f"MC 用户侧权限不足: {e}") from e
    except DmsSqlError as e:
        raise RuntimeError(f"MC 用户侧 SQL 失败: {e}") from e
    except DmsError as e:
        raise RuntimeError(f"DMS 调用失败: {e}") from e
    logger.info("用户侧返回 %d 行 request_id=%s", len(result.rows), result.request_id)
    return result.rows


def _fetch_pay_side(la_lo: str, la_hi: str) -> list[dict]:
    """从 MC 拉付费侧聚合（dwd_recharge_order_df）

    UTC 分区窗口扩展为 [la_lo - 1, la_hi + 1]，确保 LA 日订单不丢
    """
    settings = get_settings()
    client = get_default_client()
    utc_lo = (datetime.strptime(la_lo, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    utc_hi = (datetime.strptime(la_hi, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    sql = _PAY_SIDE_SQL.format(la_lo=la_lo, la_hi=la_hi, utc_lo=utc_lo, utc_hi=utc_hi)
    logger.info("MC 付费侧 SQL 区间 LA[%s,%s] UTC[%s,%s]", la_lo, la_hi, utc_lo, utc_hi)
    try:
        result = client.execute(sql, settings.dms_mc_db_id)
    except DmsAuthError as e:
        raise RuntimeError(f"MC 付费侧权限不足: {e}") from e
    except DmsSqlError as e:
        raise RuntimeError(f"MC 付费侧 SQL 失败: {e}") from e
    except DmsError as e:
        raise RuntimeError(f"DMS 调用失败: {e}") from e
    logger.info("付费侧返回 %d 行 request_id=%s", len(result.rows), result.request_id)
    return result.rows


def _fetch_spend_by_day(la_lo: str, la_hi: str) -> dict[str, float]:
    """从已落库的 biz_attribution_ad_daily 按 LA 日聚合 SUM(spend) → {ds: spend_usd}

    spend 已经在归因同步任务里换算为 USD 浮点。
    biz_attribution_ad_daily 每日由 sync_attribution_daily 任务从 MC 拉取，
    我们这里只读不写，避免运营同步对归因数据有写依赖。
    """
    out: dict[str, float] = {}
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT ds_la, SUM(spend) AS spend FROM biz_attribution_ad_daily "
            "WHERE ds_la BETWEEN %s AND %s GROUP BY ds_la",
            (la_lo, la_hi),
        )
        for row in cur.fetchall():
            ds = row.get("ds_la")
            ds_str = ds.strftime("%Y-%m-%d") if hasattr(ds, "strftime") else str(ds)[:10]
            out[ds_str] = _to_float(row.get("spend"))
    return out


def _normalize_user_row(rec: dict, spend_by_ds: dict[str, float]) -> dict:
    """ads_app_di 行 → biz_ops_daily 行（os_type=0），同时合并 ad_spend"""
    ds_str = _to_str(rec.get("ds"))[:10]
    return {
        "ds": ds_str,
        "os_type": 0,
        "new_register_uv": _to_int(rec.get("new_register_uv")),
        "new_active_uv": _to_int(rec.get("new_active_uv")),
        "active_uv": _to_int(rec.get("active_uv")),
        "d1_retained_uv": _to_int(rec.get("d1_retained_uv")),
        "d7_retained_uv": _to_int(rec.get("d7_retained_uv")),
        "d30_retained_uv": _to_int(rec.get("d30_retained_uv")),
        "total_payer_uv": _to_int(rec.get("total_payer_uv")),
        # 付费字段补 0
        "subscribe_revenue_usd": 0.0,
        "onetime_revenue_usd": 0.0,
        "first_sub_orders": 0,
        "repeat_sub_orders": 0,
        "first_iap_orders": 0,
        "repeat_iap_orders": 0,
        "payer_uv": 0,
        # 投放侧 — 从归因表 lookup（默认 0）
        "ad_spend_usd": spend_by_ds.get(ds_str, 0.0),
    }


def _normalize_pay_row(rec: dict) -> dict:
    """dwd_recharge_order_df 聚合行 → biz_ops_daily 行（os_type=1/2）"""
    return {
        "ds": _to_str(rec.get("ds"))[:10],
        "os_type": _to_int(rec.get("os_type")),
        # 用户字段补 0
        "new_register_uv": 0,
        "new_active_uv": 0,
        "active_uv": 0,
        "d1_retained_uv": 0,
        "d7_retained_uv": 0,
        "d30_retained_uv": 0,
        "total_payer_uv": 0,
        # 付费字段
        "subscribe_revenue_usd": _to_float(rec.get("subscribe_revenue_usd")),
        "onetime_revenue_usd": _to_float(rec.get("onetime_revenue_usd")),
        "first_sub_orders": _to_int(rec.get("first_sub_orders")),
        "repeat_sub_orders": _to_int(rec.get("repeat_sub_orders")),
        "first_iap_orders": _to_int(rec.get("first_iap_orders")),
        "repeat_iap_orders": _to_int(rec.get("repeat_iap_orders")),
        "payer_uv": _to_int(rec.get("payer_uv")),
        # ad_spend 只在 os_type=0 行有值
        "ad_spend_usd": 0.0,
    }


def run(*, start_ds: Optional[str] = None,
        end_ds: Optional[str] = None,
        backfill_days: int = 30,
        purge_window: bool = False) -> dict:
    """同步主入口。

    参数：
      start_ds / end_ds: 显式 LA 日窗口（YYYY-MM-DD）。优先级最高。
      backfill_days:    未指定窗口时回填最近 N 天（含今天 LA），默认 30
      purge_window:     写入前是否 DELETE 区间（默认 false，走 ON DUPLICATE KEY UPDATE）
    """
    if start_ds and end_ds:
        s_ds, e_ds = start_ds, end_ds
    else:
        today = _today_la()
        s_ds = (today - timedelta(days=backfill_days)).strftime("%Y-%m-%d")
        e_ds = today.strftime("%Y-%m-%d")

    log_id = sync_log_repo.create(task_name=TASK_NAME, sync_date=e_ds)
    started = datetime.now(timezone.utc)
    user_rows = pay_rows = 0

    try:
        if purge_window:
            removed = repo.delete_window(s_ds, e_ds)
            logger.info("清理窗口 [%s, %s] 旧数据 %d 行", s_ds, e_ds, removed)

        # 用户侧 + 投放侧（合并写一行 os_type=0）
        spend_by_ds = _fetch_spend_by_day(s_ds, e_ds)
        logger.info("BIZ 归因表 spend 命中 %d 天 (区间 [%s, %s])",
                    len(spend_by_ds), s_ds, e_ds)
        user_raw = _fetch_user_side(s_ds, e_ds)
        user_norm = [_normalize_user_row(r, spend_by_ds) for r in user_raw]
        repo.upsert_batch(user_norm)
        user_rows = len(user_norm)
        logger.info("用户侧 upsert 完成 %d 行", user_rows)

        # 付费侧
        pay_raw = _fetch_pay_side(s_ds, e_ds)
        pay_norm = [_normalize_pay_row(r) for r in pay_raw if _to_int(r.get("os_type")) in (1, 2)]
        repo.upsert_batch(pay_norm)
        pay_rows = len(pay_norm)
        logger.info("付费侧 upsert 完成 %d 行", pay_rows)

        message = f"window=[{s_ds}, {e_ds}] user_rows={user_rows} pay_rows={pay_rows}"
        sync_log_repo.finish(log_id, status="success", message=message,
                             rows_affected=user_rows + pay_rows)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info("sync_ops_daily 完成: %s 耗时 %.1fs", message, elapsed)
        return {
            "status": "success",
            "start_ds": s_ds, "end_ds": e_ds,
            "user_rows": user_rows, "pay_rows": pay_rows,
            "elapsed_sec": elapsed,
        }
    except Exception as e:
        logger.exception("sync_ops_daily 失败: %s", e)
        sync_log_repo.finish(log_id, status="failed", message=str(e),
                             rows_affected=user_rows + pay_rows)
        raise


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(description="Sync metis_dw.{ads_app_di, dwd_recharge_order_df} → biz_ops_daily")
    parser.add_argument("--start-ds", dest="start_ds", default=None,
                        help="起始 LA 日 YYYY-MM-DD")
    parser.add_argument("--end-ds", dest="end_ds", default=None,
                        help="结束 LA 日 YYYY-MM-DD")
    parser.add_argument("--backfill", dest="backfill_days", type=int, default=30,
                        help="未指定窗口时回填最近 N 天（默认 30）")
    parser.add_argument("--purge", dest="purge_window", action="store_true",
                        help="写入前先 DELETE 窗口（默认走 upsert）")
    parser.add_argument("--log-level", dest="log_level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    result = run(
        start_ds=args.start_ds,
        end_ds=args.end_ds,
        backfill_days=args.backfill_days,
        purge_window=args.purge_window,
    )
    print(result)


if __name__ == "__main__":
    _cli()
