"""sync_ops_polardb_intraday.py — 运营面板付费侧实时层（30 分钟刷新）

数据流：
    matrix_order.recharge_order (PolarDB)
        ↓ 每 30 分钟拉今日 + 昨日 LA 窗口
    adpilot_biz.biz_ops_daily_intraday

为什么单独拆出来：
    - T+1 任务 (sync_ops_polardb_daily) 每 2 小时跑 1 次，刷的是 shadow 表
    - 实时层覆盖"今日+昨日"两天 LA，由 ops API 智能路由直接读取
    - 写入独立的 biz_ops_daily_intraday 表，避免污染对账 shadow 表

口径：与 sync_ops_polardb_daily 完全一致，仅窗口和目标表不同。

调度：每 30 分钟（同 sync_ops_pay_intraday 节奏），无 AccessKey 依赖。
任务结束时自动清理超过 2 天的旧行（保留今日+昨日）。

CLI:
    python -m tasks.sync_ops_polardb_intraday                    # 默认窗口=今天+昨天 LA
    python -m tasks.sync_ops_polardb_intraday --backfill 7
    python -m tasks.sync_ops_polardb_intraday --start-ds 2026-05-09 --end-ds 2026-05-11
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from repositories import biz_ops_daily_intraday_repository as repo
from repositories import biz_sync_log_repository as sync_log_repo
from repositories import recharge_order_repository as order_repo

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_ops_polardb_intraday"

# 默认窗口：今天 + 昨天 LA（与 sync_attribution_intraday 一致）
DEFAULT_WINDOW_DAYS = 2

# 超过 N 天的旧 LA 日行任务结束时清理（保留今日+昨日 → 2）
RETAIN_DAYS = 2


def _today_la():
    return datetime.now(LA_TZ).date()


def _normalize_row(rec: dict) -> dict:
    """聚合行 → biz_ops_daily_intraday 行"""
    ds_raw = rec.get("ds")
    ds_str = ds_raw.strftime("%Y-%m-%d") if hasattr(ds_raw, "strftime") else str(ds_raw)[:10]
    return {
        "ds": ds_str,
        "os_type": int(rec.get("os_type") or 0),
        "subscribe_revenue_usd": round(float(rec.get("subscribe_revenue_usd") or 0), 4),
        "onetime_revenue_usd":   round(float(rec.get("onetime_revenue_usd") or 0), 4),
        "first_sub_orders":      int(rec.get("first_sub_orders") or 0),
        "repeat_sub_orders":     int(rec.get("repeat_sub_orders") or 0),
        "first_iap_orders":      int(rec.get("first_iap_orders") or 0),
        "repeat_iap_orders":     int(rec.get("repeat_iap_orders") or 0),
        "payer_uv":              int(rec.get("payer_uv") or 0),
        "upstream_max_id":       int(rec.get("upstream_max_id") or 0),
    }


def run(*, start_ds: Optional[str] = None,
        end_ds: Optional[str] = None,
        backfill_days: Optional[int] = None) -> dict:
    """实时同步主入口。

    参数：
      start_ds / end_ds: 显式 LA 窗口（YYYY-MM-DD），优先级最高。
      backfill_days:    未指定窗口时回填最近 N 天 LA，默认 None=今天+昨天。
    """
    if start_ds and end_ds:
        s_ds, e_ds = start_ds, end_ds
    else:
        today = _today_la()
        if backfill_days is not None and backfill_days > 0:
            s_ds = (today - timedelta(days=backfill_days)).strftime("%Y-%m-%d")
        else:
            s_ds = (today - timedelta(days=DEFAULT_WINDOW_DAYS - 1)).strftime("%Y-%m-%d")
        e_ds = today.strftime("%Y-%m-%d")

    log_id = sync_log_repo.create(task_name=TASK_NAME, sync_date=e_ds)
    started = datetime.now(timezone.utc)
    rows = 0
    pruned = 0

    try:
        raw = order_repo.fetch_pay_side_by_la_day(s_ds, e_ds)
        norm = [_normalize_row(r) for r in raw if int(r.get("os_type") or 0) in (1, 2)]
        repo.upsert_batch(norm)
        rows = len(norm)
        logger.info("intraday upsert 完成 %d 行（窗口 [%s, %s]）", rows, s_ds, e_ds)

        # 主动清理旧数据，避免实时表越长越大
        pruned = repo.prune_older_than(RETAIN_DAYS)
        if pruned:
            logger.info("intraday 清理超过 %d 天的旧行 %d 行", RETAIN_DAYS, pruned)

        message = f"window=[{s_ds}, {e_ds}] rows={rows} pruned={pruned}"
        sync_log_repo.finish(log_id, status="success", message=message, rows_affected=rows)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info("sync_ops_polardb_intraday 完成: %s 耗时 %.1fs", message, elapsed)
        return {
            "status": "success",
            "start_ds": s_ds, "end_ds": e_ds,
            "rows": rows, "pruned": pruned,
            "elapsed_sec": elapsed,
        }
    except Exception as e:
        logger.exception("sync_ops_polardb_intraday 失败: %s", e)
        sync_log_repo.finish(log_id, status="failed", message=str(e), rows_affected=rows)
        raise


def _cli():
    parser = argparse.ArgumentParser(
        description="Sync matrix_order.recharge_order → biz_ops_daily_intraday (实时 30min)"
    )
    parser.add_argument("--start-ds", dest="start_ds", default=None,
                        help="起始 LA 日 YYYY-MM-DD")
    parser.add_argument("--end-ds", dest="end_ds", default=None,
                        help="结束 LA 日 YYYY-MM-DD")
    parser.add_argument("--backfill", dest="backfill_days", type=int, default=None,
                        help="未指定窗口时回填最近 N 天 LA（默认 2 天=今天+昨天）")
    parser.add_argument("--log-level", dest="log_level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    print(run(
        start_ds=args.start_ds,
        end_ds=args.end_ds,
        backfill_days=args.backfill_days,
    ))


if __name__ == "__main__":
    _cli()
