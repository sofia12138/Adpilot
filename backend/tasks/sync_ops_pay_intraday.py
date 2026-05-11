"""sync_ops_pay_intraday.py — 运营面板付费侧高频同步 (MaxCompute via DMS)

数据流（与 sync_ops_daily 的付费侧完全一致）：
    metis_dw.dwd_recharge_order_df (UTC 日分区)
        → 按 LA 日 + os_type (1=Android, 2=iOS) 重切聚合
        → adpilot_biz.biz_ops_daily (os_type=1/2 行)

为什么单独拆出来：
- 主任务 sync_ops_daily 每天 LA 03:00 才跑 1 次，导致今天/昨天的运营面板
  「订阅收入 / IAP 收入」最长延迟到 24h+ 才能看到。
- 本任务每 30 分钟跑一次，仅刷付费侧（user 侧 ads_app_di 上游就是天级，
  没必要高频刷）。
- 与 sync_ops_daily 共享同一张表 + ON DUPLICATE KEY UPDATE，主任务跑出来的
  全量回填值会覆盖本任务的临时值；反之本任务也只覆盖付费侧两行，不动 user 侧。

口径与 sync_ops_daily 完全相同：
- 付费侧 SQL = sync_ops_daily._PAY_SIDE_SQL（已嵌入 LA 日重切 + UTC 兜底窗口）
- 字段转换 = sync_ops_daily._normalize_pay_row
- 写入接口 = repositories.biz_ops_daily_repository.upsert_batch

CLI:
    python -m tasks.sync_ops_pay_intraday                  # 默认窗口=今天+昨天 LA
    python -m tasks.sync_ops_pay_intraday --backfill 7     # 回填最近 7 天 LA
    python -m tasks.sync_ops_pay_intraday --start-ds 2026-05-09 --end-ds 2026-05-11
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from repositories import biz_ops_daily_repository as repo
from repositories import biz_sync_log_repository as sync_log_repo
from tasks.sync_ops_daily import (
    _fetch_pay_side,
    _normalize_pay_row,
    _to_int,
)

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_ops_pay_intraday"

# 默认窗口：今天 + 昨天（与 sync_attribution_intraday 一致），平衡时延和 DMS QPS
DEFAULT_WINDOW_DAYS = 2


def _today_la():
    return datetime.now(LA_TZ).date()


def run(*, start_ds: Optional[str] = None,
        end_ds: Optional[str] = None,
        backfill_days: Optional[int] = None) -> dict:
    """同步主入口（仅付费侧）。

    参数：
      start_ds / end_ds: 显式 LA 窗口（YYYY-MM-DD）。优先级最高。
      backfill_days:    未指定窗口时，回填最近 N 天 LA。默认 None=今天+昨天。
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
    pay_rows = 0

    try:
        pay_raw = _fetch_pay_side(s_ds, e_ds)
        pay_norm = [
            _normalize_pay_row(r)
            for r in pay_raw
            if _to_int(r.get("os_type")) in (1, 2)
        ]
        repo.upsert_batch(pay_norm)
        pay_rows = len(pay_norm)
        logger.info("付费侧 upsert 完成 %d 行 (window=[%s, %s])", pay_rows, s_ds, e_ds)

        message = f"window=[{s_ds}, {e_ds}] pay_rows={pay_rows}"
        sync_log_repo.finish(log_id, status="success", message=message,
                             rows_affected=pay_rows)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info("sync_ops_pay_intraday 完成: %s 耗时 %.1fs", message, elapsed)
        return {
            "status": "success",
            "start_ds": s_ds, "end_ds": e_ds,
            "pay_rows": pay_rows,
            "elapsed_sec": elapsed,
        }
    except Exception as e:
        logger.exception("sync_ops_pay_intraday 失败: %s", e)
        sync_log_repo.finish(log_id, status="failed", message=str(e),
                             rows_affected=pay_rows)
        raise


def _cli():
    parser = argparse.ArgumentParser(
        description="Sync metis_dw.dwd_recharge_order_df → biz_ops_daily (付费侧, 30min 高频)"
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
