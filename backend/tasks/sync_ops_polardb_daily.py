"""sync_ops_polardb_daily.py — 运营面板付费侧 PolarDB T+1 同步（影子表）

数据流：
    matrix_order.recharge_order (PolarDB)
        ↓ pymysql 直连，按 LA 日 × os_type 聚合
    adpilot_biz.biz_ops_daily_polardb_shadow

为什么走 PolarDB 而非 MaxCompute：
    - 业务库是订单原始真值，无 T+1 ETL 延迟
    - dwd_recharge_order_df 分区刷新有滞后（实测 5/12 ds 分区到了下午才出）
    - 用于跟现有 sync_ops_daily 走的 dwd 路径做双轨对账

口径：与 sync_ops_daily 付费侧完全一致（见 recharge_order_repository._PAY_SIDE_AGG_SQL）：
    - order_status = 1（已支付）
    - is_subscribe 拆分订阅/IAP，first_* 拆分首单/续/复购
    - pay_amount/100 转 USD（不扣 refund_amount）

调度：每 2 小时跑 1 次（与 sync_ops_daily 同节奏），默认回填 30 天 LA。
对账期稳定后可以切为主源（修改 ops_service 直读这张表）。

CLI:
    python -m tasks.sync_ops_polardb_daily                       # 默认回填 30 天
    python -m tasks.sync_ops_polardb_daily --backfill 60
    python -m tasks.sync_ops_polardb_daily --start-ds 2026-04-10 --end-ds 2026-05-09
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from repositories import biz_ops_daily_shadow_repository as repo
from repositories import biz_sync_log_repository as sync_log_repo
from repositories import recharge_order_repository as order_repo

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_ops_polardb_daily"


def _today_la():
    return datetime.now(LA_TZ).date()


def _normalize_row(rec: dict) -> dict:
    """聚合行 → biz_ops_daily_polardb_shadow 行"""
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
    }


def run(*, start_ds: Optional[str] = None,
        end_ds: Optional[str] = None,
        backfill_days: int = 30,
        purge_window: bool = False) -> dict:
    """同步主入口。

    参数：
      start_ds / end_ds: 显式 LA 日窗口（YYYY-MM-DD），优先级最高。
      backfill_days:    未指定窗口时回填最近 N 天，默认 30
      purge_window:     写入前是否 DELETE 窗口（默认 false，走 ON DUPLICATE KEY UPDATE）
    """
    if start_ds and end_ds:
        s_ds, e_ds = start_ds, end_ds
    else:
        today = _today_la()
        s_ds = (today - timedelta(days=backfill_days)).strftime("%Y-%m-%d")
        e_ds = today.strftime("%Y-%m-%d")

    log_id = sync_log_repo.create(task_name=TASK_NAME, sync_date=e_ds)
    started = datetime.now(timezone.utc)
    rows = 0

    try:
        if purge_window:
            removed = repo.delete_window(s_ds, e_ds)
            logger.info("清理窗口 [%s, %s] 旧数据 %d 行", s_ds, e_ds, removed)

        raw = order_repo.fetch_pay_side_by_la_day(s_ds, e_ds)
        norm = [_normalize_row(r) for r in raw if int(r.get("os_type") or 0) in (1, 2)]
        if not norm:
            logger.warning("PolarDB 拉取付费侧 0 行（窗口 [%s, %s]）— 可能业务库连接失败或当窗口无订单", s_ds, e_ds)
        repo.upsert_batch(norm)
        rows = len(norm)
        logger.info("PolarDB 付费侧 upsert 完成 %d 行（窗口 [%s, %s]）", rows, s_ds, e_ds)

        message = f"window=[{s_ds}, {e_ds}] rows={rows} (polardb→shadow)"
        sync_log_repo.finish(log_id, status="success", message=message, rows_affected=rows)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info("sync_ops_polardb_daily 完成: %s 耗时 %.1fs", message, elapsed)
        return {
            "status": "success",
            "start_ds": s_ds, "end_ds": e_ds,
            "rows": rows,
            "elapsed_sec": elapsed,
        }
    except Exception as e:
        logger.exception("sync_ops_polardb_daily 失败: %s", e)
        sync_log_repo.finish(log_id, status="failed", message=str(e), rows_affected=rows)
        raise


def _cli():
    parser = argparse.ArgumentParser(
        description="Sync matrix_order.recharge_order → biz_ops_daily_polardb_shadow (T+1 全量)"
    )
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

    print(run(
        start_ds=args.start_ds,
        end_ds=args.end_ds,
        backfill_days=args.backfill_days,
        purge_window=args.purge_window,
    ))


if __name__ == "__main__":
    _cli()
