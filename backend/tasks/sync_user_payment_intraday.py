"""sync_user_payment_intraday.py — 用户付费面板「实时层」同步（30min）

数据流：
    PolarDB matrix_order.recharge_order
        ↓ 每 30 分钟拉今日 + 昨日 LA 窗口
    adpilot_biz.biz_user_payment_order  (la_ds in {today_la, today_la-1})

为什么单独拆出来：
    - 主任务 sync_user_payment 每天只跑一次 (BJ 18:30 / LA 03:30)，仅处理 T+1 闭合
      日（window_end = today_la - 1），不再触碰 today_la 当天数据，避免"凌晨 03:30
      拉到 3.5 小时 partial 数据被当作完整数据写入"的老坑。
    - today_la 当天数据交给本任务每 30 分钟刷新一次，保证前端 by-window 路径
      （非 today 预设）也能看到当日 LA 实时数据。

口径：
    - 仅 upsert biz_user_payment_order；**不动 biz_user_payment_summary**
      （summary 是 90 天累计，由日任务负责；intraday 写 summary 会错误覆盖累计）
    - 窗口 = [today_la - 1, today_la]（含昨日，应对调度延迟跨日的边界）
    - 复用 sync_user_payment.run() 主流程，传 skip_summary=True

调度：每 30 分钟（同 sync_ops_polardb_intraday 节奏），无 MaxCompute 依赖
      也可运行（dim_user_df enrich 仅服务 _summary 累计行，不影响 _order）。

CLI:
    python -m tasks.sync_user_payment_intraday                    # 今天+昨天 LA
    python -m tasks.sync_user_payment_intraday --backfill 3       # 最近 3 天
    python -m tasks.sync_user_payment_intraday --start-ds 2026-05-14 --end-ds 2026-05-15
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from tasks.sync_user_payment import run as _daily_run

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_user_payment_intraday"
# 默认窗口：今天 + 昨天 LA（应对 LA 凌晨调度刚刚跨过日界）
DEFAULT_WINDOW_DAYS = 2


def _today_la():
    return datetime.now(LA_TZ).date()


def run(
    *,
    start_ds: Optional[str] = None,
    end_ds: Optional[str] = None,
    backfill_days: Optional[int] = None,
    skip_dim_user: bool = False,
) -> dict:
    """实时层主入口。

    Args:
        start_ds / end_ds: 显式 LA 日窗口（YYYY-MM-DD），优先级最高
        backfill_days:    未指定窗口时回填最近 N 天，默认 2（今天+昨天）
        skip_dim_user:    跳过 MaxCompute enrich
    """
    if start_ds and end_ds:
        s_ds, e_ds = start_ds, end_ds
    else:
        today = _today_la()
        n = backfill_days if (backfill_days and backfill_days > 0) else DEFAULT_WINDOW_DAYS
        s_ds = (today - timedelta(days=n - 1)).strftime("%Y-%m-%d")
        e_ds = today.strftime("%Y-%m-%d")

    logger.info("intraday 窗口 [%s, %s]，强制 skip_summary=True", s_ds, e_ds)
    # 复用日任务的拉取/写入逻辑，但强制不动 _summary，避免覆盖 90 天累计
    return _daily_run(
        start_ds=s_ds,
        end_ds=e_ds,
        skip_dim_user=skip_dim_user,
        skip_summary=True,
    )


def _parse_args():
    parser = argparse.ArgumentParser(
        description="实时层：PolarDB recharge_order → biz_user_payment_order (每 30min)"
    )
    parser.add_argument("--start-ds")
    parser.add_argument("--end-ds")
    parser.add_argument("--backfill", type=int, default=None)
    parser.add_argument("--skip-dim-user", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args()
    result = run(
        start_ds=args.start_ds,
        end_ds=args.end_ds,
        backfill_days=args.backfill,
        skip_dim_user=args.skip_dim_user,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
