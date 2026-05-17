"""sync_ops_region_intraday.py — 区域渠道分析实时同步（今/昨日 LA，30min）

复用 sync_ops_region_daily.run() 主流程，区别：
    - 窗口固定为 [昨天 LA, 今天 LA]（2 天）
    - 写入 _intraday 影子表（不动 _daily 历史表）
    - 注册侧 dim_user_df 分区可能尚未刷到今天，命中数会偏低（前端会标识）

调度：每 30 分钟一次（与 sync_ops_polardb_intraday 对齐）。
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from repositories import biz_ops_region_repository as repo
from tasks.sync_ops_region_daily import run as run_daily

logger = logging.getLogger(__name__)
LA_TZ = ZoneInfo("America/Los_Angeles")


def run() -> dict:
    """实时同步入口：窗口=昨天+今天 LA，写 _intraday 表。"""
    today = datetime.now(LA_TZ).date()
    yesterday = today - timedelta(days=1)
    return run_daily(
        start_ds=yesterday.strftime("%Y-%m-%d"),
        end_ds=today.strftime("%Y-%m-%d"),
        purge_window=True,
        register_table=repo.REGISTER_INTRADAY,
        revenue_table=repo.REVENUE_INTRADAY,
    )


def _parse_args():
    parser = argparse.ArgumentParser(description="区域渠道分析实时同步（今/昨日 LA → _intraday）")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = run()
    print(result)
