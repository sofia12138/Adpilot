"""运营面板付费侧实时层 — biz_ops_daily_intraday 数据访问层

数据流：
    matrix_order.recharge_order (PolarDB)
        ↓ tasks/sync_ops_polardb_intraday 每 30 分钟同步
    adpilot_biz.biz_ops_daily_intraday (今日 + 昨日 LA)

用途：
    API 智能路由 — 用户请求今日 / 昨日的运营面板数据，从这张表读，避开 T+1 延迟。
    其余历史日期仍读 biz_ops_daily。

约束：
    主键 (ds, os_type)，os_type ∈ {1=Android, 2=iOS}；不含 0 用户侧。
    仅保留今日+昨日两行 × 2 OS = 4 行；旧数据由 prune_old 主动清理。
"""
from __future__ import annotations

from datetime import date, timedelta

from db import get_biz_conn

_INSERT_COLUMNS = (
    "ds", "os_type",
    "subscribe_revenue_usd", "onetime_revenue_usd",
    "first_sub_orders", "repeat_sub_orders",
    "first_iap_orders", "repeat_iap_orders",
    "payer_uv",
    "upstream_max_id",
)

_UPDATE_COLUMNS = tuple(c for c in _INSERT_COLUMNS if c not in ("ds", "os_type"))


def upsert_batch(rows: list[dict]) -> int:
    if not rows:
        return 0

    cols_sql = ", ".join(_INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_INSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _UPDATE_COLUMNS)
    update_sql += ", synced_at = CURRENT_TIMESTAMP"
    sql = (
        f"INSERT INTO biz_ops_daily_intraday ({cols_sql}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )

    params = [tuple(r.get(c, 0) for c in _INSERT_COLUMNS) for r in rows]
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
        return cur.rowcount


def prune_older_than(retain_days: int = 2) -> int:
    """清理超过 retain_days 的旧 LA 日行（默认保留今日+昨日）。"""
    cutoff = (date.today() - timedelta(days=retain_days + 1)).strftime("%Y-%m-%d")
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM biz_ops_daily_intraday WHERE ds < %s",
            (cutoff,),
        )
        conn.commit()
        return cur.rowcount


def query_range(start_date: str, end_date: str) -> list[dict]:
    """读取 [start_date, end_date] 区间。通常只命中今日+昨日。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ds, os_type,
                   subscribe_revenue_usd, onetime_revenue_usd,
                   first_sub_orders, repeat_sub_orders,
                   first_iap_orders, repeat_iap_orders,
                   payer_uv,
                   upstream_max_id, synced_at
            FROM biz_ops_daily_intraday
            WHERE ds BETWEEN %s AND %s
            ORDER BY ds ASC, os_type ASC
            """,
            (start_date, end_date),
        )
        return list(cur.fetchall())
