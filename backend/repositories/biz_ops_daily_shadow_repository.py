"""运营面板付费侧 PolarDB 影子表 — biz_ops_daily_polardb_shadow 数据访问层

数据流：
    matrix_order.recharge_order (PolarDB)
        ↓ tasks/sync_ops_polardb_daily 每 2 小时同步
    adpilot_biz.biz_ops_daily_polardb_shadow

用途：
    双轨对账期专用，与 biz_ops_daily 的 dwd 路径同口径对比。
    确认偏差稳定后切换为主源（或下线该表）。

约束：
    主键 (ds, os_type)，os_type ∈ {1=Android, 2=iOS}；不含 0 用户侧。
"""
from __future__ import annotations

from db import get_biz_conn

_INSERT_COLUMNS = (
    "ds", "os_type",
    "subscribe_revenue_usd", "onetime_revenue_usd",
    "first_sub_orders", "repeat_sub_orders",
    "first_iap_orders", "repeat_iap_orders",
    "payer_uv",
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
        f"INSERT INTO biz_ops_daily_polardb_shadow ({cols_sql}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )

    params = [tuple(r.get(c, 0) for c in _INSERT_COLUMNS) for r in rows]
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
        return cur.rowcount


def delete_window(start_ds: str, end_ds: str) -> int:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM biz_ops_daily_polardb_shadow WHERE ds BETWEEN %s AND %s",
            (start_ds, end_ds),
        )
        conn.commit()
        return cur.rowcount


def query_range(start_date: str, end_date: str) -> list[dict]:
    """读取 [start_date, end_date] 区间内所有付费侧行（os_type=1/2）。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ds, os_type,
                   subscribe_revenue_usd, onetime_revenue_usd,
                   first_sub_orders, repeat_sub_orders,
                   first_iap_orders, repeat_iap_orders,
                   payer_uv
            FROM biz_ops_daily_polardb_shadow
            WHERE ds BETWEEN %s AND %s
            ORDER BY ds ASC, os_type ASC
            """,
            (start_date, end_date),
        )
        return list(cur.fetchall())
