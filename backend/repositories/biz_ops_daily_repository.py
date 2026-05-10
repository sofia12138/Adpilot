"""运营数据面板日报 — biz_ops_daily 数据访问层（adpilot_biz）

数据流：
    metis_dw.ads_app_di + metis_dw.dwd_recharge_order_df  (MaxCompute)
        ↓ 通过 DMS Enterprise OpenAPI 拉取
        ↓ tasks/sync_ops_daily 每日 03:00 LA 同步
    adpilot_biz.biz_ops_daily

口径：
- 主键 (ds, os_type)；同一天有 0/1/2 三行
  - os_type=0 行：用户侧全量指标（来自 ads_app_di）
  - os_type=1/2 行：付费侧 Android / iOS 拆分（来自 dwd_recharge_order_df）
- 金额已在同步层从美分转 USD（DECIMAL(14,4)）
"""
from __future__ import annotations

from db import get_biz_conn

_INSERT_COLUMNS = (
    "ds", "os_type",
    "new_register_uv", "new_active_uv", "active_uv",
    "d1_retained_uv", "d7_retained_uv", "d30_retained_uv", "total_payer_uv",
    "subscribe_revenue_usd", "onetime_revenue_usd",
    "first_sub_orders", "repeat_sub_orders",
    "first_iap_orders", "repeat_iap_orders",
    "payer_uv",
    "ad_spend_usd",
)

_UPDATE_COLUMNS = tuple(c for c in _INSERT_COLUMNS if c not in ("ds", "os_type"))


def upsert_batch(rows: list[dict]) -> int:
    """批量 upsert。rows 中每个 dict 必须包含 ds + os_type，其余字段缺省按 0 落库。"""
    if not rows:
        return 0

    cols_sql = ", ".join(_INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_INSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _UPDATE_COLUMNS)
    update_sql += ", synced_at = CURRENT_TIMESTAMP"
    sql = (
        f"INSERT INTO biz_ops_daily ({cols_sql}) "
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
    """删除 [start_ds, end_ds] 区间，用于全量回刷模式（仅在显式指定时调用）"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM biz_ops_daily WHERE ds BETWEEN %s AND %s",
            (start_ds, end_ds),
        )
        conn.commit()
        return cur.rowcount


def query_range(start_date: str, end_date: str) -> list[dict]:
    """读取 [start_date, end_date] 区间内所有行，按 (ds ASC, os_type ASC) 排。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ds, os_type,
                   new_register_uv, new_active_uv, active_uv,
                   d1_retained_uv, d7_retained_uv, d30_retained_uv, total_payer_uv,
                   subscribe_revenue_usd, onetime_revenue_usd,
                   first_sub_orders, repeat_sub_orders,
                   first_iap_orders, repeat_iap_orders,
                   payer_uv,
                   ad_spend_usd
            FROM biz_ops_daily
            WHERE ds BETWEEN %s AND %s
            ORDER BY ds ASC, os_type ASC
            """,
            (start_date, end_date),
        )
        return list(cur.fetchall())
