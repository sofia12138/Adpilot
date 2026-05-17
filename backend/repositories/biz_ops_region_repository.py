"""区域渠道分析 — biz_ops_region_{register,revenue}_{daily,intraday} 数据访问层

数据流：
    PolarDB matrix_advertise.channel_user        ─┐
    PolarDB matrix_order.recharge_order          ─┼─→ tasks/sync_ops_region_*.py
    MaxCompute metis_dw.dim_user_df              ─┘
        ↓
    adpilot_biz.biz_ops_region_register_daily       (T+1, 30 天回填)
    adpilot_biz.biz_ops_region_register_intraday    (今/昨日 LA, 30min)
    adpilot_biz.biz_ops_region_revenue_daily        (T+1, 30 天回填)
    adpilot_biz.biz_ops_region_revenue_intraday     (今/昨日 LA, 30min)

口径：
- 主键：注册侧 (ds, region, channel_kind)；充值侧 (ds, region, channel_kind, os_type)
- channel_kind: 0=organic / 1=tiktok / 2=meta / 3=other
- region 缺失填 'UNK'
- 金额以美分（BIGINT）落库，service 层再转 USD
"""
from __future__ import annotations

from typing import Optional

from db import get_biz_conn

# ─── 表名常量 ────────────────────────────────────────────────
REGISTER_DAILY = "biz_ops_region_register_daily"
REGISTER_INTRADAY = "biz_ops_region_register_intraday"
REVENUE_DAILY = "biz_ops_region_revenue_daily"
REVENUE_INTRADAY = "biz_ops_region_revenue_intraday"


# ─── 注册侧 ──────────────────────────────────────────────────
_REGISTER_INSERT_COLUMNS = ("ds", "region", "channel_kind", "register_uv")
_REGISTER_UPDATE_COLUMNS = ("register_uv",)
_REGISTER_SELECT_COLUMNS = ("ds", "region", "channel_kind", "register_uv")


def upsert_register_batch(rows: list[dict], *, table: str = REGISTER_DAILY) -> int:
    """批量 upsert 注册侧聚合行。rows 字段缺省按 0/'UNK' 落库。"""
    if not rows:
        return 0
    cols_sql = ", ".join(_REGISTER_INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_REGISTER_INSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _REGISTER_UPDATE_COLUMNS)
    update_sql += ", synced_at = CURRENT_TIMESTAMP"
    sql = (
        f"INSERT INTO {table} ({cols_sql}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )

    def _row(r: dict) -> tuple:
        return (
            r.get("ds"),
            (r.get("region") or "UNK")[:8],
            int(r.get("channel_kind") or 0),
            int(r.get("register_uv") or 0),
        )

    params = [_row(r) for r in rows]
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
        return cur.rowcount


def delete_register_window(start_ds: str, end_ds: str, *, table: str = REGISTER_DAILY) -> int:
    """删除 [start_ds, end_ds] 区间所有行（同步任务全量回刷前调用）。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {table} WHERE ds BETWEEN %s AND %s",
            (start_ds, end_ds),
        )
        conn.commit()
        return cur.rowcount


def query_register_range(
    start_ds: str,
    end_ds: str,
    *,
    table: str = REGISTER_DAILY,
) -> list[dict]:
    """读取 [start_ds, end_ds] 区间内的所有行，升序按 (ds, region, channel_kind)。"""
    cols = ", ".join(_REGISTER_SELECT_COLUMNS)
    sql = (
        f"SELECT {cols} FROM {table} "
        f"WHERE ds BETWEEN %s AND %s "
        f"ORDER BY ds ASC, region ASC, channel_kind ASC"
    )
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (start_ds, end_ds))
        return list(cur.fetchall())


# ─── 充值侧 ──────────────────────────────────────────────────
_REVENUE_INSERT_COLUMNS = (
    "ds", "region", "channel_kind", "os_type",
    "payer_uv", "order_cnt",
    "revenue_cents", "sub_revenue_cents", "iap_revenue_cents",
)
_REVENUE_UPDATE_COLUMNS = tuple(
    c for c in _REVENUE_INSERT_COLUMNS if c not in ("ds", "region", "channel_kind", "os_type")
)
_REVENUE_SELECT_COLUMNS = _REVENUE_INSERT_COLUMNS


def upsert_revenue_batch(rows: list[dict], *, table: str = REVENUE_DAILY) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(_REVENUE_INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_REVENUE_INSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _REVENUE_UPDATE_COLUMNS)
    update_sql += ", synced_at = CURRENT_TIMESTAMP"
    sql = (
        f"INSERT INTO {table} ({cols_sql}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )

    def _row(r: dict) -> tuple:
        return (
            r.get("ds"),
            (r.get("region") or "UNK")[:8],
            int(r.get("channel_kind") or 0),
            int(r.get("os_type") or 0),
            int(r.get("payer_uv") or 0),
            int(r.get("order_cnt") or 0),
            int(r.get("revenue_cents") or 0),
            int(r.get("sub_revenue_cents") or 0),
            int(r.get("iap_revenue_cents") or 0),
        )

    params = [_row(r) for r in rows]
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
        return cur.rowcount


def delete_revenue_window(start_ds: str, end_ds: str, *, table: str = REVENUE_DAILY) -> int:
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {table} WHERE ds BETWEEN %s AND %s",
            (start_ds, end_ds),
        )
        conn.commit()
        return cur.rowcount


def query_revenue_range(
    start_ds: str,
    end_ds: str,
    *,
    table: str = REVENUE_DAILY,
    os_type: Optional[int] = None,
) -> list[dict]:
    """读取 [start_ds, end_ds] 充值侧行。可按 os_type 过滤。"""
    cols = ", ".join(_REVENUE_SELECT_COLUMNS)
    where = "ds BETWEEN %s AND %s"
    params: list = [start_ds, end_ds]
    if os_type is not None:
        where += " AND os_type = %s"
        params.append(int(os_type))
    sql = (
        f"SELECT {cols} FROM {table} "
        f"WHERE {where} "
        f"ORDER BY ds ASC, region ASC, channel_kind ASC, os_type ASC"
    )
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())
