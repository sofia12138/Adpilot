"""用户付费面板 · biz_user_payment_summary + biz_user_payment_order 数据访问层

数据流：
    PolarDB matrix_order.recharge_order     ─┐
    MaxCompute metis_dw.dim_user_df         ─┼─→ tasks/sync_user_payment.py (T+1 LA 03:30)
                                              ↓
    adpilot_biz.biz_user_payment_summary   ← 每用户聚合（最近 90 天活跃用户全量覆写）
    adpilot_biz.biz_user_payment_order     ← 订单明细（90 天 LA 日分区镜像）

口径：
- 时间戳一律 LA（订单/快照日）
- 金额以美分（BIGINT cents）落库，Service 层再转 USD
- anomaly_tags 是 JSON 数组字符串，可能值见 user_payment_service._compute_anomaly_tags
"""
from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from db import get_biz_conn


# ─────────────────────────────────────────────────────────────
#  biz_user_payment_summary
# ─────────────────────────────────────────────────────────────
_SUMMARY_INSERT_COLUMNS = (
    "user_id",
    "region", "oauth_platform", "register_time_utc", "lang",
    "first_channel_id", "first_os_type", "first_pay_type",
    "total_orders", "paid_orders", "refund_orders",
    "paid_orders_ios", "paid_orders_android",
    "total_gmv_cents_ios", "total_gmv_cents_android",
    "paid_orders_subscribe", "paid_orders_inapp",
    "total_gmv_cents_subscribe", "total_gmv_cents_inapp",
    "total_gmv_cents", "attempted_gmv_cents", "refund_amount_cents",
    "first_pay_time_utc", "last_action_time_utc",
    "anomaly_tags", "snapshot_ds",
)

_SUMMARY_UPDATE_COLUMNS = tuple(c for c in _SUMMARY_INSERT_COLUMNS if c != "user_id")

_SUMMARY_SELECT_COLUMNS = (
    "user_id",
    "region", "oauth_platform", "register_time_utc", "lang",
    "first_channel_id", "first_os_type", "first_pay_type",
    "total_orders", "paid_orders", "refund_orders",
    "paid_orders_ios", "paid_orders_android",
    "total_gmv_cents_ios", "total_gmv_cents_android",
    "paid_orders_subscribe", "paid_orders_inapp",
    "total_gmv_cents_subscribe", "total_gmv_cents_inapp",
    "total_gmv_cents", "attempted_gmv_cents", "refund_amount_cents",
    "first_pay_time_utc", "last_action_time_utc",
    "anomaly_tags", "snapshot_ds", "synced_at",
)


def upsert_summary_batch(rows: list[dict]) -> int:
    """批量 upsert 用户聚合行。rows 字段缺省按 NULL/0 落库。"""
    if not rows:
        return 0
    cols_sql = ", ".join(_SUMMARY_INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_SUMMARY_INSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _SUMMARY_UPDATE_COLUMNS)
    update_sql += ", synced_at = CURRENT_TIMESTAMP"
    sql = (
        f"INSERT INTO biz_user_payment_summary ({cols_sql}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )

    def _coerce(col: str, value: Any) -> Any:
        if col == "anomaly_tags":
            if value is None:
                return None
            if isinstance(value, str):
                return value
            return json.dumps(value, ensure_ascii=False)
        return value

    params = [
        tuple(_coerce(c, r.get(c)) for c in _SUMMARY_INSERT_COLUMNS)
        for r in rows
    ]
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
        return cur.rowcount


def purge_stale_summary(keep_after_ds: str) -> int:
    """物理清理 snapshot_ds < keep_after_ds 的记录（超过 90 天活跃窗口）。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM biz_user_payment_summary WHERE snapshot_ds < %s",
            (keep_after_ds,),
        )
        conn.commit()
        return cur.rowcount


_ALLOWED_SUMMARY_ORDER = {
    "total_orders", "paid_orders", "total_gmv_cents", "attempted_gmv_cents",
    "last_action_time_utc", "register_time_utc", "user_id",
}


def query_summary(
    *,
    region: Optional[str] = None,
    oauth_platform: Optional[int] = None,
    first_channel_id: Optional[str] = None,
    first_channel_ids: Optional[list[str]] = None,
    first_os_type: Optional[int] = None,
    anomaly_tag: Optional[str] = None,
    user_id: Optional[int] = None,
    min_total_orders: Optional[int] = None,
    min_paid_orders: Optional[int] = None,
    order_by: str = "last_action_time_utc",
    order_desc: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """读取聚合表。anomaly_tag 走 JSON_CONTAINS。

    渠道过滤：
      - first_channel_id：精确匹配单个 channel_id（保留向后兼容）
      - first_channel_ids：IN 列表（如按平台类型聚合多个 channel_id 时使用）
        传空列表 → 命中 0 条；传 None → 不过滤
    """
    if order_by not in _ALLOWED_SUMMARY_ORDER:
        order_by = "last_action_time_utc"
    direction = "DESC" if order_desc else "ASC"

    where = ["1=1"]
    params: list[Any] = []
    if region:
        where.append("region = %s")
        params.append(region)
    if oauth_platform is not None:
        where.append("oauth_platform = %s")
        params.append(oauth_platform)
    if first_channel_id is not None:
        where.append("first_channel_id = %s")
        params.append(first_channel_id)
    if first_channel_ids is not None:
        if not first_channel_ids:
            where.append("1=0")
        else:
            placeholders = ", ".join(["%s"] * len(first_channel_ids))
            where.append(f"first_channel_id IN ({placeholders})")
            params.extend(first_channel_ids)
    if first_os_type is not None:
        where.append("first_os_type = %s")
        params.append(first_os_type)
    if anomaly_tag:
        where.append("JSON_CONTAINS(anomaly_tags, JSON_QUOTE(%s))")
        params.append(anomaly_tag)
    if user_id is not None:
        where.append("user_id = %s")
        params.append(user_id)
    if min_total_orders is not None:
        where.append("total_orders >= %s")
        params.append(min_total_orders)
    if min_paid_orders is not None:
        where.append("paid_orders >= %s")
        params.append(min_paid_orders)

    cols = ", ".join(_SUMMARY_SELECT_COLUMNS)
    sql = (
        f"SELECT {cols} FROM biz_user_payment_summary "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY {order_by} {direction} "
        f"LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])

    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def count_summary(
    *,
    region: Optional[str] = None,
    oauth_platform: Optional[int] = None,
    first_channel_id: Optional[str] = None,
    first_channel_ids: Optional[list[str]] = None,
    first_os_type: Optional[int] = None,
    anomaly_tag: Optional[str] = None,
    user_id: Optional[int] = None,
    min_total_orders: Optional[int] = None,
    min_paid_orders: Optional[int] = None,
) -> int:
    where = ["1=1"]
    params: list[Any] = []
    if region:
        where.append("region = %s")
        params.append(region)
    if oauth_platform is not None:
        where.append("oauth_platform = %s")
        params.append(oauth_platform)
    if first_channel_id is not None:
        where.append("first_channel_id = %s")
        params.append(first_channel_id)
    if first_channel_ids is not None:
        if not first_channel_ids:
            where.append("1=0")
        else:
            placeholders = ", ".join(["%s"] * len(first_channel_ids))
            where.append(f"first_channel_id IN ({placeholders})")
            params.extend(first_channel_ids)
    if first_os_type is not None:
        where.append("first_os_type = %s")
        params.append(first_os_type)
    if anomaly_tag:
        where.append("JSON_CONTAINS(anomaly_tags, JSON_QUOTE(%s))")
        params.append(anomaly_tag)
    if user_id is not None:
        where.append("user_id = %s")
        params.append(user_id)
    if min_total_orders is not None:
        where.append("total_orders >= %s")
        params.append(min_total_orders)
    if min_paid_orders is not None:
        where.append("paid_orders >= %s")
        params.append(min_paid_orders)

    sql = f"SELECT COUNT(*) AS cnt FROM biz_user_payment_summary WHERE {' AND '.join(where)}"
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone() or {}
        return int(row.get("cnt", 0))


def kpi_aggregate(*, exclude_whitelist_user_ids: Iterable[int] | None = None) -> dict:
    """大盘 KPI：在聚合表上做一次扫描，返回总用户/付费用户/总 GMV 等。

    exclude_whitelist_user_ids：clean 口径时传入；为 None 时计算 raw 口径。
    """
    excl = list(exclude_whitelist_user_ids or [])
    where = "1=1"
    params: list[Any] = []
    if excl:
        placeholders = ", ".join(["%s"] * len(excl))
        where += f" AND user_id NOT IN ({placeholders})"
        params.extend(excl)

    sql = f"""
        SELECT
            COUNT(*)                              AS total_users,
            SUM(CASE WHEN paid_orders > 0 THEN 1 ELSE 0 END) AS paying_users,
            SUM(CASE WHEN paid_orders = 0 AND total_orders > 0 THEN 1 ELSE 0 END) AS try_but_fail_users,
            SUM(CASE WHEN oauth_platform = -1 AND paid_orders > 0 THEN 1 ELSE 0 END) AS guest_paying_users,
            SUM(total_orders)                     AS total_orders,
            SUM(paid_orders)                      AS paid_orders,
            SUM(total_gmv_cents)                  AS total_gmv_cents,
            SUM(attempted_gmv_cents)              AS attempted_gmv_cents,
            SUM(total_gmv_cents_ios)              AS total_gmv_cents_ios,
            SUM(total_gmv_cents_android)          AS total_gmv_cents_android,
            SUM(total_gmv_cents_subscribe)        AS total_gmv_cents_subscribe,
            SUM(total_gmv_cents_inapp)            AS total_gmv_cents_inapp
        FROM biz_user_payment_summary
        WHERE {where}
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        return cur.fetchone() or {}


# ─────────────────────────────────────────────────────────────
#  biz_user_payment_order
# ─────────────────────────────────────────────────────────────
_ORDER_INSERT_COLUMNS = (
    "la_ds", "order_id", "order_no", "user_id",
    "created_at_la", "pay_time_la",
    "order_status", "os_type", "pay_type",
    "pay_amount_cents", "refund_amount_cents",
    "product_id", "is_subscribe", "stall_group",
    "channel_id", "drama_id", "episode_id",
)

_ORDER_UPDATE_COLUMNS = tuple(c for c in _ORDER_INSERT_COLUMNS if c not in ("la_ds", "order_id"))

_ORDER_SELECT_COLUMNS = (
    "la_ds", "order_id", "order_no", "user_id",
    "created_at_la", "pay_time_la",
    "order_status", "os_type", "pay_type",
    "pay_amount_cents", "refund_amount_cents",
    "product_id", "is_subscribe", "stall_group",
    "channel_id", "drama_id", "episode_id",
)


def upsert_order_batch(rows: list[dict]) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(_ORDER_INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_ORDER_INSERT_COLUMNS))
    update_sql = ", ".join(f"{c} = VALUES({c})" for c in _ORDER_UPDATE_COLUMNS)
    update_sql += ", synced_at = CURRENT_TIMESTAMP"
    sql = (
        f"INSERT INTO biz_user_payment_order ({cols_sql}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )
    params = [tuple(r.get(c) for c in _ORDER_INSERT_COLUMNS) for r in rows]
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
        return cur.rowcount


def purge_stale_orders(keep_after_la_ds: str) -> int:
    """物理清理 la_ds < keep_after_la_ds 的订单（超 90 天）。"""
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM biz_user_payment_order WHERE la_ds < %s",
            (keep_after_la_ds,),
        )
        conn.commit()
        return cur.rowcount


def query_orders_by_user(user_id: int, *, limit: int = 500) -> list[dict]:
    cols = ", ".join(_ORDER_SELECT_COLUMNS)
    sql = (
        f"SELECT {cols} FROM biz_user_payment_order "
        f"WHERE user_id = %s "
        f"ORDER BY created_at_la DESC LIMIT %s"
    )
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (user_id, limit))
        return list(cur.fetchall())


_ALLOWED_ORDER_ORDER = {
    "created_at_la", "pay_time_la", "pay_amount_cents", "order_status", "la_ds", "user_id",
}


def query_orders(
    *,
    start_ds: Optional[str] = None,
    end_ds: Optional[str] = None,
    user_id: Optional[int] = None,
    order_status: Optional[int] = None,
    os_type: Optional[int] = None,
    channel_id: Optional[str] = None,
    is_subscribe: Optional[int] = None,
    order_by: str = "created_at_la",
    order_desc: bool = True,
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    if order_by not in _ALLOWED_ORDER_ORDER:
        order_by = "created_at_la"
    direction = "DESC" if order_desc else "ASC"

    where = ["1=1"]
    params: list[Any] = []
    if start_ds:
        where.append("la_ds >= %s")
        params.append(start_ds)
    if end_ds:
        where.append("la_ds <= %s")
        params.append(end_ds)
    if user_id is not None:
        where.append("user_id = %s")
        params.append(user_id)
    if order_status is not None:
        where.append("order_status = %s")
        params.append(order_status)
    if os_type is not None:
        where.append("os_type = %s")
        params.append(os_type)
    if channel_id is not None:
        where.append("channel_id = %s")
        params.append(channel_id)
    if is_subscribe is not None:
        where.append("is_subscribe = %s")
        params.append(is_subscribe)

    cols = ", ".join(_ORDER_SELECT_COLUMNS)
    sql = (
        f"SELECT {cols} FROM biz_user_payment_order "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY {order_by} {direction} "
        f"LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def fetch_window_orders(start_ds: str, end_ds: str) -> list[dict]:
    """窗口聚合用：拉窗口内全部订单的精简字段。

    用于 service.list_users_by_window / get_kpi_dual_by_window 的内存聚合。
    一次性把 90 天 / ~60k 行拉回来，避免在 SQL 里写复杂的 CTE 兼容 MySQL 5.7。
    返回顺序按 (created_at_la asc)，方便后续取"窗口内首单"。
    """
    sql = """
        SELECT
            user_id, la_ds, order_status, os_type, pay_type,
            pay_amount_cents, refund_amount_cents, is_subscribe,
            channel_id, created_at_la, pay_time_la
        FROM biz_user_payment_order
        WHERE la_ds >= %s AND la_ds <= %s
        ORDER BY created_at_la ASC
    """
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (start_ds, end_ds))
        return list(cur.fetchall())


def fetch_summary_meta_by_users(user_ids: Iterable[int]) -> dict[int, dict]:
    """按 user_id 列表批量获取 summary 表的维度字段（dim_user enrich 结果）。

    返回 {user_id: {region, oauth_platform, register_time_utc, lang, anomaly_tags_json}}
    """
    ids = [int(u) for u in user_ids if u]
    if not ids:
        return {}
    out: dict[int, dict] = {}
    batch_size = 1000
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        placeholders = ", ".join(["%s"] * len(batch))
        sql = (
            "SELECT user_id, region, oauth_platform, register_time_utc, lang, anomaly_tags "
            f"FROM biz_user_payment_summary WHERE user_id IN ({placeholders})"
        )
        with get_biz_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, tuple(batch))
            for r in cur.fetchall():
                out[int(r["user_id"])] = r
    return out


def count_orders(
    *,
    start_ds: Optional[str] = None,
    end_ds: Optional[str] = None,
    user_id: Optional[int] = None,
    order_status: Optional[int] = None,
    os_type: Optional[int] = None,
    channel_id: Optional[str] = None,
    is_subscribe: Optional[int] = None,
) -> int:
    where = ["1=1"]
    params: list[Any] = []
    if start_ds:
        where.append("la_ds >= %s")
        params.append(start_ds)
    if end_ds:
        where.append("la_ds <= %s")
        params.append(end_ds)
    if user_id is not None:
        where.append("user_id = %s")
        params.append(user_id)
    if order_status is not None:
        where.append("order_status = %s")
        params.append(order_status)
    if os_type is not None:
        where.append("os_type = %s")
        params.append(os_type)
    if channel_id is not None:
        where.append("channel_id = %s")
        params.append(channel_id)
    if is_subscribe is not None:
        where.append("is_subscribe = %s")
        params.append(is_subscribe)

    sql = f"SELECT COUNT(*) AS cnt FROM biz_user_payment_order WHERE {' AND '.join(where)}"
    with get_biz_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone() or {}
        return int(row.get("cnt", 0))
