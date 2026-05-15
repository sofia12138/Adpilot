"""用户付费面板 · 实时接口（直查 PolarDB）

为什么需要：
- biz_user_payment_summary 是 T+1 同步，对"今日内的反复下单失败用户"无法识别
- 直查 PolarDB matrix_order.recharge_order 用 LA 时区聚合，覆盖任意时间窗（默认今日）

性能保护：
- 60s 内存缓存（按 la_ds 维度）
- 单次查询返回 ≤ 5000 行（按 total_orders DESC 截断；正常一天用户数远低于此阈值）
- 仅取 LA 当日 [00:00:00 UTC 偏移] 之后的订单（用 CONVERT_TZ）

口径：
- la_ds 必填，格式 'YYYY-MM-DD'，对应 America/Los_Angeles 日期
- 返回结构与 biz_user_payment_summary 视图基本对齐
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Optional
from zoneinfo import ZoneInfo

from config import get_settings
from db import get_order_conn
from integrations.dms_client import DmsError, get_default_client
from repositories import biz_user_payment_repository as biz_repo
from repositories import user_anomaly_application_repository as app_repo
from repositories import user_anomaly_whitelist_repository as whitelist_repo
from services.user_payment_service import (
    ANOMALY_BRUSH,
    ANOMALY_BURST,
    ANOMALY_GUEST,
    ANOMALY_LOOP,
    ANOMALY_PENDING,
    ANOMALY_WHITELISTED,
)

logger = logging.getLogger(__name__)

_LA_TZ = ZoneInfo("America/Los_Angeles")

_CACHE_TTL_SEC = 60
_MAX_ROWS = 5000

# 简单内存缓存：{la_ds: (timestamp, payload)}
_cache: dict[str, tuple[float, dict]] = {}
_cache_lock = Lock()


def _resolve_la_ds(la_ds: Optional[str]) -> str:
    if la_ds:
        # 校验 ISO 格式
        try:
            datetime.strptime(la_ds, "%Y-%m-%d")
            return la_ds
        except ValueError as e:
            raise ValueError(f"la_ds 格式错误，应为 YYYY-MM-DD：{la_ds}") from e
    return datetime.now(_LA_TZ).strftime("%Y-%m-%d")


def _fetch_polardb(la_ds: str) -> list[dict]:
    """直查 PolarDB，按 LA 日聚合所有"尝试过下单"的用户。

    去重口径（与 sync 任务 user_payment_dedupe 一致）：
      - 同一 trade_no 的已支付订单只保留一行：first_subscribe=1 优先，否则 id 最小
      - 待支付/取消/退款/支付失败行 trade_no 多为空，原样保留
    """
    sql = """
        SELECT
            user_id,
            COUNT(*)                                                AS total_orders,
            SUM(CASE WHEN order_status = 1 THEN 1 ELSE 0 END)       AS paid_orders,
            SUM(CASE WHEN order_status IN (2,3) THEN 1 ELSE 0 END)  AS refund_orders,
            SUM(CASE WHEN order_status = 1 AND os_type = 2 THEN 1 ELSE 0 END) AS paid_orders_ios,
            SUM(CASE WHEN order_status = 1 AND os_type = 1 THEN 1 ELSE 0 END) AS paid_orders_android,
            SUM(CASE WHEN order_status = 1 AND is_subscribe = 1 THEN 1 ELSE 0 END) AS paid_orders_subscribe,
            SUM(CASE WHEN order_status = 1 AND is_subscribe <> 1 THEN 1 ELSE 0 END) AS paid_orders_inapp,
            SUM(CASE WHEN order_status = 1 THEN pay_amount ELSE 0 END) AS total_gmv_cents,
            SUM(CASE WHEN order_status = 1 AND os_type = 2 THEN pay_amount ELSE 0 END) AS total_gmv_cents_ios,
            SUM(CASE WHEN order_status = 1 AND os_type = 1 THEN pay_amount ELSE 0 END) AS total_gmv_cents_android,
            SUM(CASE WHEN order_status = 1 AND is_subscribe = 1 THEN pay_amount ELSE 0 END) AS total_gmv_cents_subscribe,
            SUM(CASE WHEN order_status = 1 AND is_subscribe <> 1 THEN pay_amount ELSE 0 END) AS total_gmv_cents_inapp,
            SUM(pay_amount)                                         AS attempted_gmv_cents,
            MIN(os_type)                                            AS first_os_type,
            MIN(pay_type)                                           AS first_pay_type,
            MIN(channel_id)                                         AS first_channel_id,
            MIN(CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles')) AS first_created_la,
            MAX(CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles')) AS last_action_la,
            MIN(CASE WHEN order_status = 1 THEN CONVERT_TZ(pay_time, '+08:00', 'America/Los_Angeles') END) AS first_pay_la
        FROM recharge_order o
        WHERE o.app_id = 1
          AND DATE(CONVERT_TZ(o.created_at, '+08:00', 'America/Los_Angeles')) = %s
          AND (
                o.order_status <> 1
             OR COALESCE(o.trade_no, '') = ''
             OR o.id = (
                  SELECT o2.id FROM recharge_order o2
                  WHERE o2.app_id = o.app_id
                    AND o2.trade_no = o.trade_no
                    AND o2.order_status = 1
                  ORDER BY o2.first_subscribe DESC, o2.id ASC
                  LIMIT 1
                )
          )
        GROUP BY user_id
        ORDER BY total_orders DESC, paid_orders DESC
        LIMIT %s
    """
    with get_order_conn() as conn:
        if conn is None:
            raise RuntimeError("PolarDB 不可用")
        cur = conn.cursor()
        cur.execute(sql, (la_ds, _MAX_ROWS))
        return list(cur.fetchall())


_DIM_USER_REGISTER_SQL = """
SELECT user_id, register_time_utc
FROM metis_dw.dim_user_df
WHERE ds = (SELECT MAX(ds) FROM metis_dw.dim_user_df)
  AND user_id IN ({user_list})
"""


def _fetch_register_from_channel_user(user_ids: list[int]) -> dict[int, Any]:
    """从 PolarDB matrix_advertise.channel_user 实时反查 user_id → create_time。

    适用场景：今日刚注册（通过广告渠道）的用户在 dim_user_df / biz_user_payment_summary
    还没拉到时，这里能秒级拿到注册时间。

    口径核对（探测验证，2026-05）：
    - channel_user.create_time 与 dim_user_df.register_time_utc 完全一致（同 UTC 时区，差 0s）
    - 一个 user_id 只有一条记录（自然量用户不在表内）

    返回 {user_id: datetime|None}；查询失败时返回 {} 并记 warning，不影响主流程。
    """
    if not user_ids:
        return {}
    out: dict[int, Any] = {}
    batch_size = 1000
    try:
        with get_order_conn() as conn:
            if conn is None:
                return {}
            cur = conn.cursor()
            for i in range(0, len(user_ids), batch_size):
                batch = user_ids[i:i + batch_size]
                placeholders = ", ".join(["%s"] * len(batch))
                cur.execute(
                    "SELECT user_id, create_time "
                    f"FROM matrix_advertise.channel_user WHERE user_id IN ({placeholders})",
                    tuple(int(u) for u in batch),
                )
                for row in cur.fetchall():
                    try:
                        uid = int(row.get("user_id") or 0)
                    except (TypeError, ValueError):
                        continue
                    if not uid:
                        continue
                    out[uid] = row.get("create_time") or None
    except Exception as e:
        logger.warning("today register_time channel_user enrich 失败（忽略）: %s", e)
        return out
    logger.info(
        "today register_time channel_user 命中 %d / %d",
        len([v for v in out.values() if v]), len(user_ids),
    )
    return out


def _fetch_register_from_mc(user_ids: list[int]) -> dict[int, Any]:
    """从 MaxCompute dim_user_df 批量查 user_id → register_time_utc。

    用作 biz_user_payment_summary 之外的兜底：实时面板里今日新付费用户多半
    不在 T+1 summary 表里，但只要不是"今天才注册"，dim_user_df（昨日 ds）
    一般都能命中。
    返回 {user_id: datetime|None}；查询失败时返回 {} 并记 warning，不影响主流程。
    """
    if not user_ids:
        return {}
    try:
        settings = get_settings()
        client = get_default_client()
    except Exception as e:  # 配置/认证不全就跳过
        logger.warning("today register_time MC 客户端不可用，跳过: %s", e)
        return {}

    out: dict[int, Any] = {}
    batch_size = 1000
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        sql = _DIM_USER_REGISTER_SQL.format(user_list=", ".join(str(int(u)) for u in batch))
        try:
            result = client.execute(sql, settings.dms_mc_db_id)
        except DmsError as e:
            logger.warning("today register_time MC 查询失败（忽略）: %s", e)
            return out
        except Exception as e:
            logger.warning("today register_time MC 查询异常（忽略）: %s", e)
            return out
        for row in result.rows:
            try:
                uid = int(row.get("user_id") or 0)
            except (TypeError, ValueError):
                continue
            if not uid:
                continue
            out[uid] = row.get("register_time_utc") or None
    logger.info("today register_time MC enrich 命中 %d / %d", len([v for v in out.values() if v]), len(user_ids))
    return out


def _compute_today_tags(row: dict) -> list[str]:
    tags: list[str] = []
    total = int(row.get("total_orders") or 0)
    paid = int(row.get("paid_orders") or 0)
    if total >= 10 and (paid / max(total, 1)) < 0.1:
        tags.append(ANOMALY_BRUSH)
    if total >= 5 and paid == 0:
        tags.append(ANOMALY_LOOP)
    if total >= 3 and paid > 0 and row.get("first_os_type") is not None:
        # 实时接口拿不到 oauth_platform，这条留给 sync 任务来打；今日只标签 brush/loop/burst
        pass

    # burst: 当日内首单到末单时间间隔短 + 单数大
    first_la = row.get("first_created_la")
    last_la = row.get("last_action_la")
    if (
        first_la
        and last_la
        and isinstance(first_la, datetime)
        and isinstance(last_la, datetime)
        and total >= 5
        and (last_la - first_la) <= timedelta(minutes=30)
    ):
        tags.append(ANOMALY_BURST)
    return tags


def _format_row(
    row: dict,
    *,
    pending_set: set[int],
    whitelist_set: set[int],
    register_map: Optional[dict[int, Any]] = None,
) -> dict:
    user_id = int(row.get("user_id", 0))
    tags = _compute_today_tags(row)
    if user_id in whitelist_set:
        tags.append(ANOMALY_WHITELISTED)
    if user_id in pending_set:
        tags.append(ANOMALY_PENDING)

    total_orders = int(row.get("total_orders") or 0)
    paid_orders = int(row.get("paid_orders") or 0)
    success_rate = (paid_orders / total_orders) if total_orders > 0 else 0.0

    reg_time = (register_map or {}).get(user_id)

    return {
        "user_id": user_id,
        "register_time_utc": reg_time,
        "first_channel_id": (row.get("first_channel_id") or "") if row.get("first_channel_id") else "",
        "first_os_type": int(row.get("first_os_type") or 0),
        "first_pay_type": int(row.get("first_pay_type") or 0),
        "total_orders": total_orders,
        "paid_orders": paid_orders,
        "refund_orders": int(row.get("refund_orders") or 0),
        "success_rate": round(success_rate, 4),
        "paid_orders_ios": int(row.get("paid_orders_ios") or 0),
        "paid_orders_android": int(row.get("paid_orders_android") or 0),
        "paid_orders_subscribe": int(row.get("paid_orders_subscribe") or 0),
        "paid_orders_inapp": int(row.get("paid_orders_inapp") or 0),
        "total_gmv_usd": round(int(row.get("total_gmv_cents") or 0) / 100.0, 2),
        "total_gmv_usd_ios": round(int(row.get("total_gmv_cents_ios") or 0) / 100.0, 2),
        "total_gmv_usd_android": round(int(row.get("total_gmv_cents_android") or 0) / 100.0, 2),
        "total_gmv_usd_subscribe": round(int(row.get("total_gmv_cents_subscribe") or 0) / 100.0, 2),
        "total_gmv_usd_inapp": round(int(row.get("total_gmv_cents_inapp") or 0) / 100.0, 2),
        "attempted_gmv_usd": round(int(row.get("attempted_gmv_cents") or 0) / 100.0, 2),
        "first_created_la": row.get("first_created_la"),
        "last_action_la": row.get("last_action_la"),
        "first_pay_la": row.get("first_pay_la"),
        "anomaly_tags": tags,
    }


def list_today(la_ds: Optional[str] = None, *, force_refresh: bool = False) -> dict:
    """返回指定 LA 日（默认今日）的用户实时聚合列表。"""
    ds = _resolve_la_ds(la_ds)
    now = time.time()

    if not force_refresh:
        with _cache_lock:
            entry = _cache.get(ds)
            if entry and (now - entry[0]) < _CACHE_TTL_SEC:
                return entry[1]

    rows = _fetch_polardb(ds)
    pending_set = set(app_repo.list_pending_target_user_ids())
    whitelist_set = set(whitelist_repo.list_whitelisted_user_ids())

    # 注册时间反查（三级 fallback，全部 UTC 时区已对齐）：
    # 1) PolarDB biz_user_payment_summary：T+1 历史付费用户（最快，本机查询）
    # 2) PolarDB matrix_advertise.channel_user：实时表（秒级新鲜），覆盖广告渠道注册的今日新用户
    # 3) MaxCompute metis_dw.dim_user_df：T+1 全量用户兜底（含自然量历史用户）
    # 今天才注册的"自然量"用户仍可能落空，前端展示 "—"，明日 T+1 同步后补齐。
    user_ids = [int(r.get("user_id") or 0) for r in rows if r.get("user_id")]
    register_map: dict[int, Any] = {}
    if user_ids:
        # step 1
        try:
            meta = biz_repo.fetch_summary_meta_by_users(user_ids)
            for uid, m in meta.items():
                rt = m.get("register_time_utc")
                if rt:
                    register_map[uid] = rt
        except Exception as e:
            logger.warning("today register_time_utc summary enrich 失败，忽略: %s", e)

        # step 2 — 实时兜底（适合今日新用户）
        missing = [uid for uid in user_ids if uid not in register_map]
        if missing:
            try:
                cu_map = _fetch_register_from_channel_user(missing)
                for uid, rt in cu_map.items():
                    if rt:
                        register_map[uid] = rt
            except Exception as e:
                logger.warning("today register_time_utc channel_user enrich 失败，忽略: %s", e)

        # step 3 — T+1 兜底（适合自然量历史用户）
        missing = [uid for uid in user_ids if uid not in register_map]
        if missing:
            try:
                mc_map = _fetch_register_from_mc(missing)
                for uid, rt in mc_map.items():
                    if rt:
                        register_map[uid] = rt
            except Exception as e:
                logger.warning("today register_time_utc MC enrich 失败，忽略: %s", e)

    items = [
        _format_row(
            r,
            pending_set=pending_set,
            whitelist_set=whitelist_set,
            register_map=register_map,
        )
        for r in rows
    ]

    payload = {
        "la_ds": ds,
        "total_users": len(items),
        "items": items,
        "truncated": len(items) >= _MAX_ROWS,
        "ttl_sec": _CACHE_TTL_SEC,
    }
    with _cache_lock:
        _cache[ds] = (now, payload)
    return payload


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()
