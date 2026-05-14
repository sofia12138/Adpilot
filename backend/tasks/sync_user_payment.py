"""sync_user_payment.py — 用户付费面板日同步任务

数据流：
    PolarDB matrix_order.recharge_order ─┐
    MaxCompute metis_dw.dim_user_df     ─┼─→ adpilot_biz.biz_user_payment_order
                                          └─→ adpilot_biz.biz_user_payment_summary

口径：
- LA 日 90 天滚动窗口；窗口外的行物理清理
- 订单按 LA 日落库（biz_user_payment_order.la_ds）
- 用户聚合按全期累计（截止 snapshot_ds=今日 LA）
- 异常标签按"单日聚合"判定（suspect_brush / payment_loop），再合并到该用户的总标签里
- pending_whitelist / whitelisted 不写入持久标签，由 service 层运行时叠加

调度：每日 LA 03:30，apscheduler 在 app.py 注册

CLI:
    python -m tasks.sync_user_payment                       # 默认回填 90 天
    python -m tasks.sync_user_payment --backfill 30
    python -m tasks.sync_user_payment --start-ds 2026-04-10 --end-ds 2026-05-13
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from config import get_settings
from db import get_biz_conn, get_order_conn
from integrations.dms_client import (
    DmsAuthError,
    DmsError,
    DmsSqlError,
    get_default_client,
)
from repositories import biz_sync_log_repository as sync_log_repo
from repositories import biz_user_payment_repository as repo
from services.user_payment_service import (
    ANOMALY_BRUSH,
    ANOMALY_BURST,
    ANOMALY_GUEST,
    ANOMALY_LOOP,
)

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_user_payment"
DEFAULT_WINDOW_DAYS = 90


# ─────────────────────────────────────────────────────────────
#  辅助
# ─────────────────────────────────────────────────────────────

def _today_la() -> date:
    return datetime.now(LA_TZ).date()


def _to_int(v) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return 0


def _to_str(v) -> str:
    return "" if v is None else str(v)


# ─────────────────────────────────────────────────────────────
#  1) PolarDB → biz_user_payment_order（订单明细镜像）
# ─────────────────────────────────────────────────────────────

_POLARDB_ORDER_SQL = """
    SELECT
        id                                                     AS order_id,
        order_no,
        user_id,
        DATE(CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles'))     AS la_ds,
        CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles')           AS created_at_la,
        CASE WHEN order_status = 1 AND pay_time > '0001-01-01'
             THEN CONVERT_TZ(pay_time, '+08:00', 'America/Los_Angeles')
             ELSE NULL END                                                AS pay_time_la,
        order_status,
        os_type,
        pay_type,
        pay_amount                                             AS pay_amount_cents,
        refund_amount                                          AS refund_amount_cents,
        product_id,
        is_subscribe,
        stall_group,
        channel_id,
        drama_id,
        episode_id
    FROM recharge_order
    WHERE app_id = 1
      AND created_at >= %s AND created_at < %s
"""


def _fetch_orders_from_polardb(window_start_la: date, window_end_la: date) -> list[dict]:
    """拉 PolarDB recharge_order，时间窗用北京时区扩 ±1 天兜底 LA 边界。"""
    bj_lo = (window_start_la - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    bj_hi = (window_end_la + timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")
    logger.info("拉取 PolarDB 订单 [%s, %s)", bj_lo, bj_hi)
    with get_order_conn() as conn:
        if conn is None:
            raise RuntimeError("PolarDB 不可用")
        cur = conn.cursor()
        cur.execute(_POLARDB_ORDER_SQL, (bj_lo, bj_hi))
        rows = list(cur.fetchall())
    # 二次过滤：保留 la_ds 落在 [window_start_la, window_end_la] 内
    out: list[dict] = []
    for r in rows:
        la_ds = r.get("la_ds")
        if isinstance(la_ds, datetime):
            la_ds = la_ds.date()
        if not isinstance(la_ds, date):
            continue
        if window_start_la <= la_ds <= window_end_la:
            r["la_ds"] = la_ds
            out.append(r)
    logger.info("PolarDB 订单 %d 行（窗口外过滤前 %d）", len(out), len(rows))
    return out


# ─────────────────────────────────────────────────────────────
#  2) MaxCompute dim_user_df → user_id 维度 enrich
# ─────────────────────────────────────────────────────────────

_DIM_USER_SQL = """
SELECT
    user_id,
    oauth_platform,
    lang,
    region,
    register_time_utc
FROM metis_dw.dim_user_df
WHERE ds = (SELECT MAX(ds) FROM metis_dw.dim_user_df)
  AND user_id IN ({user_list})
"""


def _fetch_dim_user(user_ids: list[int]) -> dict[int, dict]:
    if not user_ids:
        return {}
    settings = get_settings()
    client = get_default_client()
    out: dict[int, dict] = {}
    batch_size = 1000
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        sql = _DIM_USER_SQL.format(user_list=", ".join(str(u) for u in batch))
        try:
            result = client.execute(sql, settings.dms_mc_db_id)
        except DmsAuthError as e:
            raise RuntimeError(f"MC dim_user_df 权限不足: {e}") from e
        except DmsSqlError as e:
            raise RuntimeError(f"MC dim_user_df SQL 失败: {e}") from e
        except DmsError as e:
            raise RuntimeError(f"DMS 调用失败: {e}") from e
        for row in result.rows:
            uid = _to_int(row.get("user_id"))
            if not uid:
                continue
            out[uid] = {
                "oauth_platform": _to_int(row.get("oauth_platform")) if row.get("oauth_platform") not in (None, "") else None,
                "lang": _to_str(row.get("lang")) or None,
                "region": _to_str(row.get("region")) or None,
                "register_time_utc": row.get("register_time_utc") or None,
            }
    logger.info("MaxCompute dim_user_df enrich 命中 %d / %d", len(out), len(user_ids))
    return out


# ─────────────────────────────────────────────────────────────
#  3) 在订单明细基础上聚合 + 打异常标签
# ─────────────────────────────────────────────────────────────

def _compute_user_summary(
    orders: list[dict],
    dim_user: dict[int, dict],
    snapshot_ds: date,
) -> list[dict]:
    """把同一窗口的订单按 user_id 聚合成 biz_user_payment_summary 行。

    异常标签：
      - suspect_brush: 任意单日 ≥10 单且成单率 < 10%
      - payment_loop:  任意单日 ≥5 单且 0 成单
      - instant_burst: 注册后 30 分钟内下单 ≥5
      - guest_payer:   oauth=-1 且累计下单 ≥3
    """
    # 收集每用户每日的 (total, paid)
    daily_counters: dict[int, dict[date, list[int]]] = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    # 用户聚合累加器
    agg: dict[int, dict[str, Any]] = defaultdict(lambda: {
        "user_id": 0,
        "first_channel_id": "",
        "first_os_type": 0,
        "first_pay_type": 0,
        "total_orders": 0,
        "paid_orders": 0,
        "refund_orders": 0,
        "paid_orders_ios": 0,
        "paid_orders_android": 0,
        "total_gmv_cents_ios": 0,
        "total_gmv_cents_android": 0,
        "paid_orders_subscribe": 0,
        "paid_orders_inapp": 0,
        "total_gmv_cents_subscribe": 0,
        "total_gmv_cents_inapp": 0,
        "total_gmv_cents": 0,
        "attempted_gmv_cents": 0,
        "refund_amount_cents": 0,
        "first_pay_time_utc": None,
        "last_action_time_utc": None,
        "first_order_time_la": None,
    })

    # 排序：按 created_at_la asc，确保 first_xxx 字段正确取首单值
    orders_sorted = sorted(orders, key=lambda r: r.get("created_at_la") or datetime.min)
    for o in orders_sorted:
        uid = _to_int(o.get("user_id"))
        if not uid:
            continue
        a = agg[uid]
        a["user_id"] = uid

        status = _to_int(o.get("order_status"))
        os_type = _to_int(o.get("os_type"))
        pay_amt = _to_int(o.get("pay_amount_cents"))
        refund_amt = _to_int(o.get("refund_amount_cents"))
        is_sub = _to_int(o.get("is_subscribe"))
        created_la = o.get("created_at_la")
        pay_la = o.get("pay_time_la")
        la_ds = o.get("la_ds")
        if isinstance(la_ds, datetime):
            la_ds = la_ds.date()

        # 首单字段（已按时间正序遍历，首次写入即为首单）
        if a["total_orders"] == 0:
            a["first_channel_id"] = _to_str(o.get("channel_id"))[:64]
            a["first_os_type"] = os_type
            a["first_pay_type"] = _to_int(o.get("pay_type"))
            a["first_order_time_la"] = created_la

        a["total_orders"] += 1
        a["attempted_gmv_cents"] += pay_amt
        if status == 1:
            a["paid_orders"] += 1
            a["total_gmv_cents"] += pay_amt
            if os_type == 2:
                a["paid_orders_ios"] += 1
                a["total_gmv_cents_ios"] += pay_amt
            elif os_type == 1:
                a["paid_orders_android"] += 1
                a["total_gmv_cents_android"] += pay_amt
            if is_sub == 1:
                a["paid_orders_subscribe"] += 1
                a["total_gmv_cents_subscribe"] += pay_amt
            else:
                a["paid_orders_inapp"] += 1
                a["total_gmv_cents_inapp"] += pay_amt
            if a["first_pay_time_utc"] is None and pay_la:
                a["first_pay_time_utc"] = pay_la

        if status in (2, 3):
            a["refund_orders"] += 1
        a["refund_amount_cents"] += refund_amt

        if created_la:
            if a["last_action_time_utc"] is None or created_la > a["last_action_time_utc"]:
                a["last_action_time_utc"] = created_la

        # 日级计数（用于 brush/loop 判定）
        if isinstance(la_ds, date):
            cnt = daily_counters[uid][la_ds]
            cnt[0] += 1
            if status == 1:
                cnt[1] += 1

    # 计算异常标签
    output_rows: list[dict] = []
    for uid, a in agg.items():
        tags: list[str] = []
        # brush + loop（按单日）
        for _ds, (tot, paid) in daily_counters[uid].items():
            if tot >= 10 and (paid / max(tot, 1)) < 0.1:
                if ANOMALY_BRUSH not in tags:
                    tags.append(ANOMALY_BRUSH)
            if tot >= 5 and paid == 0:
                if ANOMALY_LOOP not in tags:
                    tags.append(ANOMALY_LOOP)

        user_meta = dim_user.get(uid) or {}
        oauth = user_meta.get("oauth_platform")
        register_time_utc = user_meta.get("register_time_utc")

        # burst: 注册后 30 分钟内下单 ≥5
        if (
            register_time_utc
            and isinstance(register_time_utc, datetime)
            and a["first_order_time_la"]
            and isinstance(a["first_order_time_la"], datetime)
            and a["total_orders"] >= 5
        ):
            # register_time_utc 是 UTC datetime（naive，按 UTC 理解）
            # first_order_time_la 是 LA datetime（naive，按 LA 理解）
            try:
                reg_utc = (
                    register_time_utc.replace(tzinfo=timezone.utc)
                    if register_time_utc.tzinfo is None else register_time_utc
                )
                first_la = (
                    a["first_order_time_la"].replace(tzinfo=LA_TZ)
                    if a["first_order_time_la"].tzinfo is None else a["first_order_time_la"]
                )
                if first_la - reg_utc <= timedelta(minutes=30) and (first_la - reg_utc) >= timedelta(0):
                    tags.append(ANOMALY_BURST)
            except Exception:
                pass

        if oauth == -1 and a["total_orders"] >= 3:
            tags.append(ANOMALY_GUEST)

        row = {
            "user_id": uid,
            "region": user_meta.get("region"),
            "oauth_platform": oauth,
            "register_time_utc": register_time_utc,
            "lang": user_meta.get("lang"),
            "first_channel_id": a["first_channel_id"] or "",
            "first_os_type": a["first_os_type"],
            "first_pay_type": a["first_pay_type"],
            "total_orders": a["total_orders"],
            "paid_orders": a["paid_orders"],
            "refund_orders": a["refund_orders"],
            "paid_orders_ios": a["paid_orders_ios"],
            "paid_orders_android": a["paid_orders_android"],
            "total_gmv_cents_ios": a["total_gmv_cents_ios"],
            "total_gmv_cents_android": a["total_gmv_cents_android"],
            "paid_orders_subscribe": a["paid_orders_subscribe"],
            "paid_orders_inapp": a["paid_orders_inapp"],
            "total_gmv_cents_subscribe": a["total_gmv_cents_subscribe"],
            "total_gmv_cents_inapp": a["total_gmv_cents_inapp"],
            "total_gmv_cents": a["total_gmv_cents"],
            "attempted_gmv_cents": a["attempted_gmv_cents"],
            "refund_amount_cents": a["refund_amount_cents"],
            "first_pay_time_utc": a["first_pay_time_utc"],
            "last_action_time_utc": a["last_action_time_utc"],
            "anomaly_tags": json.dumps(tags, ensure_ascii=False) if tags else json.dumps([]),
            "snapshot_ds": snapshot_ds.strftime("%Y-%m-%d"),
        }
        output_rows.append(row)
    return output_rows


# ─────────────────────────────────────────────────────────────
#  4) 主流程
# ─────────────────────────────────────────────────────────────

def run(
    *,
    start_ds: Optional[str] = None,
    end_ds: Optional[str] = None,
    backfill_days: int = DEFAULT_WINDOW_DAYS,
    skip_dim_user: bool = False,
) -> dict:
    """同步主入口。

    Args:
        start_ds / end_ds: 显式 LA 日窗口（YYYY-MM-DD）
        backfill_days:     未指定窗口时回填最近 N 天，默认 90
        skip_dim_user:     跳过 MaxCompute enrich（仅用于离线调试 / DMS 不可用时）
    """
    if start_ds and end_ds:
        window_start = datetime.strptime(start_ds, "%Y-%m-%d").date()
        window_end = datetime.strptime(end_ds, "%Y-%m-%d").date()
    else:
        today = _today_la()
        window_end = today
        window_start = today - timedelta(days=backfill_days - 1)

    log_id = sync_log_repo.create(task_name=TASK_NAME, sync_date=window_end.strftime("%Y-%m-%d"))
    started = datetime.now(timezone.utc)
    order_rows = 0
    user_rows = 0
    purged = 0

    try:
        # 1) 拉 PolarDB 订单
        orders = _fetch_orders_from_polardb(window_start, window_end)

        # 2) 写订单明细
        order_payload = [
            {
                "la_ds": (o["la_ds"].strftime("%Y-%m-%d") if isinstance(o["la_ds"], date) else _to_str(o["la_ds"])[:10]),
                "order_id": _to_int(o.get("order_id")),
                "order_no": _to_str(o.get("order_no"))[:40],
                "user_id": _to_int(o.get("user_id")),
                "created_at_la": o.get("created_at_la"),
                "pay_time_la": o.get("pay_time_la"),
                "order_status": _to_int(o.get("order_status")),
                "os_type": _to_int(o.get("os_type")),
                "pay_type": _to_int(o.get("pay_type")),
                "pay_amount_cents": _to_int(o.get("pay_amount_cents")),
                "refund_amount_cents": _to_int(o.get("refund_amount_cents")),
                "product_id": _to_str(o.get("product_id"))[:64],
                "is_subscribe": _to_int(o.get("is_subscribe")),
                "stall_group": _to_int(o.get("stall_group")),
                "channel_id": _to_str(o.get("channel_id"))[:64],
                "drama_id": _to_int(o.get("drama_id")),
                "episode_id": _to_int(o.get("episode_id")),
            }
            for o in orders
            if _to_int(o.get("order_id")) > 0
        ]
        if order_payload:
            # 分批 upsert，每批 1000，避免 SQL 包过大
            for i in range(0, len(order_payload), 1000):
                repo.upsert_order_batch(order_payload[i:i + 1000])
            order_rows = len(order_payload)
        logger.info("订单明细 upsert %d 行", order_rows)

        # 3) MaxCompute enrich
        unique_user_ids = sorted({o["user_id"] for o in order_payload if o["user_id"] > 0})
        dim_user: dict[int, dict] = {}
        if not skip_dim_user and unique_user_ids:
            try:
                dim_user = _fetch_dim_user(unique_user_ids)
            except Exception as e:
                logger.warning("MaxCompute enrich 失败，将使用空 enrich 继续: %s", e)
                dim_user = {}

        # 4) 聚合 + 异常标签
        snapshot_ds = window_end
        summary_rows = _compute_user_summary(orders, dim_user, snapshot_ds)
        if summary_rows:
            for i in range(0, len(summary_rows), 500):
                repo.upsert_summary_batch(summary_rows[i:i + 500])
            user_rows = len(summary_rows)
        logger.info("用户聚合 upsert %d 行", user_rows)

        # 5) 物理清理超 90 天的行
        cutoff = (_today_la() - timedelta(days=DEFAULT_WINDOW_DAYS + 1)).strftime("%Y-%m-%d")
        purged = repo.purge_stale_orders(cutoff) + repo.purge_stale_summary(cutoff)
        logger.info("物理清理 %d 行（cutoff < %s）", purged, cutoff)

        message = (
            f"window=[{window_start}, {window_end}] orders={order_rows} "
            f"users={user_rows} purged={purged}"
        )
        sync_log_repo.finish(
            log_id, status="success", message=message,
            rows_affected=order_rows + user_rows,
        )
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info("sync_user_payment 完成: %s 耗时 %.1fs", message, elapsed)
        return {
            "status": "success",
            "window_start": window_start.strftime("%Y-%m-%d"),
            "window_end": window_end.strftime("%Y-%m-%d"),
            "order_rows": order_rows,
            "user_rows": user_rows,
            "purged": purged,
            "elapsed_sec": round(elapsed, 1),
        }
    except Exception as e:
        sync_log_repo.finish(log_id, status="failed", message=str(e)[:500])
        logger.exception("sync_user_payment 失败")
        raise


def _parse_args():
    parser = argparse.ArgumentParser(description="同步 PolarDB recharge_order → biz_user_payment_*")
    parser.add_argument("--start-ds")
    parser.add_argument("--end-ds")
    parser.add_argument("--backfill", type=int, default=DEFAULT_WINDOW_DAYS)
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
