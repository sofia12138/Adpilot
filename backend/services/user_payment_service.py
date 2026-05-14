"""用户付费面板 service 层

职责：
- 给 routes 层提供已转换好（cents→USD、json 解析、异常标签合并 pending_whitelist）的对象
- KPI 同时给 raw 和 clean（剔除白名单）两套口径
- 异常标签判定函数 _compute_anomaly_tags 供 sync 任务复用
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from db import get_channel_dict
from repositories import biz_user_payment_repository as repo
from repositories import user_anomaly_application_repository as app_repo
from repositories import user_anomaly_whitelist_repository as whitelist_repo

logger = logging.getLogger(__name__)


# ─── 渠道平台过滤 ─────────────────────────────────────────────
# channel_kind 是面板上「首单渠道」下拉的简化分类：
#   organic  → channel_id 为 '' / '0'（自然量）
#   tiktok   → 字典里 ad_platform=1 的所有 channel_id
#   meta     → 字典里 ad_platform=2 的所有 channel_id
#   other    → 其它（ad_platform=0 且不是 organic）

_CHANNEL_KINDS = {"organic", "tiktok", "meta", "other"}


def resolve_channel_ids_by_kind(kind: Optional[str]) -> Optional[list[str]]:
    """把 channel_kind 转成 channel_id 列表。

    - None / 空串 → None（不过滤）
    - 未知 kind   → None
    - organic    → ['', '0']
    - tiktok/meta/other → 从 channel_dict 拿满足条件的 channel_id
    返回 [] 时调用方应理解为"命中 0 条"。
    """
    if not kind or kind not in _CHANNEL_KINDS:
        return None

    if kind == "organic":
        return ["", "0"]

    target_ap = {"tiktok": 1, "meta": 2, "other": 0}[kind]

    out: list[str] = []
    try:
        d = get_channel_dict()
    except Exception as e:  # 字典查询失败时降级为不过滤，避免面板整体崩
        logger.warning(f"resolve_channel_ids_by_kind: channel_dict 不可用，跳过过滤: {e}")
        return None

    for cid, info in d.items():
        if info.get("ad_platform") != target_ap:
            continue
        if kind == "other":
            # 其它：排除 organic 占位符（'', '0' 也是 ad_platform=0）
            if cid in ("", "0"):
                continue
        out.append(cid)
    return out


# ─────────────────────────────────────────────────────────────
#  异常标签判定（同步任务 + 实时接口共用）
# ─────────────────────────────────────────────────────────────

ANOMALY_BRUSH = "suspect_brush"        # 单日下单 ≥10 且成单率 <10%
ANOMALY_LOOP = "payment_loop"          # 单日下单 ≥5 且 0 成单
ANOMALY_BURST = "instant_burst"        # 注册后 30 分钟内下单 ≥5
ANOMALY_GUEST = "guest_payer"          # 游客 且 累计下单 ≥3
ANOMALY_PENDING = "pending_whitelist"  # 工单 pending 中
ANOMALY_WHITELISTED = "whitelisted"    # 已加白名单


def compute_anomaly_tags(
    *,
    total_orders: int,
    paid_orders: int,
    oauth_platform: Optional[int],
    register_time_utc: Optional[datetime],
    first_order_time_utc: Optional[datetime],
    burst_window_minutes: int = 30,
    burst_threshold_orders: int = 5,
) -> list[str]:
    """按规则给一个用户打异常标签（仅基于该用户的聚合 + 时间画像）。

    注意：suspect_brush / payment_loop 在 sync 层是按"单日"聚合判断的，需要在 sync 任务
    里循环每个 LA 日时单独判定后合并。这里给出一个简化版基于全期累计的判定，
    可用作"全期严重程度"指标；真实"单日刷单"事件由 sync 层在按日聚合时打标。
    """
    tags: list[str] = []
    if total_orders >= 10 and paid_orders / max(total_orders, 1) < 0.1:
        tags.append(ANOMALY_BRUSH)
    if total_orders >= 5 and paid_orders == 0:
        tags.append(ANOMALY_LOOP)
    if (
        register_time_utc
        and first_order_time_utc
        and first_order_time_utc - register_time_utc <= timedelta(minutes=burst_window_minutes)
        and total_orders >= burst_threshold_orders
    ):
        tags.append(ANOMALY_BURST)
    if oauth_platform == -1 and total_orders >= 3:
        tags.append(ANOMALY_GUEST)
    return tags


# ─────────────────────────────────────────────────────────────
#  行结构转换：cents → USD + tags JSON → list
# ─────────────────────────────────────────────────────────────

def _parse_tags(raw_tags: Any) -> list[str]:
    if raw_tags is None:
        return []
    if isinstance(raw_tags, list):
        return list(raw_tags)
    if isinstance(raw_tags, (bytes, bytearray)):
        raw_tags = raw_tags.decode("utf-8", errors="ignore")
    if isinstance(raw_tags, str):
        try:
            data = json.loads(raw_tags)
            if isinstance(data, list):
                return data
        except Exception:
            return []
    return []


def _cents_to_usd(cents: Any) -> float:
    try:
        return round(int(cents or 0) / 100.0, 2)
    except Exception:
        return 0.0


def _format_summary_row(row: dict, *, pending_set: set[int], whitelist_set: set[int]) -> dict:
    """把 biz_user_payment_summary 一行 + 工单/白名单上下文 → 前端友好结构。"""
    user_id = int(row.get("user_id", 0))
    tags = _parse_tags(row.get("anomaly_tags"))

    if user_id in whitelist_set and ANOMALY_WHITELISTED not in tags:
        tags.append(ANOMALY_WHITELISTED)
    if user_id in pending_set and ANOMALY_PENDING not in tags:
        tags.append(ANOMALY_PENDING)

    total_orders = int(row.get("total_orders", 0))
    paid_orders = int(row.get("paid_orders", 0))
    success_rate = (paid_orders / total_orders) if total_orders > 0 else 0.0

    return {
        "user_id": user_id,
        "region": row.get("region"),
        "oauth_platform": row.get("oauth_platform"),
        "register_time_utc": row.get("register_time_utc"),
        "lang": row.get("lang"),
        "first_channel_id": row.get("first_channel_id") or "",
        "first_os_type": int(row.get("first_os_type") or 0),
        "first_pay_type": int(row.get("first_pay_type") or 0),

        "total_orders": total_orders,
        "paid_orders": paid_orders,
        "refund_orders": int(row.get("refund_orders") or 0),
        "success_rate": round(success_rate, 4),

        "paid_orders_ios": int(row.get("paid_orders_ios") or 0),
        "paid_orders_android": int(row.get("paid_orders_android") or 0),
        "total_gmv_usd_ios": _cents_to_usd(row.get("total_gmv_cents_ios")),
        "total_gmv_usd_android": _cents_to_usd(row.get("total_gmv_cents_android")),

        "paid_orders_subscribe": int(row.get("paid_orders_subscribe") or 0),
        "paid_orders_inapp": int(row.get("paid_orders_inapp") or 0),
        "total_gmv_usd_subscribe": _cents_to_usd(row.get("total_gmv_cents_subscribe")),
        "total_gmv_usd_inapp": _cents_to_usd(row.get("total_gmv_cents_inapp")),

        "total_gmv_usd": _cents_to_usd(row.get("total_gmv_cents")),
        "attempted_gmv_usd": _cents_to_usd(row.get("attempted_gmv_cents")),
        "refund_amount_usd": _cents_to_usd(row.get("refund_amount_cents")),

        "first_pay_time_utc": row.get("first_pay_time_utc"),
        "last_action_time_utc": row.get("last_action_time_utc"),
        "snapshot_ds": row.get("snapshot_ds"),
        "anomaly_tags": tags,
    }


def _format_order_row(row: dict) -> dict:
    return {
        "la_ds": row.get("la_ds"),
        "order_id": int(row.get("order_id", 0)),
        "order_no": row.get("order_no") or "",
        "user_id": int(row.get("user_id", 0)),
        "created_at_la": row.get("created_at_la"),
        "pay_time_la": row.get("pay_time_la"),
        "order_status": int(row.get("order_status") or 0),
        "os_type": int(row.get("os_type") or 0),
        "pay_type": int(row.get("pay_type") or 0),
        "pay_amount_usd": _cents_to_usd(row.get("pay_amount_cents")),
        "refund_amount_usd": _cents_to_usd(row.get("refund_amount_cents")),
        "product_id": row.get("product_id") or "",
        "is_subscribe": int(row.get("is_subscribe") or -1),
        "stall_group": int(row.get("stall_group") or 0),
        "channel_id": row.get("channel_id") or "",
        "drama_id": int(row.get("drama_id") or 0),
        "episode_id": int(row.get("episode_id") or 0),
    }


# ─────────────────────────────────────────────────────────────
#  查询接口（供 routes 层调用）
# ─────────────────────────────────────────────────────────────

def list_users(
    *,
    region: Optional[str] = None,
    oauth_platform: Optional[int] = None,
    first_channel_id: Optional[str] = None,
    channel_kind: Optional[str] = None,
    first_os_type: Optional[int] = None,
    anomaly_tag: Optional[str] = None,
    user_id: Optional[int] = None,
    min_total_orders: Optional[int] = None,
    min_paid_orders: Optional[int] = None,
    order_by: str = "last_action_time_utc",
    order_desc: bool = True,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """分页拉用户聚合列表。返回 {total, items, page, page_size}。"""
    page = max(1, int(page))
    page_size = max(1, min(int(page_size), 500))
    offset = (page - 1) * page_size

    pending_set = set(app_repo.list_pending_target_user_ids())
    whitelist_set = set(whitelist_repo.list_whitelisted_user_ids())

    first_channel_ids = resolve_channel_ids_by_kind(channel_kind)

    rows = repo.query_summary(
        region=region,
        oauth_platform=oauth_platform,
        first_channel_id=first_channel_id,
        first_channel_ids=first_channel_ids,
        first_os_type=first_os_type,
        anomaly_tag=anomaly_tag,
        user_id=user_id,
        min_total_orders=min_total_orders,
        min_paid_orders=min_paid_orders,
        order_by=order_by,
        order_desc=order_desc,
        limit=page_size,
        offset=offset,
    )
    total = repo.count_summary(
        region=region,
        oauth_platform=oauth_platform,
        first_channel_id=first_channel_id,
        first_channel_ids=first_channel_ids,
        first_os_type=first_os_type,
        anomaly_tag=anomaly_tag,
        user_id=user_id,
        min_total_orders=min_total_orders,
        min_paid_orders=min_paid_orders,
    )
    items = [
        _format_summary_row(r, pending_set=pending_set, whitelist_set=whitelist_set)
        for r in rows
    ]
    return {"total": total, "items": items, "page": page, "page_size": page_size}


def list_orders_for_user(user_id: int, *, limit: int = 500) -> list[dict]:
    rows = repo.query_orders_by_user(user_id, limit=limit)
    return [_format_order_row(r) for r in rows]


def list_orders(
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
    page: int = 1,
    page_size: int = 100,
) -> dict:
    page = max(1, int(page))
    page_size = max(1, min(int(page_size), 1000))
    offset = (page - 1) * page_size

    rows = repo.query_orders(
        start_ds=start_ds, end_ds=end_ds, user_id=user_id,
        order_status=order_status, os_type=os_type, channel_id=channel_id,
        is_subscribe=is_subscribe,
        order_by=order_by, order_desc=order_desc,
        limit=page_size, offset=offset,
    )
    total = repo.count_orders(
        start_ds=start_ds, end_ds=end_ds, user_id=user_id,
        order_status=order_status, os_type=os_type, channel_id=channel_id,
        is_subscribe=is_subscribe,
    )
    items = [_format_order_row(r) for r in rows]
    return {"total": total, "items": items, "page": page, "page_size": page_size}


# ─────────────────────────────────────────────────────────────
#  KPI 双口径
# ─────────────────────────────────────────────────────────────

def _format_kpi(raw_row: dict) -> dict:
    """把 repo.kpi_aggregate 返回的原始字段统一加工成前端展示用结构。"""
    total_orders = int(raw_row.get("total_orders") or 0)
    paid_orders = int(raw_row.get("paid_orders") or 0)
    success_rate = (paid_orders / total_orders) if total_orders > 0 else 0.0
    paying_users = int(raw_row.get("paying_users") or 0)
    total_gmv = _cents_to_usd(raw_row.get("total_gmv_cents"))
    arpu = (total_gmv / paying_users) if paying_users > 0 else 0.0

    return {
        "total_users": int(raw_row.get("total_users") or 0),
        "paying_users": paying_users,
        "try_but_fail_users": int(raw_row.get("try_but_fail_users") or 0),
        "guest_paying_users": int(raw_row.get("guest_paying_users") or 0),
        "total_orders": total_orders,
        "paid_orders": paid_orders,
        "success_rate": round(success_rate, 4),
        "total_gmv_usd": total_gmv,
        "attempted_gmv_usd": _cents_to_usd(raw_row.get("attempted_gmv_cents")),
        "total_gmv_usd_ios": _cents_to_usd(raw_row.get("total_gmv_cents_ios")),
        "total_gmv_usd_android": _cents_to_usd(raw_row.get("total_gmv_cents_android")),
        "total_gmv_usd_subscribe": _cents_to_usd(raw_row.get("total_gmv_cents_subscribe")),
        "total_gmv_usd_inapp": _cents_to_usd(raw_row.get("total_gmv_cents_inapp")),
        "arpu_usd": round(arpu, 2),
    }


def get_kpi_dual() -> dict:
    """同时计算 raw（原始）和 clean（剔除白名单）两套 KPI（90 天累计快照）。"""
    whitelist_ids = whitelist_repo.list_whitelisted_user_ids()
    raw = _format_kpi(repo.kpi_aggregate())
    clean = _format_kpi(repo.kpi_aggregate(exclude_whitelist_user_ids=whitelist_ids))
    return {
        "raw": raw,
        "clean": clean,
        "whitelist_count": len(whitelist_ids),
    }


# ─────────────────────────────────────────────────────────────
#  按窗口聚合（昨天/近 7/14/30/自定义）
# ─────────────────────────────────────────────────────────────

def _aggregate_window(start_ds: str, end_ds: str) -> list[dict]:
    """基于 biz_user_payment_order 在 [start_ds, end_ds] 内做用户聚合 + 异常判定。

    返回每用户的聚合行（结构跟 biz_user_payment_summary 选取字段一致），不含
    pending/whitelisted 运行时标签；这部分留给 list_users_by_window 在
    _format_summary_row 调用前叠加。

    异常标签规则同 _compute_user_summary（sync 任务版）：
      - suspect_brush:  窗口内任意单日下单 ≥10 且成单率 <10%
      - payment_loop:   窗口内任意单日下单 ≥5  且 0 成单
      - instant_burst:  注册后 30 分钟内下单 ≥5（用 summary.register_time_utc）
      - guest_payer:    summary.oauth_platform=-1 且 窗口内累计下单 ≥3
    """
    orders = repo.fetch_window_orders(start_ds, end_ds)
    if not orders:
        return []

    # 每用户每日 (total, paid) 用于 brush/loop 判定
    daily: dict[int, dict[Any, list[int]]] = defaultdict(lambda: defaultdict(lambda: [0, 0]))

    # 每用户聚合累加器（已按 created_at_la asc 排序，首行即首单）
    agg: dict[int, dict[str, Any]] = {}

    for o in orders:
        uid = int(o.get("user_id") or 0)
        if not uid:
            continue
        a = agg.setdefault(uid, {
            "user_id": uid,
            "first_channel_id": "",
            "first_os_type": 0,
            "first_pay_type": 0,
            "first_order_time_la": None,
            "total_orders": 0, "paid_orders": 0, "refund_orders": 0,
            "paid_orders_ios": 0, "paid_orders_android": 0,
            "total_gmv_cents_ios": 0, "total_gmv_cents_android": 0,
            "paid_orders_subscribe": 0, "paid_orders_inapp": 0,
            "total_gmv_cents_subscribe": 0, "total_gmv_cents_inapp": 0,
            "total_gmv_cents": 0, "attempted_gmv_cents": 0,
            "refund_amount_cents": 0,
            "first_pay_time_utc": None,
            "last_action_time_utc": None,
        })
        status = int(o.get("order_status") or 0)
        os_type = int(o.get("os_type") or 0)
        pay_amt = int(o.get("pay_amount_cents") or 0)
        refund_amt = int(o.get("refund_amount_cents") or 0)
        is_sub = int(o.get("is_subscribe") or 0)
        created_la = o.get("created_at_la")
        pay_la = o.get("pay_time_la")
        la_ds = o.get("la_ds")
        if isinstance(la_ds, datetime):
            la_ds = la_ds.date()

        if a["total_orders"] == 0:
            a["first_channel_id"] = (o.get("channel_id") or "")[:64]
            a["first_os_type"] = os_type
            a["first_pay_type"] = int(o.get("pay_type") or 0)
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
        if created_la and (a["last_action_time_utc"] is None or created_la > a["last_action_time_utc"]):
            a["last_action_time_utc"] = created_la

        if isinstance(la_ds, date):
            d = daily[uid][la_ds]
            d[0] += 1
            if status == 1:
                d[1] += 1

    # 拉 summary 维度信息做 enrich
    meta = repo.fetch_summary_meta_by_users(agg.keys())

    out: list[dict] = []
    for uid, a in agg.items():
        m = meta.get(uid) or {}
        oauth = m.get("oauth_platform")
        register_time_utc = m.get("register_time_utc")

        # 计算异常标签（在窗口内重新判定）
        tags: list[str] = []
        for _ds, (tot, paid) in daily[uid].items():
            if tot >= 10 and (paid / max(tot, 1)) < 0.1 and ANOMALY_BRUSH not in tags:
                tags.append(ANOMALY_BRUSH)
            if tot >= 5 and paid == 0 and ANOMALY_LOOP not in tags:
                tags.append(ANOMALY_LOOP)
        # instant_burst
        if (
            register_time_utc
            and isinstance(register_time_utc, datetime)
            and a["first_order_time_la"]
            and isinstance(a["first_order_time_la"], datetime)
            and a["total_orders"] >= 5
        ):
            try:
                from zoneinfo import ZoneInfo
                la_tz = ZoneInfo("America/Los_Angeles")
                reg_utc = (
                    register_time_utc.replace(tzinfo=timezone.utc)
                    if register_time_utc.tzinfo is None else register_time_utc
                )
                first_la = (
                    a["first_order_time_la"].replace(tzinfo=la_tz)
                    if a["first_order_time_la"].tzinfo is None else a["first_order_time_la"]
                )
                if timedelta(0) <= (first_la - reg_utc) <= timedelta(minutes=30):
                    tags.append(ANOMALY_BURST)
            except Exception:
                pass
        if oauth == -1 and a["total_orders"] >= 3:
            tags.append(ANOMALY_GUEST)

        out.append({
            "user_id": uid,
            "region": m.get("region"),
            "oauth_platform": oauth,
            "register_time_utc": register_time_utc,
            "lang": m.get("lang"),
            "first_channel_id": a["first_channel_id"],
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
            "snapshot_ds": end_ds,
            "anomaly_tags": tags,
        })
    return out


_ALLOWED_WINDOW_ORDER = {
    "total_orders", "paid_orders", "total_gmv_cents", "attempted_gmv_cents",
    "last_action_time_utc", "user_id",
}


def list_users_by_window(
    *,
    start_ds: str,
    end_ds: str,
    region: Optional[str] = None,
    oauth_platform: Optional[int] = None,
    first_channel_id: Optional[str] = None,
    channel_kind: Optional[str] = None,
    first_os_type: Optional[int] = None,
    anomaly_tag: Optional[str] = None,
    user_id: Optional[int] = None,
    min_total_orders: Optional[int] = None,
    min_paid_orders: Optional[int] = None,
    order_by: str = "last_action_time_utc",
    order_desc: bool = True,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """按窗口聚合的列表（与 list_users 输出结构一致）。"""
    page = max(1, int(page))
    page_size = max(1, min(int(page_size), 500))

    pending_set = set(app_repo.list_pending_target_user_ids())
    whitelist_set = set(whitelist_repo.list_whitelisted_user_ids())

    rows = _aggregate_window(start_ds, end_ds)

    channel_id_set: Optional[set[str]] = None
    resolved = resolve_channel_ids_by_kind(channel_kind)
    if resolved is not None:
        channel_id_set = set(resolved)

    # filter
    def _ok(r: dict) -> bool:
        if region and r.get("region") != region:
            return False
        if oauth_platform is not None and r.get("oauth_platform") != oauth_platform:
            return False
        if first_channel_id is not None and r.get("first_channel_id") != first_channel_id:
            return False
        if channel_id_set is not None and (r.get("first_channel_id") or "") not in channel_id_set:
            return False
        if first_os_type is not None and r.get("first_os_type") != first_os_type:
            return False
        if user_id is not None and r.get("user_id") != user_id:
            return False
        if min_total_orders is not None and r.get("total_orders", 0) < min_total_orders:
            return False
        if min_paid_orders is not None and r.get("paid_orders", 0) < min_paid_orders:
            return False
        if anomaly_tag:
            # pending / whitelisted 是运行时标签，要看 set
            tags = list(r.get("anomaly_tags") or [])
            if r["user_id"] in whitelist_set and ANOMALY_WHITELISTED not in tags:
                tags.append(ANOMALY_WHITELISTED)
            if r["user_id"] in pending_set and ANOMALY_PENDING not in tags:
                tags.append(ANOMALY_PENDING)
            if anomaly_tag not in tags:
                return False
        return True

    filtered = [r for r in rows if _ok(r)]

    # sort
    sort_key = order_by if order_by in _ALLOWED_WINDOW_ORDER else "last_action_time_utc"

    def _sk(r: dict):
        v = r.get(sort_key)
        # 排序时 None 一律视作最小，避免 datetime vs None 比较异常
        if v is None:
            return (1, 0) if order_desc else (0, 0)
        return (0, v)

    filtered.sort(key=_sk, reverse=order_desc)

    total = len(filtered)
    start = (page - 1) * page_size
    page_rows = filtered[start:start + page_size]

    items = [
        _format_summary_row(r, pending_set=pending_set, whitelist_set=whitelist_set)
        for r in page_rows
    ]
    return {"total": total, "items": items, "page": page, "page_size": page_size}


def get_kpi_dual_by_window(start_ds: str, end_ds: str) -> dict:
    """按窗口聚合的 raw + clean 双口径 KPI。"""
    whitelist_ids = set(whitelist_repo.list_whitelisted_user_ids())
    rows = _aggregate_window(start_ds, end_ds)

    def _bucket(exclude_wl: bool) -> dict:
        bucket = {
            "total_users": 0, "paying_users": 0, "try_but_fail_users": 0,
            "guest_paying_users": 0,
            "total_orders": 0, "paid_orders": 0,
            "total_gmv_cents": 0, "attempted_gmv_cents": 0,
            "total_gmv_cents_ios": 0, "total_gmv_cents_android": 0,
            "total_gmv_cents_subscribe": 0, "total_gmv_cents_inapp": 0,
        }
        for r in rows:
            if exclude_wl and r["user_id"] in whitelist_ids:
                continue
            bucket["total_users"] += 1
            tot = r["total_orders"]; paid = r["paid_orders"]
            bucket["total_orders"] += tot
            bucket["paid_orders"] += paid
            if paid > 0:
                bucket["paying_users"] += 1
                if r.get("oauth_platform") == -1:
                    bucket["guest_paying_users"] += 1
            elif tot > 0:
                bucket["try_but_fail_users"] += 1
            for k in (
                "total_gmv_cents", "attempted_gmv_cents",
                "total_gmv_cents_ios", "total_gmv_cents_android",
                "total_gmv_cents_subscribe", "total_gmv_cents_inapp",
            ):
                bucket[k] += int(r.get(k) or 0)
        return _format_kpi(bucket)

    return {
        "raw": _bucket(False),
        "clean": _bucket(True),
        "whitelist_count": len(whitelist_ids),
    }
