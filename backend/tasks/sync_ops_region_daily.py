"""sync_ops_region_daily.py — 区域渠道分析 T+1 日报同步

数据流：
    PolarDB matrix_advertise.channel_user (产研库 prd)         ─┐
    PolarDB matrix_order.recharge_order   (订单库 order)       ─┼─→ adpilot_biz.biz_ops_region_register_daily
    MaxCompute metis_dw.dim_user_df        (DMS)               ─┘   adpilot_biz.biz_ops_region_revenue_daily

口径：
- 注册侧：ds = LA(register_time_utc)，region 来自 dim_user_df.region
  channel_kind 判定：
    - user_id 不在 channel_user 表中 → 0 (organic) — 含真自然量+SEO+品牌词
    - 在 channel_user 中按 ad_platform 拆 1=tiktok / 2=meta / 0/其他=other(3)
  注册侧无 OS 维度（dim_user_df 不带设备字段）

- 充值侧：ds = LA(recharge_order.created_at)，region 来自 dim_user_df.region (按 user_id enrich)
  channel_kind 直接看 recharge_order.channel_id（不用 channel_user，更准）：
    - channel_id IN ('','0') → 0 (organic)
    - 其它 → get_channel_dict() 拿 ad_platform 拆分
  充值侧含 OS 拆分（os_type 1=Android / 2=iOS）
  金额单位：美分 BIGINT（service 层再转 USD）

调度：每日 LA 04:00（北京 19:00 / 20:00），由 app.py 注册到 apscheduler
默认回填：30 天

CLI:
    python -m tasks.sync_ops_region_daily                       # 默认 30 天
    python -m tasks.sync_ops_region_daily --backfill 60
    python -m tasks.sync_ops_region_daily --start-ds 2026-04-15 --end-ds 2026-05-14
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from config import get_settings
from db import get_channel_dict, get_order_conn, get_prd_conn
from integrations.dms_client import DmsAuthError, DmsError, DmsSqlError, get_default_client
from repositories import biz_ops_region_repository as repo
from repositories import biz_sync_log_repository as sync_log_repo

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_ops_region_daily"
DEFAULT_BACKFILL_DAYS = 30

# ─── channel_kind 编码 ──────────────────────────────────────
CK_ORGANIC = 0
CK_TIKTOK = 1
CK_META = 2
CK_OTHER = 3


# ─── 工具 ───────────────────────────────────────────────────
def _today_la() -> date:
    return datetime.now(LA_TZ).date()


def _to_int(v) -> int:
    if v is None or v == "":
        return 0
    try:
        return int(float(v))
    except Exception:
        return 0


def _to_str(v) -> str:
    return "" if v is None else str(v)


def _ds_str(ds) -> str:
    if isinstance(ds, (date, datetime)):
        return ds.strftime("%Y-%m-%d")
    return str(ds)[:10]


def _ad_platform_to_kind(ap) -> int:
    """channel_user.ad_platform → channel_kind."""
    try:
        ap_int = int(ap) if ap is not None else 0
    except (TypeError, ValueError):
        ap_int = 0
    if ap_int == 1:
        return CK_TIKTOK
    if ap_int == 2:
        return CK_META
    return CK_OTHER


def _channel_id_to_kind(channel_id: str, dict_cache: dict[str, dict]) -> int:
    """recharge_order.channel_id → channel_kind（依赖 channel_dict 拆 ad_platform）."""
    cid = (channel_id or "").strip()
    if cid in ("", "0"):
        return CK_ORGANIC
    info = dict_cache.get(cid)
    if not info:
        # 字典里查不到的 channel_id → 算 other（一般是新渠道未及时同步到 channel_user）
        return CK_OTHER
    return _ad_platform_to_kind(info.get("ad_platform"))


# ─────────────────────────────────────────────────────────────
#  数据源 1：MC dim_user_df → 窗口内注册用户
# ─────────────────────────────────────────────────────────────

# register_time_utc 本身是 UTC，按 LA 切日要 FROM_UTC_TIMESTAMP
# UTC 窗口向外扩 ±1 天，确保 LA 日边界订单不丢
_REGISTER_USERS_SQL = """
SELECT
    user_id,
    region,
    TO_CHAR(
        FROM_UTC_TIMESTAMP(CAST(register_time_utc AS TIMESTAMP), 'America/Los_Angeles'),
        'yyyy-MM-dd'
    ) AS ds_la
FROM metis_dw.dim_user_df
WHERE ds = (SELECT MAX(ds) FROM metis_dw.dim_user_df)
  AND register_time_utc IS NOT NULL
  AND register_time_utc >= '{utc_lo} 00:00:00'
  AND register_time_utc <  '{utc_hi} 00:00:00'
"""


def _fetch_register_users_from_mc(la_lo: str, la_hi: str) -> list[dict]:
    """从 MC dim_user_df 拉窗口内注册的用户（含 region），按 LA 日切。

    返回每用户一行：{user_id, region, ds_la}
    """
    settings = get_settings()
    client = get_default_client()
    utc_lo = (datetime.strptime(la_lo, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    utc_hi = (datetime.strptime(la_hi, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d")
    sql = _REGISTER_USERS_SQL.format(utc_lo=utc_lo, utc_hi=utc_hi)
    logger.info("MC dim_user_df 拉注册用户 LA[%s,%s] UTC[%s,%s)", la_lo, la_hi, utc_lo, utc_hi)
    try:
        result = client.execute(sql, settings.dms_mc_db_id)
    except DmsAuthError as e:
        raise RuntimeError(f"MC dim_user_df 权限不足: {e}") from e
    except DmsSqlError as e:
        raise RuntimeError(f"MC dim_user_df SQL 失败: {e}") from e
    except DmsError as e:
        raise RuntimeError(f"DMS 调用失败: {e}") from e

    out: list[dict] = []
    for r in result.rows:
        uid = _to_int(r.get("user_id"))
        if not uid:
            continue
        ds_la = _to_str(r.get("ds_la"))[:10]
        if not (la_lo <= ds_la <= la_hi):
            continue
        out.append({
            "user_id": uid,
            "region": (_to_str(r.get("region")) or "UNK")[:8].upper(),
            "ds_la": ds_la,
        })
    logger.info("MC 注册用户 %d 行 (request_id=%s)", len(out), result.request_id)
    return out


# ─────────────────────────────────────────────────────────────
#  数据源 2：MC dim_user_df → 按 user_id 反查 region (充值侧 enrich)
# ─────────────────────────────────────────────────────────────

_DIM_USER_REGION_SQL = """
SELECT user_id, region
FROM metis_dw.dim_user_df
WHERE ds = (SELECT MAX(ds) FROM metis_dw.dim_user_df)
  AND user_id IN ({user_list})
"""


def _fetch_user_region(user_ids: list[int]) -> dict[int, str]:
    """按 user_id 批量查 region → {user_id: region}（缺失 user_id 不在 dict 里，调用方填 'UNK'）。"""
    if not user_ids:
        return {}
    settings = get_settings()
    client = get_default_client()
    out: dict[int, str] = {}
    batch_size = 1000
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        sql = _DIM_USER_REGION_SQL.format(user_list=", ".join(str(int(u)) for u in batch))
        try:
            result = client.execute(sql, settings.dms_mc_db_id)
        except DmsError as e:
            logger.warning("MC dim_user_df region enrich 批次失败（已忽略该批）: %s", e)
            continue
        for r in result.rows:
            uid = _to_int(r.get("user_id"))
            if not uid:
                continue
            region = (_to_str(r.get("region")) or "UNK")[:8].upper()
            out[uid] = region
    logger.info("region enrich 命中 %d / %d", len(out), len(user_ids))
    return out


# ─────────────────────────────────────────────────────────────
#  数据源 3：PolarDB matrix_advertise.channel_user → 非自然量映射
# ─────────────────────────────────────────────────────────────

def _fetch_channel_user_map(user_ids: list[int]) -> dict[int, int]:
    """按 user_id 列表查 channel_user，返回 {user_id: ad_platform}。

    不在 dict 里的 user_id = 自然量（channel_user 中没记录）。
    走产研库连接（默认就是 matrix_advertise schema），失败时返回空 dict（容错）。
    """
    if not user_ids:
        return {}
    out: dict[int, int] = {}
    batch_size = 1000
    try:
        with get_prd_conn() as conn:
            if conn is None:
                logger.warning("channel_user enrich 跳过：PRD 连接不可用，所有用户都将算 organic")
                return {}
            cur = conn.cursor()
            for i in range(0, len(user_ids), batch_size):
                batch = user_ids[i:i + batch_size]
                placeholders = ", ".join(["%s"] * len(batch))
                cur.execute(
                    f"SELECT user_id, ad_platform FROM channel_user WHERE user_id IN ({placeholders})",
                    tuple(int(u) for u in batch),
                )
                for r in cur.fetchall():
                    uid = _to_int(r.get("user_id"))
                    if not uid:
                        continue
                    ap = r.get("ad_platform")
                    out[uid] = int(ap) if ap is not None else 0
    except Exception as e:
        logger.warning("channel_user enrich 异常（忽略，所有未命中算 organic）: %s", e)
    logger.info("channel_user 命中 %d / %d (剩余视为 organic)", len(out), len(user_ids))
    return out


# ─────────────────────────────────────────────────────────────
#  数据源 4：PolarDB recharge_order → 充值订单
# ─────────────────────────────────────────────────────────────

_RECHARGE_ORDERS_SQL = """
SELECT
    DATE(CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles')) AS ds_la,
    user_id,
    channel_id,
    os_type,
    is_subscribe,
    pay_amount
FROM recharge_order
WHERE order_status = 1
  AND app_id = 1
  AND os_type IN (1, 2)
  AND created_at >= %s AND created_at < %s
HAVING ds_la BETWEEN %s AND %s
"""


def _fetch_orders_from_polardb(la_lo: str, la_hi: str) -> list[dict]:
    """从 PolarDB recharge_order 拉窗口内已支付订单。"""
    bj_lo = (datetime.strptime(la_lo, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    bj_hi = (datetime.strptime(la_hi, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")
    logger.info("PolarDB recharge_order 拉单 LA[%s,%s] BJ[%s,%s)", la_lo, la_hi, bj_lo, bj_hi)
    with get_order_conn() as conn:
        if conn is None:
            raise RuntimeError("PolarDB 不可用，无法同步充值侧")
        cur = conn.cursor()
        cur.execute(_RECHARGE_ORDERS_SQL, (bj_lo, bj_hi, la_lo, la_hi))
        rows = list(cur.fetchall())
    logger.info("PolarDB 订单 %d 行", len(rows))
    return rows


# ─────────────────────────────────────────────────────────────
#  聚合
# ─────────────────────────────────────────────────────────────

def _aggregate_register(
    register_users: list[dict],
    cu_map: dict[int, int],
) -> list[dict]:
    """按 (ds_la, region, channel_kind) 聚合注册 UV。

    register_users: [{user_id, region, ds_la}, ...]
    cu_map:         {user_id: ad_platform}（不在内即 organic）
    """
    bucket: dict[tuple, set[int]] = defaultdict(set)
    for u in register_users:
        uid = u["user_id"]
        ap = cu_map.get(uid)
        kind = CK_ORGANIC if ap is None else _ad_platform_to_kind(ap)
        key = (u["ds_la"], u["region"] or "UNK", kind)
        bucket[key].add(uid)

    out = []
    for (ds_la, region, kind), uids in bucket.items():
        out.append({
            "ds": ds_la,
            "region": region,
            "channel_kind": kind,
            "register_uv": len(uids),
        })
    return out


def _aggregate_revenue(
    orders: list[dict],
    region_map: dict[int, str],
    channel_dict: dict[str, dict],
) -> list[dict]:
    """按 (ds_la, region, channel_kind, os_type) 聚合充值。

    orders:       [{ds_la, user_id, channel_id, os_type, is_subscribe, pay_amount}, ...]
    region_map:   {user_id: region}  缺失填 'UNK'
    channel_dict: get_channel_dict() 返回的字典，用于 channel_id → ad_platform 映射
    """
    bucket: dict[tuple, dict] = defaultdict(lambda: {
        "payer_uvs": set(),
        "order_cnt": 0,
        "revenue_cents": 0,
        "sub_revenue_cents": 0,
        "iap_revenue_cents": 0,
    })

    for o in orders:
        ds_la = _ds_str(o.get("ds_la"))
        uid = _to_int(o.get("user_id"))
        if not uid:
            continue
        os_type = _to_int(o.get("os_type"))
        if os_type not in (1, 2):
            continue
        cid = _to_str(o.get("channel_id"))
        kind = _channel_id_to_kind(cid, channel_dict)
        region = region_map.get(uid) or "UNK"
        is_sub = _to_int(o.get("is_subscribe"))
        amt = _to_int(o.get("pay_amount"))

        key = (ds_la, region, kind, os_type)
        b = bucket[key]
        b["payer_uvs"].add(uid)
        b["order_cnt"] += 1
        b["revenue_cents"] += amt
        if is_sub == 1:
            b["sub_revenue_cents"] += amt
        elif is_sub in (0, -1):
            b["iap_revenue_cents"] += amt

    out = []
    for (ds_la, region, kind, os_type), b in bucket.items():
        out.append({
            "ds": ds_la,
            "region": region,
            "channel_kind": kind,
            "os_type": os_type,
            "payer_uv": len(b["payer_uvs"]),
            "order_cnt": b["order_cnt"],
            "revenue_cents": b["revenue_cents"],
            "sub_revenue_cents": b["sub_revenue_cents"],
            "iap_revenue_cents": b["iap_revenue_cents"],
        })
    return out


# ─────────────────────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────────────────────

def run(
    *,
    start_ds: Optional[str] = None,
    end_ds: Optional[str] = None,
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
    purge_window: bool = True,
    register_table: str = repo.REGISTER_DAILY,
    revenue_table: str = repo.REVENUE_DAILY,
    skip_register: bool = False,
) -> dict:
    """同步主入口。

    Args:
        start_ds / end_ds: 显式 LA 日窗口
        backfill_days:     未指定窗口时回填最近 N 天
        purge_window:      写入前 DELETE 区间（默认 True，避免 user 重组导致老分桶残留）
        register_table:    注册侧目标表（默认 _daily，intraday job 复用本函数时传 _intraday）
        revenue_table:     充值侧目标表
        skip_register:     仅跑充值侧（intraday job 可能跳过 register 以加速；当前默认两侧都跑）
    """
    if start_ds and end_ds:
        s_ds, e_ds = start_ds, end_ds
    else:
        today = _today_la()
        e_ds = today.strftime("%Y-%m-%d")
        s_ds = (today - timedelta(days=backfill_days - 1)).strftime("%Y-%m-%d")

    log_id = sync_log_repo.create(task_name=TASK_NAME, sync_date=e_ds)
    started = datetime.now(timezone.utc)
    register_rows = revenue_rows = 0

    try:
        # ── 1) 充值侧：先拉订单，拿到 user_id 集合 ──
        orders = _fetch_orders_from_polardb(s_ds, e_ds)
        order_user_ids = sorted({_to_int(o.get("user_id")) for o in orders if _to_int(o.get("user_id"))})

        # ── 2) 注册侧：从 MC 拉窗口内注册用户 ──
        register_users: list[dict] = []
        if not skip_register:
            try:
                register_users = _fetch_register_users_from_mc(s_ds, e_ds)
            except Exception as e:
                logger.error("MC 注册侧拉取失败，注册侧本次跳过: %s", e)
                register_users = []
        register_user_ids = [u["user_id"] for u in register_users]

        # ── 3) channel_user enrich（注册侧 + 充值侧合并 user_ids 一次查完）──
        all_user_ids = sorted(set(order_user_ids) | set(register_user_ids))
        cu_map = _fetch_channel_user_map(all_user_ids)

        # ── 4) region enrich（仅充值侧需要）──
        # 充值侧 user_id 中没在 MC 注册侧拿过 region 的部分需要补查
        register_region_map = {u["user_id"]: u["region"] for u in register_users}
        order_user_missing = [uid for uid in order_user_ids if uid not in register_region_map]
        order_region_map: dict[int, str] = {}
        if order_user_missing:
            try:
                order_region_map = _fetch_user_region(order_user_missing)
            except Exception as e:
                logger.warning("充值侧 region enrich 失败（缺失用户填 UNK）: %s", e)
                order_region_map = {}
        # 合并：优先用注册侧已查到的
        full_region_map = {**order_region_map, **register_region_map}

        # ── 5) channel_dict（充值侧用）──
        channel_dict = get_channel_dict() or {}

        # ── 6) 聚合 ──
        register_agg = _aggregate_register(register_users, cu_map) if register_users else []
        revenue_agg = _aggregate_revenue(orders, full_region_map, channel_dict) if orders else []

        # ── 7) 写入（先 DELETE 窗口再 upsert，保证一致性）──
        if purge_window:
            if register_agg or not skip_register:
                deleted = repo.delete_register_window(s_ds, e_ds, table=register_table)
                logger.info("清理注册侧窗口 [%s, %s] 旧数据 %d 行", s_ds, e_ds, deleted)
            deleted = repo.delete_revenue_window(s_ds, e_ds, table=revenue_table)
            logger.info("清理充值侧窗口 [%s, %s] 旧数据 %d 行", s_ds, e_ds, deleted)

        if register_agg:
            for i in range(0, len(register_agg), 1000):
                repo.upsert_register_batch(register_agg[i:i + 1000], table=register_table)
            register_rows = len(register_agg)

        if revenue_agg:
            for i in range(0, len(revenue_agg), 1000):
                repo.upsert_revenue_batch(revenue_agg[i:i + 1000], table=revenue_table)
            revenue_rows = len(revenue_agg)

        message = (
            f"window=[{s_ds},{e_ds}] register_rows={register_rows} "
            f"revenue_rows={revenue_rows} register_users={len(register_users)} "
            f"orders={len(orders)} cu_match={len(cu_map)}"
        )
        sync_log_repo.finish(
            log_id, status="success", message=message,
            rows_affected=register_rows + revenue_rows,
        )
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info("sync_ops_region_daily 完成: %s 耗时 %.1fs", message, elapsed)
        return {
            "status": "success",
            "start_ds": s_ds,
            "end_ds": e_ds,
            "register_rows": register_rows,
            "revenue_rows": revenue_rows,
            "elapsed_sec": round(elapsed, 1),
        }
    except Exception as e:
        sync_log_repo.finish(log_id, status="failed", message=str(e)[:500])
        logger.exception("sync_ops_region_daily 失败")
        raise


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(description="同步区域渠道分析 T+1 日报")
    parser.add_argument("--start-ds")
    parser.add_argument("--end-ds")
    parser.add_argument("--backfill", type=int, default=DEFAULT_BACKFILL_DAYS)
    parser.add_argument("--no-purge", action="store_true", help="跳过先 DELETE 窗口（默认会 DELETE）")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = run(
        start_ds=args.start_ds,
        end_ds=args.end_ds,
        backfill_days=args.backfill,
        purge_window=not args.no_purge,
    )
    print(result)
