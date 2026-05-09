"""sync_attribution_daily.py — 投放归因日报同步任务 (DMS OpenAPI 版)

数据流：
    metis_dw.ads_ad_delivery_di (MaxCompute, 上游 ETL)
        ↓ 每日 INSERT OVERWRITE 121 个 LA cohort 分区（ds = LA 日）
        ↓ 本任务通过 阿里云 DMS Enterprise ExecuteScript (dbId=80154230) 拉取
    adpilot_biz.biz_attribution_ad_daily (MySQL)

为什么走 DMS 而不是 pyodps：
    metis_dw 项目级 ACL 没把当前 RAM 用户加入项目成员（owner 未 add user）。
    pyodps 直连 MaxCompute service endpoint 走的是 project member 鉴权，被拒；
    而 DMS OpenAPI 走的是 DMS Enterprise 安全规则鉴权，已开通 SELECT 权限。
    两条通路独立，DMS 这条已经验证可用，故 daily 也统一走 DMS。

关键约束：
- 上游每日回刷 121 天，本任务必须同步回刷 121 天（默认窗口 = 当天 LA - 120 ~ 当天 LA）
- 金额字段：spend / budget_amount 是 USD 直存；first_*_amount / cum_recharge_* 是
  美分 BIGINT，本任务统一 / 100.0 转 USD
- ds_account_local：按账户时区近似（LA + Phoenix 当前等于 ds_la；其他时区记 warning）
- 单天行数级 < 30，121 天总量级 < 5000 行，单次 DMS 调用即可，无需分页

CLI 用法：
    python -m tasks.sync_attribution_daily                # 默认 121 天回刷
    python -m tasks.sync_attribution_daily --backfill 30  # 回填最近 30 天
    python -m tasks.sync_attribution_daily --start-ds 2026-04-01 --end-ds 2026-05-08

依赖：alibabacloud-dms-enterprise20181101 + pymysql（无需 pyodps）
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Iterator, Optional
from zoneinfo import ZoneInfo

from config import get_settings
from integrations.dms_client import (
    DmsAuthError,
    DmsError,
    DmsSqlError,
    get_default_client,
)
from repositories import biz_attribution_ad_daily_repository as repo
from repositories import biz_sync_log_repository as sync_log_repo

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  常量
# ─────────────────────────────────────────────────────────────

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_attribution_daily"

# 账户时区 → ds_account_local 计算函数
# 当前业务约束：账户后续统一 UTC-7（LA / Phoenix）。两者在 cohort 日级别等价：
# - 夏令时（约 8 个月）UTC-7（PDT）vs UTC-7（Phoenix）：日界线完全重合
# - 冬令时（约 4 个月）UTC-8（PST）vs UTC-7（Phoenix）：差 1 小时，cohort 日级业务上视为等价
# 历史可能存在的非北美时区账户（如 Asia/Bangkok 等）会被记录到 unknown_tz，前端展示标签提示。
TZ_OFFSET_MAP: dict[str, str] = {
    "America/Los_Angeles": "as_la",
    "America/Phoenix":     "as_la",
}

# 金额字段（在 MC 端是美分 BIGINT，需 / 100 转 USD）
_AMOUNT_CENTS_FIELDS = (
    "first_sub_amount", "renew_sub_amount",
    "first_iap_amount", "repeat_iap_amount",
    "total_recharge_amount",
    "cum_recharge_1d", "cum_recharge_3d", "cum_recharge_7d",
    "cum_recharge_14d", "cum_recharge_30d",
    "cum_recharge_90d", "cum_recharge_120d",
)
_AMOUNT_USD_FIELDS = ("budget_amount",)

# ─────────────────────────────────────────────────────────────
#  MaxCompute 数据源
# ─────────────────────────────────────────────────────────────

_SOURCE_TABLE = "ads_ad_delivery_di"

# 注意：MC SDK 字段顺序与 PT 表一致；这里指定 SELECT 列以确保 ds 始终最后一列
_SELECT_COLUMNS = (
    "platform", "account_id", "account_name", "account_status",
    "account_timezone", "timezone_source",
    "campaign_id", "campaign_name", "delivery_method", "operator_id",
    "content_id", "objective_type", "budget_mode", "budget_amount",
    "adgroup_id", "adgroup_name", "optimize_goal", "bid_type",
    "ad_id", "ad_name", "creative_id", "video_id", "ad_status",
    "spend", "impressions", "clicks", "inline_link_clicks",
    "landing_page_view", "conversion", "install", "activation",
    "registration", "purchase",
    "cohort_activations", "cohort_first_chargers", "cohort_pay_users",
    "first_sub_count", "first_sub_amount", "renew_sub_count", "renew_sub_amount",
    "first_iap_count", "first_iap_amount", "repeat_iap_count", "repeat_iap_amount",
    "total_recharge_amount",
    "cum_recharge_1d", "cum_recharge_3d", "cum_recharge_7d", "cum_recharge_14d",
    "cum_recharge_30d", "cum_recharge_90d", "cum_recharge_120d",
    "updated_at", "ds",
)


def fetch_from_dms(start_ds: str, end_ds: str, *,
                   batch_size: int = 5000) -> Iterator[list[dict]]:
    """通过 DMS OpenAPI ExecuteScript 拉取 [start_ds, end_ds] 之间的归因日报。

    单次拉光，按 batch_size 切批 yield 给主流程做 normalize + upsert。
    daily 数据量级（121 天 < 5000 行）单次调用足够，无需分页。
    """
    settings = get_settings()
    cols_sql = ", ".join(_SELECT_COLUMNS)
    sql = (
        f"SELECT {cols_sql} "
        f"FROM metis_dw.{_SOURCE_TABLE} "
        f"WHERE ds >= '{start_ds}' AND ds <= '{end_ds}'"
    )
    logger.info(
        "DMS 查询 dbId=%s 时间窗 %s ~ %s, batch_size=%d",
        settings.dms_mc_db_id, start_ds, end_ds, batch_size,
    )

    client = get_default_client()
    try:
        result = client.execute(sql, settings.dms_mc_db_id)
    except DmsAuthError as e:
        raise RuntimeError(f"MC 权限不足，请检查 DMS 授权: {e}") from e
    except DmsSqlError as e:
        raise RuntimeError(f"MC SQL 执行失败: {e}") from e
    except DmsError as e:
        raise RuntimeError(f"DMS 调用失败: {e}") from e

    rows = result.rows
    logger.info(
        "DMS 返回 rows=%d cols=%d request_id=%s",
        len(rows), len(result.columns), result.request_id,
    )

    # DMS 返回所有值都是字符串；保留 _SELECT_COLUMNS 顺序，缺列补 None
    buf: list[dict] = []
    for raw in rows:
        rec = {col: raw.get(col) for col in _SELECT_COLUMNS}
        buf.append(rec)
        if len(buf) >= batch_size:
            yield buf
            buf = []
    if buf:
        yield buf


# ─────────────────────────────────────────────────────────────
#  字段转换
# ─────────────────────────────────────────────────────────────

def _cents_to_usd(v) -> float:
    if v is None:
        return 0.0
    try:
        return round(float(v) / 100.0, 4)
    except Exception:
        return 0.0


def _to_float_usd(v) -> float:
    if v is None:
        return 0.0
    try:
        return round(float(v), 4)
    except Exception:
        return 0.0


def _to_int(v) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        return 0


def _to_str(v, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def _to_dt_str(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


def _compute_ds_account_local(ds_la: str, account_timezone: str,
                              unknown_tz_counter: dict[str, int]) -> str:
    """根据账户时区近似 cohort 日。LA + Phoenix 都返回 ds_la；
    其他时区记入 unknown_tz_counter 后默认仍返回 ds_la 兜底。"""
    rule = TZ_OFFSET_MAP.get(account_timezone or "")
    if rule is None and account_timezone:
        unknown_tz_counter[account_timezone] = unknown_tz_counter.get(account_timezone, 0) + 1
    return ds_la


def _normalize_row(rec: dict, unknown_tz_counter: dict[str, int]) -> dict:
    ds_la = _to_str(rec.get("ds"))[:10]
    account_timezone = _to_str(rec.get("account_timezone"))
    ds_account_local = _compute_ds_account_local(ds_la, account_timezone, unknown_tz_counter)

    out = {
        "ds_la": ds_la,
        "ds_account_local": ds_account_local,
        "account_timezone": account_timezone,
        "timezone_source": _to_str(rec.get("timezone_source")),
        "platform": _to_str(rec.get("platform")),
        "account_id": _to_str(rec.get("account_id")),
        "account_name": _to_str(rec.get("account_name")),
        "account_status": _to_str(rec.get("account_status")),
        "campaign_id": _to_str(rec.get("campaign_id")),
        "campaign_name": _to_str(rec.get("campaign_name")),
        "delivery_method": _to_str(rec.get("delivery_method")),
        "operator_id": _to_str(rec.get("operator_id")),
        "content_id": _to_int(rec.get("content_id")),
        "objective_type": _to_str(rec.get("objective_type")),
        "budget_mode": _to_str(rec.get("budget_mode")),
        "adgroup_id": _to_str(rec.get("adgroup_id")),
        "adgroup_name": _to_str(rec.get("adgroup_name")),
        "optimize_goal": _to_str(rec.get("optimize_goal")),
        "bid_type": _to_str(rec.get("bid_type")),
        "ad_id": _to_str(rec.get("ad_id")),
        "ad_name": _to_str(rec.get("ad_name")),
        "creative_id": _to_str(rec.get("creative_id")),
        "video_id": _to_str(rec.get("video_id")),
        "ad_status": _to_str(rec.get("ad_status")),
        "spend": _to_float_usd(rec.get("spend")),
        "impressions": _to_int(rec.get("impressions")),
        "clicks": _to_int(rec.get("clicks")),
        "inline_link_clicks": _to_int(rec.get("inline_link_clicks")),
        "landing_page_view": _to_int(rec.get("landing_page_view")),
        "conversion": _to_int(rec.get("conversion")),
        "install": _to_int(rec.get("install")),
        "activation": _to_int(rec.get("activation")),
        "registration": _to_int(rec.get("registration")),
        "purchase": _to_int(rec.get("purchase")),
        "cohort_activations": _to_int(rec.get("cohort_activations")),
        "cohort_first_chargers": _to_int(rec.get("cohort_first_chargers")),
        "cohort_pay_users": _to_int(rec.get("cohort_pay_users")),
        "first_sub_count": _to_int(rec.get("first_sub_count")),
        "renew_sub_count": _to_int(rec.get("renew_sub_count")),
        "first_iap_count": _to_int(rec.get("first_iap_count")),
        "repeat_iap_count": _to_int(rec.get("repeat_iap_count")),
        "upstream_updated_at": _to_dt_str(rec.get("updated_at")),
    }

    for f in _AMOUNT_CENTS_FIELDS:
        out[f] = _cents_to_usd(rec.get(f))
    for f in _AMOUNT_USD_FIELDS:
        out[f] = _to_float_usd(rec.get(f))

    return out


# ─────────────────────────────────────────────────────────────
#  主入口
# ─────────────────────────────────────────────────────────────

def _today_la() -> date:
    return datetime.now(LA_TZ).date()


def run(*, start_ds: Optional[str] = None,
        end_ds: Optional[str] = None,
        backfill_days: int = 120,
        batch_size: int = 5000,
        purge_window: bool = False) -> dict:
    """同步主入口。

    参数：
      start_ds / end_ds: 显式时间窗（YYYY-MM-DD，LA cohort 日）。优先级最高。
      backfill_days:    未指定窗口时，回填最近 N 天（含今天 LA），默认 120
      batch_size:       从 MC 流式拉取的单批行数
      purge_window:     是否在写入前 DELETE 该窗口数据（默认 false，使用 ON DUPLICATE KEY UPDATE）
    """
    if start_ds and end_ds:
        s_ds, e_ds = start_ds, end_ds
    else:
        today = _today_la()
        s_ds = (today - timedelta(days=backfill_days)).strftime("%Y-%m-%d")
        e_ds = today.strftime("%Y-%m-%d")

    log_id = sync_log_repo.create(task_name=TASK_NAME, sync_date=e_ds)
    started = datetime.now(timezone.utc)
    total_rows = 0
    unknown_tz_counter: dict[str, int] = {}

    try:
        if purge_window:
            removed = repo.delete_window(s_ds, e_ds, tz_basis="la")
            logger.info(f"已清理窗口 [{s_ds}, {e_ds}] 旧数据 {removed} 行")

        for batch in fetch_from_dms(s_ds, e_ds, batch_size=batch_size):
            normalized = [_normalize_row(r, unknown_tz_counter) for r in batch]
            n = repo.upsert_batch(normalized)
            total_rows += len(normalized)
            logger.info(f"upsert_batch 写入 {len(normalized)} 行（rowcount={n}），累计 {total_rows}")

        msg_parts = [f"window=[{s_ds}, {e_ds}] rows={total_rows}"]
        if unknown_tz_counter:
            tz_summary = ", ".join(f"{k}:{v}" for k, v in sorted(unknown_tz_counter.items()))
            msg_parts.append(f"unknown_tz=[{tz_summary}]")
            logger.warning(f"遇到未注册时区，请在 TZ_OFFSET_MAP 中评估并补齐：{tz_summary}")
        message = "; ".join(msg_parts)

        sync_log_repo.finish(log_id, status="success", message=message, rows_affected=total_rows)
        ended = datetime.now(timezone.utc)
        elapsed = (ended - started).total_seconds()
        logger.info(f"sync_attribution_daily 完成：{message} 耗时 {elapsed:.1f}s")
        return {
            "status": "success",
            "start_ds": s_ds, "end_ds": e_ds,
            "rows": total_rows,
            "unknown_tz": unknown_tz_counter,
            "elapsed_sec": elapsed,
        }
    except Exception as e:
        logger.exception(f"sync_attribution_daily 失败: {e}")
        sync_log_repo.finish(log_id, status="failed", message=str(e), rows_affected=total_rows)
        raise


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(description="Sync metis_dw.ads_ad_delivery_di → biz_attribution_ad_daily")
    parser.add_argument("--start-ds", dest="start_ds", default=None,
                        help="起始 LA cohort 日 YYYY-MM-DD")
    parser.add_argument("--end-ds", dest="end_ds", default=None,
                        help="结束 LA cohort 日 YYYY-MM-DD")
    parser.add_argument("--backfill", dest="backfill_days", type=int, default=120,
                        help="未指定窗口时，回填最近 N 天（默认 120）")
    parser.add_argument("--batch-size", dest="batch_size", type=int, default=5000)
    parser.add_argument("--purge", dest="purge_window", action="store_true",
                        help="写入前先 DELETE 窗口数据（默认走 ON DUPLICATE KEY UPDATE）")
    parser.add_argument("--log-level", dest="log_level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    result = run(
        start_ds=args.start_ds,
        end_ds=args.end_ds,
        backfill_days=args.backfill_days,
        batch_size=args.batch_size,
        purge_window=args.purge_window,
    )
    print(result)


if __name__ == "__main__":
    _cli()
