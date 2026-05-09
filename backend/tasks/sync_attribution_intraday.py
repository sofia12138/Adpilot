"""sync_attribution_intraday.py — 当日实时归因同步 (CK 版)

数据流：
    metis.dwd_media_stats_rt  + metis.dwd_invest_recharge_rt   (ClickHouse)
        ↓ 走 DMS Enterprise OpenAPI ExecuteScript (dbId=79572320)
        ↓ 默认窗口 = LA 今天 ~ LA 昨天（D0 实时）
    adpilot_biz.biz_attribution_ad_intraday (MySQL)

关键约束：
- CK 端 ReplicatedReplacingMergeTree 用 FINAL 自动去重（小数据量 OK）
- spend 在 CK 端已经是 USD 数值（不是美分），原样写入 BIZ
- recharge_amount 在 CK 端是美分 BIGINT，必须 / 100 转 USD
- pay_time 是 DateTime（按 UTC 解释），用 toDate(pay_time, 'America/Los_Angeles')
  派生 LA cohort 日，再和 dwd_media_stats_rt.stat_time_day（账户日 ≈ LA 日）做 JOIN
- dwd_invest_recharge_rt.ds 是 UTC 日，预过滤窗口比 LA 窗口宽 1 天兜底跨日
- 默认窗口 [LA 昨天, LA 今天]，30min 自动刷新
- 历史数据永远走 daily 任务（cohort 累计 ROI 在 daily 表），本任务**只刷新 D0**

CLI 用法：
    python -m tasks.sync_attribution_intraday                        # 默认窗口
    python -m tasks.sync_attribution_intraday --backfill 7           # 回填最近 7 天 D0
    python -m tasks.sync_attribution_intraday --start-ds 2026-05-04 --end-ds 2026-05-08

依赖：alibabacloud-dms-enterprise20181101 + pymysql（已装）
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from config import get_settings
from integrations.dms_client import (
    DmsAuthError,
    DmsError,
    DmsSqlError,
    get_default_client,
)
from repositories import biz_attribution_ad_intraday_repository as repo
from repositories import biz_sync_log_repository as sync_log_repo

logger = logging.getLogger(__name__)

LA_TZ = ZoneInfo("America/Los_Angeles")
TASK_NAME = "sync_attribution_intraday"

# 默认每次同步的窗口大小（LA 日）
DEFAULT_WINDOW_DAYS = 2


# ─────────────────────────────────────────────────────────────
#  CK SQL 模板
# ─────────────────────────────────────────────────────────────
# 占位符 {la_start} / {la_end} 是 LA 日（YYYY-MM-DD），dwd_media_stats_rt 直接用；
# {utc_lo} / {utc_hi} 是 UTC 日预过滤窗口（比 LA 窗口宽 1 天，兜底跨日）。
# 注意：
#   - dwd_media_stats_rt.spend 已是 USD（不需要 /100）
#   - dwd_invest_recharge_rt.recharge_amount 是美分（要 /100）
#   - FINAL 是 ReplacingMergeTree 的强制去重，小数据量可接受
_CK_SQL_TEMPLATE = """
WITH media AS (
    SELECT
        platform,
        advertiser_id                        AS account_id,
        ad_id,
        stat_time_day                        AS ds_account_local,
        any(currency)                        AS currency,
        any(timezone)                        AS account_timezone,
        sum(spend)                           AS spend,
        sum(impressions)                     AS impressions,
        sum(clicks)                          AS clicks,
        sum(inline_link_clicks)              AS inline_link_clicks,
        sum(reach)                           AS reach,
        sum(conversion)                      AS conversion,
        sum(install)                         AS install,
        sum(activation)                      AS activation,
        sum(registration)                    AS registration,
        sum(purchase)                        AS purchase,
        sum(landing_page_view)               AS landing_page_view,
        sum(video_play_actions)              AS video_play_actions,
        toUnixTimestamp64Milli(max(updated_at)) AS upstream_max_updated_at_ms
    FROM metis.dwd_media_stats_rt FINAL
    WHERE stat_time_day >= toDate('{la_start}')
      AND stat_time_day <= toDate('{la_end}')
    GROUP BY platform, advertiser_id, ad_id, stat_time_day
),
recharge AS (
    SELECT
        ad_id,
        toDate(pay_time, 'America/Los_Angeles')                          AS ds_la,
        sumIf(1, order_type = 'purchase'  AND first_inapp     = 1)       AS first_iap_count,
        sumIf(recharge_amount, order_type = 'purchase'  AND first_inapp     = 1) / 100.0 AS first_iap_amount,
        sumIf(1, order_type = 'subscribe' AND first_subscribe = 1)       AS first_sub_count,
        sumIf(recharge_amount, order_type = 'subscribe' AND first_subscribe = 1) / 100.0 AS first_sub_amount,
        sum(recharge_amount) / 100.0                                     AS total_recharge_amount
    FROM metis.dwd_invest_recharge_rt FINAL
    WHERE ds >= '{utc_lo}' AND ds <= '{utc_hi}'
      AND toDate(pay_time, 'America/Los_Angeles') >= toDate('{la_start}')
      AND toDate(pay_time, 'America/Los_Angeles') <= toDate('{la_end}')
    GROUP BY ad_id, ds_la
)
SELECT
    m.platform                                AS platform,
    m.account_id                              AS account_id,
    m.ad_id                                   AS ad_id,
    m.ds_account_local                        AS ds_account_local,
    m.currency                                AS currency,
    m.account_timezone                        AS account_timezone,
    m.spend                                   AS spend,
    m.impressions                             AS impressions,
    m.clicks                                  AS clicks,
    m.inline_link_clicks                      AS inline_link_clicks,
    m.reach                                   AS reach,
    m.conversion                              AS conversion,
    m.install                                 AS install,
    m.activation                              AS activation,
    m.registration                            AS registration,
    m.purchase                                AS purchase,
    m.landing_page_view                       AS landing_page_view,
    m.video_play_actions                      AS video_play_actions,
    m.upstream_max_updated_at_ms              AS upstream_max_updated_at_ms,
    ifNull(r.first_iap_count, 0)              AS first_iap_count,
    ifNull(r.first_iap_amount, 0.0)           AS first_iap_amount,
    ifNull(r.first_sub_count, 0)              AS first_sub_count,
    ifNull(r.first_sub_amount, 0.0)           AS first_sub_amount,
    ifNull(r.total_recharge_amount, 0.0)      AS total_recharge_amount
FROM media AS m
LEFT JOIN recharge AS r
    ON m.ad_id = r.ad_id
    AND m.ds_account_local = r.ds_la
ORDER BY m.ds_account_local DESC, m.spend DESC
""".strip()


# ─────────────────────────────────────────────────────────────
#  字段转换
# ─────────────────────────────────────────────────────────────
def _to_int(v) -> int:
    if v is None or v == "":
        return 0
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _to_float(v) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return round(float(v), 4)
    except (TypeError, ValueError):
        return 0.0


def _to_str(v, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def _normalize_row(rec: dict) -> dict:
    """把 CK 返回的字符串 dict 标准化为 BIZ 表 upsert 用的 dict"""
    ds_account_local = _to_str(rec.get("ds_account_local"))[:10]
    return {
        "ds_account_local": ds_account_local,
        "ds_la":            ds_account_local,    # 账户都是 LA/Phoenix，等价
        "account_timezone": _to_str(rec.get("account_timezone")),
        "currency":         _to_str(rec.get("currency")),
        "platform":         _to_str(rec.get("platform")),
        "account_id":       _to_str(rec.get("account_id")),
        "ad_id":            _to_str(rec.get("ad_id")),
        "country":          "",                  # 我们已 SUM 跨 country，不再保留维度
        "spend":             _to_float(rec.get("spend")),
        "impressions":       _to_int(rec.get("impressions")),
        "clicks":            _to_int(rec.get("clicks")),
        "inline_link_clicks":_to_int(rec.get("inline_link_clicks")),
        "reach":             _to_int(rec.get("reach")),
        "landing_page_view": _to_int(rec.get("landing_page_view")),
        "conversion":        _to_int(rec.get("conversion")),
        "install":           _to_int(rec.get("install")),
        "activation":        _to_int(rec.get("activation")),
        "registration":      _to_int(rec.get("registration")),
        "purchase":          _to_int(rec.get("purchase")),
        "video_play_actions":_to_int(rec.get("video_play_actions")),
        "first_iap_count":   _to_int(rec.get("first_iap_count")),
        "first_iap_amount":  _to_float(rec.get("first_iap_amount")),
        "first_sub_count":   _to_int(rec.get("first_sub_count")),
        "first_sub_amount":  _to_float(rec.get("first_sub_amount")),
        "total_recharge_amount": _to_float(rec.get("total_recharge_amount")),
        "upstream_max_updated_at_ms": _to_int(rec.get("upstream_max_updated_at_ms")),
    }


# ─────────────────────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────────────────────
def _today_la() -> date:
    return datetime.now(LA_TZ).date()


def _resolve_window(
    start_ds: Optional[str],
    end_ds: Optional[str],
    backfill_days: Optional[int],
) -> tuple[date, date]:
    """返回 (la_start, la_end)；优先级 explicit > backfill > 默认窗口"""
    if start_ds and end_ds:
        return (
            datetime.strptime(start_ds, "%Y-%m-%d").date(),
            datetime.strptime(end_ds, "%Y-%m-%d").date(),
        )
    today = _today_la()
    if backfill_days is not None and backfill_days >= 0:
        return today - timedelta(days=backfill_days), today
    # 默认窗口：今天 + 昨天
    return today - timedelta(days=DEFAULT_WINDOW_DAYS - 1), today


def build_sql(la_start: date, la_end: date) -> str:
    """根据 LA 窗口构造 CK SQL（含 UTC ds 预过滤兜底）"""
    # UTC ds 预过滤：LA 窗口在 UTC 上最多偏 8 小时，兜底各 ±1 天
    utc_lo = (la_start - timedelta(days=1)).strftime("%Y-%m-%d")
    utc_hi = (la_end + timedelta(days=1)).strftime("%Y-%m-%d")
    return _CK_SQL_TEMPLATE.format(
        la_start=la_start.strftime("%Y-%m-%d"),
        la_end=la_end.strftime("%Y-%m-%d"),
        utc_lo=utc_lo,
        utc_hi=utc_hi,
    )


def run(
    *,
    start_ds: Optional[str] = None,
    end_ds: Optional[str] = None,
    backfill_days: Optional[int] = None,
    purge_window: bool = False,
) -> dict:
    """同步主入口。

    参数：
      start_ds / end_ds: 显式 LA 窗口（YYYY-MM-DD）。优先级最高。
      backfill_days:     未指定窗口时，回刷最近 N 天（含今天 LA），默认 None=2 天
      purge_window:      写入前先 DELETE 窗口数据（默认 false，走 ON DUPLICATE KEY UPDATE）
    """
    settings = get_settings()
    la_start, la_end = _resolve_window(start_ds, end_ds, backfill_days)

    log_id = sync_log_repo.create(
        task_name=TASK_NAME,
        sync_date=la_end.strftime("%Y-%m-%d"),
    )
    started = datetime.now(timezone.utc)
    total_rows = 0

    try:
        sql = build_sql(la_start, la_end)
        logger.info(
            "CK 查询 dbId=%s window=[%s, %s]",
            settings.dms_ck_db_id, la_start, la_end,
        )

        client = get_default_client()
        try:
            res = client.execute(sql, settings.dms_ck_db_id)
        except DmsAuthError as e:
            raise RuntimeError(f"CK 权限不足，请检查 DMS 授权: {e}") from e
        except DmsSqlError as e:
            raise RuntimeError(f"CK SQL 执行失败: {e}") from e
        except DmsError as e:
            raise RuntimeError(f"DMS 调用失败: {e}") from e

        logger.info("CK 返回 rows=%d cols=%d", res.row_count, len(res.columns))

        if purge_window:
            removed = repo.delete_window(
                la_start.strftime("%Y-%m-%d"),
                la_end.strftime("%Y-%m-%d"),
                tz_basis="account_local",
            ) if hasattr(repo, "delete_window") else 0
            if removed:
                logger.info("已清理窗口旧数据 %d 行", removed)

        normalized = [_normalize_row(r) for r in res.rows]
        affected = repo.upsert_batch(normalized)
        total_rows = len(normalized)
        logger.info("upsert_batch 写入 %d 行（rowcount=%d）", total_rows, affected)

        message = (
            f"window=[{la_start}, {la_end}] rows={total_rows} "
            f"upstream_request_id={res.request_id}"
        )
        sync_log_repo.finish(
            log_id, status="success", message=message, rows_affected=total_rows,
        )
        ended = datetime.now(timezone.utc)
        elapsed = (ended - started).total_seconds()
        logger.info(
            "sync_attribution_intraday 完成：%s 耗时 %.1fs", message, elapsed,
        )
        return {
            "status": "success",
            "la_start": la_start.strftime("%Y-%m-%d"),
            "la_end":   la_end.strftime("%Y-%m-%d"),
            "rows": total_rows,
            "elapsed_sec": elapsed,
            "request_id": res.request_id,
        }
    except Exception as e:
        logger.exception("sync_attribution_intraday 失败: %s", e)
        sync_log_repo.finish(
            log_id, status="failed", message=str(e), rows_affected=total_rows,
        )
        raise


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────
def _cli():
    parser = argparse.ArgumentParser(
        description="Sync metis.dwd_*_rt -> biz_attribution_ad_intraday (D0 实时)",
    )
    parser.add_argument(
        "--start-ds", dest="start_ds", default=None,
        help="LA 起始日 YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-ds", dest="end_ds", default=None,
        help="LA 结束日 YYYY-MM-DD",
    )
    parser.add_argument(
        "--backfill", dest="backfill_days", type=int, default=None,
        help="未指定窗口时，回填最近 N 天（默认 2 天）",
    )
    parser.add_argument(
        "--purge", dest="purge_window", action="store_true",
        help="写入前先 DELETE 窗口数据（默认走 ON DUPLICATE KEY UPDATE）",
    )
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
        purge_window=args.purge_window,
    )
    print(result)


if __name__ == "__main__":
    _cli()
