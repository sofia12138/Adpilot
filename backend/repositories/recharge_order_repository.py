"""matrix_order.recharge_order 查询层（PolarDB，只读）

提供「按 LA 日 × os_type 聚合付费侧指标」的核心 SQL，由两个同步任务复用：
    - tasks/sync_ops_polardb_daily      （T+1 全量回填 30 天 → 影子表）
    - tasks/sync_ops_polardb_intraday   （30 分钟刷新今日+昨日 → 实时表）

口径与 dwd_recharge_order_df 保持一致：
    - 仅统计 order_status = 1（已支付），不扣 refund_amount
    - is_subscribe IN (0, -1) 归 IAP（业务库实际只有 -1 / 1，0 兼容兜底）
    - is_subscribe = 1 归订阅
    - first_subscribe = 1 → 首订；first_inapp = 1 → 首购
    - 金额 pay_amount 单位是分，/100 转 USD
    - 时区：PolarDB 服务器 +08:00（北京），CONVERT_TZ 转 'America/Los_Angeles' 切 LA 日

性能：
    - WHERE 用 created_at 范围筛走 idx_created_at；CONVERT_TZ 仅在 SELECT/HAVING 处理
    - UTC 窗口向外扩 1 天（[bj_lo-1, bj_hi+1]）防止 LA 跨日订单丢失
    - 当前表 ~1.3k 行，30 天查询 200ms 内；后续表量上 10w 行需加 (order_status, created_at) 复合索引
"""
from __future__ import annotations

from datetime import datetime, timedelta

from db import get_order_conn

# ─────────────────────────────────────────────────────────────
#  核心聚合 SQL
# ─────────────────────────────────────────────────────────────

# 注意：
#   1. WHERE created_at 用 BJ 时间区间，走索引；ds_la 是派生列，放在 HAVING
#   2. app_id=1 当前业务唯一，预防性 hardcode；未来多 app 改为 IN (...)
#   3. is_subscribe IN (0, -1) 兼容 dwd 口径
_PAY_SIDE_AGG_SQL = """
SELECT
    DATE(CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles'))                AS ds,
    os_type                                                                       AS os_type,
    SUM(CASE WHEN is_subscribe = 1                          THEN pay_amount ELSE 0 END) / 100.0  AS subscribe_revenue_usd,
    SUM(CASE WHEN is_subscribe IN (0, -1)                   THEN pay_amount ELSE 0 END) / 100.0  AS onetime_revenue_usd,
    SUM(CASE WHEN first_subscribe = 1                       THEN 1 ELSE 0 END)             AS first_sub_orders,
    SUM(CASE WHEN is_subscribe = 1 AND first_subscribe = 0  THEN 1 ELSE 0 END)             AS repeat_sub_orders,
    SUM(CASE WHEN first_inapp = 1                           THEN 1 ELSE 0 END)             AS first_iap_orders,
    SUM(CASE WHEN is_subscribe IN (0, -1) AND first_inapp = 0 THEN 1 ELSE 0 END)           AS repeat_iap_orders,
    COUNT(DISTINCT user_id)                                                                AS payer_uv,
    MAX(id)                                                                                AS upstream_max_id
FROM recharge_order
WHERE order_status = 1
  AND app_id = 1
  AND os_type IN (1, 2)
  AND created_at >= %s
  AND created_at <  %s
GROUP BY ds, os_type
HAVING ds BETWEEN %s AND %s
ORDER BY ds, os_type
"""


# ─────────────────────────────────────────────────────────────
#  分时段（LA 小时）聚合 SQL
# ─────────────────────────────────────────────────────────────

# 与日级口径完全对齐，仅多拆一个 LA 小时维度。
# 注意：返回结果用 (ds, hour) 复合主键唯一标识，前端按 ds 分组画多日折线
_HOURLY_AGG_SQL = """
SELECT
    DATE(CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles'))   AS ds,
    HOUR(CONVERT_TZ(created_at, '+08:00', 'America/Los_Angeles'))   AS h,
    COUNT(*)                                                         AS orders,
    COUNT(DISTINCT user_id)                                          AS payer_uv,
    SUM(pay_amount) / 100.0                                          AS total_usd,
    SUM(CASE WHEN os_type = 1 THEN pay_amount ELSE 0 END) / 100.0    AS android_usd,
    SUM(CASE WHEN os_type = 2 THEN pay_amount ELSE 0 END) / 100.0    AS ios_usd,
    SUM(CASE WHEN is_subscribe = 1 THEN pay_amount ELSE 0 END) / 100.0           AS sub_usd,
    SUM(CASE WHEN is_subscribe IN (0, -1) THEN pay_amount ELSE 0 END) / 100.0    AS iap_usd
FROM recharge_order
WHERE order_status = 1
  AND app_id = 1
  AND os_type IN (1, 2)
  AND created_at >= %s
  AND created_at <  %s
GROUP BY ds, h
HAVING ds BETWEEN %s AND %s
ORDER BY ds, h
"""


def fetch_hourly_by_la_day(la_lo: str, la_hi: str) -> list[dict]:
    """按 LA 日 × 小时聚合付费侧指标，用于"分时段充值趋势"面板。

    参数：
      la_lo / la_hi: LA 日窗口（含），格式 YYYY-MM-DD

    返回：行级 dict 列表，键
      - ds          (date) LA 日
      - h           (int 0~23) LA 小时
      - orders      (int)
      - payer_uv    (int)
      - total_usd / android_usd / ios_usd / sub_usd / iap_usd (float)

    连接失败时返回空列表。
    """
    la_lo_dt = datetime.strptime(la_lo, "%Y-%m-%d")
    la_hi_dt = datetime.strptime(la_hi, "%Y-%m-%d")
    bj_lo = (la_lo_dt - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    bj_hi = (la_hi_dt + timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")

    with get_order_conn() as conn:
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(_HOURLY_AGG_SQL, (bj_lo, bj_hi, la_lo, la_hi))
        return list(cur.fetchall())


def fetch_pay_side_by_la_day(la_lo: str, la_hi: str) -> list[dict]:
    """按 LA 日 × os_type 聚合付费侧指标。

    参数：
      la_lo / la_hi: LA 日窗口（含），格式 YYYY-MM-DD

    返回：行级 dict 列表，键 = SQL 别名
      - ds            (date)
      - os_type       (int 1/2)
      - subscribe_revenue_usd / onetime_revenue_usd (float)
      - first_sub_orders / repeat_sub_orders / first_iap_orders / repeat_iap_orders (int)
      - payer_uv      (int)
      - upstream_max_id (int)  ← 实时层用作版本号

    连接失败时返回空列表（与 get_order_conn 静默降级语义一致）。
    """
    # BJ 窗口比 LA 窗口向外扩 1 天，覆盖跨日订单
    # LA = BJ - 15h（PDT）或 BJ - 16h（PST），所以 1 天足够
    la_lo_dt = datetime.strptime(la_lo, "%Y-%m-%d")
    la_hi_dt = datetime.strptime(la_hi, "%Y-%m-%d")
    bj_lo = (la_lo_dt - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    # created_at < bj_hi 是开区间，故 +2 天上界
    bj_hi = (la_hi_dt + timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")

    with get_order_conn() as conn:
        if conn is None:
            return []
        cur = conn.cursor()
        cur.execute(_PAY_SIDE_AGG_SQL, (bj_lo, bj_hi, la_lo, la_hi))
        return list(cur.fetchall())
