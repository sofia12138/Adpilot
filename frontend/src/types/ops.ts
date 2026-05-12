/**
 * 运营数据面板 — 类型定义
 *
 * 数据源：MaxCompute metis_dw.{ads_app_di, dwd_recharge_order_df}
 *        → 每日同步到 BIZ.biz_ops_daily
 *        → GET /api/ops/daily-stats?start_date=&end_date=
 *
 * 单位约定：
 *   - 金额字段（subscribe_revenue / onetime_revenue）单位 USD（已在后端从美分换算）
 *   - UV / orders 字段为整数
 *   - date 字段为 'YYYY-MM-DD'（LA 时区）
 *
 * OS 拆分约束：
 *   - 用户侧（new_register / new_active / DAU / 留存 / total_payer）当前数仓只有
 *     一个 app_id，无法按 OS 拆分，统一展示全量
 *   - 付费侧（金额 / 订单数 / 付费 UV）按 dwd_recharge_order_df.os_type 完整拆分
 *     Android(=1) → android_*
 *     iOS(=2)     → ios_*
 */
export interface DailyOpsRow {
  date: string

  /**
   * 付费侧数据来源标签：
   *   - 'platform'          来自 biz_ops_daily（MaxCompute dwd，老路径，默认）
   *   - 'intraday'          来自 biz_ops_daily_intraday（PolarDB 30min 实时层，今日/昨日 LA）
   *   - 'polardb'           来自 biz_ops_daily_polardb_shadow（PolarDB T+1，仅 source=polardb 模式）
   *   - 'intraday_fallback' 当日 MC 全量未到，由 CK 归因兜底（已有，偏低，会提示用户）
   * 前端可选展示，便于排障时一眼看出数据走的哪条链路。
   */
  revenue_source?: 'platform' | 'intraday' | 'polardb' | 'intraday_fallback'

  // ── 用户侧（全量，无 OS 拆分） ────────────────────────────
  /** 新注册账号 UV：dim_user_df.register_time_utc 转 LA = ds */
  new_register_uv: number
  /** 新激活 UV：App 首次启动 */
  new_active_uv: number
  /** DAU：当日 app_start UV */
  active_uv: number
  d1_retained_uv: number
  d7_retained_uv: number
  d30_retained_uv: number
  /** 当日充值付费 UV（来自 ads_app_di.recharge_pay_uv，全量不拆 OS） */
  total_payer_uv: number

  // ── 投放侧（全量平台合计；单位 USD） ───────────────────────
  /** 当日广告消耗 USD（全平台合计，来自 biz_attribution_ad_daily.spend SUM） */
  ad_spend: number

  // ── iOS 付费侧 ─────────────────────────────────────────────
  ios_subscribe_revenue: number    // USD
  ios_onetime_revenue: number      // USD
  ios_first_sub_orders: number
  ios_repeat_sub_orders: number
  ios_first_iap_orders: number
  ios_repeat_iap_orders: number
  ios_payer_uv: number

  // ── Android 付费侧 ────────────────────────────────────────
  android_subscribe_revenue: number
  android_onetime_revenue: number
  android_first_sub_orders: number
  android_repeat_sub_orders: number
  android_first_iap_orders: number
  android_repeat_iap_orders: number
  android_payer_uv: number
}

/** 时间范围 preset — UI 上的快捷按钮 */
export type DatePreset = 'yesterday' | 'today' | 'last7' | 'last14' | 'last30' | 'custom'

/**
 * 时间范围筛选 — 统一以「起止日期」为底层概念
 *   start / end 均为 'YYYY-MM-DD'，闭区间（含 start、含 end）
 *   preset 仅用于 UI 高亮当前按钮 / KPI 卡标题文案
 */
export interface DateRange {
  preset: DatePreset
  start: string
  end: string
}
