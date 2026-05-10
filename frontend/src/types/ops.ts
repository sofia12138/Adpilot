/**
 * 运营数据面板 — 类型定义
 *
 * 与后端约定：
 *   - 金额字段单位为「分」（cents），渲染时统一转「¥X.XX 万」（÷ 1_000_000）
 *   - 日期字段为 'YYYY-MM-DD' 格式
 *   - new_users / payers 单位为「人」，整数
 *
 * TODO: 当后端接口落地后（GET /api/v1/ops/daily-stats?days={days}）这里类型保持不变
 */
export interface DailyOpsRow {
  /** 统计日期，'YYYY-MM-DD' */
  date: string

  // ── 新注册用户（单位：人） ──
  /** iOS 新注册用户数 */
  ios_new_users: number
  /** Android 新注册用户数 */
  android_new_users: number

  // ── 充值金额（单位：分 / cents） ──
  /** iOS 订阅充值 */
  ios_sub_revenue: number
  /** iOS 普通充值 */
  ios_onetime_revenue: number
  /** Android 订阅充值 */
  android_sub_revenue: number
  /** Android 普通充值 */
  android_onetime_revenue: number

  // ── 付费人数（单位：人） ──
  ios_sub_payers: number
  ios_onetime_payers: number
  android_sub_payers: number
  android_onetime_payers: number
}

/** 时间范围筛选 — 7/14/30 天 */
export type DateRange = 7 | 14 | 30
