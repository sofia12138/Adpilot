/**
 * 用户付费面板 — 类型定义
 *
 * 数据源：
 *   - GET /api/ops/users/summary    T+1 用户聚合（biz_user_payment_summary）
 *   - GET /api/ops/users/today      实时（直查 PolarDB）
 *   - GET /api/ops/users/orders     订单明细
 *   - 审批工单：/api/ops/users/anomaly/*
 *
 * 单位约定：金额已转 USD（前端不再除 100）
 */

/** 已知的异常标签 */
export type AnomalyTag =
  | 'suspect_brush'       // 单日下单≥10 且成单率<10%（刷单嫌疑）
  | 'payment_loop'        // 单日下单≥5 且 0 成单（支付失败循环）
  | 'instant_burst'       // 注册后 30 分钟内下单≥5（注册即狂下）
  | 'guest_payer'         // 游客且累计下单≥3（游客高频付费）
  | 'pending_whitelist'   // 工单 pending 中（运行时叠加）
  | 'whitelisted'         // 已加白名单（运行时叠加）

export interface UserPaymentSummaryRow {
  user_id: number
  region: string | null
  /** -1 游客 / 1 google / 2 facebook / 3 apple */
  oauth_platform: number | null
  register_time_utc: string | null
  lang: string | null

  first_channel_id: string
  /** 1 Android / 2 iOS */
  first_os_type: number
  /** 1 ApplePay / 2 GooglePay */
  first_pay_type: number

  total_orders: number
  paid_orders: number
  refund_orders: number
  /** 0~1 */
  success_rate: number

  paid_orders_ios: number
  paid_orders_android: number
  total_gmv_usd_ios: number
  total_gmv_usd_android: number

  paid_orders_subscribe: number
  paid_orders_inapp: number
  total_gmv_usd_subscribe: number
  total_gmv_usd_inapp: number

  total_gmv_usd: number
  attempted_gmv_usd: number
  refund_amount_usd: number

  first_pay_time_utc: string | null
  last_action_time_utc: string | null
  snapshot_ds: string

  anomaly_tags: AnomalyTag[]
}

export interface UserPaymentListResponse {
  total: number
  items: UserPaymentSummaryRow[]
  page: number
  page_size: number
}

export interface UserPaymentOrderRow {
  la_ds: string
  order_id: number
  order_no: string
  user_id: number
  created_at_la: string | null
  pay_time_la: string | null
  /** 0 待支付 / 1 已支付 / 2 全退 / 3 部退 / 4 用户取消 / 5 发起支付失败 / 6 超时 */
  order_status: number
  os_type: number
  pay_type: number
  pay_amount_usd: number
  refund_amount_usd: number
  product_id: string
  is_subscribe: number
  stall_group: number
  channel_id: string
  drama_id: number
  episode_id: number
}

export interface OrderListResponse {
  total: number
  items: UserPaymentOrderRow[]
  page: number
  page_size: number
}

export interface TodayUserRow {
  user_id: number
  first_channel_id: string
  first_os_type: number
  first_pay_type: number
  total_orders: number
  paid_orders: number
  refund_orders: number
  success_rate: number

  paid_orders_ios: number
  paid_orders_android: number
  paid_orders_subscribe: number
  paid_orders_inapp: number

  total_gmv_usd: number
  total_gmv_usd_ios: number
  total_gmv_usd_android: number
  total_gmv_usd_subscribe: number
  total_gmv_usd_inapp: number
  attempted_gmv_usd: number

  first_created_la: string | null
  last_action_la: string | null
  first_pay_la: string | null

  anomaly_tags: AnomalyTag[]
}

export interface TodayResponse {
  la_ds: string
  total_users: number
  items: TodayUserRow[]
  truncated: boolean
  ttl_sec: number
}

export interface UserPaymentKpiBucket {
  total_users: number
  paying_users: number
  try_but_fail_users: number
  guest_paying_users: number
  total_orders: number
  paid_orders: number
  success_rate: number
  total_gmv_usd: number
  attempted_gmv_usd: number
  total_gmv_usd_ios: number
  total_gmv_usd_android: number
  total_gmv_usd_subscribe: number
  total_gmv_usd_inapp: number
  arpu_usd: number
}

export interface UserPaymentKpiResponse {
  raw: UserPaymentKpiBucket
  clean: UserPaymentKpiBucket
  whitelist_count: number
}

export type ApplicationStatus = 'pending' | 'approved' | 'rejected' | 'withdrawn'
export type AnomalyAction = 'add' | 'remove'
export type AnomalyTagKind = 'whitelist' | 'blacklist' | 'internal_test'

export interface AnomalyApplication {
  id: number
  target_user_id: number
  requested_tag: AnomalyTagKind
  action: AnomalyAction
  reason: string
  status: ApplicationStatus
  applicant_user: string
  applied_at: string
  reviewer_user: string | null
  review_note: string
  reviewed_at: string | null
  created_at: string
  updated_at: string
}

export interface ApplicationListResponse {
  items: AnomalyApplication[]
  page: number
  page_size: number
  pending_count: number
}

export interface AnomalyWhitelistRow {
  user_id: number
  tag: AnomalyTagKind
  reason: string
  marked_by: string
  marked_at: string
  application_id: number | null
}

/** 渠道字典条目（channel_id → 元信息） */
export interface ChannelInfo {
  /** 0=其它/自然 1=TikTok 2=投放（其它平台） */
  ad_platform: number | null
  advertiser_id: string | null
  /** 后端预渲染的可读标签：自然量 / TikTok-78-3366 / 投放-100 / … */
  label: string
}

export interface ChannelDictResponse {
  items: Record<string, ChannelInfo>
}

/** 双口径切换 */
export type KpiMode = 'raw' | 'clean'

/** 首单渠道筛选：按平台分类（与字典里的 ad_platform 对应） */
export type ChannelKind = 'organic' | 'tiktok' | 'meta' | 'other'

export interface UserFilterState {
  region?: string
  oauth_platform?: number
  first_channel_id?: string
  channel_kind?: ChannelKind
  first_os_type?: number
  anomaly_tag?: AnomalyTag
  user_id?: number
  min_total_orders?: number
}
