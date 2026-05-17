/**
 * 区域渠道分析子面板 — 类型定义
 *
 * 数据源：
 *   - PolarDB matrix_advertise.channel_user
 *   - PolarDB matrix_order.recharge_order
 *   - MaxCompute metis_dw.dim_user_df
 * 后端聚合表：
 *   - adpilot_biz.biz_ops_region_register_{daily,intraday}
 *   - adpilot_biz.biz_ops_region_revenue_{daily,intraday}
 * 后端接口：GET /api/ops/region-channel/daily-stats?start_date=&end_date=&source=
 *
 * 口径：
 *   - 日期 'YYYY-MM-DD'（LA 时区）
 *   - 金额 USD（已在后端从美分换算）
 *   - region 缺失填 'UNK'
 */
export type ChannelKind = 'organic' | 'tiktok' | 'meta' | 'other'

export type RegionDataSource = 'daily' | 'intraday'

/** 注册侧聚合行（无 OS 维度） */
export interface RegionRegisterRow {
  ds: string
  region: string
  channel_kind: ChannelKind
  register_uv: number
  data_source: RegionDataSource
}

/** 充值侧聚合行（含 OS 拆分） */
export interface RegionRevenueRow {
  ds: string
  region: string
  channel_kind: ChannelKind
  /** 1=Android / 2=iOS */
  os_type: 1 | 2
  payer_uv: number
  order_cnt: number
  revenue_usd: number
  sub_revenue_usd: number
  iap_revenue_usd: number
  data_source: RegionDataSource
}

/** API 响应 */
export interface RegionDailyStatsResponse {
  register_rows: RegionRegisterRow[]
  revenue_rows: RegionRevenueRow[]
  data_source: 'auto' | 'daily' | 'intraday'
}

/** 充值侧 OS 视图选项（前端 Tab） */
export type OsView = 'all' | 'ios' | 'android'

/** 渠道 kind 中文显示 */
export const CHANNEL_KIND_LABELS: Record<ChannelKind, string> = {
  organic: '自然量',
  tiktok: 'TikTok',
  meta: 'Meta',
  other: '其它',
}

/** 渠道 kind 颜色（与 chartColors 协调） */
export const CHANNEL_KIND_COLORS: Record<ChannelKind, string> = {
  organic: '#3BC99A',  // 绿
  tiktok:  '#378ADD',  // 蓝
  meta:    '#7F77DD',  // 紫
  other:   '#94A3B8',  // 灰
}
