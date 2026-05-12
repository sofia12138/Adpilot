import { apiFetch } from './api'
import type { DailyOpsRow, DateRange } from '@/types/ops'

interface OpsStatsResponse {
  rows: DailyOpsRow[]
  /** 后端回声请求时使用的付费侧数据源（auto / dwd / polardb） */
  source?: OpsRevenueSource
}

/** 付费侧数据源选择：
 *  - 'auto'    （默认）今日/昨日 LA 走 PolarDB 实时层（30min 刷新），其余日期走 MaxCompute dwd
 *  - 'dwd'     全部走 MaxCompute dwd（老行为，T+1 延迟，已知 T-1 当天数据可能漏 80%）
 *  - 'polardb' 历史走 PolarDB shadow（T+1 + 2h 刷新），今日/昨日仍走实时层
 */
export type OpsRevenueSource = 'auto' | 'dwd' | 'polardb'

/**
 * 运营数据 service 层
 *
 * 调后端 GET /api/ops/daily-stats（需 ops_dashboard 面板权限）
 * 多请求 1 天 baseline（start - 1）让 KPI 卡能算环比
 *
 * source 默认 'auto'，自动用 PolarDB 实时层修复 T-1 当天 dwd 漏数问题；
 * 如需对账或调试可显式传 'dwd' / 'polardb'。
 */
export async function fetchOpsStats(
  range: DateRange,
  source: OpsRevenueSource = 'auto',
): Promise<DailyOpsRow[]> {
  const start = shiftDate(range.start, -1)
  const end = range.end
  const params = new URLSearchParams({ start_date: start, end_date: end, source })
  const resp = await apiFetch<OpsStatsResponse>(`/api/ops/daily-stats?${params}`)
  return resp.rows ?? []
}

// ─── 分时段（每日 × LA 小时）充值趋势 ────────────────────────
export interface HourlyRevenueBucket {
  h: number          // 0~23 LA hour
  orders: number
  payer_uv: number
  total_usd: number
  android_usd: number
  ios_usd: number
  sub_usd: number
  iap_usd: number
}

export interface HourlyRevenueDay {
  ds: string                       // YYYY-MM-DD (LA)
  hours: HourlyRevenueBucket[]     // 长度 24，h 从 0 到 23
}

export interface HourlyRevenueResponse {
  days: string[]
  series: HourlyRevenueDay[]
}

/**
 * 拉取 [start, end] 区间内每日 × 每小时的充值数据（LA 时区）
 *
 * 调后端 GET /api/ops/hourly-revenue（需 ops_dashboard 面板权限）
 * 后端最大区间 31 天
 */
export async function fetchHourlyRevenue(
  startDate: string,
  endDate: string,
): Promise<HourlyRevenueResponse> {
  const params = new URLSearchParams({ start_date: startDate, end_date: endDate })
  return apiFetch<HourlyRevenueResponse>(`/api/ops/hourly-revenue?${params}`)
}


/** 'YYYY-MM-DD' 偏移 N 天，返回 'YYYY-MM-DD'（按本地时区零点） */
function shiftDate(s: string, days: number): string {
  const [y, m, d] = s.split('-').map(Number)
  const dt = new Date(y, (m || 1) - 1, d || 1)
  dt.setDate(dt.getDate() + days)
  const yy = dt.getFullYear()
  const mm = String(dt.getMonth() + 1).padStart(2, '0')
  const dd = String(dt.getDate()).padStart(2, '0')
  return `${yy}-${mm}-${dd}`
}
