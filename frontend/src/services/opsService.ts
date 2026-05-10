import { apiFetch } from './api'
import type { DailyOpsRow, DateRange } from '@/types/ops'

interface OpsStatsResponse {
  rows: DailyOpsRow[]
}

/**
 * 运营数据 service 层
 *
 * 调后端 GET /api/ops/daily-stats（需 ops_dashboard 面板权限）
 * 多请求 1 天 baseline（start - 1）让 KPI 卡能算环比
 */
export async function fetchOpsStats(range: DateRange): Promise<DailyOpsRow[]> {
  const start = shiftDate(range.start, -1)
  const end = range.end
  const params = new URLSearchParams({ start_date: start, end_date: end })
  const resp = await apiFetch<OpsStatsResponse>(`/api/ops/daily-stats?${params}`)
  return resp.rows ?? []
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
