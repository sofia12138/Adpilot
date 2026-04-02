import { apiFetch } from './api'

// ─── Types ───────────────────────────────────────────────

export type MetaReportLevel = 'campaign' | 'adset' | 'ad'

export interface MetaInsightParams {
  startDate: string
  endDate: string
  level?: MetaReportLevel
  adAccountId?: string
  limit?: number
}

export interface MetaInsightResponse {
  data: Record<string, unknown>[]
}

// ─── API ─────────────────────────────────────────────────

export function fetchMetaInsights(
  params: MetaInsightParams,
): Promise<MetaInsightResponse> {
  const sp = new URLSearchParams()
  sp.set('start_date', params.startDate)
  sp.set('end_date', params.endDate)
  if (params.level) sp.set('level', params.level)
  if (params.adAccountId) sp.set('ad_account_id', params.adAccountId)
  if (params.limit !== undefined) sp.set('limit', String(params.limit))
  return apiFetch<MetaInsightResponse>(`/api/meta/reports/insights?${sp.toString()}`)
}
