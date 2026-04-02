import { apiFetch } from './api'

// ─── Types ───────────────────────────────────────────────

export interface TikTokReportParams {
  startDate: string
  endDate: string
  advertiserId?: string
  page?: number
  pageSize?: number
}

export interface TikTokReportResponse {
  data: Record<string, unknown>[]
}

// ─── API ─────────────────────────────────────────────────

function buildQs(p: TikTokReportParams): string {
  const sp = new URLSearchParams()
  sp.set('start_date', p.startDate)
  sp.set('end_date', p.endDate)
  if (p.advertiserId) sp.set('advertiser_id', p.advertiserId)
  if (p.page !== undefined) sp.set('page', String(p.page))
  if (p.pageSize !== undefined) sp.set('page_size', String(p.pageSize))
  return `?${sp.toString()}`
}

export function fetchTikTokCampaignReport(
  params: TikTokReportParams,
): Promise<TikTokReportResponse> {
  return apiFetch<TikTokReportResponse>(`/api/reports/campaign${buildQs(params)}`)
}

export function fetchTikTokAdGroupReport(
  params: TikTokReportParams,
): Promise<TikTokReportResponse> {
  return apiFetch<TikTokReportResponse>(`/api/reports/adgroup${buildQs(params)}`)
}

export function fetchTikTokAdReport(
  params: TikTokReportParams,
): Promise<TikTokReportResponse> {
  return apiFetch<TikTokReportResponse>(`/api/reports/ad${buildQs(params)}`)
}
