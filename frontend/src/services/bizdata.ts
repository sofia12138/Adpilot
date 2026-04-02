import { apiFetch } from './api'

export interface PrdSummary {
  register_count: number
  first_subscribe_count: number
  first_subscribe_amount: number
  repeat_subscribe_count: number
  repeat_subscribe_amount: number
  first_inapp_count: number
  first_inapp_amount: number
  repeat_inapp_count: number
  repeat_inapp_amount: number
  inapp_total_amount: number
  subscribe_total_amount: number
  recharge_total_amount: number
  ad_cost_amount: number
  day1_roi: number
  day3_roi: number
  day7_roi: number
  day14_roi: number
  day30_roi: number
  [key: string]: number
}

interface DataResp<T> { data: T }

export async function fetchPrdSummary(
  startDate: string,
  endDate: string,
  adPlatform?: number,
): Promise<PrdSummary> {
  const params = new URLSearchParams({ start_date: startDate, end_date: endDate })
  if (adPlatform != null) params.set('ad_platform', String(adPlatform))
  const r = await apiFetch<DataResp<PrdSummary>>(`/api/bizdata/channel_report_summary?${params}`)
  return r.data
}
