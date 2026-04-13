import { apiFetch } from './api'

// ---------------------------------------------------------------------------
// 类型定义
// ---------------------------------------------------------------------------

export interface OptimizerSummaryItem {
  optimizer_name: string
  total_spend: number
  spend_share: number
  avg_daily_spend: number
  active_days: number
  campaign_count: number
  impressions: number
  clicks: number
  installs: number
  registrations: number
  purchase_value: number
  roas: number | null
}

export interface OptimizerSummaryMeta {
  grand_total_spend: number
  unidentified_spend: number
  unidentified_ratio: number
}

export interface OptimizerSummaryResponse {
  rows: OptimizerSummaryItem[]
  meta: OptimizerSummaryMeta
}

export interface OptimizerDetailItem {
  campaign_id: string
  campaign_name: string
  platform: string
  account_id: string
  match_source: string
  match_confidence: number | null
  match_position: string
  spend: number
  impressions: number
  clicks: number
  installs: number
  purchase_value: number
  active_days: number
  roas: number | null
}

export interface MatchDistributionItem {
  match_source: string
  match_source_label: string
  campaign_count: number
  total_spend: number
}

export interface OptimizerSummaryParams {
  startDate: string
  endDate: string
  platform?: string
  sourceType?: string
  keyword?: string
}

export interface OptimizerDetailParams {
  startDate: string
  endDate: string
  optimizerName: string
  platform?: string
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

interface ApiResp<T> { code: number; message: string; data: T; meta?: OptimizerSummaryMeta }

function qs(params: Record<string, string | number | undefined | null>): string {
  const parts: string[] = []
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== '') parts.push(`${k}=${encodeURIComponent(v)}`)
  }
  return parts.length ? `?${parts.join('&')}` : ''
}

// ---------------------------------------------------------------------------
// API 函数
// ---------------------------------------------------------------------------

export async function fetchOptimizerSummary(
  params: OptimizerSummaryParams,
): Promise<OptimizerSummaryResponse> {
  const r = await apiFetch<ApiResp<OptimizerSummaryItem[]>>(
    `/api/optimizer-performance/summary${qs({
      start_date:  params.startDate,
      end_date:    params.endDate,
      platform:    params.platform,
      source_type: params.sourceType,
      keyword:     params.keyword,
    })}`,
  )
  return {
    rows: r.data,
    meta: r.meta ?? { grand_total_spend: 0, unidentified_spend: 0, unidentified_ratio: 0 },
  }
}

export async function fetchOptimizerDetail(
  params: OptimizerDetailParams,
): Promise<OptimizerDetailItem[]> {
  const r = await apiFetch<ApiResp<OptimizerDetailItem[]>>(
    `/api/optimizer-performance/detail${qs({
      start_date:     params.startDate,
      end_date:       params.endDate,
      optimizer_name: params.optimizerName,
      platform:       params.platform,
    })}`,
  )
  return r.data
}

export async function fetchMatchDistribution(
  params: { startDate: string; endDate: string; platform?: string },
): Promise<MatchDistributionItem[]> {
  const r = await apiFetch<ApiResp<MatchDistributionItem[]>>(
    `/api/optimizer-performance/match-distribution${qs({
      start_date: params.startDate,
      end_date:   params.endDate,
      platform:   params.platform,
    })}`,
  )
  return r.data
}

export async function triggerOptimizerSync(): Promise<unknown> {
  const r = await apiFetch<ApiResp<unknown>>(
    '/api/optimizer-performance/sync',
    { method: 'POST' },
  )
  return r.data
}
