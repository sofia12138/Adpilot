/**
 * drama.ts — 剧级分析 API 服务
 *
 * 对应后端 /api/drama/* 三个接口：
 *   fetchDramaSummary      — 剧级总览（按 content_key 聚合）
 *   fetchLocaleBreakdown   — 语言版本明细（按 language_code 聚合）
 *   fetchDramaTrend        — 按天趋势
 *   triggerDramaSync       — 手动触发剧级数据同步
 */

// ─────────────────────────────────────────────────────────────
// 类型定义
// ─────────────────────────────────────────────────────────────

export interface DramaSummaryRow {
  content_key: string
  drama_id: string
  drama_type: string
  localized_drama_name: string
  spend: number
  impressions: number
  clicks: number
  installs: number
  registrations: number
  purchase_value: number
  ctr: number
  cpc: number
  roas: number
  language_count: number
}

export interface DramaSummaryResponse {
  total: number
  page: number
  page_size: number
  rows: DramaSummaryRow[]
}

export interface LocaleBreakdownRow {
  language_code: string
  localized_drama_name: string
  spend: number
  clicks: number
  installs: number
  registrations: number
  purchase_value: number
  roas: number
}

export interface DramaTrendRow {
  stat_date: string
  spend: number
  clicks: number
  installs: number
  registrations: number
  purchase_value: number
}

export interface DramaSyncResult {
  status: string
  mapping_upserted: number
  fact_upserted: number
  failed_count: number
}

// ─────────────────────────────────────────────────────────────
// 通用筛选参数
// ─────────────────────────────────────────────────────────────

export interface DramaBaseFilter {
  startDate: string
  endDate: string
  sourceType?: string
  platform?: string
  channel?: string
  country?: string
}

export interface DramaSummaryFilter extends DramaBaseFilter {
  keyword?: string
  languageCode?: string
  page?: number
  pageSize?: number
}

export interface LocaleBreakdownFilter extends DramaBaseFilter {
  contentKey?: string
  dramaId?: string
}

export interface DramaTrendFilter extends DramaBaseFilter {
  contentKey?: string
  languageCode?: string
}

// ─────────────────────────────────────────────────────────────
// API 函数
// ─────────────────────────────────────────────────────────────

import { apiFetch } from './api'

function buildParams(obj: Record<string, string | number | undefined | null>): string {
  const p = new URLSearchParams()
  for (const [k, v] of Object.entries(obj)) {
    if (v !== undefined && v !== null && v !== '') {
      p.set(k, String(v))
    }
  }
  return p.toString()
}

/**
 * 剧级总览
 *
 * keyword 只匹配 localized_drama_name，不会匹配 remark_raw（由后端保证）。
 */
export async function fetchDramaSummary(
  filter: DramaSummaryFilter
): Promise<DramaSummaryResponse> {
  const q = buildParams({
    start_date: filter.startDate,
    end_date: filter.endDate,
    source_type: filter.sourceType,
    platform: filter.platform,
    channel: filter.channel,
    country: filter.country,
    keyword: filter.keyword,
    language_code: filter.languageCode,
    page: filter.page ?? 1,
    page_size: filter.pageSize ?? 50,
  })
  return apiFetch(`/api/drama/summary?${q}`)
}

/**
 * 语言版本明细（展开行使用）
 */
export async function fetchLocaleBreakdown(
  filter: LocaleBreakdownFilter
): Promise<{ rows: LocaleBreakdownRow[] }> {
  const q = buildParams({
    start_date: filter.startDate,
    end_date: filter.endDate,
    content_key: filter.contentKey,
    drama_id: filter.dramaId,
    source_type: filter.sourceType,
    platform: filter.platform,
    channel: filter.channel,
    country: filter.country,
  })
  return apiFetch(`/api/drama/locale-breakdown?${q}`)
}

/**
 * 按天趋势
 */
export async function fetchDramaTrend(
  filter: DramaTrendFilter
): Promise<{ rows: DramaTrendRow[] }> {
  const q = buildParams({
    start_date: filter.startDate,
    end_date: filter.endDate,
    content_key: filter.contentKey,
    language_code: filter.languageCode,
    source_type: filter.sourceType,
    platform: filter.platform,
    channel: filter.channel,
    country: filter.country,
  })
  return apiFetch(`/api/drama/trend?${q}`)
}

/**
 * 手动触发剧级数据同步
 */
export async function triggerDramaSync(
  startDate: string,
  endDate: string
): Promise<DramaSyncResult> {
  const q = buildParams({ start_date: startDate, end_date: endDate })
  return apiFetch(`/api/drama/sync?${q}`, { method: 'POST' })
}
