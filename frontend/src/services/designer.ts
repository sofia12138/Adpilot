import { apiFetch } from './api'

// ---------------------------------------------------------------------------
// 类型定义
// ---------------------------------------------------------------------------

export interface DesignerSummaryItem {
  designer_name: string
  material_count: number
  total_spend: number
  impressions: number
  clicks: number
  installs: number
  conversions: number
  purchase_value: number
  ctr: number | null
  roas: number | null
}

export interface DesignerMaterialItem {
  ad_id: string
  ad_name: string
  platform: string
  campaign_name: string
  spend: number
  impressions: number
  clicks: number
  installs: number
  registrations: number
  purchase_value: number
  ctr: number | null
  roas: number | null
}

export interface DesignerSummaryParams {
  startDate: string
  endDate: string
  platform?: string
  keyword?: string
}

export interface DesignerMaterialsParams {
  startDate: string
  endDate: string
  designerName: string
  platform?: string
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

interface ApiResp<T> { code: number; message: string; data: T }

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

export async function fetchDesignerSummary(
  params: DesignerSummaryParams,
): Promise<DesignerSummaryItem[]> {
  const r = await apiFetch<ApiResp<DesignerSummaryItem[]>>(
    `/api/designer-performance/summary${qs({
      start_date: params.startDate,
      end_date:   params.endDate,
      platform:   params.platform,
      keyword:    params.keyword,
    })}`,
  )
  return r.data
}

export async function fetchDesignerMaterials(
  params: DesignerMaterialsParams,
): Promise<DesignerMaterialItem[]> {
  const r = await apiFetch<ApiResp<DesignerMaterialItem[]>>(
    `/api/designer-performance/materials${qs({
      start_date:    params.startDate,
      end_date:      params.endDate,
      designer_name: params.designerName,
      platform:      params.platform,
    })}`,
  )
  return r.data
}
