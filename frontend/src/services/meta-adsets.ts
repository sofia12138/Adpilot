import { apiFetch } from './api'

// ─── Types ───────────────────────────────────────────────

export interface MetaAdSet {
  id: string
  name: string
  status?: string
  campaign_id?: string
  daily_budget?: number
  [key: string]: unknown
}

export interface MetaAdSetListResponse {
  data: MetaAdSet[]
}

// ─── API ─────────────────────────────────────────────────

function qs(params: Record<string, string | number | undefined>): string {
  const sp = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') sp.set(k, String(v))
  }
  const s = sp.toString()
  return s ? `?${s}` : ''
}

export function fetchMetaAdSets(
  adAccountId?: string,
  campaignId?: string,
  limit?: number,
): Promise<MetaAdSetListResponse> {
  return apiFetch<MetaAdSetListResponse>(
    `/api/meta/adsets/${qs({ ad_account_id: adAccountId, campaign_id: campaignId, limit })}`,
  )
}

export function updateMetaAdSetStatus(
  adsetId: string,
  status: string,
  adAccountId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(
    `/api/meta/adsets/${adsetId}/status${qs({ status, ad_account_id: adAccountId })}`,
    { method: 'POST' },
  )
}
