import { apiFetch } from './api'

// ─── Types ───────────────────────────────────────────────

export interface MetaCampaign {
  id: string
  name: string
  status?: string
  objective?: string
  daily_budget?: number
  [key: string]: unknown
}

export interface MetaCampaignNormalized {
  id: string
  name: string
  platform: string
  status: string
  objective: string
  budget: number
  [key: string]: unknown
}

export interface MetaCampaignListResponse {
  data: MetaCampaign[]
  normalized: MetaCampaignNormalized[]
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

export function fetchMetaCampaigns(
  adAccountId?: string,
  limit?: number,
): Promise<MetaCampaignListResponse> {
  return apiFetch<MetaCampaignListResponse>(
    `/api/meta/campaigns/${qs({ ad_account_id: adAccountId, limit })}`,
  )
}

export function updateMetaCampaignStatus(
  campaignId: string,
  status: string,
  adAccountId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(
    `/api/meta/campaigns/${campaignId}/status${qs({ status, ad_account_id: adAccountId })}`,
    { method: 'POST' },
  )
}
