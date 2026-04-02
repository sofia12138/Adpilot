import { apiFetch } from './api'

// ─── Types ───────────────────────────────────────────────

export interface TikTokCampaign {
  campaign_id: string
  campaign_name: string
  objective_type?: string
  budget_mode?: string
  budget?: number
  status?: string
  [key: string]: unknown
}

export interface TikTokCampaignNormalized {
  id: string
  name: string
  platform: string
  status: string
  objective: string
  budget: number
  [key: string]: unknown
}

export interface TikTokCampaignListResponse {
  data: { list: TikTokCampaign[] }
  normalized: TikTokCampaignNormalized[]
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

export function fetchTikTokCampaigns(
  advertiserId?: string,
  page?: number,
  pageSize?: number,
): Promise<TikTokCampaignListResponse> {
  return apiFetch<TikTokCampaignListResponse>(
    `/api/campaigns/${qs({ advertiser_id: advertiserId, page, page_size: pageSize })}`,
  )
}

export function updateTikTokCampaign(
  data: Record<string, unknown>,
  advertiserId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(`/api/campaigns/${qs({ advertiser_id: advertiserId })}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function updateTikTokCampaignStatus(
  campaignIds: string[],
  status: string,
  advertiserId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(`/api/campaigns/status${qs({ status, advertiser_id: advertiserId })}`, {
    method: 'POST',
    body: JSON.stringify(campaignIds),
  })
}
