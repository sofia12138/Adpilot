import { apiFetch } from './api'

// ─── Types ───────────────────────────────────────────────

export interface TikTokAdGroup {
  adgroup_id: string
  adgroup_name: string
  campaign_id?: string
  budget?: number
  status?: string
  [key: string]: unknown
}

export interface TikTokAdGroupListResponse {
  data: TikTokAdGroup[] | { list: TikTokAdGroup[] }
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

export function fetchTikTokAdGroups(
  advertiserId?: string,
  campaignId?: string,
  page?: number,
  pageSize?: number,
): Promise<TikTokAdGroupListResponse> {
  return apiFetch<TikTokAdGroupListResponse>(
    `/api/adgroups/${qs({ advertiser_id: advertiserId, campaign_id: campaignId, page, page_size: pageSize })}`,
  )
}

export function updateTikTokAdGroup(
  data: Record<string, unknown>,
  advertiserId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(`/api/adgroups/${qs({ advertiser_id: advertiserId })}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function updateTikTokAdGroupStatus(
  adgroupIds: string[],
  status: string,
  advertiserId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(`/api/adgroups/status${qs({ status, advertiser_id: advertiserId })}`, {
    method: 'POST',
    body: JSON.stringify(adgroupIds),
  })
}
