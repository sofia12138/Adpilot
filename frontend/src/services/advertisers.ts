import { apiFetch } from './api'

// ─── Types ───────────────────────────────────────────────

export interface Advertiser {
  advertiser_id: string
  advertiser_name: string
  [key: string]: unknown
}

export interface MetaAccount {
  id: string
  name: string
  account_status?: number
  [key: string]: unknown
}

export interface MetaAccountInfo {
  id: string
  name: string
  currency?: string
  timezone_name?: string
  [key: string]: unknown
}

// ─── API ─────────────────────────────────────────────────

export function fetchTikTokAdvertisers(): Promise<{ data: Advertiser[] }> {
  return apiFetch<{ data: Advertiser[] }>('/api/advertisers/')
}

export function fetchMetaAccounts(): Promise<{ data: MetaAccount[] }> {
  return apiFetch<{ data: MetaAccount[] }>('/api/meta/accounts/')
}

export function fetchMetaAccountInfo(
  adAccountId?: string,
): Promise<{ data: MetaAccountInfo }> {
  const sp = new URLSearchParams()
  if (adAccountId) sp.set('ad_account_id', adAccountId)
  const q = sp.toString()
  return apiFetch<{ data: MetaAccountInfo }>(`/api/meta/accounts/info${q ? `?${q}` : ''}`)
}
