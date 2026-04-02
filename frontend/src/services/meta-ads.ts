import { apiFetch } from './api'

function qs(params: Record<string, string | number | undefined>): string {
  const sp = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') sp.set(k, String(v))
  }
  const s = sp.toString()
  return s ? `?${s}` : ''
}

export function updateMetaAdStatus(
  adId: string,
  status: string,
  adAccountId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(
    `/api/meta/ads/${adId}/status${qs({ status, ad_account_id: adAccountId })}`,
    { method: 'POST' },
  )
}
