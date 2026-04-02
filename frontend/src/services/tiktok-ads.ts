import { apiFetch } from './api'

function qs(params: Record<string, string | number | undefined>): string {
  const sp = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') sp.set(k, String(v))
  }
  const s = sp.toString()
  return s ? `?${s}` : ''
}

export function updateTikTokAdStatus(
  adIds: string[],
  status: string,
  advertiserId?: string,
): Promise<{ message: string; data: unknown }> {
  return apiFetch(`/api/ads/status${qs({ status, advertiser_id: advertiserId })}`, {
    method: 'POST',
    body: JSON.stringify(adIds),
  })
}
