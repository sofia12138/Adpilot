import { apiFetch } from './api'

/* ═══ Types ═══ */

export interface LandingPageAsset {
  id: number
  org_id: string
  name: string
  landing_page_url: string
  product_name: string
  channel: string
  language: string
  region_tags: string[]
  remark: string
  status: string
  usage_count: number
  last_used_at: string | null
  created_by: string
  created_at: string
  updated_at: string
}

export interface CopyPackAsset {
  id: number
  org_id: string
  name: string
  primary_text: string
  headline: string
  description: string
  language: string
  product_name: string
  channel: string
  country_tags: string[]
  theme_tags: string[]
  remark: string
  status: string
  usage_count: number
  last_used_at: string | null
  created_by: string
  created_at: string
  updated_at: string
}

export interface RegionGroupAsset {
  id: number
  org_id: string
  name: string
  country_codes: string[]
  country_count: number
  language_hint: string
  remark: string
  status: string
  usage_count: number
  last_used_at: string | null
  created_by: string
  created_at: string
  updated_at: string
}

/* ═══ Landing Pages ═══ */

export async function fetchLandingPages(params?: { status?: string; keyword?: string }) {
  const q = new URLSearchParams()
  if (params?.status) q.set('status', params.status)
  if (params?.keyword) q.set('keyword', params.keyword)
  const qs = q.toString()
  const r = await apiFetch<{ data: LandingPageAsset[] }>(`/api/ad-assets/landing-pages${qs ? '?' + qs : ''}`)
  return r.data
}

export async function createLandingPage(body: Partial<LandingPageAsset>) {
  const r = await apiFetch<{ data: LandingPageAsset }>('/api/ad-assets/landing-pages', {
    method: 'POST', body: JSON.stringify(body),
  })
  return r.data
}

export async function updateLandingPage(id: number, body: Partial<LandingPageAsset>) {
  const r = await apiFetch<{ data: LandingPageAsset }>(`/api/ad-assets/landing-pages/${id}`, {
    method: 'PUT', body: JSON.stringify(body),
  })
  return r.data
}

export async function deleteLandingPage(id: number) {
  await apiFetch<{ ok: boolean }>(`/api/ad-assets/landing-pages/${id}`, { method: 'DELETE' })
}

export async function toggleLandingPage(id: number) {
  const r = await apiFetch<{ data: LandingPageAsset }>(`/api/ad-assets/landing-pages/${id}/toggle`, { method: 'POST' })
  return r.data
}

/* ═══ Copy Packs ═══ */

export async function fetchCopyPacks(params?: { status?: string; keyword?: string }) {
  const q = new URLSearchParams()
  if (params?.status) q.set('status', params.status)
  if (params?.keyword) q.set('keyword', params.keyword)
  const qs = q.toString()
  const r = await apiFetch<{ data: CopyPackAsset[] }>(`/api/ad-assets/copy-packs${qs ? '?' + qs : ''}`)
  return r.data
}

export async function createCopyPack(body: Partial<CopyPackAsset>) {
  const r = await apiFetch<{ data: CopyPackAsset }>('/api/ad-assets/copy-packs', {
    method: 'POST', body: JSON.stringify(body),
  })
  return r.data
}

export async function updateCopyPack(id: number, body: Partial<CopyPackAsset>) {
  const r = await apiFetch<{ data: CopyPackAsset }>(`/api/ad-assets/copy-packs/${id}`, {
    method: 'PUT', body: JSON.stringify(body),
  })
  return r.data
}

export async function deleteCopyPack(id: number) {
  await apiFetch<{ ok: boolean }>(`/api/ad-assets/copy-packs/${id}`, { method: 'DELETE' })
}

export async function toggleCopyPack(id: number) {
  const r = await apiFetch<{ data: CopyPackAsset }>(`/api/ad-assets/copy-packs/${id}/toggle`, { method: 'POST' })
  return r.data
}

/* ═══ Region Groups ═══ */

export async function fetchRegionGroups(params?: { status?: string; keyword?: string }) {
  const q = new URLSearchParams()
  if (params?.status) q.set('status', params.status)
  if (params?.keyword) q.set('keyword', params.keyword)
  const qs = q.toString()
  const r = await apiFetch<{ data: RegionGroupAsset[] }>(`/api/ad-assets/region-groups${qs ? '?' + qs : ''}`)
  return r.data
}

export async function createRegionGroup(body: Partial<RegionGroupAsset>) {
  const r = await apiFetch<{ data: RegionGroupAsset }>('/api/ad-assets/region-groups', {
    method: 'POST', body: JSON.stringify(body),
  })
  return r.data
}

export async function updateRegionGroup(id: number, body: Partial<RegionGroupAsset>) {
  const r = await apiFetch<{ data: RegionGroupAsset }>(`/api/ad-assets/region-groups/${id}`, {
    method: 'PUT', body: JSON.stringify(body),
  })
  return r.data
}

export async function deleteRegionGroup(id: number) {
  await apiFetch<{ ok: boolean }>(`/api/ad-assets/region-groups/${id}`, { method: 'DELETE' })
}

export async function toggleRegionGroup(id: number) {
  const r = await apiFetch<{ data: RegionGroupAsset }>(`/api/ad-assets/region-groups/${id}/toggle`, { method: 'POST' })
  return r.data
}
