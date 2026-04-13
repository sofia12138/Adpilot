import { apiFetch } from './api'

// ---------------------------------------------------------------------------
// 类型
// ---------------------------------------------------------------------------

export interface OptimizerDirectoryItem {
  id: number
  optimizer_name: string
  optimizer_code: string
  aliases: string
  is_active: number
  remark: string
  created_at: string
  updated_at: string
}

export interface UnassignedSample {
  optimizer_name_raw: string
  occurrence_count: number
  total_spend: number
  last_seen_at: string
}

export interface DirectoryCreatePayload {
  optimizer_name: string
  optimizer_code: string
  aliases?: string
  is_active?: number
  remark?: string
}

export interface DirectoryUpdatePayload {
  id: number
  optimizer_name?: string
  optimizer_code?: string
  aliases?: string
  is_active?: number
  remark?: string
}

// ---------------------------------------------------------------------------
// 工具
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
// API
// ---------------------------------------------------------------------------

export async function fetchDirectoryList(params?: {
  keyword?: string
  is_active?: number
}): Promise<OptimizerDirectoryItem[]> {
  const r = await apiFetch<ApiResp<OptimizerDirectoryItem[]>>(
    `/api/optimizer-directory/list${qs({
      keyword: params?.keyword,
      is_active: params?.is_active,
    })}`,
  )
  return r.data
}

export async function createDirectory(body: DirectoryCreatePayload): Promise<{ id: number }> {
  const r = await apiFetch<ApiResp<{ id: number }>>(
    '/api/optimizer-directory/create',
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
  )
  return r.data
}

export async function updateDirectory(body: DirectoryUpdatePayload): Promise<{ affected: number }> {
  const r = await apiFetch<ApiResp<{ affected: number }>>(
    '/api/optimizer-directory/update',
    { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
  )
  return r.data
}

export async function toggleDirectoryStatus(id: number, is_active: number): Promise<{ affected: number }> {
  const r = await apiFetch<ApiResp<{ affected: number }>>(
    '/api/optimizer-directory/toggle-status',
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, is_active }) },
  )
  return r.data
}

export async function deleteDirectory(id: number): Promise<{ deleted: number }> {
  const r = await apiFetch<ApiResp<{ deleted: number }>>(
    `/api/optimizer-directory/delete?id=${id}`,
    { method: 'DELETE' },
  )
  return r.data
}

export async function fetchUnassignedSamples(limit = 200): Promise<UnassignedSample[]> {
  const r = await apiFetch<ApiResp<UnassignedSample[]>>(
    `/api/optimizer-directory/unassigned-samples?limit=${limit}`,
  )
  return r.data
}

export async function assignSample(optimizer_name_raw: string, optimizer_id: number): Promise<{ affected: number }> {
  const r = await apiFetch<ApiResp<{ affected: number }>>(
    '/api/optimizer-directory/assign-sample',
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ optimizer_name_raw, optimizer_id }) },
  )
  return r.data
}

export async function rebuildMapping(): Promise<unknown> {
  const r = await apiFetch<ApiResp<unknown>>(
    '/api/optimizer-directory/rebuild-mapping',
    { method: 'POST' },
  )
  return r.data
}
