import { apiFetch } from './api'

export interface SyncStatus {
  is_running: boolean
  last_synced_at: string | null   // ISO 8601
  last_error: string | null
  last_range: string | null
}

export async function fetchSyncStatus(): Promise<SyncStatus> {
  const res = await apiFetch<{ code: number; data: SyncStatus }>('/api/sync/status')
  return res.data
}

export async function triggerSync(days = 2): Promise<{ message: string }> {
  const res = await apiFetch<{ code: number; message: string }>(
    `/api/sync/trigger?days=${days}`,
    { method: 'POST' },
  )
  return { message: res.message }
}
