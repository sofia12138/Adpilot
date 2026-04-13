import { apiFetch } from './api'

export type SyncModule = 'structure' | 'reports' | 'returned'

export interface ModuleSyncStatus {
  is_running: boolean
  last_synced_at: string | null   // ISO 8601
  last_error: string | null
  last_range: string | null
}

/** 全模块状态（/api/sync/status 返回） */
export type AllSyncStatus = Record<SyncModule, ModuleSyncStatus>

export async function fetchSyncStatus(): Promise<AllSyncStatus> {
  const res = await apiFetch<{ code: number; data: AllSyncStatus }>('/api/sync/status')
  return res.data
}

export async function fetchModuleSyncStatus(module: SyncModule): Promise<ModuleSyncStatus> {
  const res = await apiFetch<{ code: number; data: ModuleSyncStatus }>(`/api/sync/status/${module}`)
  return res.data
}

export async function triggerSync(days = 2): Promise<{ message: string }> {
  const res = await apiFetch<{ code: number; message: string }>(
    `/api/sync/trigger?days=${days}`,
    { method: 'POST' },
  )
  return { message: res.message }
}

export async function triggerModuleSync(module: SyncModule | 'all', days = 2): Promise<{ message: string }> {
  const res = await apiFetch<{ code: number; message: string }>(
    `/api/sync/trigger/${module}?days=${days}`,
    { method: 'POST' },
  )
  return { message: res.message }
}
