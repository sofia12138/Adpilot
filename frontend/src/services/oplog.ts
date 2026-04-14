import { apiFetch } from './api'

export interface OplogEntry {
  id: number
  time: string
  user: string
  action: string
  target: string
  detail: string
  platform: string
  status: string
  before_data?: Record<string, unknown> | null
  after_data?: Record<string, unknown> | null
}

interface OplogResp {
  data: OplogEntry[]
  total: number
}

export async function fetchOplog(page = 1, pageSize = 30): Promise<{ list: OplogEntry[]; total: number }> {
  const r = await apiFetch<OplogResp>(
    `/api/oplog/?page=${page}&page_size=${pageSize}`,
  )
  return { list: r.data, total: r.total }
}
