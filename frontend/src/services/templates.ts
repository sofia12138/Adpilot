import { apiFetch } from './api'

export interface Template {
  id: string
  name: string
  platform: string
  created_at: string
  updated_at: string
  [key: string]: unknown
}

interface DataResp { data: Template[] }
interface SingleResp { data: Template }

export async function fetchTemplates(): Promise<Template[]> {
  const r = await apiFetch<DataResp>('/api/templates/')
  return r.data
}

export async function fetchTemplate(tplId: string): Promise<Template> {
  const r = await apiFetch<SingleResp>(`/api/templates/${tplId}`)
  return r.data
}

export async function createTemplate(body: Record<string, unknown>): Promise<Template> {
  const r = await apiFetch<SingleResp>('/api/templates/', {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return r.data
}

export async function updateTemplate(tplId: string, body: Record<string, unknown>): Promise<Template> {
  const r = await apiFetch<SingleResp>(`/api/templates/${tplId}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  })
  return r.data
}

export async function deleteTemplate(tplId: string): Promise<void> {
  await apiFetch<{ message: string }>(`/api/templates/${tplId}`, {
    method: 'DELETE',
  })
}
