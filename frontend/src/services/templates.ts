import { apiFetch } from './api'

export interface Template {
  id: string
  name: string
  platform: string
  template_type?: string
  template_subtype?: string
  is_builtin?: boolean
  is_system?: boolean
  is_editable?: boolean
  template_key?: string | null
  parent_template_id?: string | null
  /** 模板允许的投放语种白名单（强制开启平台语言定向），后端兜底为 ['en'] */
  delivery_languages?: string[]
  /** 默认投放语种，必须在 delivery_languages 中 */
  default_delivery_language?: string
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

export async function cloneTemplate(tplId: string, body: { name: string; notes?: string }): Promise<Template> {
  const r = await apiFetch<SingleResp>(`/api/templates/${tplId}/clone`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return r.data
}
