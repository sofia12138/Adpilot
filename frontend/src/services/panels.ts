import { apiFetch } from './api'

export interface PanelDef {
  id: number
  panel_key: string
  panel_name: string
  panel_group: string
  route_path: string
  sort_order: number
  is_enabled: number
}

export interface UserPanelResult {
  username: string
  role: string
  has_override: boolean
  allowed_panels: string[]
}

export async function fetchPanels(): Promise<PanelDef[]> {
  const r = await apiFetch<{ panels: PanelDef[] }>('/api/panels/')
  return r.panels
}

export async function fetchMyPanels(): Promise<{ allowed_panels: string[]; role: string }> {
  return apiFetch('/api/panels/my')
}

export async function fetchRolePanels(roleKey: string): Promise<string[]> {
  const r = await apiFetch<{ panel_keys: string[] }>(`/api/panels/roles/${roleKey}`)
  return r.panel_keys
}

export async function updateRolePanels(roleKey: string, panelKeys: string[]): Promise<void> {
  await apiFetch(`/api/panels/roles/${roleKey}`, {
    method: 'PUT',
    body: JSON.stringify({ panel_keys: panelKeys }),
  })
}

export async function fetchUserPanels(username: string): Promise<UserPanelResult> {
  return apiFetch(`/api/panels/users/${username}`)
}

export async function updateUserPanels(username: string, panelKeys: string[]): Promise<void> {
  await apiFetch(`/api/panels/users/${username}`, {
    method: 'PUT',
    body: JSON.stringify({ panel_keys: panelKeys }),
  })
}

export async function resetUserPanels(username: string): Promise<void> {
  await apiFetch(`/api/panels/users/${username}`, { method: 'DELETE' })
}
