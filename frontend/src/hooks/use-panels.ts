import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchPanels,
  fetchMyPanels,
  fetchRolePanels,
  updateRolePanels,
  fetchUserPanels,
  updateUserPanels,
  resetUserPanels,
  type PanelDef,
  type UserPanelResult,
} from '@/services/panels'

export function usePanels() {
  return useQuery<PanelDef[]>({
    queryKey: ['panels'],
    queryFn: fetchPanels,
  })
}

export function useMyPanels() {
  return useQuery<{ allowed_panels: string[]; role: string }>({
    queryKey: ['panels', 'my'],
    queryFn: fetchMyPanels,
    staleTime: 5 * 60 * 1000,
  })
}

export function useRolePanels(roleKey: string) {
  return useQuery<string[]>({
    queryKey: ['panels', 'role', roleKey],
    queryFn: () => fetchRolePanels(roleKey),
    enabled: !!roleKey,
  })
}

export function useUpdateRolePanels() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ roleKey, panelKeys }: { roleKey: string; panelKeys: string[] }) =>
      updateRolePanels(roleKey, panelKeys),
    onSuccess: (_, vars) => {
      qc.invalidateQueries({ queryKey: ['panels', 'role', vars.roleKey] })
    },
  })
}

export function useUserPanels(username: string) {
  return useQuery<UserPanelResult>({
    queryKey: ['panels', 'user', username],
    queryFn: () => fetchUserPanels(username),
    enabled: !!username,
  })
}

export function useUpdateUserPanels() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ username, panelKeys }: { username: string; panelKeys: string[] }) =>
      updateUserPanels(username, panelKeys),
    onSuccess: (_, vars) => {
      qc.invalidateQueries({ queryKey: ['panels', 'user', vars.username] })
    },
  })
}

export function useResetUserPanels() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (username: string) => resetUserPanels(username),
    onSuccess: (_, username) => {
      qc.invalidateQueries({ queryKey: ['panels', 'user', username] })
    },
  })
}
