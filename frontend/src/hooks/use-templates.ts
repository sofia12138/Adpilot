import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchTemplates,
  createTemplate,
  updateTemplate,
  deleteTemplate,
  cloneTemplate,
  type Template,
} from '@/services/templates'

const KEY = ['templates'] as const

export function useTemplates() {
  return useQuery<Template[]>({
    queryKey: KEY,
    queryFn: fetchTemplates,
  })
}

export function useCreateTemplate() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (body: Record<string, unknown>) => createTemplate(body),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}

export function useUpdateTemplate() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ tplId, body }: { tplId: string; body: Record<string, unknown> }) =>
      updateTemplate(tplId, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}

export function useDeleteTemplate() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (tplId: string) => deleteTemplate(tplId),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}

export function useCloneTemplate() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ tplId, body }: { tplId: string; body: { name: string; notes?: string } }) =>
      cloneTemplate(tplId, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}
