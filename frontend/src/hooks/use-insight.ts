import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getInsightConfig, updateInsightConfig } from '@/services/insight-config'
import type { InsightThresholds } from '@/config/insight-thresholds'
import { DEFAULT_INSIGHT_THRESHOLDS } from '@/config/insight-thresholds'

const KEY = ['insight-config'] as const

export function useInsightConfig() {
  return useQuery<InsightThresholds>({
    queryKey: KEY,
    queryFn: getInsightConfig,
    staleTime: 5 * 60 * 1000,
    placeholderData: DEFAULT_INSIGHT_THRESHOLDS,
  })
}

export function useUpdateInsightConfig() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (data: InsightThresholds) => updateInsightConfig(data),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
  })
}
