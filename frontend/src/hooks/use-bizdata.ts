import { useQuery } from '@tanstack/react-query'
import { fetchPrdSummary, type PrdSummary } from '@/services/bizdata'

export function usePrdSummary(startDate: string, endDate: string, adPlatform?: number) {
  return useQuery<PrdSummary>({
    queryKey: ['prd', 'summary', startDate, endDate, adPlatform],
    queryFn: () => fetchPrdSummary(startDate, endDate, adPlatform),
  })
}
