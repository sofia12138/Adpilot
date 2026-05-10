import { useQuery } from '@tanstack/react-query'
import { fetchOpsStats } from '@/services/opsService'
import type { DailyOpsRow, DateRange } from '@/types/ops'

/**
 * 运营数据查询 hook（TanStack Query v5）
 *
 * staleTime 5 分钟：日级粒度数据，避免频繁刷新
 */
export function useOpsStats(days: DateRange) {
  return useQuery<DailyOpsRow[]>({
    queryKey: ['ops-stats', days],
    queryFn: () => fetchOpsStats(days),
    staleTime: 5 * 60 * 1000,
  })
}
