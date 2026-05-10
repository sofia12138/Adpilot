import { useQuery } from '@tanstack/react-query'
import { fetchOpsStats } from '@/services/opsService'
import type { DailyOpsRow, DateRange } from '@/types/ops'
import { rangeKey } from '@/components/ops/rangeUtils'

/**
 * 运营数据查询 hook（TanStack Query v5）
 *
 * staleTime 5 分钟：日级粒度数据，避免频繁刷新
 * queryKey 用稳定字符串（start~end），不依赖对象引用
 */
export function useOpsStats(range: DateRange) {
  return useQuery<DailyOpsRow[]>({
    queryKey: ['ops-stats', rangeKey(range)],
    queryFn: () => fetchOpsStats(range),
    staleTime: 5 * 60 * 1000,
  })
}
