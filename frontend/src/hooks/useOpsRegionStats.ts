import { useQuery } from '@tanstack/react-query'
import { fetchRegionStats, type RegionSourceMode } from '@/services/opsRegionService'
import type { DateRange } from '@/types/ops'
import type { RegionDailyStatsResponse } from '@/types/opsRegion'
import { rangeKey } from '@/components/ops/rangeUtils'

/**
 * 区域渠道分析数据 hook（TanStack Query v5）
 *
 * staleTime 5 分钟，缓存键含 source 模式 + 日期区间
 */
export function useOpsRegionStats(range: DateRange, source: RegionSourceMode = 'auto') {
  return useQuery<RegionDailyStatsResponse>({
    queryKey: ['ops-region-stats', rangeKey(range), source],
    queryFn: () => fetchRegionStats(range, source),
    staleTime: 5 * 60 * 1000,
  })
}
