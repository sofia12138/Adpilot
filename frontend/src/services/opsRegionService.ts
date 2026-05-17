import { apiFetch } from './api'
import type { DateRange } from '@/types/ops'
import type { RegionDailyStatsResponse } from '@/types/opsRegion'

/** 数据源选择：
 *  - 'auto'    （默认）今/昨日 LA 走 _intraday，其余走 _daily
 *  - 'daily'   全部走 _daily（T+1 历史）
 *  - 'intraday' 全部走 _intraday（仅今/昨日 LA 有数据，调试用）
 */
export type RegionSourceMode = 'auto' | 'daily' | 'intraday'

/**
 * 拉取 [start, end] LA 区间的区域渠道分析数据
 *
 * 调后端 GET /api/ops/region-channel/daily-stats（需 ops_dashboard 面板权限）
 * 区间最大 90 天（与运营总览一致）
 */
export async function fetchRegionStats(
  range: DateRange,
  source: RegionSourceMode = 'auto',
): Promise<RegionDailyStatsResponse> {
  const params = new URLSearchParams({
    start_date: range.start,
    end_date: range.end,
    source,
  })
  return apiFetch<RegionDailyStatsResponse>(
    `/api/ops/region-channel/daily-stats?${params}`,
  )
}
