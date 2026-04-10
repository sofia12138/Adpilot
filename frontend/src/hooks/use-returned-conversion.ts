import { useQuery } from '@tanstack/react-query'
import {
  fetchReturnedConversion,
  type ReturnedConversionFilter,
} from '@/services/returned-conversion'

export function useReturnedConversion(filter: ReturnedConversionFilter) {
  return useQuery({
    queryKey: ['returned-conversion', filter],
    queryFn: () => fetchReturnedConversion(filter),
    // 日期范围未设置时不请求
    enabled: Boolean(filter.start_date && filter.end_date),
    staleTime: 5 * 60 * 1000,
  })
}
