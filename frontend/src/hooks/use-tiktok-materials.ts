import { useQuery } from '@tanstack/react-query'
import { fetchMaterialList, type MaterialListResult } from '@/services/tiktok-materials'

export function useTikTokMaterials(params: {
  advertiser_id?: string
  status?: string
  keyword?: string
  page?: number
  page_size?: number
}, hasActiveUpload = false) {
  return useQuery<{ data: MaterialListResult }>({
    queryKey: ['tiktok-materials', params],
    queryFn: () => fetchMaterialList(params),
    refetchInterval: hasActiveUpload ? 3_000 : 10_000,
  })
}
