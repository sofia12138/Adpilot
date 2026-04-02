import { useQuery } from '@tanstack/react-query'
import { fetchVideos, fetchImages, type VideoItem, type ImageItem } from '@/services/creatives'

export function useVideos(page = 1, pageSize = 20) {
  return useQuery<{ list: VideoItem[]; total: number }>({
    queryKey: ['creatives', 'videos', page, pageSize],
    queryFn: () => fetchVideos(page, pageSize),
  })
}

export function useImages(page = 1, pageSize = 20) {
  return useQuery<{ list: ImageItem[]; total: number }>({
    queryKey: ['creatives', 'images', page, pageSize],
    queryFn: () => fetchImages(page, pageSize),
  })
}
