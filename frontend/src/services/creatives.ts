import { apiFetch } from './api'

export interface VideoItem {
  video_id: string
  material_id?: string
  file_name?: string
  duration?: number
  width?: number
  height?: number
  bit_rate?: number
  format?: string
  preview_url?: string
  preview_url_expire_time?: string
  create_time?: string
  [key: string]: unknown
}

export interface ImageItem {
  image_id?: string
  id?: string
  material_id?: string
  file_name?: string
  width?: number
  height?: number
  format?: string
  url?: string
  preview_url?: string
  create_time?: string
  [key: string]: unknown
}

interface TikTokListResp {
  list: unknown[]
  page_info?: { page: number; page_size: number; total_number: number; total_page: number }
}

interface DataResp { data: TikTokListResp }

export async function fetchVideos(page = 1, pageSize = 20): Promise<{ list: VideoItem[]; total: number }> {
  const r = await apiFetch<DataResp>(`/api/creatives/videos?page=${page}&page_size=${pageSize}`)
  const d = r.data
  return {
    list: (d.list ?? []) as VideoItem[],
    total: d.page_info?.total_number ?? 0,
  }
}

export async function fetchImages(page = 1, pageSize = 20): Promise<{ list: ImageItem[]; total: number }> {
  const r = await apiFetch<DataResp>(`/api/creatives/images?page=${page}&page_size=${pageSize}`)
  const d = r.data
  return {
    list: (d.list ?? []) as ImageItem[],
    total: d.page_info?.total_number ?? 0,
  }
}
