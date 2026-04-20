import { apiFetch } from './api'

export interface TikTokMaterialRecord {
  id: number
  advertiser_id: string
  local_file_name: string
  file_size_bytes: number
  duration_sec: number | null
  upload_channel: string
  tiktok_video_id: string | null
  tiktok_file_name: string | null
  tiktok_url: string | null
  tiktok_width: number | null
  tiktok_height: number | null
  tiktok_format: string | null
  upload_status: 'pending' | 'uploading' | 'success' | 'failed'
  error_code: string | null
  error_message: string | null
  can_use_for_ad: boolean
  ad_usage_note: string
  created_by: string
  created_at: string
  updated_at: string
  tiktok_progress?: { sent: number; total: number; pct: number; phase: string }
}

export interface UploadResult {
  success: boolean
  data?: TikTokMaterialRecord
  upload_time_ms?: number
  error?: string
}

export interface MaterialListResult {
  items: TikTokMaterialRecord[]
  total: number
  page: number
  page_size: number
}

const ALLOWED_VIDEO_EXT = ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v']
const VIDEO_MAX_MB = 1024

function getExt(name: string): string {
  const i = name.lastIndexOf('.')
  return i >= 0 ? name.slice(i).toLowerCase() : ''
}

export function validateVideoFile(file: File): string | null {
  const ext = getExt(file.name)
  if (!ALLOWED_VIDEO_EXT.includes(ext))
    return `不支持的视频格式 ${ext}，支持: ${ALLOWED_VIDEO_EXT.join(', ')}`
  if (file.size > VIDEO_MAX_MB * 1024 * 1024)
    return `视频大小 ${(file.size / 1024 / 1024).toFixed(0)}MB 超过 ${VIDEO_MAX_MB}MB 限制`
  return null
}

/** 读取视频文件时长（秒） */
export function getVideoDuration(file: File): Promise<number> {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(file)
    const video = document.createElement('video')
    video.preload = 'metadata'
    video.onloadedmetadata = () => {
      URL.revokeObjectURL(url)
      resolve(video.duration)
    }
    video.onerror = () => {
      URL.revokeObjectURL(url)
      reject(new Error('无法读取视频时长'))
    }
    video.src = url
  })
}

/** 上传视频到 TikTok Asset Library（XHR 带进度回调） */
export function uploadTikTokVideo(
  advertiser_id: string,
  file: File,
  duration_sec: number | null,
  onProgress?: (pct: number) => void,
): { promise: Promise<UploadResult>; abort: () => void } {
  const token = localStorage.getItem('auth_token') ?? ''
  const form = new FormData()
  form.append('advertiser_id', advertiser_id)
  form.append('file', file)
  form.append('file_name', file.name)
  if (duration_sec != null) form.append('duration_sec', String(duration_sec))

  const xhr = new XMLHttpRequest()

  const promise = new Promise<UploadResult>((resolve) => {
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable && onProgress) onProgress(Math.round((e.loaded / e.total) * 100))
    })
    xhr.addEventListener('load', () => {
      const raw = xhr.responseText ?? ''
      if (xhr.status === 0) { resolve({ success: false, error: '连接中断，请重试' }); return }
      if (!raw.trim()) { resolve({ success: false, error: `空响应 (HTTP ${xhr.status})` }); return }
      try { resolve(JSON.parse(raw)) }
      catch { resolve({ success: false, error: `非 JSON 响应: ${raw.slice(0, 200)}` }) }
    })
    xhr.addEventListener('error', () => resolve({ success: false, error: '网络错误' }))
    xhr.addEventListener('abort', () => resolve({ success: false, error: '已取消' }))
    xhr.addEventListener('timeout', () => resolve({ success: false, error: '上传超时' }))

    xhr.open('POST', '/api/materials/tiktok/upload')
    xhr.timeout = 1200000  // 20 min for large files
    if (token) xhr.setRequestHeader('Authorization', `Bearer ${token}`)
    xhr.send(form)
  })

  return { promise, abort: () => xhr.abort() }
}

export function fetchMaterialList(params: {
  advertiser_id?: string
  status?: string
  keyword?: string
  page?: number
  page_size?: number
}): Promise<{ data: MaterialListResult }> {
  const qs = new URLSearchParams()
  if (params.advertiser_id) qs.set('advertiser_id', params.advertiser_id)
  if (params.status) qs.set('status', params.status)
  if (params.keyword) qs.set('keyword', params.keyword)
  qs.set('page', String(params.page ?? 1))
  qs.set('page_size', String(params.page_size ?? 20))
  return apiFetch(`/api/materials/tiktok?${qs.toString()}`)
}

export function fetchMaterial(id: number): Promise<{ data: TikTokMaterialRecord }> {
  return apiFetch(`/api/materials/tiktok/${id}`)
}

export function deleteMaterial(id: number): Promise<{ success: boolean }> {
  return apiFetch(`/api/materials/tiktok/${id}`, { method: 'DELETE' })
}

export const DURATION_THRESHOLD = 600 // 10 分钟

export function isDurationOverLimit(sec: number | null): boolean {
  return sec != null && sec > DURATION_THRESHOLD
}
