import { apiFetch } from './api'

export interface MetaPageOption { id: string; name: string }
export interface MetaPixelOption { id: string; name: string }

export interface UploadImageResult {
  success: boolean; image_hash?: string; name?: string
  size?: number; upload_time_ms?: number; error?: string
}

export interface UploadVideoResult {
  success: boolean; video_id?: string; name?: string
  size?: number; upload_time_ms?: number; error?: string
  stage?: string; upload_mode?: string; retry_count?: number
  meta_response?: string
}

const ALLOWED_IMAGE_EXT = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
const ALLOWED_VIDEO_EXT = ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v']
const IMAGE_MAX_MB = 30
const VIDEO_MAX_MB = 500

function getExt(name: string): string {
  const i = name.lastIndexOf('.')
  return i >= 0 ? name.slice(i).toLowerCase() : ''
}

export function validateImageFile(file: File): string | null {
  const ext = getExt(file.name)
  if (!ALLOWED_IMAGE_EXT.includes(ext)) return `不支持的图片格式 ${ext}，支持: ${ALLOWED_IMAGE_EXT.join(', ')}`
  if (file.size > IMAGE_MAX_MB * 1024 * 1024) return `图片大小 ${(file.size / 1024 / 1024).toFixed(1)}MB 超过 ${IMAGE_MAX_MB}MB 限制`
  return null
}

export function validateVideoFile(file: File): string | null {
  const ext = getExt(file.name)
  if (!ALLOWED_VIDEO_EXT.includes(ext)) return `不支持的视频格式 ${ext}，支持: ${ALLOWED_VIDEO_EXT.join(', ')}`
  if (file.size > VIDEO_MAX_MB * 1024 * 1024) return `视频大小 ${(file.size / 1024 / 1024).toFixed(1)}MB 超过 ${VIDEO_MAX_MB}MB 限制`
  return null
}

export function fetchMetaPages(adAccountId: string): Promise<{ data: MetaPageOption[]; error?: string }> {
  return apiFetch(`/api/meta/assets/pages?ad_account_id=${encodeURIComponent(adAccountId)}`)
}

export function fetchMetaPixels(adAccountId: string): Promise<{ data: MetaPixelOption[]; error?: string }> {
  return apiFetch(`/api/meta/assets/pixels?ad_account_id=${encodeURIComponent(adAccountId)}`)
}

async function _safeParseJson<T>(res: Response, label: string): Promise<T & { success: boolean; error?: string }> {
  const text = await res.text()
  if (!text.trim()) {
    return { success: false, error: `${label}返回了空响应 (HTTP ${res.status})` } as T & { success: boolean; error?: string }
  }
  try {
    return JSON.parse(text)
  } catch {
    const preview = text.slice(0, 200)
    return { success: false, error: `${label}返回了非 JSON 响应 (HTTP ${res.status}): ${preview}` } as T & { success: boolean; error?: string }
  }
}

export async function uploadMetaImage(adAccountId: string, file: File): Promise<UploadImageResult> {
  const token = localStorage.getItem('auth_token') ?? ''
  const form = new FormData()
  form.append('ad_account_id', adAccountId)
  form.append('file', file)

  try {
    const res = await fetch('/api/meta/assets/upload-image', {
      method: 'POST',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      body: form,
    })
    return await _safeParseJson<UploadImageResult>(res, '图片上传接口')
  } catch (e) {
    return { success: false, error: `图片上传网络错误: ${(e as Error).message}` }
  }
}

/** 使用 XMLHttpRequest 上传视频，支持 progress 回调 */
export function uploadMetaVideo(
  adAccountId: string,
  file: File,
  onProgress?: (pct: number) => void,
): { promise: Promise<UploadVideoResult>; abort: () => void } {
  const token = localStorage.getItem('auth_token') ?? ''
  const form = new FormData()
  form.append('ad_account_id', adAccountId)
  form.append('file', file)

  const xhr = new XMLHttpRequest()

  const promise = new Promise<UploadVideoResult>((resolve) => {
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable && onProgress) onProgress(Math.round((e.loaded / e.total) * 100))
    })
    xhr.addEventListener('load', () => {
      const raw = xhr.responseText ?? ''
      if (xhr.status === 0) {
        resolve({ success: false, error: '连接被中断或代理超时，请重试' }); return
      }
      if (!raw.trim()) {
        resolve({ success: false, error: `上传接口返回了空响应 (HTTP ${xhr.status})` }); return
      }
      try {
        const data = JSON.parse(raw)
        if (xhr.status >= 400 && !data.error) {
          data.error = data.detail || data.message || `HTTP ${xhr.status}`
          data.success = false
        }
        resolve(data as UploadVideoResult)
      } catch {
        const preview = raw.slice(0, 200)
        resolve({ success: false, error: `上传接口返回非 JSON (HTTP ${xhr.status}): ${preview}` })
      }
    })
    xhr.addEventListener('error', () => resolve({ success: false, error: '网络错误，请检查网络连接后重试' }))
    xhr.addEventListener('abort', () => resolve({ success: false, error: '上传已取消' }))
    xhr.addEventListener('timeout', () => resolve({ success: false, error: '上传超时，视频可能过大，请压缩后重试' }))

    xhr.open('POST', '/api/meta/assets/upload-video')
    xhr.timeout = 600000  // 10 min
    if (token) xhr.setRequestHeader('Authorization', `Bearer ${token}`)
    xhr.send(form)
  })

  return { promise, abort: () => xhr.abort() }
}
