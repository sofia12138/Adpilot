const BASE = ''

export class AuthError extends Error {
  constructor() {
    super('需要登录')
    this.name = 'AuthError'
  }
}

/**
 * API 业务错误：包含状态码 + 后端返回的 detail。
 * 兼容两种 detail 格式：
 *   - 字符串：HTTPException(detail="xxx")        → message='xxx', code=''
 *   - 对象：  HTTPException(detail={code, message}) → message+code 完整携带
 */
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly detail: unknown,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export async function apiFetch<T>(path: string, opts?: RequestInit): Promise<T> {
  const token = localStorage.getItem('auth_token') ?? ''
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }
  const res = await fetch(`${BASE}${path}`, { headers, ...opts })
  if (res.status === 401) {
    localStorage.removeItem('auth_token')
    throw new AuthError()
  }
  if (!res.ok) {
    let body: unknown = null
    try {
      body = await res.json()
    } catch {
      /* 非 JSON 错误，忽略 */
    }
    const detail = (body && typeof body === 'object' && 'detail' in body)
      ? (body as { detail: unknown }).detail
      : body
    if (detail && typeof detail === 'object' && 'message' in (detail as Record<string, unknown>)) {
      const obj = detail as { code?: string; message?: string }
      throw new ApiError(res.status, obj.code ?? '', obj.message ?? `API ${res.status}`, detail)
    }
    const msg = typeof detail === 'string' && detail ? detail : `API ${res.status}`
    throw new ApiError(res.status, '', msg, detail)
  }
  return res.json() as Promise<T>
}
