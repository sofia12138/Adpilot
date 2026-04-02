const BASE = ''

export class AuthError extends Error {
  constructor() {
    super('需要登录')
    this.name = 'AuthError'
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
  if (!res.ok) throw new Error(`API ${res.status}`)
  return res.json() as Promise<T>
}
