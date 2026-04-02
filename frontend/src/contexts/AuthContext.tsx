import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from 'react'

interface AuthState {
  allowedPanels: string[]
  role: string
  username: string
  loaded: boolean
}

interface AuthContextValue extends AuthState {
  setAuth: (panels: string[], role: string, username: string) => void
  hasPanel: (panelKey: string) => boolean
  refreshPanels: () => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}

function loadFromStorage(): AuthState {
  try {
    const raw = localStorage.getItem('auth_panels')
    const panels = raw ? JSON.parse(raw) as string[] : []
    const role = localStorage.getItem('auth_role') || ''
    const username = localStorage.getItem('auth_user') || ''
    const token = localStorage.getItem('auth_token') || ''
    return { allowedPanels: panels, role, username, loaded: !!token }
  } catch {
    return { allowedPanels: [], role: '', username: '', loaded: false }
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>(loadFromStorage)

  const setAuth = useCallback((panels: string[], role: string, username: string) => {
    localStorage.setItem('auth_panels', JSON.stringify(panels))
    localStorage.setItem('auth_role', role)
    localStorage.setItem('auth_user', username)
    setState({ allowedPanels: panels, role, username, loaded: true })
  }, [])

  const hasPanel = useCallback((panelKey: string) => {
    if (state.role === 'super_admin') return true
    return state.allowedPanels.includes(panelKey)
  }, [state.allowedPanels, state.role])

  const refreshPanels = useCallback(async () => {
    const token = localStorage.getItem('auth_token')
    if (!token) return
    try {
      const res = await fetch('/api/panels/my', {
        headers: { Authorization: `Bearer ${token}` },
      })
      if (res.ok) {
        const data = await res.json()
        setAuth(data.allowed_panels ?? [], data.role ?? state.role, state.username)
      }
    } catch { /* silent */ }
  }, [setAuth, state.role, state.username])

  const logout = useCallback(() => {
    localStorage.removeItem('auth_token')
    localStorage.removeItem('auth_user')
    localStorage.removeItem('auth_role')
    localStorage.removeItem('auth_panels')
    setState({ allowedPanels: [], role: '', username: '', loaded: false })
  }, [])

  useEffect(() => {
    const token = localStorage.getItem('auth_token')
    if (token && state.allowedPanels.length === 0) {
      refreshPanels()
    }
  }, [])

  return (
    <AuthContext.Provider value={{ ...state, setAuth, hasPanel, refreshPanels, logout }}>
      {children}
    </AuthContext.Provider>
  )
}
