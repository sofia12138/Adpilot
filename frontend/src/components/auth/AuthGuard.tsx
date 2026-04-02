import { Navigate } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'

interface AuthGuardProps {
  panelKey: string
  children: React.ReactNode
}

export function AuthGuard({ panelKey, children }: AuthGuardProps) {
  const { hasPanel, loaded } = useAuth()
  const token = localStorage.getItem('auth_token')

  if (!token) {
    return <Navigate to="/login" replace />
  }

  if (!loaded) return null

  if (!hasPanel(panelKey)) {
    return <Navigate to="/forbidden" replace />
  }

  return <>{children}</>
}
