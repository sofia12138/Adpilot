import { Navigate, useLocation } from 'react-router-dom'

export function AuthGuard({ children }: { children: React.ReactNode }) {
  const token = localStorage.getItem('auth_token')
  const location = useLocation()

  if (!token) {
    return <Navigate to="/login" state={{ from: location.pathname }} replace />
  }

  return <>{children}</>
}
