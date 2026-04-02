import { Navigate, Outlet, useLocation } from 'react-router-dom'
import { Sidebar } from './sidebar'
import { AuthProvider } from '@/contexts/AuthContext'

function InnerLayout() {
  const location = useLocation()
  const token = localStorage.getItem('auth_token')

  if (!token && location.pathname !== '/login') {
    return <Navigate to="/login" replace />
  }

  return (
    <div className="min-h-screen bg-background">
      <Sidebar />
      <main className="ml-[220px] px-8 py-7">
        <Outlet />
      </main>
    </div>
  )
}

export function MainLayout() {
  return (
    <AuthProvider>
      <InnerLayout />
    </AuthProvider>
  )
}
