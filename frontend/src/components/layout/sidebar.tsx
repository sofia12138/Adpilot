import { useState } from 'react'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'
import { ChevronRight, LogOut } from 'lucide-react'
import { cn } from '@/utils/cn'
import { menuConfig } from '@/app/menu-config'
import type { MenuItem } from '@/types/menu'
import { useAuth } from '@/contexts/AuthContext'

function filterMenu(items: MenuItem[], hasPanel: (key: string) => boolean): MenuItem[] {
  return items.reduce<MenuItem[]>((acc, item) => {
    if (item.children) {
      const filtered = filterMenu(item.children, hasPanel)
      if (filtered.length > 0) {
        acc.push({ ...item, children: filtered })
      }
    } else if (item.panelKey) {
      if (hasPanel(item.panelKey)) acc.push(item)
    } else {
      acc.push(item)
    }
    return acc
  }, [])
}

function SidebarItem({ item, depth = 0 }: { item: MenuItem; depth?: number }) {
  const location = useLocation()
  const hasChildren = item.children && item.children.length > 0
  const isChildActive = hasChildren
    ? item.children!.some(c => c.path && location.pathname.startsWith(c.path))
    : false
  const [open, setOpen] = useState(isChildActive)

  if (hasChildren) {
    return (
      <div>
        <button
          onClick={() => setOpen(!open)}
          className={cn(
            'w-full flex items-center gap-2.5 px-5 py-2 text-[13px] text-sidebar-foreground',
            'hover:bg-sidebar-hover hover:text-white transition-colors rounded-md mx-2',
            isChildActive && 'text-white',
          )}
        >
          {item.icon && <item.icon className="w-4 h-4 shrink-0 opacity-70" />}
          <span className="flex-1 text-left">{item.label}</span>
          <ChevronRight className={cn('w-3.5 h-3.5 transition-transform opacity-50', open && 'rotate-90')} />
        </button>
        {open && (
          <div className="mt-0.5 space-y-0.5">
            {item.children!.map(child => (
              <SidebarItem key={child.id} item={child} depth={depth + 1} />
            ))}
          </div>
        )}
      </div>
    )
  }

  if (!item.path) return null

  return (
    <NavLink
      to={item.path}
      className={({ isActive }) =>
        cn(
          'flex items-center gap-2.5 py-2 text-[13px] transition-colors rounded-md mx-2',
          depth > 0 ? 'pl-11 pr-4' : 'px-5',
          isActive
            ? 'bg-primary/15 text-blue-400 font-medium'
            : 'text-sidebar-foreground hover:bg-sidebar-hover hover:text-white',
        )
      }
    >
      {item.icon && depth === 0 && <item.icon className="w-4 h-4 shrink-0 opacity-70" />}
      <span>{item.label}</span>
    </NavLink>
  )
}

export function Sidebar() {
  const navigate = useNavigate()
  const { hasPanel, logout, username } = useAuth()

  const visibleMenu = filterMenu(menuConfig, hasPanel)

  function handleLogout() {
    logout()
    navigate('/login', { replace: true })
  }

  const displayUser = username || localStorage.getItem('auth_user') || '用户'

  return (
    <aside className="fixed left-0 top-0 bottom-0 w-[220px] bg-sidebar flex flex-col z-40">
      <div className="flex items-center gap-3 px-5 py-5">
        <div className="w-9 h-9 bg-gradient-to-br from-blue-500 to-purple-600 rounded-xl flex items-center justify-center text-white text-sm font-bold shadow-lg shadow-blue-500/20">
          A
        </div>
        <span className="text-white font-semibold text-base tracking-tight">AdPilot</span>
      </div>

      <nav className="flex-1 overflow-y-auto py-2 space-y-0.5">
        {visibleMenu.map(item => (
          <SidebarItem key={item.id} item={item} />
        ))}
      </nav>

      <div className="border-t border-white/5 px-4 py-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 min-w-0">
            <div className="w-7 h-7 rounded-full bg-slate-600 flex items-center justify-center text-white text-xs font-medium shrink-0">
              {displayUser.charAt(0).toUpperCase()}
            </div>
            <span className="text-xs text-slate-400 truncate">{displayUser}</span>
          </div>
          <button onClick={handleLogout} className="p-1.5 rounded-md text-slate-500 hover:text-white hover:bg-slate-700 transition" title="退出登录">
            <LogOut className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
    </aside>
  )
}
