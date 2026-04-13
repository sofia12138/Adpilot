import type React from 'react'
import { Link } from 'react-router-dom'
import { cn } from '@/utils/cn'
import type { LucideIcon } from 'lucide-react'

interface Props {
  label: string
  value: string | number
  change?: string
  changeType?: 'up' | 'down' | 'neutral'
  icon?: LucideIcon
  className?: string
  href?: string
  extra?: React.ReactNode
}

export function StatCard({ label, value, change, changeType = 'neutral', icon: Icon, className, href, extra }: Props) {
  const content = (
    <>
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs font-medium text-gray-400 tracking-wide uppercase">{label}</span>
        {Icon && (
          <div className="w-8 h-8 rounded-lg bg-blue-50/60 flex items-center justify-center">
            <Icon className="w-4 h-4 text-blue-400/70" />
          </div>
        )}
      </div>
      <div className="text-2xl font-bold text-gray-900 tabular-nums leading-none">{value}</div>
      {change && (
        <p className={cn(
          'text-xs mt-2',
          changeType === 'up' && 'text-green-600',
          changeType === 'down' && 'text-red-500',
          changeType === 'neutral' && 'text-gray-400',
        )}>
          {changeType === 'up' && '↑ '}
          {changeType === 'down' && '↓ '}
          {change}
        </p>
      )}
      {extra && <div className="mt-1.5">{extra}</div>}
    </>
  )

  const cls = cn(
    'bg-white rounded-xl border border-[var(--color-card-border)] shadow-[var(--shadow-card)] p-5',
    href && 'hover:border-blue-200 hover:shadow-md transition-all cursor-pointer',
    className,
  )

  if (href) {
    return <Link to={href} className={cn(cls, 'block')}>{content}</Link>
  }

  return <div className={cls}>{content}</div>
}
