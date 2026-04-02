import { Link } from 'react-router-dom'
import { ChevronRight } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { cn } from '@/utils/cn'

interface Props {
  to: string
  icon: LucideIcon
  title: string
  description: string
  className?: string
}

export function LinkCard({ to, icon: Icon, title, description, className }: Props) {
  return (
    <Link
      to={to}
      className={cn(
        'flex items-center gap-4 bg-white rounded-xl border border-[var(--color-card-border)] shadow-[var(--shadow-card)] p-4',
        'hover:border-blue-200 hover:shadow-md transition-all group',
        className,
      )}
    >
      <div className="w-10 h-10 rounded-lg bg-blue-50 flex items-center justify-center shrink-0 group-hover:bg-blue-100 transition-colors">
        <Icon className="w-5 h-5 text-blue-500" />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-gray-800">{title}</p>
        <p className="text-xs text-gray-400 mt-0.5">{description}</p>
      </div>
      <ChevronRight className="w-4 h-4 text-gray-300 group-hover:text-blue-400 transition-colors" />
    </Link>
  )
}
