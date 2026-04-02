import { cn } from '@/utils/cn'

interface Props {
  children: React.ReactNode
  className?: string
}

export function FilterBar({ children, className }: Props) {
  return (
    <div className={cn(
      'bg-white rounded-xl border border-[var(--color-card-border)] shadow-[var(--shadow-card)] px-5 py-3.5',
      'flex flex-wrap items-center gap-3',
      className,
    )}>
      {children}
    </div>
  )
}
