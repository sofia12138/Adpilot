import { cn } from '@/utils/cn'

interface Props {
  title: string
  extra?: React.ReactNode
  children: React.ReactNode
  className?: string
  noPadding?: boolean
}

export function SectionCard({ title, extra, children, className, noPadding }: Props) {
  return (
    <div className={cn(
      'bg-white rounded-xl border border-[var(--color-card-border)] shadow-[var(--shadow-card)]',
      className,
    )}>
      <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100">
        <h2 className="text-sm font-semibold text-gray-800">{title}</h2>
        {extra && <div className="text-xs text-gray-400">{extra}</div>}
      </div>
      <div className={noPadding ? '' : 'p-5'}>{children}</div>
    </div>
  )
}
