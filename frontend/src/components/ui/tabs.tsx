import { useState, type ReactNode } from 'react'
import { cn } from '@/utils/cn'

interface Tab {
  key: string
  label: string
  content: ReactNode
}

interface TabsProps {
  tabs: Tab[]
  defaultTab?: string
  className?: string
}

export function Tabs({ tabs, defaultTab, className }: TabsProps) {
  const [active, setActive] = useState(defaultTab ?? tabs[0]?.key ?? '')
  const current = tabs.find(t => t.key === active)

  return (
    <div className={className}>
      <div className="flex gap-1 border-b border-border mb-4">
        {tabs.map(t => (
          <button
            key={t.key}
            onClick={() => setActive(t.key)}
            className={cn(
              'px-4 py-2 text-sm transition-colors -mb-px',
              active === t.key
                ? 'border-b-2 border-primary text-primary font-medium'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {t.label}
          </button>
        ))}
      </div>
      {current?.content}
    </div>
  )
}
