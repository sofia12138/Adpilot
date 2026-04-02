import { AlertTriangle, AlertCircle, CheckCircle2, Lightbulb } from 'lucide-react'
import type { Insight } from '@/services/insight-engine'

const SEVERITY_CONFIG = {
  danger: {
    icon: AlertCircle,
    border: 'border-red-200',
    bg: 'bg-red-50',
    iconColor: 'text-red-500',
    titleColor: 'text-red-700',
  },
  warning: {
    icon: AlertTriangle,
    border: 'border-amber-200',
    bg: 'bg-amber-50',
    iconColor: 'text-amber-500',
    titleColor: 'text-amber-700',
  },
  success: {
    icon: CheckCircle2,
    border: 'border-green-200',
    bg: 'bg-green-50',
    iconColor: 'text-green-500',
    titleColor: 'text-green-700',
  },
} as const

interface InsightPanelProps {
  insights: Insight[]
  loading?: boolean
  className?: string
}

export function InsightPanel({ insights, loading, className = '' }: InsightPanelProps) {
  if (loading) {
    return (
      <div className={`rounded-xl border border-[var(--color-card-border)] shadow-[var(--shadow-card)] bg-white p-5 ${className}`}>
        <div className="flex items-center gap-2 mb-4">
          <Lightbulb className="w-4 h-4 text-gray-400" />
          <h3 className="text-sm font-semibold text-gray-700">结论与建议</h3>
        </div>
        <div className="space-y-3">
          {[1, 2, 3].map(i => (
            <div key={i} className="h-14 rounded-lg bg-gray-100 animate-pulse" />
          ))}
        </div>
      </div>
    )
  }

  if (!insights.length) {
    return (
      <div className={`rounded-xl border border-[var(--color-card-border)] shadow-[var(--shadow-card)] bg-white p-5 ${className}`}>
        <div className="flex items-center gap-2 mb-3">
          <Lightbulb className="w-4 h-4 text-gray-400" />
          <h3 className="text-sm font-semibold text-gray-700">结论与建议</h3>
        </div>
        <p className="text-xs text-gray-400">暂无数据可供分析</p>
      </div>
    )
  }

  return (
    <div className={`rounded-xl border border-[var(--color-card-border)] shadow-[var(--shadow-card)] bg-white p-5 ${className}`}>
      <div className="flex items-center gap-2 mb-4">
        <Lightbulb className="w-4 h-4 text-amber-500" />
        <h3 className="text-sm font-semibold text-gray-700">结论与建议</h3>
        <span className="text-[10px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-400 ml-auto">
          基于规则引擎自动生成
        </span>
      </div>
      <div className="grid gap-3 sm:grid-cols-1 lg:grid-cols-2 xl:grid-cols-3">
        {insights.map((item, idx) => {
          const cfg = SEVERITY_CONFIG[item.type]
          const Icon = cfg.icon
          return (
            <div
              key={idx}
              className={`flex items-start gap-3 rounded-lg border ${cfg.border} ${cfg.bg} px-4 py-3`}
            >
              <Icon className={`w-4 h-4 mt-0.5 shrink-0 ${cfg.iconColor}`} />
              <div className="min-w-0">
                <p className={`text-sm font-medium leading-tight ${cfg.titleColor}`}>{item.title}</p>
                <p className="text-xs text-gray-600 mt-1 leading-relaxed">{item.desc}</p>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
