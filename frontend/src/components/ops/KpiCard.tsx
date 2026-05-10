import { ArrowUp, ArrowDown } from 'lucide-react'
import { cn } from '@/utils/cn'

interface KpiCardProps {
  /** 卡片标题 */
  title: string
  /** 主数字（已格式化的字符串） */
  value: string
  /** 较昨日涨跌百分比；正数显示绿色↑，负数显示红色↓，null/undefined 不显示 */
  delta?: number | null
  /** 副文字（小一号字号、灰色） */
  subtitle?: string
}

/**
 * 运营数据 KPI 卡片
 *
 * 视觉规范：
 *   - bg-muted（与项目主题一致；项目无 secondary 色，等价 #f1f5f9）
 *   - 圆角 rounded-xl，无阴影
 *   - 涨跌色：上涨 text-green-600，下跌 text-red-500
 */
export function KpiCard({ title, value, delta, subtitle }: KpiCardProps) {
  const showDelta = typeof delta === 'number' && isFinite(delta)
  const isUp = showDelta && (delta as number) >= 0
  const deltaAbs = showDelta ? Math.abs(delta as number) : 0

  return (
    <div className="bg-muted rounded-xl px-4 py-3.5 flex flex-col gap-1.5">
      <div className="text-xs text-muted-foreground">{title}</div>

      <div className="flex items-baseline gap-2 flex-wrap">
        <span className="text-xl font-semibold text-foreground tabular-nums">{value}</span>
        {showDelta && (
          <span
            className={cn(
              'inline-flex items-center text-xs font-medium tabular-nums',
              isUp ? 'text-green-600' : 'text-red-500',
            )}
            title="较昨日"
          >
            {isUp
              ? <ArrowUp className="w-3 h-3" />
              : <ArrowDown className="w-3 h-3" />}
            {deltaAbs.toFixed(1)}%
          </span>
        )}
      </div>

      {subtitle && (
        <div className="text-xs text-muted-foreground tabular-nums">{subtitle}</div>
      )}
    </div>
  )
}
