import { Calendar } from 'lucide-react'
import { cn } from '@/utils/cn'
import type { DatePreset, DateRange } from '@/types/ops'

const PRESET_OPTIONS: { value: DatePreset; label: string }[] = [
  { value: 'yesterday', label: '昨天' },
  { value: 'today',     label: '今天' },
  { value: 'last7',     label: '近7天' },
  { value: 'last14',    label: '近14天' },
  { value: 'last30',    label: '近30天' },
  { value: 'custom',    label: '自定义' },
]

interface RangeSwitchProps {
  range: DateRange
  customDraft: { start: string; end: string }
  onPresetChange: (p: DatePreset) => void
  onCustomChange: (next: { start: string; end: string }) => void
}

/**
 * 通用时间范围切换器（昨天 / 今天 / 近7 / 近14 / 近30 / 自定义）
 *
 * 设计：纯展示组件，状态由父组件管理。LA 时区计算见 rangeUtils.ts。
 */
export function RangeSwitch({ range, customDraft, onPresetChange, onCustomChange }: RangeSwitchProps) {
  return (
    <div className="flex flex-col items-end gap-2">
      <div className="inline-flex bg-muted rounded-lg p-0.5 text-xs">
        {PRESET_OPTIONS.map(opt => (
          <button
            key={opt.value}
            onClick={() => onPresetChange(opt.value)}
            className={cn(
              'px-3 py-1.5 rounded-md transition-colors flex items-center gap-1',
              range.preset === opt.value
                ? 'bg-white text-gray-900 shadow-sm font-medium'
                : 'text-muted-foreground hover:text-gray-700',
            )}
          >
            {opt.value === 'custom' && <Calendar className="w-3 h-3" />}
            {opt.label}
          </button>
        ))}
      </div>

      {range.preset === 'custom' && (
        <div className="inline-flex items-center gap-2 bg-card border border-card-border rounded-lg px-3 py-1.5 text-xs">
          <input
            type="date"
            value={customDraft.start}
            max={customDraft.end || undefined}
            onChange={e => onCustomChange({ ...customDraft, start: e.target.value })}
            className="bg-transparent outline-none text-gray-700"
          />
          <span className="text-muted-foreground">至</span>
          <input
            type="date"
            value={customDraft.end}
            min={customDraft.start || undefined}
            onChange={e => onCustomChange({ ...customDraft, end: e.target.value })}
            className="bg-transparent outline-none text-gray-700"
          />
        </div>
      )}
    </div>
  )
}
