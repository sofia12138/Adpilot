import { useState, useCallback } from 'react'
import { Calendar } from 'lucide-react'

// ─── 公共日期工具 ────────────────────────────────────────

export interface DateRange {
  startDate: string   // YYYY-MM-DD
  endDate: string     // YYYY-MM-DD
}

function toStr(d: Date): string { return d.toISOString().slice(0, 10) }

const PRESETS = [
  { id: 'today',   label: '今天',   calc: () => { const d = toStr(new Date()); return { startDate: d, endDate: d } } },
  { id: 'yest',    label: '昨天',   calc: () => { const d = new Date(); d.setDate(d.getDate() - 1); const s = toStr(d); return { startDate: s, endDate: s } } },
  { id: '7d',      label: '近7天',  calc: () => { const e = new Date(); const s = new Date(); s.setDate(e.getDate() - 6); return { startDate: toStr(s), endDate: toStr(e) } } },
  { id: '30d',     label: '近30天', calc: () => { const e = new Date(); const s = new Date(); s.setDate(e.getDate() - 29); return { startDate: toStr(s), endDate: toStr(e) } } },
  { id: '90d',     label: '近90天', calc: () => { const e = new Date(); const s = new Date(); s.setDate(e.getDate() - 89); return { startDate: toStr(s), endDate: toStr(e) } } },
  { id: 'all',     label: '全部',   calc: () => ({ startDate: '2024-01-01', endDate: toStr(new Date()) }) },
] as const

type PresetId = typeof PRESETS[number]['id'] | 'custom'

// ─── 默认值 ──────────────────────────────────────────────

export function getDefaultDateRange(presetId: string = '7d'): DateRange {
  const p = PRESETS.find(p => p.id === presetId)
  return p ? p.calc() : PRESETS[2].calc()
}

// ─── Props ───────────────────────────────────────────────

interface DateRangeFilterProps {
  value: DateRange
  onChange: (range: DateRange) => void
  className?: string
}

// ─── Component ───────────────────────────────────────────

export function DateRangeFilter({ value, onChange, className = '' }: DateRangeFilterProps) {
  const [activePreset, setActivePreset] = useState<PresetId>(() => {
    for (const p of PRESETS) {
      const r = p.calc()
      if (r.startDate === value.startDate && r.endDate === value.endDate) return p.id
    }
    return 'custom'
  })

  const [showCustom, setShowCustom] = useState(activePreset === 'custom')

  const handlePreset = useCallback((id: PresetId) => {
    if (id === 'custom') {
      setActivePreset('custom')
      setShowCustom(true)
      return
    }
    const p = PRESETS.find(p => p.id === id)!
    setActivePreset(id)
    setShowCustom(false)
    onChange(p.calc())
  }, [onChange])

  const handleCustomStart = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const s = e.target.value
    if (s) {
      setActivePreset('custom')
      onChange({ startDate: s, endDate: value.endDate < s ? s : value.endDate })
    }
  }, [onChange, value.endDate])

  const handleCustomEnd = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const end = e.target.value
    if (end) {
      setActivePreset('custom')
      onChange({ startDate: value.startDate > end ? end : value.startDate, endDate: end })
    }
  }, [onChange, value.startDate])

  return (
    <div className={`flex flex-wrap items-center gap-2 ${className}`}>
      <Calendar className="w-3.5 h-3.5 text-gray-400 shrink-0" />

      <div className="flex rounded-lg border border-gray-200 overflow-hidden text-xs">
        {PRESETS.map(p => (
          <button key={p.id} type="button" onClick={() => handlePreset(p.id)}
            className={`px-3 py-1.5 transition ${
              activePreset === p.id
                ? 'bg-blue-500 text-white font-medium'
                : 'bg-white text-gray-600 hover:bg-gray-50'
            }`}>
            {p.label}
          </button>
        ))}
        <button type="button" onClick={() => handlePreset('custom')}
          className={`px-3 py-1.5 transition ${
            activePreset === 'custom'
              ? 'bg-blue-500 text-white font-medium'
              : 'bg-white text-gray-600 hover:bg-gray-50'
          }`}>
          自定义
        </button>
      </div>

      {showCustom && (
        <div className="flex items-center gap-1.5 text-xs">
          <input type="date" value={value.startDate} onChange={handleCustomStart}
            className="px-2 py-1.5 border border-gray-200 rounded-lg text-xs focus:outline-none focus:border-blue-300 transition" />
          <span className="text-gray-300">—</span>
          <input type="date" value={value.endDate} onChange={handleCustomEnd}
            className="px-2 py-1.5 border border-gray-200 rounded-lg text-xs focus:outline-none focus:border-blue-300 transition" />
        </div>
      )}
    </div>
  )
}
