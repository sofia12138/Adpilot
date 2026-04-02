import { useMemo } from 'react'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from 'recharts'
import type { BizCampaignDaily } from '@/services/biz'

interface Props {
  data: BizCampaignDaily[]
  loading?: boolean
}

interface DayPoint {
  date: string
  spend: number
  revenue: number
}

function aggregateByDate(rows: BizCampaignDaily[]): DayPoint[] {
  const map = new Map<string, { spend: number; revenue: number }>()
  for (const r of rows) {
    const d = r.stat_date
    const cur = map.get(d) ?? { spend: 0, revenue: 0 }
    cur.spend += r.spend
    cur.revenue += r.revenue
    map.set(d, cur)
  }
  return Array.from(map.entries())
    .map(([date, v]) => ({ date, spend: +v.spend.toFixed(2), revenue: +v.revenue.toFixed(2) }))
    .sort((a, b) => a.date.localeCompare(b.date))
}

const fmtDate = (v: string) => {
  if (!v) return ''
  const parts = v.split('-')
  return `${parts[1]}/${parts[2]}`
}

const fmtUsd = (v: number) => `$${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`

export function SpendRevenueTrendChart({ data, loading }: Props) {
  const chartData = useMemo(() => aggregateByDate(data), [data])

  if (loading) {
    return (
      <div className="h-60 flex items-center justify-center text-gray-300 text-sm">
        加载中…
      </div>
    )
  }

  if (chartData.length === 0) {
    return (
      <div className="h-60 flex items-center justify-center text-gray-300 text-sm border border-dashed border-gray-200 rounded-lg">
        暂无趋势数据
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={240}>
      <LineChart data={chartData} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
        <XAxis
          dataKey="date"
          tickFormatter={fmtDate}
          tick={{ fontSize: 11, fill: '#9ca3af' }}
          axisLine={{ stroke: '#e5e7eb' }}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          tick={{ fontSize: 11, fill: '#9ca3af' }}
          axisLine={false}
          tickLine={false}
          tickFormatter={(v: number) => v >= 1000 ? `$${(v / 1000).toFixed(0)}k` : `$${v}`}
          width={56}
        />
        <Tooltip
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
          formatter={(value, name) => [fmtUsd(Number(value ?? 0)), name === 'spend' ? '消耗' : '收入']}
          labelFormatter={(label) => `日期：${label}`}
        />
        <Legend
          iconType="circle"
          iconSize={8}
          wrapperStyle={{ fontSize: 12, paddingTop: 8 }}
          formatter={(value: string) => (value === 'spend' ? '消耗' : '收入')}
        />
        <Line
          type="monotone"
          dataKey="spend"
          stroke="#6366f1"
          strokeWidth={2}
          dot={{ r: 2.5, fill: '#6366f1' }}
          activeDot={{ r: 4 }}
        />
        <Line
          type="monotone"
          dataKey="revenue"
          stroke="#10b981"
          strokeWidth={2}
          dot={{ r: 2.5, fill: '#10b981' }}
          activeDot={{ r: 4 }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
