import {
  ResponsiveContainer, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import { fmtDateMMDD, pickTicks } from '../formatters'
import { CHANNEL_KIND_COLORS, CHANNEL_KIND_LABELS } from '@/types/opsRegion'

/**
 * 通用四色堆叠柱状图（自然量 / TikTok / Meta / 其它）
 * 用于注册和充值两个趋势图，仅 yLabelFormatter 不同
 */
interface Point {
  date: string
  organic: number
  tiktok: number
  meta: number
  other: number
}

interface Props {
  data: Point[]
  /** Y 轴 + Tooltip 数值格式化 */
  formatValue?: (v: number) => string
  /** Tooltip 单位后缀，如 '人' / '$' */
  unit?: string
}

export function ChannelStackedBar({ data, formatValue, unit }: Props) {
  const ticks = pickTicks(data.map(d => d.date), 7)
  const fmt = formatValue ?? ((v: number) => v.toLocaleString())

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={data} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
        <XAxis
          dataKey="date"
          tickFormatter={fmtDateMMDD}
          ticks={ticks}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={{ stroke: '#e5e7eb' }}
          tickLine={false}
        />
        <YAxis
          tickFormatter={fmt}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={56}
        />
        <Tooltip
          labelFormatter={(label) => fmtDateMMDD(String(label ?? ''))}
          formatter={(v, name) => {
            const num = Number(v) || 0
            const suffix = unit ?? ''
            return [`${fmt(num)}${suffix}`, name] as [string, string]
          }}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend wrapperStyle={{ fontSize: 12, paddingTop: 4 }} iconType="circle" />
        <Bar
          dataKey="organic"
          name={CHANNEL_KIND_LABELS.organic}
          stackId="kind"
          fill={CHANNEL_KIND_COLORS.organic}
          isAnimationActive={false}
        />
        <Bar
          dataKey="tiktok"
          name={CHANNEL_KIND_LABELS.tiktok}
          stackId="kind"
          fill={CHANNEL_KIND_COLORS.tiktok}
          isAnimationActive={false}
        />
        <Bar
          dataKey="meta"
          name={CHANNEL_KIND_LABELS.meta}
          stackId="kind"
          fill={CHANNEL_KIND_COLORS.meta}
          isAnimationActive={false}
        />
        <Bar
          dataKey="other"
          name={CHANNEL_KIND_LABELS.other}
          stackId="kind"
          fill={CHANNEL_KIND_COLORS.other}
          radius={[4, 4, 0, 0]}
          isAnimationActive={false}
        />
      </BarChart>
    </ResponsiveContainer>
  )
}
