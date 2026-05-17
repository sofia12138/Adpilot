import {
  ResponsiveContainer, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import { CHANNEL_KIND_COLORS, CHANNEL_KIND_LABELS } from '@/types/opsRegion'

interface CountryBarPoint {
  region: string
  organic: number
  tiktok: number
  meta: number
  other: number
}

interface Props {
  data: CountryBarPoint[]
  /** Y 轴 + Tooltip 数值格式化 */
  formatValue?: (v: number) => string
  /** Tooltip 单位后缀 */
  unit?: string
}

/**
 * Top 10 国家堆叠柱图（横向）
 * X 轴是 region，Y 轴是值；4 系列堆叠（与趋势图同色）
 */
export function CountryTopBars({ data, formatValue, unit }: Props) {
  const fmt = formatValue ?? ((v: number) => v.toLocaleString())
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart
        data={data}
        layout="vertical"
        margin={{ top: 10, right: 16, left: 0, bottom: 0 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" horizontal={false} />
        <XAxis
          type="number"
          tickFormatter={fmt}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          type="category"
          dataKey="region"
          tick={{ fontSize: 11, fill: '#374151' }}
          axisLine={{ stroke: '#e5e7eb' }}
          tickLine={false}
          width={56}
        />
        <Tooltip
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
          isAnimationActive={false}
        />
      </BarChart>
    </ResponsiveContainer>
  )
}
