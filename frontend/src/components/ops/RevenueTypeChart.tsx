import {
  ResponsiveContainer, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { DailyOpsRow } from '@/types/ops'
import { COLORS } from './chartColors'
import { fmtDateMMDD, fmtCentsToWan, pickTicks } from './formatters'

/**
 * 充值类型维度堆叠柱状图
 *
 * 系列（在数据层把同类型 iOS+Android 合并）：
 *   - 订阅总充值     = ios_sub_revenue + android_sub_revenue
 *   - 普通充值总额   = ios_onetime_revenue + android_onetime_revenue
 */
export function RevenueTypeChart({ data }: { data: DailyOpsRow[] }) {
  const transformed = data.map(d => ({
    date: d.date,
    sub_total: (d.ios_sub_revenue || 0) + (d.android_sub_revenue || 0),
    onetime_total: (d.ios_onetime_revenue || 0) + (d.android_onetime_revenue || 0),
  }))
  const ticks = pickTicks(transformed.map(d => d.date), 7)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={transformed} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
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
          tickFormatter={(v: number) => `${(v / 1_000_000).toFixed(1)}万`}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={56}
        />
        <Tooltip
          labelFormatter={(label) => fmtDateMMDD(String(label ?? ''))}
          formatter={(v) => [fmtCentsToWan(Number(v) || 0), ''] as [string, string]}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
          iconType="circle"
        />
        <Bar
          dataKey="sub_total"
          name="订阅充值"
          stackId="type"
          fill={COLORS.sub}
          radius={[0, 0, 0, 0]}
          isAnimationActive={false}
        />
        <Bar
          dataKey="onetime_total"
          name="普通充值"
          stackId="type"
          fill={COLORS.onetime}
          radius={[4, 4, 0, 0]}
          isAnimationActive={false}
        />
      </BarChart>
    </ResponsiveContainer>
  )
}
