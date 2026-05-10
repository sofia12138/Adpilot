import {
  ResponsiveContainer, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { DailyOpsRow } from '@/types/ops'
import { COLORS } from './chartColors'
import { fmtDateMMDD, fmtUsd, pickTicks } from './formatters'

/**
 * 双端充值对比堆叠柱状图（iOS vs Android）
 *
 * 系列（在数据层把订阅 + 内购合并为「平台总充值」）：
 *   - iOS 总充值     = ios_subscribe_revenue + ios_onetime_revenue
 *   - Android 总充值 = android_subscribe_revenue + android_onetime_revenue
 *
 * 单位 USD（已从美分换算）
 */
export function RevenuePlatformChart({ data }: { data: DailyOpsRow[] }) {
  const transformed = data.map(d => ({
    date: d.date,
    ios_total: (d.ios_subscribe_revenue || 0) + (d.ios_onetime_revenue || 0),
    android_total: (d.android_subscribe_revenue || 0) + (d.android_onetime_revenue || 0),
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
          tickFormatter={(v: number) => fmtUsd(v)}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={64}
        />
        <Tooltip
          labelFormatter={(label) => fmtDateMMDD(String(label ?? ''))}
          formatter={(v, name) => [fmtUsd(Number(v) || 0), name] as [string, string]}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
          iconType="circle"
        />
        <Bar
          dataKey="ios_total"
          name="iOS"
          stackId="platform"
          fill={COLORS.ios}
          isAnimationActive={false}
        />
        <Bar
          dataKey="android_total"
          name="Android"
          stackId="platform"
          fill={COLORS.android}
          radius={[4, 4, 0, 0]}
          isAnimationActive={false}
        />
      </BarChart>
    </ResponsiveContainer>
  )
}
