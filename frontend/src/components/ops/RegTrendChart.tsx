import {
  ResponsiveContainer, ComposedChart,
  Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { DailyOpsRow } from '@/types/ops'
import { COLORS } from './chartColors'
import { fmtDateMMDD, fmtPersons, pickTicks } from './formatters'

/**
 * 新注册用户趋势图（双折线 + 面积填充）
 *
 * 系列：
 *   - iOS 新增用户（ios_new_users）
 *   - Android 新增用户（android_new_users）
 */
export function RegTrendChart({ data }: { data: DailyOpsRow[] }) {
  const ticks = pickTicks(data.map(d => d.date), 7)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={data} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="grad-reg-ios" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={COLORS.ios} stopOpacity={0.28} />
            <stop offset="100%" stopColor={COLORS.ios} stopOpacity={0} />
          </linearGradient>
          <linearGradient id="grad-reg-android" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={COLORS.android} stopOpacity={0.28} />
            <stop offset="100%" stopColor={COLORS.android} stopOpacity={0} />
          </linearGradient>
        </defs>
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
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={48}
        />
        <Tooltip
          labelFormatter={(label) => fmtDateMMDD(String(label ?? ''))}
          formatter={(v) => [fmtPersons(Number(v) || 0), ''] as [string, string]}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
          iconType="circle"
        />
        <Area
          type="monotone"
          dataKey="ios_new_users"
          name="iOS"
          stroke="none"
          fill="url(#grad-reg-ios)"
          legendType="none"
          isAnimationActive={false}
        />
        <Area
          type="monotone"
          dataKey="android_new_users"
          name="Android"
          stroke="none"
          fill="url(#grad-reg-android)"
          legendType="none"
          isAnimationActive={false}
        />
        <Line
          type="monotone"
          dataKey="ios_new_users"
          name="iOS"
          stroke={COLORS.ios}
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4 }}
          isAnimationActive={false}
        />
        <Line
          type="monotone"
          dataKey="android_new_users"
          name="Android"
          stroke={COLORS.android}
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4 }}
          isAnimationActive={false}
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}
