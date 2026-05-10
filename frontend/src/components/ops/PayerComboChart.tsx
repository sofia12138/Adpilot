import {
  ResponsiveContainer, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { DailyOpsRow } from '@/types/ops'
import { COLORS } from './chartColors'
import { fmtDateMMDD, fmtPersons, pickTicks } from './formatters'

/**
 * 付费用户构成堆叠柱状图（4 个系列同 stackId 形成 100% 视觉）
 *
 * 系列（按"平台 × 充值类型"四象限）：
 *   - iOS 订阅付费       (ios_sub_payers)
 *   - iOS 普通付费       (ios_onetime_payers)
 *   - Android 订阅付费   (android_sub_payers)
 *   - Android 普通付费   (android_onetime_payers)
 *
 * 同色族浅深区分订阅/普通：
 *   - iOS 蓝（深 sub / 浅 onetime）
 *   - Android 绿（深 sub / 浅 onetime）
 */
export function PayerComboChart({ data }: { data: DailyOpsRow[] }) {
  const ticks = pickTicks(data.map(d => d.date), 7)

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
          tickFormatter={(v: number) => v.toLocaleString()}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={56}
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
        <Bar
          dataKey="ios_sub_payers"
          name="iOS 订阅"
          stackId="payer"
          fill={COLORS.ios_sub}
          isAnimationActive={false}
        />
        <Bar
          dataKey="ios_onetime_payers"
          name="iOS 普通"
          stackId="payer"
          fill={COLORS.ios_onetime}
          isAnimationActive={false}
        />
        <Bar
          dataKey="android_sub_payers"
          name="Android 订阅"
          stackId="payer"
          fill={COLORS.android_sub}
          isAnimationActive={false}
        />
        <Bar
          dataKey="android_onetime_payers"
          name="Android 普通"
          stackId="payer"
          fill={COLORS.android_onetime}
          radius={[4, 4, 0, 0]}
          isAnimationActive={false}
        />
      </BarChart>
    </ResponsiveContainer>
  )
}
