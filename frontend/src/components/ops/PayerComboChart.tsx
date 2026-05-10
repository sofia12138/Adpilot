import {
  ResponsiveContainer, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { DailyOpsRow } from '@/types/ops'
import { COLORS } from './chartColors'
import { fmtDateMMDD, pickTicks } from './formatters'

/**
 * 订单结构堆叠柱状图（4 个系列同 stackId 形成"订阅×首/复 × 内购×首/复"四象限）
 *
 * 系列（双端合并）：
 *   - 首订订单     = ios_first_sub_orders  + android_first_sub_orders
 *   - 续订订单     = ios_repeat_sub_orders + android_repeat_sub_orders
 *   - 首购订单     = ios_first_iap_orders  + android_first_iap_orders
 *   - 复购订单     = ios_repeat_iap_orders + android_repeat_iap_orders
 *
 * 同色族浅深区分首/复：
 *   - 订阅紫（深 sub / 浅 sub_repeat）
 *   - 内购橙（深 onetime / 浅 onetime_repeat）
 */
export function PayerComboChart({ data }: { data: DailyOpsRow[] }) {
  const transformed = data.map(d => ({
    date: d.date,
    first_sub:    (d.ios_first_sub_orders  || 0) + (d.android_first_sub_orders  || 0),
    repeat_sub:   (d.ios_repeat_sub_orders || 0) + (d.android_repeat_sub_orders || 0),
    first_iap:    (d.ios_first_iap_orders  || 0) + (d.android_first_iap_orders  || 0),
    repeat_iap:   (d.ios_repeat_iap_orders || 0) + (d.android_repeat_iap_orders || 0),
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
          tickFormatter={(v: number) => v.toLocaleString()}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={48}
        />
        <Tooltip
          labelFormatter={(label) => fmtDateMMDD(String(label ?? ''))}
          formatter={(v, name) => [`${(Number(v) || 0).toLocaleString()} 笔`, name] as [string, string]}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
          iconType="circle"
        />
        <Bar
          dataKey="first_sub"
          name="首订"
          stackId="orders"
          fill={COLORS.sub}
          isAnimationActive={false}
        />
        <Bar
          dataKey="repeat_sub"
          name="续订"
          stackId="orders"
          fill={'#B8B3F0'}
          isAnimationActive={false}
        />
        <Bar
          dataKey="first_iap"
          name="首购"
          stackId="orders"
          fill={COLORS.onetime}
          isAnimationActive={false}
        />
        <Bar
          dataKey="repeat_iap"
          name="复购"
          stackId="orders"
          fill={'#F8C982'}
          radius={[4, 4, 0, 0]}
          isAnimationActive={false}
        />
      </BarChart>
    </ResponsiveContainer>
  )
}
