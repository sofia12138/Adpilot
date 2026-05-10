import {
  ResponsiveContainer, ComposedChart, Bar, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ReferenceLine,
} from 'recharts'
import type { DailyOpsRow } from '@/types/ops'
import { COLORS } from './chartColors'
import { fmtDateMMDD, fmtUsd, pickTicks } from './formatters'

/**
 * 广告消耗 vs 总充值 + D0 ROI 趋势图
 *
 * 双轴：
 *   - 左轴: 金额（USD），柱状显示 spend / revenue
 *   - 右轴: ROI 倍数，折线 + 1.0 参考线（盈利分水岭）
 *
 * D0 流水 ROI = 当日总充值 / 当日 spend
 */
export function SpendRoiChart({ data }: { data: DailyOpsRow[] }) {
  const transformed = data.map(d => {
    const revenue = (d.ios_subscribe_revenue || 0) + (d.ios_onetime_revenue || 0)
                  + (d.android_subscribe_revenue || 0) + (d.android_onetime_revenue || 0)
    const spend = d.ad_spend || 0
    const roi = spend > 0 ? revenue / spend : null
    return { date: d.date, spend, revenue, roi }
  })
  const ticks = pickTicks(transformed.map(d => d.date), 7)

  // ROI 轴上限 — 取数据最大值与 1.5 的较大值，确保 ROI=1 参考线显眼
  const roiMax = Math.max(
    1.5,
    ...transformed.map(d => (d.roi == null ? 0 : d.roi)),
  )

  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={transformed} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
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
          yAxisId="usd"
          tickFormatter={(v: number) => fmtUsd(v)}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={64}
        />
        <YAxis
          yAxisId="roi"
          orientation="right"
          domain={[0, roiMax]}
          tickFormatter={(v: number) => v.toFixed(1)}
          tick={{ fontSize: 11, fill: '#6b7280' }}
          axisLine={false}
          tickLine={false}
          width={48}
        />
        <Tooltip
          labelFormatter={(label) => fmtDateMMDD(String(label ?? ''))}
          formatter={(value, name) => {
            const v = Number(value) || 0
            if (name === 'ROI') {
              return [v > 0 ? v.toFixed(2) : '--', name] as [string, string]
            }
            return [fmtUsd(v), name] as [string, string]
          }}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
          iconType="circle"
        />
        <Bar
          yAxisId="usd"
          dataKey="spend"
          name="广告消耗"
          fill="#9CA3AF"
          radius={[3, 3, 0, 0]}
          isAnimationActive={false}
        />
        <Bar
          yAxisId="usd"
          dataKey="revenue"
          name="总充值"
          fill={COLORS.sub}
          radius={[3, 3, 0, 0]}
          isAnimationActive={false}
        />
        <ReferenceLine
          yAxisId="roi"
          y={1}
          stroke="#ef4444"
          strokeDasharray="4 4"
          label={{ value: 'ROI=1', position: 'right', fontSize: 10, fill: '#ef4444' }}
        />
        <Line
          yAxisId="roi"
          type="monotone"
          dataKey="roi"
          name="ROI"
          stroke="#10b981"
          strokeWidth={2}
          dot={{ r: 3 }}
          connectNulls
          isAnimationActive={false}
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}
