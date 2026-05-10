import {
  ResponsiveContainer, AreaChart,
  Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { DailyOpsRow } from '@/types/ops'
import { COLORS } from './chartColors'
import { fmtDateMMDD, pickTicks } from './formatters'

/**
 * 用户增长趋势图（双线 + 面积填充）
 *
 * 系列（用户侧无 OS 拆分）：
 *   - 新注册账号 UV (new_register_uv)
 *   - 新激活 UV     (new_active_uv)
 *
 * 实现：用 <Area> 同时承担 stroke（折线）+ fill（渐变面积）的角色，避免
 * 之前 Area + Line 双系列同 dataKey 导致 Tooltip 显示 2 倍行数的问题。
 */
export function RegTrendChart({ data }: { data: DailyOpsRow[] }) {
  const ticks = pickTicks(data.map(d => d.date), 7)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={data} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="grad-reg-register" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={COLORS.ios} stopOpacity={0.28} />
            <stop offset="100%" stopColor={COLORS.ios} stopOpacity={0} />
          </linearGradient>
          <linearGradient id="grad-reg-active" x1="0" y1="0" x2="0" y2="1">
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
          formatter={(v, name) => [(Number(v) || 0).toLocaleString(), name] as [string, string]}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
          iconType="circle"
        />
        <Area
          type="monotone"
          dataKey="new_register_uv"
          name="新注册"
          stroke={COLORS.ios}
          strokeWidth={2}
          fill="url(#grad-reg-register)"
          activeDot={{ r: 4 }}
          isAnimationActive={false}
        />
        <Area
          type="monotone"
          dataKey="new_active_uv"
          name="新激活"
          stroke={COLORS.android}
          strokeWidth={2}
          fill="url(#grad-reg-active)"
          activeDot={{ r: 4 }}
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}
