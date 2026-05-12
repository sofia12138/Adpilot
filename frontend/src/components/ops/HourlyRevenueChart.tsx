import { useMemo } from 'react'
import {
  ResponsiveContainer, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import type { HourlyRevenueResponse } from '@/services/opsService'
import { fmtUsd } from './formatters'

/**
 * 分时段（LA 小时）充值趋势图
 *
 * 每条线 = 1 天，X 轴 0~23 小时（LA），Y 轴 = 当小时新增 USD。
 * 最近一天（"今日"）高亮加粗 + 实线；其余日期半透明虚线。
 *
 * 数据契约：data.series 已被后端补齐到每日 24 个 bucket（缺数填 0）。
 */
interface Props {
  /** 后端 GET /api/ops/hourly-revenue 直接响应 */
  data: HourlyRevenueResponse | null
  /** 选哪个指标：total / android / ios / sub / iap（默认 total） */
  metric?: 'total_usd' | 'android_usd' | 'ios_usd' | 'sub_usd' | 'iap_usd'
}

// 多日对比的调色板（最近一天用第一个）
const PALETTE = [
  '#378ADD',  // 今日 — iOS 蓝（与现有面板一致）
  '#7F77DD',  // 紫
  '#EF9F27',  // 橙
  '#3BC99A',  // 绿
  '#EC7C92',  // 粉
  '#9CA3AF',  // 灰
  '#6366F1',  // 靛
]

export function HourlyRevenueChart({ data, metric = 'total_usd' }: Props) {
  /**
   * 把 series（每天 24 项）转成 recharts 友好的「24 行 × N 列」表格：
   *   [{ h: 0, '5/06': 0,  '5/07': 0,  ..., '5/12': 9.99 }, { h: 1, ... }, ...]
   * Recharts 会按 h 在 X 轴上画 N 条线
   */
  const chartData = useMemo(() => {
    if (!data || !data.series.length) return []
    const rows: Array<Record<string, number | string>> = []
    for (let h = 0; h < 24; h++) {
      const row: Record<string, number | string> = { h }
      for (const day of data.series) {
        const bucket = day.hours.find(b => b.h === h)
        row[day.ds] = bucket ? (bucket[metric] || 0) : 0
      }
      rows.push(row)
    }
    return rows
  }, [data, metric])

  const days = data?.series.map(s => s.ds) ?? []
  // 最近一天放在数组末尾时 z-index 高（recharts 后画的覆盖先画的）→ 倒序遍历让今日在最上
  const orderedDays = [...days].reverse()

  if (!data || days.length === 0) {
    return <EmptyHourlyChart />
  }

  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={chartData} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
        <XAxis
          dataKey="h"
          tickFormatter={(h: number) => `${String(h).padStart(2, '0')}:00`}
          ticks={[0, 4, 8, 12, 16, 20, 23]}
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
          labelFormatter={(label) => `LA ${String(label).padStart(2, '0')}:00`}
          formatter={(v, name) => [fmtUsd(Number(v) || 0), fmtDay(String(name))] as [string, string]}
          contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
        />
        <Legend
          wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
          iconType="circle"
          formatter={(val) => fmtDay(String(val))}
        />
        {orderedDays.map((ds, idx) => {
          // 倒序时 idx=0 才是最新一天
          const isLatest = idx === 0
          const color = PALETTE[idx % PALETTE.length]
          return (
            <Line
              key={ds}
              type="monotone"
              dataKey={ds}
              stroke={color}
              strokeWidth={isLatest ? 2.5 : 1.5}
              strokeDasharray={isLatest ? '0' : '4 3'}
              strokeOpacity={isLatest ? 1 : 0.55}
              dot={isLatest ? { r: 3, fill: color, strokeWidth: 0 } : false}
              activeDot={{ r: 4 }}
              isAnimationActive={false}
            />
          )
        })}
      </LineChart>
    </ResponsiveContainer>
  )
}

function EmptyHourlyChart() {
  return (
    <div className="h-full w-full flex items-center justify-center text-sm text-muted-foreground">
      暂无数据
    </div>
  )
}

/** 'YYYY-MM-DD' → 'MM/DD' */
function fmtDay(ds: string): string {
  if (!ds || ds.length < 10) return ds
  return `${ds.slice(5, 7)}/${ds.slice(8, 10)}`
}
