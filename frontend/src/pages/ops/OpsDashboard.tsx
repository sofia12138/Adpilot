import { useMemo, useState } from 'react'
import { Loader2, AlertCircle } from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { cn } from '@/utils/cn'
import { useOpsStats } from '@/hooks/useOpsStats'
import type { DailyOpsRow, DateRange } from '@/types/ops'
import { KpiCard } from '@/components/ops/KpiCard'
import { RegTrendChart } from '@/components/ops/RegTrendChart'
import { RevenuePlatformChart } from '@/components/ops/RevenuePlatformChart'
import { RevenueTypeChart } from '@/components/ops/RevenueTypeChart'
import { PayerComboChart } from '@/components/ops/PayerComboChart'
import { fmtCentsToWan, calcDelta } from '@/components/ops/formatters'

// ─── 通用样式 ─────────────────────────────────────
const chartCardCls = 'bg-card border border-card-border rounded-xl p-4'
const chartTitleCls = 'text-sm font-semibold text-gray-700 mb-3'

// ─── 时间范围按钮组 ───────────────────────────────
const RANGE_OPTIONS: { value: DateRange; label: string }[] = [
  { value: 7,  label: '7天' },
  { value: 14, label: '14天' },
  { value: 30, label: '30天' },
]

function RangeSwitch({ value, onChange }: { value: DateRange; onChange: (v: DateRange) => void }) {
  return (
    <div className="inline-flex bg-muted rounded-lg p-0.5 text-xs">
      {RANGE_OPTIONS.map(opt => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={cn(
            'px-3 py-1.5 rounded-md transition-colors',
            value === opt.value
              ? 'bg-white text-gray-900 shadow-sm font-medium'
              : 'text-muted-foreground hover:text-gray-700',
          )}
        >
          {opt.label}
        </button>
      ))}
    </div>
  )
}

// ─── KPI 计算（取 data 末尾两条对比） ────────────
interface Kpis {
  todayRegTotal: number
  regDelta: number | null
  regIos: number
  regAndroid: number

  todayRevenueTotal: number
  revenueDelta: number | null

  iosRevenueToday: number
  androidRevenueToday: number
  iosShare: number   // 0~100

  subRevenueToday: number
  onetimeRevenueToday: number
  subShare: number   // 0~100
}

function computeKpis(rows: DailyOpsRow[]): Kpis | null {
  if (!rows || rows.length === 0) return null
  const last = rows[rows.length - 1]
  const prev = rows.length >= 2 ? rows[rows.length - 2] : undefined

  const lastRegTotal = (last.ios_new_users || 0) + (last.android_new_users || 0)
  const prevRegTotal = prev ? (prev.ios_new_users || 0) + (prev.android_new_users || 0) : undefined

  const lastIosRevenue = (last.ios_sub_revenue || 0) + (last.ios_onetime_revenue || 0)
  const lastAndroidRevenue = (last.android_sub_revenue || 0) + (last.android_onetime_revenue || 0)
  const lastSubRevenue = (last.ios_sub_revenue || 0) + (last.android_sub_revenue || 0)
  const lastOnetimeRevenue = (last.ios_onetime_revenue || 0) + (last.android_onetime_revenue || 0)
  const lastRevenueTotal = lastIosRevenue + lastAndroidRevenue

  const prevRevenueTotal = prev
    ? (prev.ios_sub_revenue || 0) + (prev.ios_onetime_revenue || 0)
      + (prev.android_sub_revenue || 0) + (prev.android_onetime_revenue || 0)
    : undefined

  return {
    todayRegTotal: lastRegTotal,
    regDelta: calcDelta(lastRegTotal, prevRegTotal),
    regIos: last.ios_new_users || 0,
    regAndroid: last.android_new_users || 0,

    todayRevenueTotal: lastRevenueTotal,
    revenueDelta: calcDelta(lastRevenueTotal, prevRevenueTotal),

    iosRevenueToday: lastIosRevenue,
    androidRevenueToday: lastAndroidRevenue,
    iosShare: lastRevenueTotal > 0 ? (lastIosRevenue / lastRevenueTotal) * 100 : 0,

    subRevenueToday: lastSubRevenue,
    onetimeRevenueToday: lastOnetimeRevenue,
    subShare: lastRevenueTotal > 0 ? (lastSubRevenue / lastRevenueTotal) * 100 : 0,
  }
}

// ─── 页面 ─────────────────────────────────────────
export default function OpsDashboard() {
  const [activeRange, setActiveRange] = useState<DateRange>(14)
  const { data, isLoading, isError } = useOpsStats(activeRange)

  const kpis = useMemo(() => computeKpis(data ?? []), [data])

  return (
    <div className="max-w-7xl mx-auto space-y-5">
      <PageHeader
        title="运营数据"
        description="iOS / Android 双端注册、充值、付费人数趋势 — 仅超管可见"
        action={<RangeSwitch value={activeRange} onChange={setActiveRange} />}
      />

      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中...</span>
        </div>
      )}

      {isError && (
        <div className="flex flex-col items-center justify-center py-24 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" />
          <p className="text-sm font-medium">数据加载失败</p>
        </div>
      )}

      {!isLoading && !isError && kpis && (
        <>
          {/* ── 1) KPI 卡片行 ── */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
            <KpiCard
              title="今日新注册用户"
              value={kpis.todayRegTotal.toLocaleString()}
              delta={kpis.regDelta}
              subtitle={`iOS ${kpis.regIos.toLocaleString()} / Android ${kpis.regAndroid.toLocaleString()}`}
            />
            <KpiCard
              title="今日总充值"
              value={fmtCentsToWan(kpis.todayRevenueTotal)}
              delta={kpis.revenueDelta}
              subtitle="iOS + Android · 订阅 + 普通"
            />
            <KpiCard
              title="iOS / Android"
              value={`iOS ${kpis.iosShare.toFixed(1)}%`}
              subtitle={`iOS ${fmtCentsToWan(kpis.iosRevenueToday)} / Android ${fmtCentsToWan(kpis.androidRevenueToday)}`}
            />
            <KpiCard
              title="订阅 / 普通"
              value={`订阅 ${kpis.subShare.toFixed(1)}%`}
              subtitle={`订阅 ${fmtCentsToWan(kpis.subRevenueToday)} / 普通 ${fmtCentsToWan(kpis.onetimeRevenueToday)}`}
            />
          </div>

          {/* ── 2) 注册趋势（全宽） ── */}
          <div className={chartCardCls}>
            <h3 className={chartTitleCls}>新注册用户趋势</h3>
            <div style={{ height: 220 }}>
              <RegTrendChart data={data!} />
            </div>
          </div>

          {/* ── 3) 充值双图（两列等宽） ── */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>iOS / Android 充值（堆叠）</h3>
              <div style={{ height: 220 }}>
                <RevenuePlatformChart data={data!} />
              </div>
            </div>
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>订阅 / 普通 充值（堆叠）</h3>
              <div style={{ height: 220 }}>
                <RevenueTypeChart data={data!} />
              </div>
            </div>
          </div>

          {/* ── 4) 付费人数构成（全宽，4 系列堆叠） ── */}
          <div className={chartCardCls}>
            <h3 className={chartTitleCls}>付费人数构成（平台 × 类型）</h3>
            <div style={{ height: 220 }}>
              <PayerComboChart data={data!} />
            </div>
          </div>
        </>
      )}
    </div>
  )
}
