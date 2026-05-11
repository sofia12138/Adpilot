import { useMemo, useState } from 'react'
import { Loader2, AlertCircle, Calendar, Info } from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { cn } from '@/utils/cn'
import { useOpsStats } from '@/hooks/useOpsStats'
import type { DailyOpsRow, DatePreset, DateRange } from '@/types/ops'
import { KpiCard } from '@/components/ops/KpiCard'
import { RegTrendChart } from '@/components/ops/RegTrendChart'
import { RevenuePlatformChart } from '@/components/ops/RevenuePlatformChart'
import { RevenueTypeChart } from '@/components/ops/RevenueTypeChart'
import { PayerComboChart } from '@/components/ops/PayerComboChart'
import { SpendRoiChart } from '@/components/ops/SpendRoiChart'
import { fmtUsd, calcDelta, calcNetRevenue } from '@/components/ops/formatters'
import { presetToRange, periodLabel, isSingleDay, rangeDisplay } from '@/components/ops/rangeUtils'

// ─── 通用样式 ─────────────────────────────────────
const chartCardCls = 'bg-card border border-card-border rounded-xl p-4'
const chartTitleCls = 'text-sm font-semibold text-gray-700 mb-3'

// ─── 时间范围按钮组 ───────────────────────────────
const PRESET_OPTIONS: { value: DatePreset; label: string }[] = [
  { value: 'yesterday', label: '昨天' },
  { value: 'today',     label: '今天' },
  { value: 'last7',     label: '近7天' },
  { value: 'last14',    label: '近14天' },
  { value: 'last30',    label: '近30天' },
  { value: 'custom',    label: '自定义' },
]

interface RangeSwitchProps {
  range: DateRange
  customDraft: { start: string; end: string }
  onPresetChange: (p: DatePreset) => void
  onCustomChange: (next: { start: string; end: string }) => void
}

function RangeSwitch({ range, customDraft, onPresetChange, onCustomChange }: RangeSwitchProps) {
  return (
    <div className="flex flex-col items-end gap-2">
      <div className="inline-flex bg-muted rounded-lg p-0.5 text-xs">
        {PRESET_OPTIONS.map(opt => (
          <button
            key={opt.value}
            onClick={() => onPresetChange(opt.value)}
            className={cn(
              'px-3 py-1.5 rounded-md transition-colors flex items-center gap-1',
              range.preset === opt.value
                ? 'bg-white text-gray-900 shadow-sm font-medium'
                : 'text-muted-foreground hover:text-gray-700',
            )}
          >
            {opt.value === 'custom' && <Calendar className="w-3 h-3" />}
            {opt.label}
          </button>
        ))}
      </div>

      {range.preset === 'custom' && (
        <div className="inline-flex items-center gap-2 bg-card border border-card-border rounded-lg px-3 py-1.5 text-xs">
          <input
            type="date"
            value={customDraft.start}
            max={customDraft.end || undefined}
            onChange={e => onCustomChange({ ...customDraft, start: e.target.value })}
            className="bg-transparent outline-none text-gray-700"
          />
          <span className="text-muted-foreground">至</span>
          <input
            type="date"
            value={customDraft.end}
            min={customDraft.start || undefined}
            onChange={e => onCustomChange({ ...customDraft, end: e.target.value })}
            className="bg-transparent outline-none text-gray-700"
          />
        </div>
      )}
    </div>
  )
}

// ─── KPI 计算 ────────────────────────────────────
interface Kpis {
  // 用户
  regTotal: number
  regDelta: number | null
  newActive: number

  // 充值
  revenueTotal: number    // USD
  revenueDelta: number | null

  iosRevenue: number
  androidRevenue: number
  iosShare: number   // 0~100

  subRevenue: number
  onetimeRevenue: number
  subShare: number   // 0~100

  // 付费 UV
  //   单日：等于该天 ios_payer_uv + android_payer_uv
  //   多日：区间累计「人次」（同一用户多天付费会被多次计入）
  payerUv: number

  // 投放 / ROI
  /** 当日（或区间累计）广告消耗 USD */
  adSpend: number
  /** D0 流水 ROI = revenueTotal / adSpend，spend=0 时为 null */
  roi: number | null
  /** ROI 较前日变化（仅单日时有值） */
  roiDelta: number | null

  /** 净收入（扣通道费）USD，按 calcNetRevenue 公式 */
  revenueNet: number
  /** 净 ROI = revenueNet / adSpend，spend=0 时为 null */
  roiNet: number | null
  /** 净 ROI 较前日变化（仅单日时有值） */
  roiNetDelta: number | null

  /** 该 KPI 是单日口径还是区间累计 — 用于 subtitle 用词 */
  isAggregate: boolean
}

/**
 * 计算 KPI
 *   rows[0]      = baseline（start - 1 那一天，由 service 多请求一天）
 *   rows[1..n]   = 用户选区间
 *
 *   单日 preset：本期 = rows[1]，上期 = rows[0]，显示当日值 + 较前日 delta
 *   多日区间：本期 = sum(rows[1..n])，上期不算（区间累计无对照），不显示 delta
 *
 * 这样设计避免「近 N 天」KPI 被「末日 = 今天 = 数据未出齐」拉低成全 0。
 */
function computeKpis(rows: DailyOpsRow[], range: DateRange): Kpis | null {
  if (!rows || rows.length === 0) return null

  const baseline = rows[0]
  const main = rows.slice(1).length > 0 ? rows.slice(1) : rows

  if (isSingleDay(range)) {
    const last = main[main.length - 1]
    const prev = rows.length > 1 ? baseline : undefined
    return singleDayKpis(last, prev)
  }
  return aggregateKpis(main)
}

function safeRoi(rev: number, spend: number): number | null {
  if (!isFinite(rev) || !isFinite(spend) || spend <= 0) return null
  return rev / spend
}

function singleDayKpis(last: DailyOpsRow, prev: DailyOpsRow | undefined): Kpis {
  const lastReg = last.new_register_uv || 0
  const prevReg = prev?.new_register_uv

  const lastIosRev      = (last.ios_subscribe_revenue || 0) + (last.ios_onetime_revenue || 0)
  const lastAndroidRev  = (last.android_subscribe_revenue || 0) + (last.android_onetime_revenue || 0)
  const lastSubRev      = (last.ios_subscribe_revenue || 0) + (last.android_subscribe_revenue || 0)
  const lastOnetimeRev  = (last.ios_onetime_revenue || 0) + (last.android_onetime_revenue || 0)
  const lastTotalRev    = lastIosRev + lastAndroidRev
  const lastSpend       = last.ad_spend || 0
  const lastRoi         = safeRoi(lastTotalRev, lastSpend)

  const lastNetRev = calcNetRevenue({
    iosSubscribe: last.ios_subscribe_revenue,
    iosOnetime: last.ios_onetime_revenue,
    androidSubscribe: last.android_subscribe_revenue,
    androidOnetime: last.android_onetime_revenue,
  })
  const lastRoiNet = safeRoi(lastNetRev, lastSpend)

  const prevTotalRev = prev
    ? (prev.ios_subscribe_revenue || 0) + (prev.ios_onetime_revenue || 0)
      + (prev.android_subscribe_revenue || 0) + (prev.android_onetime_revenue || 0)
    : undefined
  const prevSpend = prev?.ad_spend
  const prevRoi = prev != null && prevTotalRev != null && prevSpend != null
    ? safeRoi(prevTotalRev, prevSpend)
    : null

  const prevNetRev = prev
    ? calcNetRevenue({
        iosSubscribe: prev.ios_subscribe_revenue,
        iosOnetime: prev.ios_onetime_revenue,
        androidSubscribe: prev.android_subscribe_revenue,
        androidOnetime: prev.android_onetime_revenue,
      })
    : undefined
  const prevRoiNet = prev != null && prevNetRev != null && prevSpend != null
    ? safeRoi(prevNetRev, prevSpend)
    : null

  return {
    regTotal: lastReg,
    regDelta: calcDelta(lastReg, prevReg),
    newActive: last.new_active_uv || 0,

    revenueTotal: lastTotalRev,
    revenueDelta: calcDelta(lastTotalRev, prevTotalRev),

    iosRevenue: lastIosRev,
    androidRevenue: lastAndroidRev,
    iosShare: lastTotalRev > 0 ? (lastIosRev / lastTotalRev) * 100 : 0,

    subRevenue: lastSubRev,
    onetimeRevenue: lastOnetimeRev,
    subShare: lastTotalRev > 0 ? (lastSubRev / lastTotalRev) * 100 : 0,

    payerUv: (last.ios_payer_uv || 0) + (last.android_payer_uv || 0),

    adSpend: lastSpend,
    roi: lastRoi,
    roiDelta: calcDelta(lastRoi ?? undefined, prevRoi ?? undefined),

    revenueNet: lastNetRev,
    roiNet: lastRoiNet,
    roiNetDelta: calcDelta(lastRoiNet ?? undefined, prevRoiNet ?? undefined),

    isAggregate: false,
  }
}

function aggregateKpis(rows: DailyOpsRow[]): Kpis {
  let reg = 0
  let act = 0
  let iosRev = 0
  let androidRev = 0
  let subRev = 0
  let onetimeRev = 0
  let netRev = 0
  let payerUv = 0
  let spend = 0

  for (const r of rows) {
    reg += r.new_register_uv || 0
    act += r.new_active_uv || 0
    iosRev      += (r.ios_subscribe_revenue || 0) + (r.ios_onetime_revenue || 0)
    androidRev  += (r.android_subscribe_revenue || 0) + (r.android_onetime_revenue || 0)
    subRev      += (r.ios_subscribe_revenue || 0) + (r.android_subscribe_revenue || 0)
    onetimeRev  += (r.ios_onetime_revenue || 0) + (r.android_onetime_revenue || 0)
    netRev      += calcNetRevenue({
      iosSubscribe: r.ios_subscribe_revenue,
      iosOnetime: r.ios_onetime_revenue,
      androidSubscribe: r.android_subscribe_revenue,
      androidOnetime: r.android_onetime_revenue,
    })
    payerUv     += (r.ios_payer_uv || 0) + (r.android_payer_uv || 0)
    spend       += r.ad_spend || 0
  }

  const totalRev = iosRev + androidRev

  return {
    regTotal: reg,
    regDelta: null,
    newActive: act,

    revenueTotal: totalRev,
    revenueDelta: null,

    iosRevenue: iosRev,
    androidRevenue: androidRev,
    iosShare: totalRev > 0 ? (iosRev / totalRev) * 100 : 0,

    subRevenue: subRev,
    onetimeRevenue: onetimeRev,
    subShare: totalRev > 0 ? (subRev / totalRev) * 100 : 0,

    payerUv,

    adSpend: spend,
    roi: safeRoi(totalRev, spend),
    roiDelta: null,

    revenueNet: netRev,
    roiNet: safeRoi(netRev, spend),
    roiNetDelta: null,

    isAggregate: true,
  }
}

// ─── 页面 ─────────────────────────────────────────
export default function OpsDashboard() {
  // 默认近 14 天
  const [range, setRange] = useState<DateRange>(() => presetToRange('last14'))

  // 自定义模式下用户正在编辑的草稿
  const [customDraft, setCustomDraft] = useState<{ start: string; end: string }>(() => ({
    start: presetToRange('last7').start,
    end:   presetToRange('today').end,
  }))

  const handlePresetChange = (preset: DatePreset) => {
    if (preset === 'custom') {
      setRange(presetToRange('custom', customDraft))
    } else {
      setRange(presetToRange(preset))
    }
  }

  const handleCustomChange = (next: { start: string; end: string }) => {
    setCustomDraft(next)
    if (next.start && next.end && next.start <= next.end) {
      setRange({ preset: 'custom', start: next.start, end: next.end })
    }
  }

  const { data, isLoading, isError } = useOpsStats(range)

  // 图表用的数据：去掉 baseline 行，只保留用户选定区间内的天数
  const chartData = useMemo(() => (data && data.length > 1 ? data.slice(1) : data ?? []), [data])

  const kpis = useMemo(() => computeKpis(data ?? [], range), [data, range])
  const period = periodLabel(range)

  return (
    <div className="max-w-7xl mx-auto space-y-5">
      <PageHeader
        title="运营数据"
        description="新注册 / 激活 / 双端充值 / 付费 UV 全景看板 — 仅超管可见"
        action={
          <RangeSwitch
            range={range}
            customDraft={customDraft}
            onPresetChange={handlePresetChange}
            onCustomChange={handleCustomChange}
          />
        }
      />

      {/* 数据口径 + 当前区间 */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 px-3 py-2 rounded-lg bg-blue-50 border border-blue-100 text-xs text-blue-700">
        <div className="inline-flex items-center gap-1.5">
          <Info className="w-3.5 h-3.5 shrink-0" />
          <span className="font-medium">时区:</span>
          <span className="font-mono">America/Los_Angeles (LA)</span>
        </div>
        <div className="inline-flex items-center gap-1.5">
          <span className="font-medium">当前区间:</span>
          <span className="font-mono">{rangeDisplay(range)}</span>
          <span className="text-blue-500/80">· {periodLabel(range)}</span>
        </div>
        <div className="text-blue-500/80">
          T-1 延迟（今天数据次日同步）· 用户侧无 OS 拆分，付费侧已拆 iOS / Android
        </div>
        <div className="text-blue-500/80">
          净 ROI = (订阅 + 内购) × (1 − 通道费) / 广告消耗 · 当前 iOS/Android 订阅与内购通道费均按 15% 计算
        </div>
      </div>

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
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-7 gap-3">
            <KpiCard
              title={kpis.isAggregate ? `${period}累计广告消耗` : `${period}广告消耗`}
              value={fmtUsd(kpis.adSpend)}
              subtitle="全平台合计 (TikTok + Meta)"
            />
            <KpiCard
              title={kpis.isAggregate ? `${period}累计 ROI` : `${period} ROI`}
              value={kpis.roi == null ? '--' : kpis.roi.toFixed(2)}
              delta={kpis.roiDelta}
              subtitle={
                kpis.roi == null
                  ? '当日无消耗'
                  : kpis.roi >= 1
                    ? '盈利 (≥ 1.00)'
                    : '亏损 (< 1.00)'
              }
              valueClassName={
                kpis.roi == null
                  ? undefined
                  : kpis.roi >= 1
                    ? 'text-green-600'
                    : 'text-red-500'
              }
            />
            <KpiCard
              title={kpis.isAggregate ? `${period}累计净 ROI` : `${period}净 ROI`}
              value={kpis.roiNet == null ? '--' : kpis.roiNet.toFixed(2)}
              delta={kpis.roiNetDelta}
              subtitle={
                kpis.roiNet == null
                  ? '当日无消耗'
                  : `净收入 ${fmtUsd(kpis.revenueNet)} · 已扣 15% 通道费`
              }
              valueClassName={
                kpis.roiNet == null
                  ? undefined
                  : kpis.roiNet >= 1
                    ? 'text-green-600'
                    : 'text-red-500'
              }
            />
            <KpiCard
              title={kpis.isAggregate ? `${period}累计总充值` : `${period}总充值`}
              value={fmtUsd(kpis.revenueTotal)}
              delta={kpis.revenueDelta}
              subtitle="iOS + Android · 订阅 + 内购"
            />
            <KpiCard
              title={kpis.isAggregate ? `${period}累计新注册` : `${period}新注册用户`}
              value={kpis.regTotal.toLocaleString()}
              delta={kpis.regDelta}
              subtitle={
                kpis.isAggregate
                  ? `累计新激活 ${kpis.newActive.toLocaleString()} · 累计付费人次 ${kpis.payerUv.toLocaleString()}`
                  : `新激活 ${kpis.newActive.toLocaleString()} · 付费 UV ${kpis.payerUv.toLocaleString()}`
              }
            />
            <KpiCard
              title="iOS / Android"
              value={`iOS ${kpis.iosShare.toFixed(1)}%`}
              subtitle={`iOS ${fmtUsd(kpis.iosRevenue)} / Android ${fmtUsd(kpis.androidRevenue)}`}
            />
            <KpiCard
              title="订阅 / 内购"
              value={`订阅 ${kpis.subShare.toFixed(1)}%`}
              subtitle={`订阅 ${fmtUsd(kpis.subRevenue)} / 内购 ${fmtUsd(kpis.onetimeRevenue)}`}
            />
          </div>

          {/* ── 2) 广告消耗 vs 总充值 + 净 ROI（最重要 — 盈利监控） ── */}
          <div className={chartCardCls}>
            <h3 className={chartTitleCls}>广告消耗 vs 总充值（含 D0 净 ROI）</h3>
            <div style={{ height: 240 }}>
              <SpendRoiChart data={chartData} />
            </div>
          </div>

          {/* ── 3) 用户增长趋势 ── */}
          <div className={chartCardCls}>
            <h3 className={chartTitleCls}>新注册 / 新激活趋势</h3>
            <div style={{ height: 220 }}>
              <RegTrendChart data={chartData} />
            </div>
          </div>

          {/* ── 3) 充值双图（两列等宽） ── */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>iOS / Android 充值（堆叠）</h3>
              <div style={{ height: 220 }}>
                <RevenuePlatformChart data={chartData} />
              </div>
            </div>
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>订阅 / 内购 充值（堆叠）</h3>
              <div style={{ height: 220 }}>
                <RevenueTypeChart data={chartData} />
              </div>
            </div>
          </div>

          {/* ── 4) 订单结构（全宽，4 系列堆叠） ── */}
          <div className={chartCardCls}>
            <h3 className={chartTitleCls}>订单结构（首/复订 × 首/复购）</h3>
            <div style={{ height: 220 }}>
              <PayerComboChart data={chartData} />
            </div>
          </div>
        </>
      )}
    </div>
  )
}
