import { useMemo, useState } from 'react'
import { Loader2, AlertCircle, Info } from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { useOpsRegionStats } from '@/hooks/useOpsRegionStats'
import type { DatePreset, DateRange } from '@/types/ops'
import type { OsView } from '@/types/opsRegion'
import { KpiCard } from '@/components/ops/KpiCard'
import { fmtUsd } from '@/components/ops/formatters'
import {
  presetToRange, periodLabel, rangeDisplay,
} from '@/components/ops/rangeUtils'
import { RangeSwitch } from '@/components/ops/RangeSwitch'
import { ChannelStackedBar } from '@/components/ops/region/ChannelStackedBar'
import { CountryTopBars } from '@/components/ops/region/CountryTopBars'
import { CountryRegisterTable } from '@/components/ops/region/CountryRegisterTable'
import { CountryRevenueTable } from '@/components/ops/region/CountryRevenueTable'
import {
  aggregateCountryRegister,
  aggregateCountryRevenue,
  buildRegisterTrend,
  buildRevenueTrend,
  collapseRegisterRows,
  collapseRevenueRows,
  collectDates,
  mergeTopN,
  summarizeRegister,
  summarizeRevenue,
} from '@/components/ops/region/reshape'
import { OpsTabs } from './OpsTabs'
import { cn } from '@/utils/cn'
import { ApiError } from '@/services/api'

const chartCardCls = 'bg-card border border-card-border rounded-xl p-4'
const chartTitleCls = 'text-sm font-semibold text-gray-700 mb-3'

// ─── 页面 ─────────────────────────────────────────────────
export default function RegionChannelPanel() {
  // 默认近 14 天
  const [range, setRange] = useState<DateRange>(() => presetToRange('last14'))
  const [customDraft, setCustomDraft] = useState<{ start: string; end: string }>(() => ({
    start: presetToRange('last7').start,
    end: presetToRange('today').end,
  }))
  const [osView, setOsView] = useState<OsView>('all')

  const handlePresetChange = (preset: DatePreset) => {
    if (preset === 'custom') setRange(presetToRange('custom', customDraft))
    else setRange(presetToRange(preset))
  }
  const handleCustomChange = (next: { start: string; end: string }) => {
    setCustomDraft(next)
    if (next.start && next.end && next.start <= next.end) {
      setRange({ preset: 'custom', start: next.start, end: next.end })
    }
  }

  const { data, isLoading, isError, error } = useOpsRegionStats(range)

  // ─── 数据 reshape ───
  const registerRows = data?.register_rows ?? []
  const revenueRows = data?.revenue_rows ?? []

  const allDates = useMemo(
    () => collectDates(registerRows, revenueRows, range.start, range.end),
    [registerRows, revenueRows, range.start, range.end],
  )

  const regSummary = useMemo(() => summarizeRegister(registerRows), [registerRows])
  const revSummary = useMemo(() => summarizeRevenue(revenueRows, osView), [revenueRows, osView])

  const registerTrend = useMemo(
    () => buildRegisterTrend(registerRows, allDates),
    [registerRows, allDates],
  )
  const revenueTrend = useMemo(
    () => buildRevenueTrend(revenueRows, allDates, osView),
    [revenueRows, allDates, osView],
  )

  const countryRegister = useMemo(() => aggregateCountryRegister(registerRows), [registerRows])
  const countryRevenue = useMemo(
    () => aggregateCountryRevenue(revenueRows, osView),
    [revenueRows, osView],
  )

  // Top 10 + OTHER 合并
  const top10Register = useMemo(
    () => mergeTopN(countryRegister, 10, collapseRegisterRows),
    [countryRegister],
  )
  const top10Revenue = useMemo(
    () => mergeTopN(countryRevenue, 10, collapseRevenueRows),
    [countryRevenue],
  )

  const period = periodLabel(range)
  const isAggregate = range.start !== range.end

  // KPI 占比
  const nonOrganicReg = regSummary.total - regSummary.byKind.organic
  const organicShareReg = regSummary.total > 0
    ? (regSummary.byKind.organic / regSummary.total) * 100 : 0

  const nonOrganicRev = revSummary.totalUsd - revSummary.byKind.organic
  const organicShareRev = revSummary.totalUsd > 0
    ? (revSummary.byKind.organic / revSummary.totalUsd) * 100 : 0

  return (
    <div className="max-w-7xl mx-auto space-y-5">
      <PageHeader
        title="运营数据"
        description="区域渠道分析 — 自然量 vs 非自然量 + 各国家明细 — 仅超管可见"
        action={
          <RangeSwitch
            range={range}
            customDraft={customDraft}
            onPresetChange={handlePresetChange}
            onCustomChange={handleCustomChange}
          />
        }
      />

      <div className="flex items-center justify-between gap-3">
        <OpsTabs />
        <OsTabs value={osView} onChange={setOsView} />
      </div>

      {/* 数据口径提示 */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 px-3 py-2 rounded-lg bg-blue-50 border border-blue-100 text-xs text-blue-700">
        <div className="inline-flex items-center gap-1.5">
          <Info className="w-3.5 h-3.5 shrink-0" />
          <span className="font-medium">时区:</span>
          <span className="font-mono">America/Los_Angeles (LA)</span>
        </div>
        <div className="inline-flex items-center gap-1.5">
          <span className="font-medium">当前区间:</span>
          <span className="font-mono">{rangeDisplay(range)}</span>
          <span className="text-blue-500/80">· {period}</span>
        </div>
        <div className="text-blue-500/80">
          自然量 = 总注册 − channel_user 命中（含真自然量+SEO+品牌词）·
          注册侧无 OS 维度（dim_user_df 不带设备字段）·
          今/昨日数据 30min 实时刷新
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
          {error instanceof Error && (
            <p className="text-xs text-red-300/90 mt-2 max-w-lg text-center font-mono break-all">
              {error.message}
            </p>
          )}
          {error instanceof ApiError && error.status === 403 && (
            <p className="text-xs text-slate-500 mt-2 max-w-md text-center">
              需要账号具备「运营数据」面板权限；本地请用 super_admin，或在用户权限里勾选运营数据。
            </p>
          )}
          {(error instanceof TypeError
            || (error instanceof Error && /fetch|network|load failed/i.test(error.message))) && (
            <p className="text-xs text-slate-500 mt-2 max-w-md text-center">
              请确认后端已在 <span className="font-mono">127.0.0.1:8000</span> 启动（与 Vite 代理一致）。
            </p>
          )}
        </div>
      )}

      {!isLoading && !isError && data && (
        <>
          {/* ── KPI 卡片行（6 张） ── */}
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            <KpiCard
              title={`${period}自然量注册`}
              value={regSummary.byKind.organic.toLocaleString()}
              subtitle={`占比 ${organicShareReg.toFixed(1)}%`}
              valueClassName="text-emerald-600"
            />
            <KpiCard
              title={`${period}非自然量注册`}
              value={nonOrganicReg.toLocaleString()}
              subtitle={
                `TT ${regSummary.byKind.tiktok.toLocaleString()} · ` +
                `Meta ${regSummary.byKind.meta.toLocaleString()} · ` +
                `其它 ${regSummary.byKind.other.toLocaleString()}`
              }
            />
            <KpiCard
              title={isAggregate ? `${period}累计总注册` : `${period}总注册`}
              value={regSummary.total.toLocaleString()}
              subtitle={`覆盖国家 ${countryRegister.length}`}
            />
            <KpiCard
              title={`${period}自然量充值`}
              value={fmtUsd(revSummary.byKind.organic)}
              subtitle={`占比 ${organicShareRev.toFixed(1)}%`}
              valueClassName="text-emerald-600"
            />
            <KpiCard
              title={`${period}非自然量充值`}
              value={fmtUsd(nonOrganicRev)}
              subtitle={
                `TT ${fmtUsd(revSummary.byKind.tiktok)} · ` +
                `Meta ${fmtUsd(revSummary.byKind.meta)} · ` +
                `其它 ${fmtUsd(revSummary.byKind.other)}`
              }
            />
            <KpiCard
              title={isAggregate ? `${period}累计总充值` : `${period}总充值`}
              value={fmtUsd(revSummary.totalUsd)}
              subtitle={
                `付费UV ${revSummary.payerUv.toLocaleString()} · ` +
                `订单 ${revSummary.orderCnt.toLocaleString()}`
              }
            />
          </div>

          {/* ── 趋势图（注册 / 充值 双列） ── */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>每日注册分布（按渠道堆叠）</h3>
              <div style={{ height: 240 }}>
                <ChannelStackedBar data={registerTrend} unit=" 人" />
              </div>
            </div>
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>
                每日充值分布（按渠道堆叠）
                <span className="ml-2 text-xs font-normal text-muted-foreground">
                  {osView === 'all' ? '全平台' : osView === 'ios' ? 'iOS 端' : 'Android 端'}
                </span>
              </h3>
              <div style={{ height: 240 }}>
                <ChannelStackedBar
                  data={revenueTrend}
                  formatValue={(v) => fmtUsd(v)}
                />
              </div>
            </div>
          </div>

          {/* ── Top 10 国家双柱图 ── */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>Top 10 国家注册分布</h3>
              <div style={{ height: 360 }}>
                <CountryTopBars data={top10Register} unit=" 人" />
              </div>
            </div>
            <div className={chartCardCls}>
              <h3 className={chartTitleCls}>
                Top 10 国家充值分布
                <span className="ml-2 text-xs font-normal text-muted-foreground">
                  {osView === 'all' ? '全平台' : osView === 'ios' ? 'iOS 端' : 'Android 端'}
                </span>
              </h3>
              <div style={{ height: 360 }}>
                <CountryTopBars
                  data={top10Revenue.map(r => ({
                    region: r.region,
                    organic: r.organicUsd,
                    tiktok: r.tiktokUsd,
                    meta: r.metaUsd,
                    other: r.otherUsd,
                  }))}
                  formatValue={(v) => fmtUsd(v)}
                />
              </div>
            </div>
          </div>

          {/* ── 各国家注册情况表 ── */}
          <div className={chartCardCls + ' p-0'}>
            <div className="flex items-center justify-between px-4 py-3 border-b border-card-border">
              <h3 className="text-sm font-semibold text-gray-700">各国家注册情况</h3>
              <div className="text-xs text-muted-foreground">共 {countryRegister.length} 个国家</div>
            </div>
            <CountryRegisterTable rows={countryRegister} />
          </div>

          {/* ── 各国家充值情况表 ── */}
          <div className={chartCardCls + ' p-0'}>
            <div className="flex items-center justify-between px-4 py-3 border-b border-card-border">
              <h3 className="text-sm font-semibold text-gray-700">
                各国家充值情况
                <span className="ml-2 text-xs font-normal text-muted-foreground">
                  {osView === 'all' ? '全平台' : osView === 'ios' ? 'iOS 端' : 'Android 端'}
                </span>
              </h3>
              <div className="text-xs text-muted-foreground">共 {countryRevenue.length} 个国家</div>
            </div>
            <CountryRevenueTable rows={countryRevenue} />
          </div>
        </>
      )}
    </div>
  )
}


// ─── 内部：充值侧 OS Tab ───
function OsTabs({ value, onChange }: { value: OsView; onChange: (v: OsView) => void }) {
  const items: { v: OsView; label: string }[] = [
    { v: 'all', label: '全平台' },
    { v: 'ios', label: 'iOS' },
    { v: 'android', label: 'Android' },
  ]
  return (
    <div className="inline-flex bg-muted rounded-lg p-0.5 text-xs">
      {items.map(it => (
        <button
          key={it.v}
          onClick={() => onChange(it.v)}
          className={cn(
            'px-3 py-1.5 rounded-md transition-colors',
            value === it.v
              ? 'bg-white text-gray-900 shadow-sm font-medium'
              : 'text-muted-foreground hover:text-gray-700',
          )}
        >
          {it.label}
        </button>
      ))}
    </div>
  )
}
