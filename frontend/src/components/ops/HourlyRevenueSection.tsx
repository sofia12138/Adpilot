import { useEffect, useMemo, useState } from 'react'
import { Loader2, AlertCircle, RefreshCw, TrendingUp, Calendar } from 'lucide-react'
import { cn } from '@/utils/cn'
import { fetchHourlyRevenue, type HourlyRevenueResponse } from '@/services/opsService'
import { HourlyRevenueChart } from './HourlyRevenueChart'
import { fmtUsd } from './formatters'

/**
 * 分时段（LA 小时）充值趋势区块
 *
 * 自管理数据：组件内拉 /api/ops/hourly-revenue，独立于父面板的 useOpsStats。
 * 这样设计避免污染父面板的 KPI 计算，且支持单独刷新（实时性更敏感）。
 *
 * 时间窗：支持 今天 / 昨天 / 近7/14/30 天 / 自定义（最多 31 天，与后端校验一致）
 */

type RangePreset = 'today' | 'yesterday' | 'last7' | 'last14' | 'last30' | 'custom'
type MetricOption = 'total_usd' | 'android_usd' | 'ios_usd' | 'sub_usd' | 'iap_usd'

const METRIC_OPTIONS: { value: MetricOption; label: string }[] = [
  { value: 'total_usd',   label: '总充值' },
  { value: 'sub_usd',     label: '订阅' },
  { value: 'iap_usd',     label: 'IAP' },
  { value: 'ios_usd',     label: 'iOS' },
  { value: 'android_usd', label: 'Android' },
]

const PRESET_OPTIONS: { value: RangePreset; label: string }[] = [
  { value: 'today',     label: '今天' },
  { value: 'yesterday', label: '昨天' },
  { value: 'last7',     label: '近 7 天' },
  { value: 'last14',    label: '近 14 天' },
  { value: 'last30',    label: '近 30 天' },
  { value: 'custom',    label: '自定义' },
]

const MAX_CUSTOM_DAYS = 31  // 与后端 hourly-revenue 接口校验一致

export function HourlyRevenueSection() {
  const [preset, setPreset] = useState<RangePreset>('last7')
  // 自定义日期草稿（仅 preset=custom 时使用）；默认 = 今天 / 昨天 ~ 今天
  const [customDraft, setCustomDraft] = useState(() => {
    const today = laToday()
    return { start: laShift(today, -6), end: today }
  })
  const [metric, setMetric] = useState<MetricOption>('total_usd')

  const [data, setData] = useState<HourlyRevenueResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)

  // 计算实际请求的时间窗
  const { startDate, endDate, windowDays, validateError } = useMemo(
    () => resolveRange(preset, customDraft),
    [preset, customDraft],
  )

  useEffect(() => {
    if (validateError) {
      setData(null)
      setErr(validateError)
      return
    }
    let alive = true
    setLoading(true)
    setErr(null)
    fetchHourlyRevenue(startDate, endDate)
      .then(res => { if (alive) setData(res) })
      .catch(e => { if (alive) setErr(e?.message || '加载失败') })
      .finally(() => { if (alive) setLoading(false) })
    return () => { alive = false }
  }, [startDate, endDate, validateError, refreshKey])

  const summary = useMemo(
    () => computeSummary(data, metric),
    [data, metric],
  )

  const isSingleDay = windowDays === 1
  const focusDayLabel = preset === 'today' ? '今日'
    : preset === 'yesterday' ? '昨日'
    : '最近一天'

  return (
    <div className="bg-card border border-card-border rounded-xl p-4">
      <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <TrendingUp className="w-4 h-4 text-gray-600" />
          <h3 className="text-sm font-semibold text-gray-700">分时段充值趋势（LA 时区，实时）</h3>
          {data?.days?.length ? (
            <span className="text-xs text-muted-foreground">
              {data.days[0]}{data.days.length > 1 ? ` ~ ${data.days[data.days.length - 1]}` : ''}
            </span>
          ) : null}
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          {/* 指标切换 */}
          <div className="inline-flex bg-muted rounded-lg p-0.5 text-xs">
            {METRIC_OPTIONS.map(opt => (
              <button
                key={opt.value}
                onClick={() => setMetric(opt.value)}
                className={cn(
                  'px-2.5 py-1 rounded-md transition-colors',
                  metric === opt.value
                    ? 'bg-white text-gray-900 shadow-sm font-medium'
                    : 'text-muted-foreground hover:text-gray-700',
                )}
              >
                {opt.label}
              </button>
            ))}
          </div>
          {/* 区间预设切换 */}
          <div className="inline-flex bg-muted rounded-lg p-0.5 text-xs">
            {PRESET_OPTIONS.map(opt => (
              <button
                key={opt.value}
                onClick={() => setPreset(opt.value)}
                className={cn(
                  'px-2.5 py-1 rounded-md transition-colors flex items-center gap-1',
                  preset === opt.value
                    ? 'bg-white text-gray-900 shadow-sm font-medium'
                    : 'text-muted-foreground hover:text-gray-700',
                )}
              >
                {opt.value === 'custom' && <Calendar className="w-3 h-3" />}
                {opt.label}
              </button>
            ))}
          </div>
          <button
            onClick={() => setRefreshKey(k => k + 1)}
            className="p-1.5 rounded-md hover:bg-muted text-muted-foreground hover:text-gray-700"
            title="刷新"
            disabled={loading}
          >
            <RefreshCw className={cn('w-3.5 h-3.5', loading && 'animate-spin')} />
          </button>
        </div>
      </div>

      {/* 自定义日期范围输入 */}
      {preset === 'custom' && (
        <div className="mb-3 flex items-center gap-2 flex-wrap">
          <div className="inline-flex items-center gap-2 bg-muted/40 border border-card-border rounded-lg px-3 py-1.5 text-xs">
            <Calendar className="w-3.5 h-3.5 text-muted-foreground" />
            <input
              type="date"
              value={customDraft.start}
              max={customDraft.end || undefined}
              onChange={e => setCustomDraft(d => ({ ...d, start: e.target.value }))}
              className="bg-transparent outline-none text-gray-700"
            />
            <span className="text-muted-foreground">至</span>
            <input
              type="date"
              value={customDraft.end}
              min={customDraft.start || undefined}
              max={laToday()}
              onChange={e => setCustomDraft(d => ({ ...d, end: e.target.value }))}
              className="bg-transparent outline-none text-gray-700"
            />
          </div>
          <span className="text-[11px] text-muted-foreground">
            最多 {MAX_CUSTOM_DAYS} 天，LA 时区
          </span>
        </div>
      )}

      {/* 摘要小卡（单日 / 多日两种布局） */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        {isSingleDay ? (
          <>
            <MiniKpi
              label={`${focusDayLabel}累计`}
              value={fmtUsd(summary.focusTotal)}
              hint={summary.focusHourLabel}
              highlight
            />
            <MiniKpi
              label="峰值时段"
              value={summary.peakHour}
              hint={fmtUsd(summary.peakHourUsd)}
            />
            <MiniKpi
              label="峰值占比"
              value={summary.peakSharePct}
              hint="峰值小时 / 全天"
            />
            <MiniKpi
              label="活跃小时"
              value={`${summary.activeHours}`}
              hint="该日产生过付费的小时数"
            />
          </>
        ) : (
          <>
            <MiniKpi
              label={`${focusDayLabel}累计`}
              value={fmtUsd(summary.focusTotal)}
              hint={summary.focusHourLabel}
              highlight
            />
            <MiniKpi
              label={`${windowDays - 1} 日均日`}
              value={fmtUsd(summary.avgDaily)}
              hint={`${summary.completeDays} 个对比日`}
            />
            <MiniKpi
              label={`${focusDayLabel}峰值时段`}
              value={summary.peakHour}
              hint={fmtUsd(summary.peakHourUsd)}
            />
            <MiniKpi
              label={`${focusDayLabel} vs 均日`}
              value={summary.focusVsAvgPct}
              hint={summary.focusVsAvgHint}
            />
          </>
        )}
      </div>

      <div className="h-[280px]">
        {err ? (
          <div className="h-full w-full flex flex-col items-center justify-center text-sm text-red-600 gap-2">
            <AlertCircle className="w-5 h-5" />
            {err}
          </div>
        ) : loading && !data ? (
          <div className="h-full w-full flex items-center justify-center text-muted-foreground">
            <Loader2 className="w-4 h-4 animate-spin mr-2" />
            加载中…
          </div>
        ) : (
          <HourlyRevenueChart data={data} metric={metric} />
        )}
      </div>

      <p className="text-[11px] text-muted-foreground mt-2">
        {isSingleDay
          ? '展示该日逐小时新增充值；峰值小时高亮在 KPI 卡。'
          : '每条线 = 1 天的逐小时新增充值；最近一天实线高亮、历史半透明虚线。'}
        {' '}数据源：PolarDB recharge_order（实时，&lt;30s 延迟）。
      </p>
    </div>
  )
}

function MiniKpi({ label, value, hint, highlight }: {
  label: string
  value: string
  hint?: string
  highlight?: boolean
}) {
  return (
    <div className={cn(
      'border rounded-lg px-3 py-2',
      highlight ? 'bg-blue-50 border-blue-200' : 'bg-muted/30 border-card-border',
    )}>
      <div className="text-[11px] text-muted-foreground">{label}</div>
      <div className={cn('text-lg font-semibold', highlight ? 'text-blue-700' : 'text-gray-800')}>
        {value}
      </div>
      {hint ? <div className="text-[11px] text-muted-foreground mt-0.5">{hint}</div> : null}
    </div>
  )
}

// ─── 摘要计算 ─────────────────────────────────────────
interface Summary {
  // 多日/单日通用：focus 指的是"重点关注的那天" — 多日时是最近一天，单日时是该天
  focusTotal: number
  focusHourLabel: string
  peakHour: string
  peakHourUsd: number
  // 单日专用
  activeHours: number
  peakSharePct: string
  // 多日专用
  avgDaily: number
  completeDays: number
  focusVsAvgPct: string
  focusVsAvgHint: string
}

function computeSummary(
  data: HourlyRevenueResponse | null,
  metric: MetricOption,
): Summary {
  const empty: Summary = {
    focusTotal: 0, focusHourLabel: '—',
    peakHour: '—', peakHourUsd: 0,
    activeHours: 0, peakSharePct: '—',
    avgDaily: 0, completeDays: 0,
    focusVsAvgPct: '—', focusVsAvgHint: '',
  }
  if (!data || data.series.length === 0) return empty

  const focus = data.series[data.series.length - 1]
  const history = data.series.slice(0, -1)

  const sumDay = (d: typeof focus) =>
    d.hours.reduce((acc, h) => acc + (h[metric] || 0), 0)

  const focusTotal = sumDay(focus)

  // 重点日峰值 + 活跃小时 + 最后入帐小时
  let peakH = -1, peakV = -1, lastHourWithData = -1, activeHours = 0
  for (const b of focus.hours) {
    const v = b[metric] || 0
    if (v > 0) {
      activeHours++
      if (b.h > lastHourWithData) lastHourWithData = b.h
    }
    if (v > peakV) { peakV = v; peakH = b.h }
  }
  const peakHour = peakH < 0 || peakV <= 0 ? '—' : `LA ${String(peakH).padStart(2, '0')}:00`
  const focusHourLabel = lastHourWithData < 0
    ? '该日暂无数据'
    : `截至 LA ${String(lastHourWithData).padStart(2, '0')}:59`

  const peakSharePct = focusTotal > 0 && peakV > 0
    ? `${(peakV / focusTotal * 100).toFixed(1)}%`
    : '—'

  // 多日相关：平均
  let avgDaily = 0
  let focusVsAvgPct = '—'
  let focusVsAvgHint = ''
  if (history.length > 0) {
    avgDaily = history.reduce((acc, d) => acc + sumDay(d), 0) / history.length
    if (avgDaily > 0) {
      const pct = (focusTotal - avgDaily) / avgDaily * 100
      const sign = pct >= 0 ? '+' : ''
      focusVsAvgPct = `${sign}${pct.toFixed(1)}%`
      focusVsAvgHint = pct >= 0 ? '高于均日' : '低于均日'
    }
  }

  return {
    focusTotal,
    focusHourLabel,
    peakHour,
    peakHourUsd: peakV < 0 ? 0 : peakV,
    activeHours,
    peakSharePct,
    avgDaily,
    completeDays: history.length,
    focusVsAvgPct,
    focusVsAvgHint,
  }
}


// ─── 时间窗口解析 ──────────────────────────────────────

interface ResolvedRange {
  startDate: string
  endDate: string
  windowDays: number
  /** 非空 = 用户输入有效性错误（如自定义起止反向 / 超过 31 天 / 任一为空） */
  validateError: string | null
}

function resolveRange(preset: RangePreset, customDraft: { start: string; end: string }): ResolvedRange {
  const today = laToday()
  if (preset === 'today') {
    return { startDate: today, endDate: today, windowDays: 1, validateError: null }
  }
  if (preset === 'yesterday') {
    const y = laShift(today, -1)
    return { startDate: y, endDate: y, windowDays: 1, validateError: null }
  }
  if (preset === 'last7' || preset === 'last14' || preset === 'last30') {
    const n = preset === 'last7' ? 7 : preset === 'last14' ? 14 : 30
    return {
      startDate: laShift(today, -(n - 1)),
      endDate: today,
      windowDays: n,
      validateError: null,
    }
  }
  // custom
  const { start, end } = customDraft
  if (!start || !end) {
    return { startDate: today, endDate: today, windowDays: 1, validateError: '请选择起止日期' }
  }
  if (start > end) {
    return { startDate: today, endDate: today, windowDays: 1, validateError: '起始日不能晚于结束日' }
  }
  const days = diffDays(start, end) + 1
  if (days > MAX_CUSTOM_DAYS) {
    return {
      startDate: today, endDate: today, windowDays: 1,
      validateError: `区间过大（${days} 天），最多 ${MAX_CUSTOM_DAYS} 天`,
    }
  }
  return { startDate: start, endDate: end, windowDays: days, validateError: null }
}


// ─── LA 时区日期工具 ──────────────────────────────────

/** 返回 LA 时区的"今天" YYYY-MM-DD */
function laToday(): string {
  const now = new Date()
  const la = new Date(now.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }))
  return isoDay(la)
}

/** YYYY-MM-DD 偏移 N 天 */
function laShift(ds: string, days: number): string {
  const [y, m, d] = ds.split('-').map(Number)
  const dt = new Date(y, (m || 1) - 1, d || 1)
  dt.setDate(dt.getDate() + days)
  return isoDay(dt)
}

/** 两个 YYYY-MM-DD 之间的天数差（end - start） */
function diffDays(start: string, end: string): number {
  const [y1, m1, d1] = start.split('-').map(Number)
  const [y2, m2, d2] = end.split('-').map(Number)
  const t1 = Date.UTC(y1, m1 - 1, d1)
  const t2 = Date.UTC(y2, m2 - 1, d2)
  return Math.round((t2 - t1) / 86_400_000)
}

function isoDay(d: Date): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const dd = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${dd}`
}
