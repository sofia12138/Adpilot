import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ClipboardList, Filter, Info, Loader2, AlertCircle, X } from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { useAuth } from '@/contexts/AuthContext'
import { fetchChannelDict, fetchKpi, fetchToday, fetchUsers } from '@/services/userPaymentService'
import type { AnomalyTag, ChannelInfo, ChannelKind, KpiMode, TodayUserRow, UserPaymentKpiResponse, UserPaymentListResponse, UserPaymentSummaryRow } from '@/types/userPayment'
import type { DatePreset, DateRange } from '@/types/ops'
import { OpsTabs } from './OpsTabs'
import { RangeSwitch } from '@/components/ops/RangeSwitch'
import { presetToRange, periodLabel, rangeDisplay, rangeKey } from '@/components/ops/rangeUtils'
import { UserPaymentTable } from '@/components/users/UserPaymentTable'
import { ApplicationDialog } from '@/components/users/ApplicationDialog'
import { ApprovalDrawer } from '@/components/users/ApprovalDrawer'
import { UserDetailDrawer } from '@/components/users/UserDetailDrawer'
import { TodayRealtimeBanner } from '@/components/users/TodayRealtimeBanner'

function fmtUsd(v: number) {
  if (!v) return '$0'
  if (Math.abs(v) >= 1000) return `$${(v / 1000).toFixed(1)}k`
  return `$${v.toFixed(2)}`
}
function fmtRate(v: number) { return `${(v * 100).toFixed(1)}%` }
function fmtNum(v: number) {
  if (Math.abs(v) >= 1000) return `${(v / 1000).toFixed(1)}k`
  return v.toLocaleString()
}

const KPI_TITLE: Record<string, string> = {
  total_users: '尝试付费用户',
  paying_users: '成功付费用户',
  try_but_fail_users: '尝试未成功',
  guest_paying_users: '游客付费用户',
  total_orders: '总创单',
  paid_orders: '成功支付单',
  success_rate: '订单成功率',
  total_gmv_usd: 'GMV (USD)',
  attempted_gmv_usd: '尝试金额',
  arpu_usd: 'ARPU (USD)',
}

export default function UserPaymentPanel() {
  const auth = useAuth()
  const isSuperAdmin = auth.role === 'super_admin'

  // ── 时间范围（默认近 14 天）──
  const [range, setRange] = useState<DateRange>(() => presetToRange('last14'))
  const [customDraft, setCustomDraft] = useState<{ start: string; end: string }>(() => ({
    start: presetToRange('last7').start,
    end:   presetToRange('today').end,
  }))
  const handlePresetChange = (preset: DatePreset) => {
    setPage(1)
    if (preset === 'custom') setRange(presetToRange('custom', customDraft))
    else setRange(presetToRange(preset))
  }
  const handleCustomChange = (next: { start: string; end: string }) => {
    setCustomDraft(next)
    if (next.start && next.end && next.start <= next.end) {
      setPage(1)
      setRange({ preset: 'custom', start: next.start, end: next.end })
    }
  }

  // 「今天」走 today 实时层，其他走 by-window 接口
  const isToday = range.preset === 'today'

  // ── 筛选 ──
  const [filters, setFilters] = useState<{
    user_id?: number
    oauth_platform?: number
    first_os_type?: number
    anomaly_tag?: AnomalyTag
    channel_kind?: ChannelKind
    region?: string
  }>({})
  const [filterDraft, setFilterDraft] = useState(filters)
  const [page, setPage] = useState(1)
  const pageSize = 50
  const [orderBy, setOrderBy] = useState('last_action_time_utc')
  const [orderDesc, setOrderDesc] = useState(true)

  // ── 双口径 ──
  const [mode, setMode] = useState<KpiMode>('raw')
  // ── 「只看成功付费用户」开关：默认关闭（产品口径=尝试付费用户全量） ──
  const [paidOnly, setPaidOnly] = useState(false)

  // ── Drawer/Dialog ──
  const [approvalOpen, setApprovalOpen] = useState(false)
  const [applyTarget, setApplyTarget] = useState<number | null>(null)
  const [detailTarget, setDetailTarget] = useState<number | null>(null)

  // ── 数据查询：今天走 today，其他走 by-window ──
  const kpiQuery = useQuery({
    queryKey: ['user-payment-kpi', isToday ? 'today' : rangeKey(range)],
    queryFn: () => fetchKpi({ start_ds: range.start, end_ds: range.end }),
    enabled: !isToday,
    refetchInterval: 60000,
  })
  const listQuery = useQuery({
    queryKey: ['user-payment-users', rangeKey(range), filters, paidOnly, page, orderBy, orderDesc],
    queryFn: () => fetchUsers({
      start_ds: range.start, end_ds: range.end,
      ...filters,
      min_paid_orders: paidOnly ? 1 : undefined,
      page, page_size: pageSize, order_by: orderBy, order_desc: orderDesc,
    }),
    enabled: !isToday,
    placeholderData: (prev) => prev,
  })
  // 今天模式：直接拉 today 实时层（PolarDB 60s 缓存），前端聚合 KPI 和列表
  const todayQuery = useQuery({
    queryKey: ['user-payment-today-full', isToday],
    queryFn: () => fetchToday(),
    enabled: isToday,
    refetchInterval: 60000,
  })
  // 渠道字典：进入面板时拉一次，10 分钟缓存
  const channelDictQuery = useQuery({
    queryKey: ['user-payment-channel-dict'],
    queryFn: () => fetchChannelDict(),
    staleTime: 10 * 60_000,
  })
  const channelDict: Record<string, ChannelInfo> | null = channelDictQuery.data ?? null

  // KPI bucket：根据当前模式选数据
  const kpiData: UserPaymentKpiResponse | null = useMemo(() => {
    if (isToday) {
      if (!todayQuery.data) return null
      return aggregateTodayKpi(todayQuery.data.items)
    }
    return kpiQuery.data ?? null
  }, [isToday, todayQuery.data, kpiQuery.data])

  const bucket = useMemo(() => {
    if (!kpiData) return null
    return mode === 'raw' ? kpiData.raw : kpiData.clean
  }, [kpiData, mode])

  // 列表：今天模式直接用 today.items 转换为 summary row 结构
  const listData: UserPaymentListResponse | null = useMemo(() => {
    if (isToday) {
      if (!todayQuery.data) return null
      return todayToList(todayQuery.data.items, filters, paidOnly, page, pageSize, orderBy, orderDesc, channelDict)
    }
    return listQuery.data ?? null
  }, [isToday, todayQuery.data, listQuery.data, filters, paidOnly, page, orderBy, orderDesc, channelDict])

  const queryLoading = isToday ? todayQuery.isLoading : (kpiQuery.isLoading || listQuery.isLoading)
  const queryError   = isToday ? todayQuery.isError   : (kpiQuery.isError   || listQuery.isError)
  const queryFetching = isToday ? todayQuery.isFetching : listQuery.isFetching

  const periodTxt = periodLabel(range)

  const handleSort = (field: string) => {
    if (orderBy === field) {
      setOrderDesc(d => !d)
    } else {
      setOrderBy(field)
      setOrderDesc(true)
    }
  }

  const applyFilters = () => {
    setFilters(filterDraft)
    setPage(1)
  }
  const resetFilters = () => {
    setFilterDraft({})
    setFilters({})
    setPage(1)
  }

  return (
    <div className="max-w-7xl mx-auto space-y-4">
      <PageHeader
        title="用户付费"
        description="单用户维度的付费明细、异常识别、白名单审批 — 仅超管可见"
        action={
          <div className="flex items-center gap-3">
            <button
              onClick={() => setApprovalOpen(true)}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-blue-50 text-blue-600 border border-blue-200 hover:bg-blue-100 text-xs font-medium"
            >
              <ClipboardList className="w-3.5 h-3.5" />
              审批工单
            </button>
            <RangeSwitch
              range={range}
              customDraft={customDraft}
              onPresetChange={handlePresetChange}
              onCustomChange={handleCustomChange}
            />
          </div>
        }
      />

      <OpsTabs />

      {/* 实时今日 banner */}
      <TodayRealtimeBanner
        onUserClick={(uid) => setDetailTarget(uid)}
        onApply={(uid) => setApplyTarget(uid)}
      />

      {/* 数据口径说明 */}
      <div className="flex flex-wrap items-center justify-between gap-2 px-3 py-2 rounded-lg bg-blue-50 border border-blue-100 text-xs text-blue-700">
        <div className="inline-flex items-center gap-1.5 flex-wrap">
          <Info className="w-3.5 h-3.5 shrink-0" />
          <span className="font-medium">时区:</span>
          <span className="font-mono">America/Los_Angeles (LA)</span>
          <span className="mx-1">·</span>
          <span className="font-medium">区间:</span>
          <span className="font-mono">{rangeDisplay(range)}</span>
          <span className="text-blue-500/80">· {periodTxt}</span>
          <span className="mx-1">·</span>
          {isToday
            ? <span>实时层（PolarDB 直查 + 60s 缓存，今日 LA 当天）</span>
            : <span>T+1 同步聚合（biz_user_payment_order 窗口重算）</span>}
        </div>
        {kpiData && (
          <div className="text-blue-500/80">
            白名单 <span className="font-mono">{kpiData.whitelist_count}</span> 人
          </div>
        )}
      </div>

      {/* 列表筛选模式 + 双口径切换 */}
      <div className="flex flex-wrap items-center justify-between gap-3 text-xs">
        <label className="inline-flex items-center gap-2 cursor-pointer select-none">
          <span className="text-gray-500">列表范围：</span>
          <button
            type="button"
            role="switch"
            aria-checked={paidOnly}
            onClick={() => { setPaidOnly(v => !v); setPage(1) }}
            className={`relative inline-flex items-center h-5 w-9 rounded-full transition-colors ${paidOnly ? 'bg-blue-500' : 'bg-gray-300'}`}
          >
            <span className={`inline-block w-4 h-4 bg-white rounded-full shadow-sm transform transition-transform ${paidOnly ? 'translate-x-4' : 'translate-x-0.5'}`} />
          </button>
          <span className={paidOnly ? 'text-gray-900 font-medium' : 'text-gray-500'}>
            只看成功付费用户
          </span>
          <span className="text-gray-400">
            {paidOnly ? '（已隐藏全失败用户）' : '（含尝试未成功）'}
          </span>
        </label>

        <div className="inline-flex items-center gap-2">
          <span className="text-gray-500">口径：</span>
          <div className="inline-flex bg-muted rounded-lg p-0.5">
            <button
              onClick={() => setMode('raw')}
              className={`px-3 py-1 rounded-md ${mode === 'raw' ? 'bg-white text-gray-900 shadow-sm font-medium' : 'text-gray-500'}`}
            >
              原始口径
            </button>
            <button
              onClick={() => setMode('clean')}
              disabled={isToday}
              title={isToday ? '今日实时层暂不支持白名单剔除（明日 T+1 数据归档后可用）' : ''}
              className={`px-3 py-1 rounded-md ${mode === 'clean' ? 'bg-white text-gray-900 shadow-sm font-medium' : 'text-gray-500'} ${isToday ? 'opacity-40 cursor-not-allowed' : ''}`}
            >
              剔除白名单
            </button>
          </div>
        </div>
      </div>

      {queryLoading && (
        <div className="flex items-center justify-center py-16 text-gray-400">
          <Loader2 className="w-5 h-5 animate-spin mr-2" />
          <span className="text-sm">加载中…</span>
        </div>
      )}
      {queryError && (
        <div className="flex items-center justify-center py-10 text-red-400">
          <AlertCircle className="w-5 h-5 mr-2" />
          <span className="text-sm">数据加载失败</span>
        </div>
      )}

      {bucket && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          <Kpi title={`${periodTxt}${KPI_TITLE.total_users}`} value={fmtNum(bucket.total_users)} subtitle={`含 ${fmtNum(bucket.try_but_fail_users)} 个仅尝试未成功`} />
          <Kpi title={`${periodTxt}${KPI_TITLE.paying_users}`} value={fmtNum(bucket.paying_users)} subtitle={`游客 ${fmtNum(bucket.guest_paying_users)}`} />
          <Kpi title={`${periodTxt} GMV`} value={fmtUsd(bucket.total_gmv_usd)} subtitle={`尝试 ${fmtUsd(bucket.attempted_gmv_usd)}`} />
          <Kpi title={`${periodTxt} ${KPI_TITLE.arpu_usd}`} value={fmtUsd(bucket.arpu_usd)} subtitle="GMV / 付费用户" />
          <Kpi
            title={`${periodTxt}${KPI_TITLE.success_rate}`}
            value={fmtRate(bucket.success_rate)}
            subtitle={`${fmtNum(bucket.paid_orders)} / ${fmtNum(bucket.total_orders)}`}
            valueClass={bucket.success_rate >= 0.5 ? 'text-green-600' : bucket.success_rate >= 0.2 ? 'text-orange-600' : 'text-red-500'}
          />
          <Kpi
            title="iOS GMV"
            value={fmtUsd(bucket.total_gmv_usd_ios)}
            subtitle={`占比 ${bucket.total_gmv_usd > 0 ? ((bucket.total_gmv_usd_ios / bucket.total_gmv_usd) * 100).toFixed(0) : 0}%`}
          />
          <Kpi
            title="Android GMV"
            value={fmtUsd(bucket.total_gmv_usd_android)}
            subtitle={`占比 ${bucket.total_gmv_usd > 0 ? ((bucket.total_gmv_usd_android / bucket.total_gmv_usd) * 100).toFixed(0) : 0}%`}
          />
          <Kpi
            title="订阅 GMV"
            value={fmtUsd(bucket.total_gmv_usd_subscribe)}
            subtitle={`内购 ${fmtUsd(bucket.total_gmv_usd_inapp)}`}
          />
        </div>
      )}

      {/* 筛选 */}
      <div className="bg-card border border-card-border rounded-xl p-3 space-y-3">
        <div className="flex items-center gap-2 text-xs text-gray-600 font-medium">
          <Filter className="w-3.5 h-3.5" />
          筛选
        </div>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2 text-xs">
          <NumInput
            label="user_id"
            value={filterDraft.user_id}
            onChange={(v) => setFilterDraft(s => ({ ...s, user_id: v }))}
          />
          <Select
            label="登录平台"
            value={filterDraft.oauth_platform ?? ''}
            onChange={(v) => setFilterDraft(s => ({ ...s, oauth_platform: v === '' ? undefined : Number(v) }))}
            options={[
              { value: '', label: '全部' },
              { value: -1, label: '游客' },
              { value: 1, label: 'Google' },
              { value: 2, label: 'Facebook' },
              { value: 3, label: 'Apple' },
            ]}
          />
          <Select
            label="首单平台"
            value={filterDraft.first_os_type ?? ''}
            onChange={(v) => setFilterDraft(s => ({ ...s, first_os_type: v === '' ? undefined : Number(v) }))}
            options={[
              { value: '', label: '全部' },
              { value: 1, label: 'Android' },
              { value: 2, label: 'iOS' },
            ]}
          />
          <Select
            label="异常标签"
            value={filterDraft.anomaly_tag ?? ''}
            onChange={(v) => setFilterDraft(s => ({ ...s, anomaly_tag: v === '' ? undefined : v as AnomalyTag }))}
            options={[
              { value: '', label: '全部' },
              { value: 'suspect_brush', label: '刷单嫌疑' },
              { value: 'payment_loop', label: '支付失败循环' },
              { value: 'instant_burst', label: '注册即狂下' },
              { value: 'guest_payer', label: '游客付费' },
              { value: 'pending_whitelist', label: '审批中' },
              { value: 'whitelisted', label: '已加白' },
            ]}
          />
          <Select
            label="首单渠道"
            value={filterDraft.channel_kind ?? ''}
            onChange={(v) => setFilterDraft(s => ({ ...s, channel_kind: v === '' ? undefined : v as ChannelKind }))}
            options={[
              { value: '', label: '全部' },
              { value: 'organic', label: '自然量' },
              { value: 'tiktok', label: 'TikTok' },
              { value: 'meta', label: 'Meta' },
              { value: 'other', label: '其它' },
            ]}
          />
          <TextInput
            label="国家"
            value={filterDraft.region ?? ''}
            onChange={(v) => setFilterDraft(s => ({ ...s, region: v || undefined }))}
          />
        </div>
        <div className="flex justify-end gap-2">
          <button onClick={resetFilters} className="px-3 py-1.5 rounded text-xs border border-gray-200 text-gray-600 hover:bg-gray-50 inline-flex items-center gap-1">
            <X className="w-3 h-3" />重置
          </button>
          <button onClick={applyFilters} className="px-3 py-1.5 rounded text-xs bg-blue-500 text-white hover:bg-blue-600">
            应用筛选
          </button>
        </div>
      </div>

      {listData && (
        <UserPaymentTable
          rows={listData.items}
          total={listData.total}
          page={page}
          pageSize={pageSize}
          onPageChange={setPage}
          onSort={handleSort}
          orderBy={orderBy}
          orderDesc={orderDesc}
          onUserClick={(uid) => setDetailTarget(uid)}
          onApply={(uid) => setApplyTarget(uid)}
          channelDict={channelDict}
        />
      )}
      {queryFetching && (
        <div className="text-xs text-gray-400 text-center py-1">刷新中…</div>
      )}

      <UserDetailDrawer
        open={detailTarget !== null}
        user_id={detailTarget}
        onClose={() => setDetailTarget(null)}
        onApply={(uid) => { setDetailTarget(null); setApplyTarget(uid) }}
      />

      <ApplicationDialog
        open={applyTarget !== null}
        user_id={applyTarget}
        onClose={() => setApplyTarget(null)}
        onSuccess={() => {
          if (isToday) todayQuery.refetch()
          else {
            listQuery.refetch()
            kpiQuery.refetch()
          }
        }}
      />

      <ApprovalDrawer
        open={approvalOpen}
        onClose={() => setApprovalOpen(false)}
        currentUser={auth.username}
        canApprove={isSuperAdmin}
      />
    </div>
  )
}

// ───────────── 小组件 ─────────────

function Kpi({ title, value, subtitle, valueClass }: {
  title: string; value: string; subtitle?: string; valueClass?: string
}) {
  return (
    <div className="bg-card border border-card-border rounded-xl p-3">
      <div className="text-[11px] text-gray-500">{title}</div>
      <div className={`text-2xl font-semibold tabular-nums mt-1 ${valueClass || 'text-gray-900'}`}>
        {value}
      </div>
      {subtitle && <div className="text-[10px] text-gray-400 mt-0.5">{subtitle}</div>}
    </div>
  )
}

function NumInput({ label, value, onChange }: { label: string; value?: number; onChange: (v: number | undefined) => void }) {
  return (
    <label className="block">
      <span className="text-gray-500">{label}</span>
      <input
        type="number"
        value={value ?? ''}
        onChange={e => onChange(e.target.value ? Number(e.target.value) : undefined)}
        className="mt-0.5 w-full px-2 py-1 border border-gray-200 rounded text-xs focus:outline-none focus:border-blue-400"
        placeholder="不限"
      />
    </label>
  )
}

function TextInput({ label, value, onChange }: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <label className="block">
      <span className="text-gray-500">{label}</span>
      <input
        type="text"
        value={value}
        onChange={e => onChange(e.target.value)}
        className="mt-0.5 w-full px-2 py-1 border border-gray-200 rounded text-xs focus:outline-none focus:border-blue-400"
        placeholder="不限"
      />
    </label>
  )
}

function Select({ label, value, onChange, options }: {
  label: string
  value: string | number
  onChange: (v: string) => void
  options: { value: string | number; label: string }[]
}) {
  return (
    <label className="block">
      <span className="text-gray-500">{label}</span>
      <select
        value={String(value)}
        onChange={e => onChange(e.target.value)}
        className="mt-0.5 w-full px-2 py-1 border border-gray-200 rounded text-xs bg-white focus:outline-none focus:border-blue-400"
      >
        {options.map(o => (
          <option key={String(o.value)} value={String(o.value)}>{o.label}</option>
        ))}
      </select>
    </label>
  )
}

// ───────────── 「今天」模式下的前端聚合 ─────────────
// today 接口返回的是 PolarDB 实时层逐用户聚合行，没有 KPI 结构。
// 在前端把它合成 raw KPI（whitelist_count=0、clean=raw 双口径），保持
// 与窗口模式一致的渲染逻辑。

function aggregateTodayKpi(items: TodayUserRow[]): UserPaymentKpiResponse {
  const bucket = {
    total_users: 0,
    paying_users: 0,
    try_but_fail_users: 0,
    guest_paying_users: 0,
    total_orders: 0,
    paid_orders: 0,
    success_rate: 0,
    total_gmv_usd: 0,
    attempted_gmv_usd: 0,
    total_gmv_usd_ios: 0,
    total_gmv_usd_android: 0,
    total_gmv_usd_subscribe: 0,
    total_gmv_usd_inapp: 0,
    arpu_usd: 0,
  }
  for (const u of items) {
    bucket.total_users += 1
    bucket.total_orders += u.total_orders
    bucket.paid_orders += u.paid_orders
    bucket.total_gmv_usd += u.total_gmv_usd
    bucket.attempted_gmv_usd += u.attempted_gmv_usd
    bucket.total_gmv_usd_ios += u.total_gmv_usd_ios
    bucket.total_gmv_usd_android += u.total_gmv_usd_android
    bucket.total_gmv_usd_subscribe += u.total_gmv_usd_subscribe
    bucket.total_gmv_usd_inapp += u.total_gmv_usd_inapp
    if (u.paid_orders > 0) {
      bucket.paying_users += 1
      if (u.first_os_type === 0) bucket.guest_paying_users += 1
    } else if (u.total_orders > 0) {
      bucket.try_but_fail_users += 1
    }
  }
  bucket.success_rate = bucket.total_orders > 0
    ? Number((bucket.paid_orders / bucket.total_orders).toFixed(4))
    : 0
  bucket.arpu_usd = bucket.paying_users > 0
    ? Number((bucket.total_gmv_usd / bucket.paying_users).toFixed(2))
    : 0
  bucket.total_gmv_usd = round2(bucket.total_gmv_usd)
  bucket.attempted_gmv_usd = round2(bucket.attempted_gmv_usd)
  bucket.total_gmv_usd_ios = round2(bucket.total_gmv_usd_ios)
  bucket.total_gmv_usd_android = round2(bucket.total_gmv_usd_android)
  bucket.total_gmv_usd_subscribe = round2(bucket.total_gmv_usd_subscribe)
  bucket.total_gmv_usd_inapp = round2(bucket.total_gmv_usd_inapp)
  return { raw: bucket, clean: bucket, whitelist_count: 0 }
}

function round2(v: number) { return Math.round(v * 100) / 100 }

function todayToList(
  items: TodayUserRow[],
  filters: {
    user_id?: number; oauth_platform?: number; first_os_type?: number;
    channel_kind?: ChannelKind; region?: string; anomaly_tag?: AnomalyTag;
  },
  paidOnly: boolean,
  page: number,
  pageSize: number,
  orderBy: string,
  orderDesc: boolean,
  channelDict: Record<string, ChannelInfo> | null,
): UserPaymentListResponse {
  // today 没有 region/oauth_platform 信息，相关筛选会过滤掉所有；
  // register_time_utc 由后端从 biz_user_payment_summary 反查补齐（命中历史付费用户）。
  // 这里只对 today 自带字段做筛选，避免误杀
  let rows: UserPaymentSummaryRow[] = items.map(u => ({
    user_id: u.user_id,
    region: null,
    oauth_platform: null,
    register_time_utc: u.register_time_utc ?? null,
    lang: null,
    first_channel_id: u.first_channel_id || '',
    first_os_type: u.first_os_type,
    first_pay_type: u.first_pay_type,
    total_orders: u.total_orders,
    paid_orders: u.paid_orders,
    refund_orders: u.refund_orders,
    success_rate: u.success_rate,
    paid_orders_ios: u.paid_orders_ios,
    paid_orders_android: u.paid_orders_android,
    total_gmv_usd_ios: u.total_gmv_usd_ios,
    total_gmv_usd_android: u.total_gmv_usd_android,
    paid_orders_subscribe: u.paid_orders_subscribe,
    paid_orders_inapp: u.paid_orders_inapp,
    total_gmv_usd_subscribe: u.total_gmv_usd_subscribe,
    total_gmv_usd_inapp: u.total_gmv_usd_inapp,
    total_gmv_usd: u.total_gmv_usd,
    attempted_gmv_usd: u.attempted_gmv_usd,
    refund_amount_usd: 0,
    first_pay_time_utc: u.first_pay_la,
    last_action_time_utc: u.last_action_la,
    snapshot_ds: '',
    anomaly_tags: u.anomaly_tags,
  }))

  // filter（只支持 today 模式有意义的字段）
  if (filters.user_id != null) rows = rows.filter(r => r.user_id === filters.user_id)
  if (filters.first_os_type != null) rows = rows.filter(r => r.first_os_type === filters.first_os_type)
  if (filters.channel_kind) {
    const kindOk = (cid: string): boolean => {
      const c = (cid || '').trim()
      if (filters.channel_kind === 'organic') return c === '' || c === '0'
      if (c === '' || c === '0') return false
      const info = channelDict?.[c]
      const ap = info?.ad_platform
      if (filters.channel_kind === 'tiktok') return ap === 1
      if (filters.channel_kind === 'meta')   return ap === 2
      if (filters.channel_kind === 'other')  return ap === 0 || ap == null
      return true
    }
    rows = rows.filter(r => kindOk(r.first_channel_id))
  }
  if (filters.anomaly_tag) rows = rows.filter(r => r.anomaly_tags.includes(filters.anomaly_tag!))
  if (paidOnly) rows = rows.filter(r => r.paid_orders > 0)

  // sort
  const k = (orderBy === 'total_gmv_cents' ? 'total_gmv_usd'
    : orderBy === 'attempted_gmv_cents' ? 'attempted_gmv_usd'
    : orderBy) as keyof UserPaymentSummaryRow
  rows.sort((a, b) => {
    const av = a[k] as unknown as (number | string | null)
    const bv = b[k] as unknown as (number | string | null)
    if (av === bv) return 0
    if (av == null) return orderDesc ? 1 : -1
    if (bv == null) return orderDesc ? -1 : 1
    return (av < bv ? -1 : 1) * (orderDesc ? -1 : 1)
  })

  const total = rows.length
  const start = (page - 1) * pageSize
  return {
    total,
    items: rows.slice(start, start + pageSize),
    page,
    page_size: pageSize,
  }
}
