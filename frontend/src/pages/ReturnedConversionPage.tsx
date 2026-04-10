/**
 * 广告回传分析页面 — Campaign → Adset → Ad 层级视图
 *
 * 数据口径：广告平台归因回传口径（returned），非订单真值，仅用于投放优化。
 *
 * 层级交互：
 *   1. 默认展示 Campaign 汇总列表
 *   2. 点击 Campaign 行 → 展开该 Campaign 的 Adset，数据按需加载
 *   3. 点击 Adset 行   → 展开该 Adset 的 Ad，数据按需加载
 *
 * 筛选：时间范围 / 媒体来源 / 平台 / 国家 / 名称搜索（campaign/adset/ad 名称模糊匹配）
 * 已移除：Campaign ID / Adset ID / Ad ID 输入框；group_by 切换按钮
 */
import { useState, useCallback, useMemo, Fragment } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ChevronRight, ChevronDown, Loader2, AlertCircle,
  DollarSign, Users, ShoppingCart, TrendingUp, BarChart3, Info,
} from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import {
  fetchCampaignRows, fetchAdsetRows, fetchAdRows,
  type BaseReturnedFilter, type ReturnedAvailability,
  type ReturnedFieldAvailability, type ReturnedRow,
} from '@/services/returned-conversion'

// ── 格式化工具 ────────────────────────────────────────────────

const fmtUsd = (n: number) =>
  `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
const fmtInt = (n: number) => n.toLocaleString()
const fmtRoi = (n: number): React.ReactNode => {
  if (n <= 0) return <span className="text-gray-300">-</span>
  return (
    <span className={n >= 1 ? 'text-emerald-600 font-semibold' : 'text-amber-600 font-semibold'}>
      {n.toFixed(4)}
    </span>
  )
}

// ── 小组件 ────────────────────────────────────────────────────

function AvailCell({
  field, value, fmt,
}: {
  field: ReturnedFieldAvailability
  value: number
  fmt: (n: number) => React.ReactNode
}) {
  if (!field.supported) return <span className="text-gray-300">-</span>
  return <>{fmt(value)}</>
}

function Badge({ label, color = 'amber' }: { label: string; color?: 'amber' | 'red' }) {
  const cls = color === 'red'
    ? 'bg-red-50 text-red-600 border border-red-200'
    : 'bg-amber-50 text-amber-700 border border-amber-200'
  return <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${cls}`}>{label}</span>
}

function StatCard({
  label, value, icon: Icon, sub,
}: {
  label: string
  value: React.ReactNode
  icon: React.ElementType
  sub?: string
}) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 p-4 flex flex-col gap-1 shadow-sm">
      <div className="flex items-center gap-2 text-gray-400">
        <Icon className="w-4 h-4" />
        <span className="text-xs">{label}</span>
      </div>
      <div className="text-xl font-bold text-gray-800 truncate">{value}</div>
      {sub && <div className="text-[11px] text-gray-400">{sub}</div>}
    </div>
  )
}

// ── 表格行指标单元格（共用于三个层级） ───────────────────────

function MetricCells({ row, avail }: { row: ReturnedRow; avail: ReturnedAvailability }) {
  return (
    <>
      <td className="text-right px-3 py-2.5 text-sm tabular-nums whitespace-nowrap">
        {fmtUsd(row.spend)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-gray-500 tabular-nums whitespace-nowrap">
        {fmtInt(row.impressions)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-gray-500 tabular-nums whitespace-nowrap">
        {fmtInt(row.clicks)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-gray-500 tabular-nums whitespace-nowrap">
        {fmtInt(row.installs)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-blue-700 font-medium tabular-nums whitespace-nowrap">
        <AvailCell field={avail.registrations_returned} value={row.registrations_returned} fmt={fmtInt} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-emerald-700 tabular-nums whitespace-nowrap">
        <AvailCell field={avail.purchase_value_returned} value={row.purchase_value_returned} fmt={fmtUsd} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-teal-700 tabular-nums whitespace-nowrap">
        <AvailCell field={avail.subscribe_value_returned} value={row.subscribe_value_returned} fmt={fmtUsd} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm font-semibold text-emerald-800 tabular-nums whitespace-nowrap">
        {fmtUsd(row.total_value_returned)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm tabular-nums whitespace-nowrap">
        {fmtRoi(row.cumulative_roi_returned)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm tabular-nums whitespace-nowrap">
        {fmtRoi(row.d0_roi_returned)}
      </td>
    </>
  )
}

// ── Ad 行组（第三层，Adset 展开后渲染） ─────────────────────

const COL_COUNT = 11 // 名称列 + 10 指标列

function AdRowGroup({
  adsetId, base, avail,
}: { adsetId: string; base: BaseReturnedFilter; avail: ReturnedAvailability }) {
  const { data, isLoading } = useQuery({
    queryKey: ['returned-conversion', 'ads', adsetId, base],
    queryFn: () => fetchAdRows(adsetId, base),
    staleTime: 2 * 60_000,
  })

  if (isLoading) {
    return (
      <tr>
        <td colSpan={COL_COUNT} className="py-2 pl-24 text-xs text-gray-400">
          <Loader2 className="w-3 h-3 animate-spin inline mr-1" />加载广告…
        </td>
      </tr>
    )
  }

  const ads = data?.rows ?? []
  if (ads.length === 0) {
    return (
      <tr>
        <td colSpan={COL_COUNT} className="py-2 pl-24 text-xs text-gray-300 italic">暂无 Ad 数据</td>
      </tr>
    )
  }

  return (
    <>
      {ads.map(ad => (
        <tr key={ad.dimension_key} className="border-t border-gray-50 hover:bg-gray-50/50 transition-colors">
          <td className="py-2 pl-20 pr-3">
            <div className="flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-gray-300 shrink-0" />
              <span
                className="text-xs text-gray-500 line-clamp-2 break-words leading-tight"
                title={ad.dimension_label}
              >
                {ad.dimension_label || ad.dimension_key || '-'}
              </span>
            </div>
          </td>
          <MetricCells row={ad} avail={avail} />
        </tr>
      ))}
    </>
  )
}

// ── Adset 行组（第二层，Campaign 展开后渲染） ────────────────

function AdsetRowGroup({
  campaignId, base, avail,
}: { campaignId: string; base: BaseReturnedFilter; avail: ReturnedAvailability }) {
  const [expandedAdsets, setExpandedAdsets] = useState(new Set<string>())

  const toggleAdset = useCallback((id: string) => {
    setExpandedAdsets(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }, [])

  const { data, isLoading } = useQuery({
    queryKey: ['returned-conversion', 'adsets', campaignId, base],
    queryFn: () => fetchAdsetRows(campaignId, base),
    staleTime: 2 * 60_000,
  })

  if (isLoading) {
    return (
      <tr>
        <td colSpan={COL_COUNT} className="py-2 pl-10 text-xs text-gray-400">
          <Loader2 className="w-3 h-3 animate-spin inline mr-1" />加载 Adset…
        </td>
      </tr>
    )
  }

  const adsets = data?.rows ?? []
  if (adsets.length === 0) {
    return (
      <tr>
        <td colSpan={COL_COUNT} className="py-2 pl-10 text-xs text-gray-300 italic">暂无 Adset 数据</td>
      </tr>
    )
  }

  return (
    <>
      {adsets.map(adset => {
        const isExpanded = expandedAdsets.has(adset.dimension_key)
        return (
          <Fragment key={adset.dimension_key}>
            <tr
              className="border-t border-gray-100 bg-white hover:bg-blue-50/30 cursor-pointer transition-colors"
              onClick={() => toggleAdset(adset.dimension_key)}
            >
              <td className="py-2.5 pl-9 pr-3">
                <div className="flex items-center gap-2">
                  {/* 左侧竖线缩进指示 */}
                  <span className="w-px h-4 bg-gray-200 shrink-0" />
                  <span className="text-gray-400 shrink-0">
                    {isExpanded
                      ? <ChevronDown className="w-3.5 h-3.5" />
                      : <ChevronRight className="w-3.5 h-3.5" />
                    }
                  </span>
                  <span
                    className="text-xs text-gray-700 line-clamp-2 break-words leading-tight"
                    title={adset.dimension_label}
                  >
                    {adset.dimension_label || adset.dimension_key || '-'}
                  </span>
                </div>
              </td>
              <MetricCells row={adset} avail={avail} />
            </tr>
            {isExpanded && (
              <AdRowGroup adsetId={adset.dimension_key} base={base} avail={avail} />
            )}
          </Fragment>
        )
      })}
    </>
  )
}

// ── 筛选选项 ─────────────────────────────────────────────────

const MEDIA_OPTIONS = [
  { value: '', label: '全部媒体' },
  { value: 'meta', label: 'Meta' },
  { value: 'tiktok', label: 'TikTok' },
  { value: 'google', label: 'Google' },
]

const PLATFORM_OPTIONS = [
  { value: '', label: '全部平台' },
  { value: 'ios', label: 'iOS' },
  { value: 'android', label: 'Android' },
  { value: 'mixed', label: 'Mixed' },
]

const TABLE_HEADERS = [
  'Campaign', '花费', '展示', '点击', '安装',
  '回传注册数', '回传充值价值', '回传订阅价值', '回传总价值', '累计回传ROI', 'D0 ROI（回传）',
]

const _unsupported: ReturnedFieldAvailability = { supported: false, has_nonzero_data: false }
const DEFAULT_AVAIL: ReturnedAvailability = {
  registrations_returned:      _unsupported,
  purchase_value_returned:     _unsupported,
  subscribe_value_returned:    _unsupported,
  d1_value_returned:           _unsupported,
  d0_registrations_returned:   _unsupported,
  d0_purchase_value_returned:  _unsupported,
  d0_subscribe_value_returned: _unsupported,
}

// ── 主页面组件 ────────────────────────────────────────────────

export default function ReturnedConversionPage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('7d'))
  const [mediaSource, setMediaSource] = useState('')
  const [platform, setPlatform] = useState('')
  const [country, setCountry] = useState('')
  const [searchKeyword, setSearchKeyword] = useState('')
  const [expandedCampaigns, setExpandedCampaigns] = useState(new Set<string>())

  const baseFilter = useMemo<BaseReturnedFilter>(() => ({
    start_date:     dateRange.startDate,
    end_date:       dateRange.endDate,
    media_source:   mediaSource || undefined,
    platform:       platform || undefined,
    country:        country || undefined,
    search_keyword: searchKeyword || undefined,
  }), [dateRange, mediaSource, platform, country, searchKeyword])

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['returned-conversion', 'campaigns', baseFilter],
    queryFn: () => fetchCampaignRows(baseFilter),
    staleTime: 2 * 60_000,
  })

  const summary   = data?.summary
  const campaigns = data?.rows ?? []
  const avail     = data?.availability ?? DEFAULT_AVAIL

  const toggleCampaign = useCallback((id: string) => {
    setExpandedCampaigns(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }, [])

  // ── 渲染 ─────────────────────────────────────────────────

  return (
    <div className="max-w-[1440px] mx-auto">

      {/* 页头 */}
      <PageHeader
        title={
          <span className="flex items-center gap-2">
            广告回传分析
            <Badge label="回传口径" color="amber" />
            <Badge label="非订单真值" color="red" />
          </span>
        }
        description="广告平台归因回传指标 · Campaign → Adset → Ad 层级视图"
      />

      {/* 免责声明 */}
      <div className="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 mb-4 text-sm text-amber-800">
        <Info className="w-4 h-4 mt-0.5 shrink-0 text-amber-500" />
        <span>
          基于广告平台归因回传，仅用于投放优化分析，不等同于后端订单真值。
          所有金额为回传口径（returned），不反映实际财务收入。
        </span>
      </div>

      {/* 筛选区 — 仅保留 4 个维度筛选 + 日期 */}
      <div className="bg-white rounded-xl border border-gray-100 p-4 mb-4 shadow-sm">
        <div className="mb-3">
          <DateRangeFilter value={dateRange} onChange={setDateRange} />
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div>
            <label className="text-xs text-gray-500 block mb-1">媒体来源</label>
            <select
              value={mediaSource}
              onChange={e => setMediaSource(e.target.value)}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            >
              {MEDIA_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">平台</label>
            <select
              value={platform}
              onChange={e => setPlatform(e.target.value)}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            >
              {PLATFORM_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">国家</label>
            <input
              value={country}
              onChange={e => setCountry(e.target.value)}
              placeholder="如 US / CN"
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            />
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">名称搜索</label>
            <input
              value={searchKeyword}
              onChange={e => setSearchKeyword(e.target.value)}
              placeholder="Campaign / Adset / Ad 名称"
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            />
          </div>
        </div>
      </div>

      {/* 加载状态 */}
      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中…</span>
        </div>
      )}

      {/* 错误状态 */}
      {isError && (
        <div className="flex flex-col items-center justify-center py-24 text-red-400 gap-2">
          <AlertCircle className="w-8 h-8" />
          <p className="text-sm font-medium">数据加载失败</p>
          <p className="text-xs text-gray-400">{String(error)}</p>
        </div>
      )}

      {!isLoading && !isError && (
        <>
          {/* 指标卡 — 两组：投放 / 回传 */}
          <div className="mb-4 space-y-3">

            <div>
              <div className="text-xs text-gray-400 mb-2 font-medium tracking-wide uppercase">投放指标</div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <StatCard
                  label="花费"
                  value={summary ? fmtUsd(summary.spend) : '-'}
                  icon={DollarSign}
                />
                <StatCard
                  label="展示"
                  value={summary ? fmtInt(summary.impressions) : '-'}
                  icon={BarChart3}
                />
                <StatCard
                  label="点击"
                  value={summary ? fmtInt(summary.clicks) : '-'}
                  icon={BarChart3}
                />
                <StatCard
                  label="安装"
                  value={summary ? fmtInt(summary.installs) : '-'}
                  icon={BarChart3}
                />
              </div>
            </div>

            <div>
              <div className="text-xs text-gray-400 mb-2 font-medium tracking-wide uppercase">回传指标</div>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
                <StatCard
                  label="回传注册数"
                  value={
                    !summary ? '-'
                    : avail.registrations_returned.supported
                      ? fmtInt(summary.registrations_returned)
                      : '暂未提供'
                  }
                  icon={Users}
                  sub={avail.registrations_returned.supported ? '广告平台归因' : '当前平台不支持'}
                />
                <StatCard
                  label="回传充值价值"
                  value={
                    !summary ? '-'
                    : avail.purchase_value_returned.supported
                      ? fmtUsd(summary.purchase_value_returned)
                      : '暂未提供'
                  }
                  icon={ShoppingCart}
                  sub={avail.purchase_value_returned.supported ? '广告平台归因' : '当前平台不支持'}
                />
                <StatCard
                  label="回传订阅价值"
                  value={
                    !summary ? '-'
                    : avail.subscribe_value_returned.supported
                      ? fmtUsd(summary.subscribe_value_returned)
                      : '暂未提供'
                  }
                  icon={TrendingUp}
                  sub={avail.subscribe_value_returned.supported ? '广告平台归因' : '当前平台不支持'}
                />
                <StatCard
                  label="回传总价值"
                  value={summary ? fmtUsd(summary.total_value_returned) : '-'}
                  icon={TrendingUp}
                  sub="充值 + 订阅"
                />
                <StatCard
                  label="累计回传ROI"
                  value={summary && summary.cumulative_roi_returned > 0
                    ? summary.cumulative_roi_returned.toFixed(4)
                    : '-'}
                  icon={BarChart3}
                  sub="回传总价值 / 花费"
                />
                <StatCard
                  label="D0 ROI（回传口径）"
                  value={summary && summary.d0_roi_returned > 0 ? summary.d0_roi_returned.toFixed(4) : '-'}
                  icon={BarChart3}
                  sub="total_value / spend"
                />
              </div>
            </div>

          </div>

          {/* 层级展开表格 */}
          <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden mb-6">

            {/* 表头提示 */}
            <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
              <span className="text-sm font-medium text-gray-700">
                Campaign 层级
                <span className="ml-2 text-xs font-normal text-gray-400">
                  ({campaigns.length} 个)
                </span>
              </span>
              <span className="text-xs text-gray-400 flex items-center gap-1">
                <ChevronRight className="w-3 h-3" />
                点击行展开查看 Adset → Ad
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-gray-100 bg-gray-50/80">
                    {TABLE_HEADERS.map((h, i) => (
                      <th
                        key={h}
                        className={`px-3 py-2.5 text-xs font-medium text-gray-500 whitespace-nowrap ${
                          i === 0 ? 'text-left min-w-[320px]' : 'text-right'
                        }`}
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>

                <tbody>
                  {campaigns.length === 0 ? (
                    <tr>
                      <td colSpan={COL_COUNT} className="py-16 text-center">
                        <BarChart3 className="w-10 h-10 text-gray-200 mx-auto mb-3" />
                        <p className="text-sm text-gray-400">暂无回传数据</p>
                        <p className="text-xs text-gray-300 mt-1">
                          请先通过数据同步任务将广告回传数据写入 ad_returned_conversion_daily 表
                        </p>
                      </td>
                    </tr>
                  ) : (
                    campaigns.map(campaign => {
                      const isExpanded = expandedCampaigns.has(campaign.dimension_key)
                      return (
                        <Fragment key={campaign.dimension_key}>
                          {/* Campaign 行 — 一级，整行可点击 */}
                          <tr
                            className={`border-t border-gray-100 cursor-pointer transition-colors select-none ${
                              isExpanded
                                ? 'bg-blue-50/50'
                                : 'hover:bg-gray-50'
                            }`}
                            onClick={() => toggleCampaign(campaign.dimension_key)}
                          >
                            <td className="py-3 pl-3 pr-3">
                              <div className="flex items-center gap-2">
                                <span className="text-gray-400 shrink-0">
                                  {isExpanded
                                    ? <ChevronDown className="w-4 h-4" />
                                    : <ChevronRight className="w-4 h-4" />
                                  }
                                </span>
                                <span
                                  className="text-sm font-semibold text-gray-800 line-clamp-2 break-words leading-tight"
                                  title={campaign.dimension_label}
                                >
                                  {campaign.dimension_label || campaign.dimension_key || '-'}
                                </span>
                              </div>
                            </td>
                            <MetricCells row={campaign} avail={avail} />
                          </tr>

                          {/* Adset 行组 — 按需加载，仅展开时挂载 */}
                          {isExpanded && (
                            <AdsetRowGroup
                              campaignId={campaign.dimension_key}
                              base={baseFilter}
                              avail={avail}
                            />
                          )}
                        </Fragment>
                      )
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
