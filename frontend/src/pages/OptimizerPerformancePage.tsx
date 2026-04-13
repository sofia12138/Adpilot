import { useState, useMemo, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  ChevronDown, ChevronRight, Users, DollarSign, UserCheck,
  Loader2, AlertCircle, Search, X, RefreshCw, AlertTriangle,
} from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { StatCard } from '@/components/common/StatCard'
import { SectionCard } from '@/components/common/SectionCard'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { GlobalSyncBar } from '@/components/common/GlobalSyncBar'
import {
  fetchOptimizerSummary,
  fetchOptimizerDetail,
  triggerOptimizerSync,
  type OptimizerSummaryItem,
  type OptimizerDetailItem,
  type OptimizerSummaryMeta,
} from '@/services/optimizer'

const fmtUsd = (n: number | null | undefined) =>
  n != null ? `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}` : '--'
const fmt = (n: number | null | undefined) => n != null ? n.toLocaleString() : '--'
const fmtPct = (n: number | null | undefined) => n != null ? `${(n * 100).toFixed(1)}%` : '--'
const fmtRoas = (n: number | null | undefined) => n != null ? n.toFixed(2) : '--'

const MATCH_SOURCE_LABELS: Record<string, string> = {
  structured_field:   '结构化字段',
  campaign_name:      '活动名称解析',
  historical_mapping: '历史映射复用',
  default_rule:       '默认规则兜底',
  unassigned:         '未识别',
}

const MATCH_SOURCE_COLORS: Record<string, string> = {
  structured_field:   'bg-green-50 text-green-700',
  campaign_name:      'bg-blue-50 text-blue-700',
  historical_mapping: 'bg-purple-50 text-purple-700',
  default_rule:       'bg-amber-50 text-amber-700',
  unassigned:         'bg-red-50 text-red-700',
}

const MATCH_POSITION_LABELS: Record<string, string> = {
  field_6:    'F6(小程序)',
  field_11:   'F11(APP)',
  field_12:   'F12(APP)',
  unassigned: '-',
}

function MatchSourceBadge({ source }: { source: string }) {
  const label = MATCH_SOURCE_LABELS[source] || source
  const colorClass = MATCH_SOURCE_COLORS[source] || 'bg-gray-50 text-gray-600'
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${colorClass}`}>
      {label}
    </span>
  )
}

function PlatformBadge({ platform }: { platform: string }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${
      platform === 'tiktok' ? 'bg-sky-50 text-sky-600' : 'bg-indigo-50 text-indigo-600'
    }`}>
      {platform === 'tiktok' ? 'TikTok' : 'Meta'}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Campaign 明细子表（含匹配来源）
// ---------------------------------------------------------------------------

function CampaignDetailTable({ items }: { items: OptimizerDetailItem[] }) {
  if (items.length === 0) {
    return <div className="px-6 py-6 text-center text-sm text-gray-300">暂无 Campaign 数据</div>
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-gray-100 bg-gray-50/60">
            <th className="px-4 py-2.5 text-left font-medium text-gray-400 whitespace-nowrap" style={{ width: '240px', maxWidth: '240px' }}>Campaign 名称</th>
            <th className="px-4 py-2.5 text-left font-medium text-gray-400 whitespace-nowrap">平台</th>
            <th className="px-4 py-2.5 text-left font-medium text-gray-400 whitespace-nowrap">匹配来源</th>
            <th className="px-4 py-2.5 text-left font-medium text-gray-400 whitespace-nowrap">匹配位置</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">消耗</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">展示</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">点击</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">活跃天数</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">收入</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">ROAS</th>
          </tr>
        </thead>
        <tbody>
          {items.map((m, idx) => (
            <tr key={`${m.campaign_id}-${idx}`} className="border-b border-gray-50 last:border-0 hover:bg-blue-50/20 transition-colors">
              <td className="px-4 py-2.5" style={{ width: '240px', maxWidth: '240px' }}>
                <span className="block max-w-[240px] whitespace-normal break-words line-clamp-2 text-xs text-gray-700 leading-snug" title={m.campaign_name || m.campaign_id}>
                  {m.campaign_name || m.campaign_id || '--'}
                </span>
              </td>
              <td className="px-4 py-2.5"><PlatformBadge platform={m.platform} /></td>
              <td className="px-4 py-2.5"><MatchSourceBadge source={m.match_source} /></td>
              <td className="px-4 py-2.5 text-xs text-gray-500">{MATCH_POSITION_LABELS[m.match_position] || m.match_position || '-'}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-700">{fmtUsd(m.spend)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmt(m.impressions)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmt(m.clicks)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{m.active_days}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmtUsd(m.purchase_value)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmtRoas(m.roas)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// 优化师行（可展开）
// ---------------------------------------------------------------------------

interface OptimizerRowProps {
  row: OptimizerSummaryItem
  startDate: string
  endDate: string
  platform: string
}

function OptimizerRow({ row, startDate, endDate, platform }: OptimizerRowProps) {
  const [expanded, setExpanded] = useState(false)
  const isUnidentified = row.optimizer_name === '未识别'

  const { data: details, isLoading: detailLoading } = useQuery({
    queryKey: ['optimizer-detail', row.optimizer_name, startDate, endDate, platform],
    queryFn: () => fetchOptimizerDetail({
      startDate,
      endDate,
      optimizerName: row.optimizer_name,
      platform: platform || undefined,
    }),
    enabled: expanded,
    staleTime: 60_000,
  })

  const roasClass = row.roas == null
    ? 'text-gray-300'
    : row.roas < 1.5 ? 'text-red-600 font-medium' : 'text-green-600 font-medium'

  const rowBgClass = isUnidentified
    ? 'bg-amber-50/40 hover:bg-amber-50/60 border-b border-amber-100'
    : 'border-b border-gray-50 hover:bg-blue-50/30'

  return (
    <>
      <tr className={`${rowBgClass} transition-colors cursor-pointer`} onClick={() => setExpanded(v => !v)}>
        <td className="px-4 py-3 text-sm">
          <div className="flex items-center gap-2">
            <span className="text-gray-400 flex-shrink-0">
              {expanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
            </span>
            <span className={`font-medium ${isUnidentified ? 'text-amber-700' : 'text-gray-800'}`}>
              {isUnidentified && <AlertTriangle className="w-3.5 h-3.5 inline mr-1 -mt-0.5 text-amber-500" />}
              {row.optimizer_name}
            </span>
          </div>
        </td>
        <td className="px-4 py-3 text-sm text-right tabular-nums font-medium text-gray-800">{fmtUsd(row.total_spend)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmtPct(row.spend_share)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmtUsd(row.avg_daily_spend)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{row.active_days}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{row.campaign_count}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmt(row.registrations)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmtUsd(row.purchase_value)}</td>
        <td className={`px-4 py-3 text-sm text-right tabular-nums ${roasClass}`}>{fmtRoas(row.roas)}</td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={10} className="p-0 bg-gray-50/50">
            {detailLoading ? (
              <div className="flex items-center justify-center py-6 text-gray-400 text-sm gap-2">
                <Loader2 className="w-4 h-4 animate-spin" />加载 Campaign 明细...
              </div>
            ) : (
              <CampaignDetailTable items={details ?? []} />
            )}
          </td>
        </tr>
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// 主页面
// ---------------------------------------------------------------------------

export default function OptimizerPerformancePage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('30d'))
  const [platform, setPlatform] = useState('')
  const [keyword, setKeyword] = useState('')
  const [keywordInput, setKeywordInput] = useState('')

  const queryClient = useQueryClient()

  const { data: summaryResp, isLoading, isError } = useQuery({
    queryKey: ['optimizer-performance', 'summary', dateRange.startDate, dateRange.endDate, platform, keyword],
    queryFn: () => fetchOptimizerSummary({
      startDate: dateRange.startDate,
      endDate: dateRange.endDate,
      platform: platform || undefined,
      keyword: keyword || undefined,
    }),
    staleTime: 30_000,
  })

  const syncMutation = useMutation({
    mutationFn: triggerOptimizerSync,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['optimizer-performance'] })
    },
  })

  const rows = summaryResp?.rows ?? []
  const meta: OptimizerSummaryMeta = summaryResp?.meta ?? { grand_total_spend: 0, unidentified_spend: 0, unidentified_ratio: 0 }

  const overview = useMemo(() => {
    const identifiedRows = rows.filter(r => r.optimizer_name !== '未识别')
    const totalSpend = rows.reduce((s, r) => s + r.total_spend, 0)
    const count = identifiedRows.length
    const avgSpend = count > 0 ? identifiedRows.reduce((s, r) => s + r.total_spend, 0) / count : 0
    return { totalSpend, count, avgSpend }
  }, [rows])

  const handleSearch = useCallback(() => { setKeyword(keywordInput.trim()) }, [keywordInput])
  const handleClearKeyword = useCallback(() => { setKeywordInput(''); setKeyword('') }, [])

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="优化师人效报表" description="按优化师维度汇总投放表现，点击行可展开 Campaign 明细" />
      <GlobalSyncBar />

      {/* 筛选区 */}
      <div className="mb-4 space-y-2">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />
        <div className="flex flex-wrap items-center gap-3">
          <select
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100"
            value={platform}
            onChange={e => setPlatform(e.target.value)}
          >
            <option value="">全部平台</option>
            <option value="tiktok">TikTok</option>
            <option value="meta">Meta</option>
          </select>

          <div className="flex items-center gap-1.5">
            <div className="relative flex items-center">
              <Search className="absolute left-2.5 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
              <input
                type="text"
                placeholder="搜索优化师..."
                className="text-sm border border-gray-200 rounded-lg pl-8 pr-8 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100 w-44"
                value={keywordInput}
                onChange={e => setKeywordInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleSearch()}
              />
              {keywordInput && (
                <button className="absolute right-2 text-gray-300 hover:text-gray-500" onClick={handleClearKeyword}>
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
            <button className="text-sm px-3 py-1.5 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors" onClick={handleSearch}>搜索</button>
          </div>

          <button
            className="text-sm px-3 py-1.5 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors flex items-center gap-1.5 disabled:opacity-50"
            onClick={() => syncMutation.mutate()}
            disabled={syncMutation.isPending}
          >
            <RefreshCw className={`w-3.5 h-3.5 ${syncMutation.isPending ? 'animate-spin' : ''}`} />
            {syncMutation.isPending ? '同步中...' : '同步数据'}
          </button>
        </div>
      </div>

      {/* 加载中 */}
      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" /><span className="text-sm">加载中...</span>
        </div>
      )}

      {/* 错误 */}
      {isError && (
        <div className="flex flex-col items-center justify-center py-24 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" />
          <p className="text-sm font-medium">数据加载失败</p>
          <p className="text-xs mt-1 text-gray-400">请检查后端服务或网络连接</p>
        </div>
      )}

      {!isLoading && !isError && (
        <>
          {/* KPI 概览 */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
            <StatCard label="总消耗" value={fmtUsd(overview.totalSpend)} icon={DollarSign} />
            <StatCard label="优化师人数" value={overview.count} icon={Users} />
            <StatCard label="人均消耗" value={fmtUsd(overview.avgSpend)} icon={UserCheck} />
            <StatCard
              label="未识别消耗占比"
              value={fmtPct(meta.unidentified_ratio)}
              icon={AlertTriangle}
              extra={
                meta.unidentified_spend > 0
                  ? <span className="text-xs text-amber-500">{fmtUsd(meta.unidentified_spend)} 未归属</span>
                  : undefined
              }
            />
          </div>

          {/* 主表格 */}
          <SectionCard
            title="优化师人效汇总"
            extra={<span className="text-xs text-gray-400">共 {rows.filter(r => r.optimizer_name !== '未识别').length} 位优化师 · 点击行展开 Campaign 明细</span>}
            noPadding
          >
            {rows.length === 0 ? (
              <div className="px-5 py-12 text-center text-sm text-gray-300">
                {syncMutation.isSuccess
                  ? '数据同步完成，但当前时间段暂无优化师数据'
                  : '暂无数据，请先点击"同步数据"按钮生成优化师报表'}
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-100">
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 whitespace-nowrap" style={{ minWidth: '140px' }}>优化师</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">总消耗</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">消耗占比</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">日均消耗</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">活跃天数</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">Campaign 数</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">注册</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">收入</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">ROAS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map(row => (
                      <OptimizerRow key={row.optimizer_name} row={row} startDate={dateRange.startDate} endDate={dateRange.endDate} platform={platform} />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </SectionCard>
        </>
      )}
    </div>
  )
}
