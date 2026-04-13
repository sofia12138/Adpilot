import { useState, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, ChevronRight, Users, DollarSign, Image, MousePointerClick, Loader2, AlertCircle, Search, X } from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { StatCard } from '@/components/common/StatCard'
import { SectionCard } from '@/components/common/SectionCard'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { GlobalSyncBar } from '@/components/common/GlobalSyncBar'
import {
  fetchDesignerSummary,
  fetchDesignerMaterials,
  type DesignerSummaryItem,
  type DesignerMaterialItem,
} from '@/services/designer'

// ---------------------------------------------------------------------------
// 格式化工具
// ---------------------------------------------------------------------------

const fmtUsd = (n: number | null | undefined) =>
  n != null ? `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}` : '--'
const fmt = (n: number | null | undefined) => n != null ? n.toLocaleString() : '--'
const fmtPct = (n: number | null | undefined) => n != null ? `${(n * 100).toFixed(2)}%` : '--'
const fmtRoas = (n: number | null | undefined) => n != null ? n.toFixed(2) : '--'

// ---------------------------------------------------------------------------
// 平台徽标
// ---------------------------------------------------------------------------

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
// 素材明细表（折叠子表）
// ---------------------------------------------------------------------------

function MaterialDetailTable({ items }: { items: DesignerMaterialItem[] }) {
  if (items.length === 0) {
    return <div className="px-6 py-6 text-center text-sm text-gray-300">暂无素材数据</div>
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-gray-100 bg-gray-50/60">
            <th className="px-4 py-2.5 text-left font-medium text-gray-400 whitespace-nowrap" style={{ width: '280px', maxWidth: '280px' }}>素材名称</th>
            <th className="px-4 py-2.5 text-left font-medium text-gray-400 whitespace-nowrap">剧集/活动</th>
            <th className="px-4 py-2.5 text-left font-medium text-gray-400 whitespace-nowrap">平台</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">消耗</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">展示</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">点击</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">注册</th>
            <th className="px-4 py-2.5 text-right font-medium text-gray-400 whitespace-nowrap tabular-nums">收入</th>
          </tr>
        </thead>
        <tbody>
          {items.map((m, idx) => (
            <tr key={`${m.ad_id}-${idx}`} className="border-b border-gray-50 last:border-0 hover:bg-blue-50/20 transition-colors">
              <td className="px-4 py-2.5" style={{ width: '280px', maxWidth: '280px' }}>
                <span
                  className="block max-w-[280px] whitespace-normal break-words line-clamp-2 text-xs text-gray-700 leading-snug"
                  title={m.ad_name || m.ad_id}
                >
                  {m.ad_name || m.ad_id || '--'}
                </span>
              </td>
              <td className="px-4 py-2.5">
                <span
                  className="block max-w-[160px] text-xs text-gray-500 truncate"
                  title={m.campaign_name}
                >
                  {m.campaign_name || '--'}
                </span>
              </td>
              <td className="px-4 py-2.5"><PlatformBadge platform={m.platform} /></td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-700">{fmtUsd(m.spend)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmt(m.impressions)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmt(m.clicks)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmt(m.registrations)}</td>
              <td className="px-4 py-2.5 text-right tabular-nums text-gray-600">{fmtUsd(m.purchase_value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// 设计师行（支持展开）
// ---------------------------------------------------------------------------

interface DesignerRowProps {
  row: DesignerSummaryItem
  startDate: string
  endDate: string
  platform: string
}

function DesignerRow({ row, startDate, endDate, platform }: DesignerRowProps) {
  const [expanded, setExpanded] = useState(false)

  const { data: materials, isLoading: matLoading } = useQuery({
    queryKey: ['designer-materials', row.designer_name, startDate, endDate, platform],
    queryFn: () => fetchDesignerMaterials({
      startDate,
      endDate,
      designerName: row.designer_name,
      platform: platform || undefined,
    }),
    enabled: expanded,
    staleTime: 60_000,
  })

  const roasClass = row.roas == null
    ? 'text-gray-300'
    : row.roas < 1.5
      ? 'text-red-600 font-medium'
      : 'text-green-600 font-medium'

  return (
    <>
      <tr
        className="border-b border-gray-50 hover:bg-blue-50/30 transition-colors cursor-pointer"
        onClick={() => setExpanded(v => !v)}
      >
        {/* 展开箭头 + 设计师名 */}
        <td className="px-4 py-3 text-sm">
          <div className="flex items-center gap-2">
            <span className="text-gray-400 flex-shrink-0">
              {expanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
            </span>
            <span className="font-medium text-gray-800">{row.designer_name}</span>
          </div>
        </td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{row.material_count}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums font-medium text-gray-800">{fmtUsd(row.total_spend)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmt(row.impressions)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmt(row.clicks)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmtPct(row.ctr)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmt(row.conversions)}</td>
        <td className="px-4 py-3 text-sm text-right tabular-nums text-gray-600">{fmtUsd(row.purchase_value)}</td>
        <td className={`px-4 py-3 text-sm text-right tabular-nums ${roasClass}`}>{fmtRoas(row.roas)}</td>
      </tr>

      {expanded && (
        <tr>
          <td colSpan={9} className="p-0 bg-gray-50/50">
            {matLoading ? (
              <div className="flex items-center justify-center py-6 text-gray-400 text-sm gap-2">
                <Loader2 className="w-4 h-4 animate-spin" />加载素材明细...
              </div>
            ) : (
              <MaterialDetailTable items={materials ?? []} />
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

export default function DesignerPerformancePage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('30d'))
  const [platform, setPlatform] = useState('')
  const [keyword, setKeyword] = useState('')
  const [keywordInput, setKeywordInput] = useState('')

  const { data, isLoading, isError } = useQuery({
    queryKey: ['designer-performance', 'summary', dateRange.startDate, dateRange.endDate, platform, keyword],
    queryFn: () => fetchDesignerSummary({
      startDate: dateRange.startDate,
      endDate: dateRange.endDate,
      platform: platform || undefined,
      keyword: keyword || undefined,
    }),
    staleTime: 30_000,
  })

  const rows = data ?? []

  const overview = useMemo(() => {
    const totalSpend = rows.reduce((s, r) => s + r.total_spend, 0)
    const totalImpressions = rows.reduce((s, r) => s + r.impressions, 0)
    const totalClicks = rows.reduce((s, r) => s + r.clicks, 0)
    const totalMaterials = rows.reduce((s, r) => s + r.material_count, 0)
    return { totalSpend, totalImpressions, totalClicks, totalMaterials, designerCount: rows.length }
  }, [rows])

  const handleSearch = useCallback(() => {
    setKeyword(keywordInput.trim())
  }, [keywordInput])

  const handleClearKeyword = useCallback(() => {
    setKeywordInput('')
    setKeyword('')
  }, [])

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="设计师人效报表" description="按设计师维度汇总素材投放表现，点击设计师行可展开素材明细" />
      <GlobalSyncBar />

      {/* 日期 + 筛选区 */}
      <div className="mb-6 space-y-3">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />

        <div className="flex flex-wrap items-center gap-3">
          {/* 平台筛选 */}
          <select
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100"
            value={platform}
            onChange={e => setPlatform(e.target.value)}
          >
            <option value="">全部平台</option>
            <option value="tiktok">TikTok</option>
            <option value="meta">Meta</option>
          </select>

          {/* 设计师关键词搜索 */}
          <div className="flex items-center gap-1.5">
            <div className="relative flex items-center">
              <Search className="absolute left-2.5 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
              <input
                type="text"
                placeholder="搜索设计师..."
                className="text-sm border border-gray-200 rounded-lg pl-8 pr-8 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100 w-44"
                value={keywordInput}
                onChange={e => setKeywordInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleSearch()}
              />
              {keywordInput && (
                <button
                  className="absolute right-2 text-gray-300 hover:text-gray-500"
                  onClick={handleClearKeyword}
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
            <button
              className="text-sm px-3 py-1.5 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors"
              onClick={handleSearch}
            >
              搜索
            </button>
          </div>
        </div>
      </div>

      {/* 加载中 */}
      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中...</span>
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
          {/* KPI 概览卡片 */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
            <StatCard label="设计师数" value={overview.designerCount} icon={Users} />
            <StatCard label="素材数" value={overview.totalMaterials} icon={Image} />
            <StatCard label="总消耗" value={fmtUsd(overview.totalSpend)} icon={DollarSign} />
            <StatCard label="总展示" value={fmt(overview.totalImpressions)} icon={MousePointerClick} />
            <StatCard label="总点击" value={fmt(overview.totalClicks)} icon={MousePointerClick} />
          </div>

          {/* 主表格 */}
          <SectionCard
            title="设计师人效汇总"
            extra={<span className="text-xs text-gray-400">共 {rows.length} 位设计师 · 点击行展开素材明细</span>}
            noPadding
          >
            {rows.length === 0 ? (
              <div className="px-5 py-12 text-center text-sm text-gray-300">
                当前筛选条件下暂无数据
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-100">
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 whitespace-nowrap" style={{ minWidth: '140px' }}>设计师</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">素材数</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">总消耗</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">展示</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">点击</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">CTR</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">注册</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">收入</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-400 whitespace-nowrap">ROAS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map(row => (
                      <DesignerRow
                        key={row.designer_name}
                        row={row}
                        startDate={dateRange.startDate}
                        endDate={dateRange.endDate}
                        platform={platform}
                      />
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
