import { useState, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { PageHeader } from '@/components/common/PageHeader'
import { StatCard } from '@/components/common/StatCard'
import { SectionCard } from '@/components/common/SectionCard'
import { DataTable, type Column } from '@/components/common/DataTable'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import {
  Image, MousePointerClick, Eye, TrendingUp, Trophy, AlertTriangle,
  Loader2, AlertCircle, DollarSign, Search, X,
} from 'lucide-react'
import {
  fetchCreativeAnalysis,
  fetchCreativeDramaOptions,
  type CreativeItem,
} from '@/services/biz'
import { GlobalSyncBar } from '@/components/common/GlobalSyncBar'

const fmtUsd = (n: number | null | undefined) =>
  n != null ? `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}` : '--'
const fmt = (n: number | null | undefined) => n != null ? n.toLocaleString() : '--'
const fmtPct = (n: number | null | undefined) => n != null ? `${(n * 100).toFixed(2)}%` : '--'
const fmtRoas = (n: number | null | undefined) => n != null ? n.toFixed(2) : '--'

function PlatformBadge({ platform }: { platform: string }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${
      platform === 'tiktok' ? 'bg-sky-50 text-sky-600' : 'bg-indigo-50 text-indigo-600'
    }`}>
      {platform === 'tiktok' ? 'TikTok' : 'Meta'}
    </span>
  )
}

export default function CreativeAnalysisPage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('30d'))
  const [platform, setPlatform] = useState('')
  const [contentKey, setContentKey] = useState('')          // 精确剧
  const [languageCode, setLanguageCode] = useState('')      // 语言
  const [dramaKwInput, setDramaKwInput] = useState('')      // 剧名搜索框
  const [dramaKeyword, setDramaKeyword] = useState('')      // 已确认的剧名关键词

  // 剧筛选选项（按日期 + 平台动态获取）
  const { data: dramaOpts } = useQuery({
    queryKey: ['biz', 'creative-drama-options', dateRange.startDate, dateRange.endDate, platform],
    queryFn: () => fetchCreativeDramaOptions({
      startDate: dateRange.startDate,
      endDate:   dateRange.endDate,
      platform:  platform || undefined,
    }),
    staleTime: 60_000,
  })

  const { data, isLoading, isError } = useQuery({
    queryKey: [
      'biz', 'creative-analysis',
      dateRange.startDate, dateRange.endDate, platform,
      contentKey, dramaKeyword, languageCode,
    ],
    queryFn: () => fetchCreativeAnalysis({
      startDate:    dateRange.startDate,
      endDate:      dateRange.endDate,
      platform:     platform || undefined,
      contentKey:   contentKey || undefined,
      dramaKeyword: dramaKeyword || undefined,
      languageCode: languageCode || undefined,
    }),
    staleTime: 30_000,
  })

  const overview = data?.overview
  const topCreatives = data?.top ?? []
  const lowCreatives = data?.low ?? []
  const allItems = data?.list ?? []

  const handleSearchDrama = useCallback(() => setDramaKeyword(dramaKwInput.trim()), [dramaKwInput])
  const handleClearDramaKw = useCallback(() => { setDramaKwInput(''); setDramaKeyword('') }, [])
  const handleResetDramaFilters = useCallback(() => {
    setContentKey('')
    setLanguageCode('')
    setDramaKwInput('')
    setDramaKeyword('')
  }, [])

  const hasDramaFilter = !!(contentKey || dramaKeyword || languageCode)

  const columns: Column<CreativeItem>[] = useMemo(() => [
    { key: 'ad_name', title: '素材名称', width: '320px', render: (r) => (
      <span
        className="text-sm text-gray-800 font-medium block max-w-[320px] whitespace-normal break-words line-clamp-2 leading-snug"
        title={r.ad_name || r.ad_id}
      >
        {r.ad_name || r.ad_id}
      </span>
    )},
    { key: 'drama', title: '剧名', width: '200px', render: (r) => {
      const name = r.localized_drama_name || ''
      if (!name) return <span className="text-gray-300 text-xs">--</span>
      return (
        <div className="flex flex-col gap-0.5">
          <span
            className="text-xs text-gray-700 block max-w-[200px] truncate"
            title={name}
          >
            {name}
          </span>
          {r.language_code && (
            <span className="text-[10px] text-gray-400">{r.language_code}</span>
          )}
        </div>
      )
    }},
    { key: 'platform', title: '渠道', render: (r) => <PlatformBadge platform={r.platform} /> },
    { key: 'impressions', title: '展示', align: 'right', render: (r) => fmt(r.impressions) },
    { key: 'clicks', title: '点击', align: 'right', render: (r) => fmt(r.clicks) },
    { key: 'ctr', title: 'CTR', align: 'right', render: (r) => fmtPct(r.ctr) },
    { key: 'completion_rate', title: '完播率', align: 'right', render: () => <span className="text-gray-300">--</span> },
    { key: 'spend', title: '消耗', align: 'right', render: (r) => fmtUsd(r.spend) },
    { key: 'revenue', title: '收入', align: 'right', render: (r) => fmtUsd(r.revenue) },
    { key: 'roas', title: 'ROI', align: 'right', render: (r) => {
      const v = r.roas
      if (v == null) return <span className="text-gray-300">--</span>
      return <span className={v < 1.5 ? 'text-red-600 font-medium' : 'text-green-600 font-medium'}>{fmtRoas(v)}</span>
    }},
  ], [])

  const miniCols: Column<CreativeItem>[] = useMemo(() => [
    { key: 'ad_name', title: '素材', width: '200px', render: (r) => (
      <div className="flex flex-col gap-0.5 max-w-[200px]">
        <span
          className="text-sm block whitespace-normal break-words line-clamp-2 leading-snug"
          title={r.ad_name || r.ad_id}
        >
          {r.ad_name || r.ad_id}
        </span>
        {r.localized_drama_name && (
          <span
            className="text-[10px] text-gray-400 truncate"
            title={`${r.localized_drama_name}${r.language_code ? ` · ${r.language_code}` : ''}`}
          >
            {r.localized_drama_name}{r.language_code ? ` · ${r.language_code}` : ''}
          </span>
        )}
      </div>
    )},
    { key: 'roas', title: 'ROI', align: 'right', render: (r) => {
      const v = r.roas
      if (v == null) return <span className="text-gray-300">--</span>
      return <span className={v < 1.5 ? 'text-red-600 font-medium' : 'text-green-600 font-medium'}>{fmtRoas(v)}</span>
    }},
    { key: 'spend', title: '消耗', align: 'right', render: (r) => fmtUsd(r.spend) },
  ], [])

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="素材分析" description="分析素材表现，发现高效与低效素材" />
      <GlobalSyncBar />

      {/* 日期 + 筛选区 */}
      <div className="mb-6 space-y-3">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />

        <div className="flex flex-wrap items-center gap-3">
          {/* 平台 */}
          <select
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100"
            value={platform}
            onChange={e => setPlatform(e.target.value)}
          >
            <option value="">全部平台</option>
            <option value="tiktok">TikTok</option>
            <option value="meta">Meta</option>
          </select>

          {/* 剧（content_key 精确匹配） */}
          <select
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100 max-w-[260px]"
            value={contentKey}
            onChange={e => setContentKey(e.target.value)}
            title={dramaOpts?.dramas.length ? `共 ${dramaOpts.dramas.length} 部剧（按消耗排序）` : ''}
          >
            <option value="">全部剧（{dramaOpts?.dramas.length ?? 0}）</option>
            {(dramaOpts?.dramas ?? []).map(d => (
              <option key={d.content_key} value={d.content_key}>
                {d.localized_drama_name || d.content_key}
                {d.language_code ? ` · ${d.language_code}` : ''}
              </option>
            ))}
          </select>

          {/* 语言 */}
          <select
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100"
            value={languageCode}
            onChange={e => setLanguageCode(e.target.value)}
          >
            <option value="">全部语言</option>
            {(dramaOpts?.languages ?? []).map(l => (
              <option key={l} value={l}>{l}</option>
            ))}
          </select>

          {/* 剧名关键词 */}
          <div className="flex items-center gap-1.5">
            <div className="relative flex items-center">
              <Search className="absolute left-2.5 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
              <input
                type="text"
                placeholder="搜索剧名..."
                className="text-sm border border-gray-200 rounded-lg pl-8 pr-8 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100 w-44"
                value={dramaKwInput}
                onChange={e => setDramaKwInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleSearchDrama()}
              />
              {dramaKwInput && (
                <button
                  className="absolute right-2 text-gray-300 hover:text-gray-500"
                  onClick={handleClearDramaKw}
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
            <button
              className="text-sm px-3 py-1.5 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors"
              onClick={handleSearchDrama}
            >
              搜索
            </button>
          </div>

          {/* 重置剧筛选 */}
          {hasDramaFilter && (
            <button
              className="text-xs px-2 py-1 text-gray-400 hover:text-gray-700 hover:bg-gray-50 rounded transition-colors"
              onClick={handleResetDramaFilters}
            >
              清除剧筛选
            </button>
          )}
        </div>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" /><span className="text-sm">加载中...</span>
        </div>
      )}

      {isError && (
        <div className="flex flex-col items-center justify-center py-24 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" />
          <p className="text-sm font-medium">数据加载失败</p>
          <p className="text-xs mt-1 text-gray-400">请检查后端服务或网络连接</p>
        </div>
      )}

      {!isLoading && !isError && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
            <StatCard label="素材总数" value={overview?.total_creatives ?? 0} icon={Image} />
            <StatCard label="总消耗" value={overview?.total_spend != null ? fmtUsd(overview.total_spend) : '--'} icon={DollarSign} />
            <StatCard label="平均 CTR" value={overview?.avg_ctr != null ? fmtPct(overview.avg_ctr) : '--'} icon={MousePointerClick} />
            <StatCard label="平均完播率" value="--" icon={Eye} />
            <StatCard label="平均 ROI" value={overview?.avg_roas != null ? fmtRoas(overview.avg_roas) : '--'} icon={TrendingUp} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <SectionCard title="Top 素材" extra={<Trophy className="w-4 h-4 text-amber-400" />} noPadding>
              {topCreatives.length > 0
                ? <DataTable columns={miniCols} data={topCreatives} rowKey={(r) => r.ad_id} />
                : <div className="px-5 py-8 text-center text-sm text-gray-300">当前时间段暂无数据</div>}
            </SectionCard>
            <SectionCard title="低表现素材" extra={<AlertTriangle className="w-4 h-4 text-red-400" />} noPadding>
              {lowCreatives.length > 0
                ? <DataTable columns={miniCols} data={lowCreatives} rowKey={(r) => r.ad_id} />
                : <div className="px-5 py-8 text-center text-sm text-gray-300">当前时间段暂无数据</div>}
            </SectionCard>
          </div>

          <SectionCard
            title="素材表现明细"
            extra={
              <span className="text-xs text-gray-400">
                共 {allItems.length} 个{hasDramaFilter && ' · 已应用剧筛选'}
              </span>
            }
            noPadding
          >
            {allItems.length > 0
              ? <DataTable columns={columns} data={allItems} rowKey={(r) => r.ad_id} />
              : <div className="px-5 py-12 text-center text-sm text-gray-300">当前时间段暂无素材数据</div>}
          </SectionCard>
        </>
      )}
    </div>
  )
}
