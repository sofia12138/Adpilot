import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { PageHeader } from '@/components/common/PageHeader'
import { StatCard } from '@/components/common/StatCard'
import { SectionCard } from '@/components/common/SectionCard'
import { DataTable, type Column } from '@/components/common/DataTable'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { Image, MousePointerClick, Eye, TrendingUp, Trophy, AlertTriangle, Loader2, AlertCircle } from 'lucide-react'
import { fetchCreativeAnalysis, type CreativeItem } from '@/services/biz'

const fmtUsd = (n: number | null) => n != null ? `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}` : '--'
const fmt = (n: number | null) => n != null ? n.toLocaleString() : '--'
const fmtPct = (n: number | null) => n != null ? `${(n * 100).toFixed(2)}%` : '--'
const fmtRoas = (n: number | null) => n != null ? n.toFixed(2) : '--'

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

  const { data, isLoading, isError } = useQuery({
    queryKey: ['biz', 'creative-analysis', dateRange.startDate, dateRange.endDate],
    queryFn: () => fetchCreativeAnalysis({
      startDate: dateRange.startDate,
      endDate: dateRange.endDate,
    }),
    staleTime: 30_000,
  })

  const overview = data?.overview
  const topCreatives = data?.top ?? []
  const lowCreatives = data?.low ?? []
  const allItems = data?.list ?? []

  const columns: Column<CreativeItem>[] = useMemo(() => [
    { key: 'ad_name', title: '素材名称', render: (r) => (
      <span className="text-sm text-gray-800 font-medium truncate max-w-[220px] block" title={r.ad_name || r.ad_id}>
        {r.ad_name || r.ad_id}
      </span>
    )},
    { key: 'platform', title: '渠道', render: (r) => <PlatformBadge platform={r.platform} /> },
    { key: 'impressions', title: '展示', align: 'right', render: (r) => fmt(r.impressions) },
    { key: 'clicks', title: '点击', align: 'right', render: (r) => fmt(r.clicks) },
    { key: 'ctr', title: 'CTR', align: 'right', render: (r) => fmtPct(r.ctr) },
    { key: 'spend', title: '消耗', align: 'right', render: (r) => fmtUsd(r.spend) },
    { key: 'revenue', title: '收入', align: 'right', render: (r) => fmtUsd(r.revenue) },
    { key: 'roas', title: 'ROI', align: 'right', render: (r) => {
      const v = r.roas
      if (v == null) return <span className="text-gray-300">--</span>
      return <span className={v < 1.5 ? 'text-red-600 font-medium' : 'text-green-600 font-medium'}>{fmtRoas(v)}</span>
    }},
  ], [])

  const miniCols: Column<CreativeItem>[] = useMemo(() => [
    { key: 'ad_name', title: '素材', render: (r) => (
      <span className="text-sm truncate max-w-[160px] block" title={r.ad_name || r.ad_id}>
        {r.ad_name || r.ad_id}
      </span>
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

      <div className="mb-6">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />
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
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <StatCard label="素材总数" value={overview?.total_creatives ?? 0} icon={Image} />
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

          <SectionCard title="素材表现明细" extra={<span className="text-xs text-gray-400">共 {allItems.length} 个</span>} noPadding>
            {allItems.length > 0
              ? <DataTable columns={columns} data={allItems} rowKey={(r) => r.ad_id} />
              : <div className="px-5 py-12 text-center text-sm text-gray-300">当前时间段暂无素材数据</div>}
          </SectionCard>
        </>
      )}
    </div>
  )
}
