import { useState, useMemo } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { StatCard } from '@/components/common/StatCard'
import { SectionCard } from '@/components/common/SectionCard'
import { InsightPanel } from '@/components/common/InsightPanel'
import { DataTable, type Column } from '@/components/common/DataTable'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { DollarSign, TrendingUp, BarChart3, Loader2 } from 'lucide-react'
import { useBizOverview, useBizTopCampaigns } from '@/hooks/use-biz'
import { useInsightConfig } from '@/hooks/use-insight'
import { buildBizInsights } from '@/services/insight-engine'

const fmtUsd = (n: number) => `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`

export default function BizAnalysisPage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('all'))

  const { data: overview, isLoading } = useBizOverview(dateRange)
  const { data: topCampaigns } = useBizTopCampaigns({ ...dateRange, limit: 10 })
  const { data: insightCfg } = useInsightConfig()

  const insights = useMemo(() => {
    if (!insightCfg) return []
    return buildBizInsights(
      { overview: overview ?? null, topCampaigns: topCampaigns ?? [] },
      insightCfg,
    )
  }, [overview, topCampaigns, insightCfg])

  const campaigns = (topCampaigns ?? []).map(c => ({
    id: c.campaign_id,
    name: c.campaign_name || c.campaign_id,
    platform: c.platform,
    spend: c.total_spend,
    revenue: c.total_revenue,
    roas: c.avg_roas ?? 0,
    roi_d0: c.roi_d0,
    roi_d7: c.roi_d7,
    roi_d30: c.roi_d30,
  }))

  const fmtRoi = (v: number | null | undefined) =>
    v != null && v > 0 ? v.toFixed(2) : '-'

  const columns: Column<typeof campaigns[number]>[] = [
    { key: 'name', title: 'Campaign', render: (r) => <span className="text-sm text-gray-800 font-medium truncate max-w-[180px] block">{r.name}</span> },
    { key: 'platform', title: '渠道', render: (r) => (
      <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${r.platform === 'tiktok' ? 'bg-sky-50 text-sky-600' : 'bg-indigo-50 text-indigo-600'}`}>
        {r.platform === 'tiktok' ? 'TikTok' : 'Meta'}
      </span>
    )},
    { key: 'spend', title: '消耗', align: 'right', render: (r) => fmtUsd(r.spend) },
    { key: 'revenue', title: '收入', align: 'right', render: (r) => fmtUsd(r.revenue) },
    { key: 'roas', title: 'ROAS', align: 'right', render: (r) => (
      <span className={r.roas > 0 && r.roas < 1.5 ? 'text-red-600 font-medium' : ''}>
        {r.roas > 0 ? r.roas.toFixed(2) : '-'}
      </span>
    )},
    { key: 'roi_d0', title: 'D0 ROI', align: 'right', render: (r) => <span className="tabular-nums text-xs">{fmtRoi(r.roi_d0)}</span> },
    { key: 'roi_d7', title: 'D7 ROI', align: 'right', render: (r) => <span className="tabular-nums text-xs">{fmtRoi(r.roi_d7)}</span> },
    { key: 'roi_d30', title: 'D30 ROI', align: 'right', render: (r) => <span className="tabular-nums text-xs">{fmtRoi(r.roi_d30)}</span> },
  ]
  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="业务分析" description="分析业务核心指标与 Campaign 表现" />

      <div className="mb-6">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中...</span>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
            <StatCard label="总消耗" value={overview ? fmtUsd(overview.total_spend) : '-'} icon={DollarSign} />
            <StatCard label="总收入" value={overview ? fmtUsd(overview.total_revenue) : '-'} icon={TrendingUp} />
            <StatCard label="ROAS" value={overview?.avg_roas ? overview.avg_roas.toFixed(2) : '-'} icon={BarChart3} />
            <StatCard label="D0 ROI" value={overview?.avg_roi_d0 != null ? overview.avg_roi_d0.toFixed(2) : '-'} icon={BarChart3} />
            <StatCard label="D7 ROI" value={overview?.avg_roi_d7 != null ? overview.avg_roi_d7.toFixed(2) : '-'} icon={BarChart3} />
            <StatCard label="D30 ROI" value={overview?.avg_roi_d30 != null ? overview.avg_roi_d30.toFixed(2) : '-'} icon={BarChart3} />
          </div>

          <SectionCard title="Top 10 Campaigns" noPadding className="mb-6">
            <DataTable columns={columns} data={campaigns} rowKey={(r) => r.id} />
          </SectionCard>

          <InsightPanel insights={insights} loading={isLoading} />
        </>
      )}
    </div>
  )
}
