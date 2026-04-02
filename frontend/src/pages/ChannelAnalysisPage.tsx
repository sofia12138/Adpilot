import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { PageHeader } from '@/components/common/PageHeader'
import { StatCard } from '@/components/common/StatCard'
import { SectionCard } from '@/components/common/SectionCard'
import { InsightPanel } from '@/components/common/InsightPanel'
import { DataTable, type Column } from '@/components/common/DataTable'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { DollarSign, TrendingUp, BarChart3, Loader2, ArrowRight } from 'lucide-react'
import { useBizOverview, useBizTopCampaigns } from '@/hooks/use-biz'
import { useInsightConfig } from '@/hooks/use-insight'
import { buildChannelInsights } from '@/services/insight-engine'

const fmtUsd = (n: number) => `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`
const fmt = (n: number) => n.toLocaleString()

interface ChannelRow {
  platform: string
  spend: number
  revenue: number
  roas: number
  impressions: number
  clicks: number
  installs: number
  share: number
}

export default function ChannelAnalysisPage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('all'))

  const { data: ttOv, isLoading: ttL } = useBizOverview({ ...dateRange, platform: 'tiktok' })
  const { data: metaOv, isLoading: metaL } = useBizOverview({ ...dateRange, platform: 'meta' })
  const { data: totalOv, isLoading: totalL } = useBizOverview(dateRange)

  const { data: ttCamps } = useBizTopCampaigns({ ...dateRange, platform: 'tiktok', limit: 5 })
  const { data: metaCamps } = useBizTopCampaigns({ ...dateRange, platform: 'meta', limit: 5 })

  const { data: insightCfg } = useInsightConfig()
  const isLoading = ttL || metaL || totalL

  const insights = useMemo(() => {
    if (!insightCfg) return []
    return buildChannelInsights(
      {
        totalOverview: totalOv ?? null,
        tiktokOverview: ttOv ?? null,
        metaOverview: metaOv ?? null,
        tiktokTopCampaigns: ttCamps ?? [],
        metaTopCampaigns: metaCamps ?? [],
      },
      insightCfg,
    )
  }, [totalOv, ttOv, metaOv, ttCamps, metaCamps, insightCfg])

  const channels = useMemo<ChannelRow[]>(() => {
    const totalSpend = (ttOv?.total_spend ?? 0) + (metaOv?.total_spend ?? 0)
    const result: ChannelRow[] = []
    if (ttOv) {
      result.push({
        platform: 'TikTok',
        spend: ttOv.total_spend, revenue: ttOv.total_revenue,
        roas: ttOv.avg_roas ?? 0,
        impressions: ttOv.total_impressions, clicks: ttOv.total_clicks, installs: ttOv.total_installs,
        share: totalSpend > 0 ? Math.round(ttOv.total_spend / totalSpend * 100) : 0,
      })
    }
    if (metaOv) {
      result.push({
        platform: 'Meta',
        spend: metaOv.total_spend, revenue: metaOv.total_revenue,
        roas: metaOv.avg_roas ?? 0,
        impressions: metaOv.total_impressions, clicks: metaOv.total_clicks, installs: metaOv.total_installs,
        share: totalSpend > 0 ? Math.round(metaOv.total_spend / totalSpend * 100) : 0,
      })
    }
    return result
  }, [ttOv, metaOv])

  const compareColumns: Column<ChannelRow>[] = [
    { key: 'platform', title: '渠道', render: (r) => (
      <span className={`inline-block px-2.5 py-0.5 rounded-full text-xs font-medium ${
        r.platform === 'TikTok' ? 'bg-sky-50 text-sky-600' : 'bg-indigo-50 text-indigo-600'
      }`}>{r.platform}</span>
    )},
    { key: 'spend', title: '消耗', align: 'right', render: (r) => fmtUsd(r.spend) },
    { key: 'revenue', title: '收入', align: 'right', render: (r) => fmtUsd(r.revenue) },
    { key: 'roas', title: 'ROAS', align: 'right', render: (r) => (
      <span className={r.roas > 0 && r.roas < 1.5 ? 'text-red-600 font-medium' : ''}>
        {r.roas > 0 ? r.roas.toFixed(2) : '-'}
      </span>
    )},
    { key: 'impressions', title: '展示', align: 'right', render: (r) => fmt(r.impressions) },
    { key: 'clicks', title: '点击', align: 'right', render: (r) => fmt(r.clicks) },
    { key: 'installs', title: '安装', align: 'right', render: (r) => fmt(r.installs) },
    { key: 'share', title: '消耗占比', align: 'right', render: (r) => `${r.share}%` },
    { key: 'action', title: '', render: (r) => (
      <Link to={`/ads?platform=${r.platform.toLowerCase()}`} className="text-blue-500 hover:text-blue-700 text-xs flex items-center gap-0.5">
        明细 <ArrowRight className="w-3 h-3" />
      </Link>
    )},
  ]

  const topCampColumns: Column<{ name: string; spend: number; roas: number }>[] = [
    { key: 'name', title: 'Campaign', render: (r) => <span className="text-sm text-gray-800 truncate max-w-[180px] block">{r.name}</span> },
    { key: 'spend', title: '消耗', align: 'right', render: (r) => fmtUsd(r.spend) },
    { key: 'roas', title: 'ROAS', align: 'right', render: (r) => (
      <span className={r.roas > 0 && r.roas < 1.5 ? 'text-red-600 font-medium' : ''}>
        {r.roas > 0 ? r.roas.toFixed(2) : '-'}
      </span>
    )},
  ]

  const ttTopRows = (ttCamps ?? []).map(c => ({ name: c.campaign_name || c.campaign_id, spend: c.total_spend, roas: c.avg_roas ?? 0 }))
  const metaTopRows = (metaCamps ?? []).map(c => ({ name: c.campaign_name || c.campaign_id, spend: c.total_spend, roas: c.avg_roas ?? 0 }))

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="渠道分析" description="对比各广告渠道的投放效果" />

      <div className="mb-6">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中…</span>
        </div>
      )}

      {!isLoading && (
        <>
          <div className="grid grid-cols-3 gap-4 mb-6">
            <StatCard label="总消耗" value={totalOv ? fmtUsd(totalOv.total_spend) : '-'} icon={DollarSign} />
            <StatCard label="总收入" value={totalOv ? fmtUsd(totalOv.total_revenue) : '-'} icon={TrendingUp} />
            <StatCard label="综合 ROAS" value={totalOv?.avg_roas ? totalOv.avg_roas.toFixed(2) : '-'} icon={BarChart3} />
          </div>

          <SectionCard title="渠道对比" noPadding className="mb-6">
            <DataTable columns={compareColumns} data={channels} rowKey={(r) => r.platform} />
          </SectionCard>

          {channels.length >= 2 && (
            <SectionCard title="消耗占比" className="mb-6">
              <div className="flex items-center gap-4">
                {channels.map(ch => (
                  <div key={ch.platform} className="flex-1">
                    <div className="flex items-center justify-between mb-2">
                      <span className={`text-sm font-medium ${ch.platform === 'TikTok' ? 'text-sky-600' : 'text-indigo-600'}`}>
                        {ch.platform}
                      </span>
                      <span className="text-sm font-bold">{ch.share}%</span>
                    </div>
                    <div className="h-3 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${ch.platform === 'TikTok' ? 'bg-sky-400' : 'bg-indigo-400'}`}
                        style={{ width: `${ch.share}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </SectionCard>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <SectionCard title="TikTok Top 5 Campaigns" noPadding>
              {ttTopRows.length > 0
                ? <DataTable columns={topCampColumns} data={ttTopRows} rowKey={(r) => r.name} />
                : <div className="px-5 py-8 text-center text-sm text-gray-300">暂无数据</div>}
            </SectionCard>
            <SectionCard title="Meta Top 5 Campaigns" noPadding>
              {metaTopRows.length > 0
                ? <DataTable columns={topCampColumns} data={metaTopRows} rowKey={(r) => r.name} />
                : <div className="px-5 py-8 text-center text-sm text-gray-300">暂无数据</div>}
            </SectionCard>
          </div>

          <InsightPanel insights={insights} loading={isLoading} />
        </>
      )}
    </div>
  )
}
