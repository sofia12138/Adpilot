import { useState, useMemo } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { PageHeader } from '@/components/common/PageHeader'
import { StatCard } from '@/components/common/StatCard'
import { SectionCard } from '@/components/common/SectionCard'
import { InsightPanel } from '@/components/common/InsightPanel'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { DollarSign, TrendingUp, BarChart3, ArrowRight, Loader2, AlertCircle } from 'lucide-react'
import { useBizOverview, useCampaignDaily } from '@/hooks/use-biz'
import { GlobalSyncBar } from '@/components/common/GlobalSyncBar'
import { SpendRevenueTrendChart } from '@/components/charts/SpendRevenueTrendChart'
import { useInsightConfig } from '@/hooks/use-insight'
import { buildOverviewInsights } from '@/services/insight-engine'

const platformOptions = ['全部', 'TikTok', 'Meta'] as const
type PlatformOption = typeof platformOptions[number]

const platformApiValue = (p: PlatformOption) =>
  p === '全部' ? undefined : p.toLowerCase()

const fmtUsd = (n: number) => `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`

function buildAdsLink(platform?: string, dateRange?: DateRange) {
  const params = new URLSearchParams()
  if (platform) params.set('platform', platform)
  if (dateRange) {
    params.set('startDate', dateRange.startDate)
    params.set('endDate', dateRange.endDate)
  }
  const q = params.toString()
  return q ? `/ads?${q}` : '/ads'
}

export default function OverviewPage() {
  const [searchParams] = useSearchParams()
  const initPlatform = (searchParams.get('platform') ?? '全部') as PlatformOption

  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('all'))
  const [platform, setPlatform] = useState<PlatformOption>(
    platformOptions.includes(initPlatform) ? initPlatform : '全部',
  )

  const apiPlatform = platformApiValue(platform)

  const { data: overview, isLoading, isError } = useBizOverview({
    ...dateRange,
    platform: apiPlatform,
  })

  const { data: tiktokOverview } = useBizOverview({ ...dateRange, platform: 'tiktok' })
  const { data: metaOverview }   = useBizOverview({ ...dateRange, platform: 'meta' })
  const { data: dailyData, isLoading: dailyLoading } = useCampaignDaily({ ...dateRange, platform: apiPlatform, page_size: 200 })
  const { data: insightCfg } = useInsightConfig()

  const hasData = overview && overview.total_spend > 0

  const insights = useMemo(() => {
    if (!insightCfg) return []
    return buildOverviewInsights(
      { overview: overview ?? null, tiktokOverview: tiktokOverview ?? null, metaOverview: metaOverview ?? null },
      insightCfg,
    )
  }, [overview, tiktokOverview, metaOverview, insightCfg])

  const channels = useMemo(() => {
    if (!tiktokOverview && !metaOverview) return []
    const totalSpend = (tiktokOverview?.total_spend ?? 0) + (metaOverview?.total_spend ?? 0)
    const result: { platform: string; apiKey: string; spend: number; revenue: number; roi: number; share: number }[] = []
    if (tiktokOverview) {
      result.push({
        platform: 'TikTok', apiKey: 'tiktok',
        spend: tiktokOverview.total_spend, revenue: tiktokOverview.total_revenue,
        roi: tiktokOverview.avg_roas ?? 0,
        share: totalSpend > 0 ? Math.round(tiktokOverview.total_spend / totalSpend * 100) : 0,
      })
    }
    if (metaOverview) {
      result.push({
        platform: 'Meta', apiKey: 'meta',
        spend: metaOverview.total_spend, revenue: metaOverview.total_revenue,
        roi: metaOverview.avg_roas ?? 0,
        share: totalSpend > 0 ? Math.round(metaOverview.total_spend / totalSpend * 100) : 0,
      })
    }
    return result
  }, [tiktokOverview, metaOverview])

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader
        title="数据总览"
        description="分析消耗、收入与 ROI 的变化原因"
        action={
          <Link
            to={buildAdsLink(apiPlatform, dateRange)}
            className="text-sm text-blue-500 hover:text-blue-700 flex items-center gap-1"
          >
            查看广告明细 <ArrowRight className="w-3.5 h-3.5" />
          </Link>
        }
      />

      {/* 同步状态栏 */}
      <GlobalSyncBar />

      {/* 筛选栏 */}
      <div className="flex items-center gap-4 mb-6">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />
        <div className="flex rounded-lg border border-gray-200 overflow-hidden text-xs">
          {platformOptions.map(p => (
            <button
              key={p}
              onClick={() => setPlatform(p)}
              className={`px-3 py-1.5 transition ${
                platform === p ? 'bg-blue-500 text-white font-medium' : 'bg-white text-gray-600 hover:bg-gray-50'
              }`}
            >
              {p}
            </button>
          ))}
        </div>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中…</span>
        </div>
      )}

      {isError && (
        <div className="flex flex-col items-center justify-center py-24 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" />
          <p className="text-sm font-medium">数据加载失败</p>
        </div>
      )}

      {!isLoading && !isError && (
        <>
          <div className="grid grid-cols-3 gap-4 mb-6">
            <StatCard label="总消耗" value={hasData ? fmtUsd(overview.total_spend) : '-'} icon={DollarSign} href={buildAdsLink(apiPlatform, dateRange)} />
            <StatCard label="总收入" value={hasData ? fmtUsd(overview.total_revenue) : '-'} icon={TrendingUp} href={buildAdsLink(apiPlatform, dateRange)} />
            <StatCard
              label="综合 ROAS"
              value={hasData && overview.avg_roas ? overview.avg_roas.toFixed(2) : '-'}
              icon={BarChart3}
              href={buildAdsLink(apiPlatform, dateRange)}
            />
          </div>

          <SectionCard title="消耗 & 收入趋势" className="mb-6">
            <SpendRevenueTrendChart data={dailyData?.list ?? []} loading={dailyLoading} />
          </SectionCard>

          {platform === '全部' && channels.length > 0 && (
            <SectionCard title="渠道拆分" extra="点击渠道卡片查看广告明细" className="mb-6">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {channels.map(ch => (
                  <Link
                    key={ch.platform}
                    to={buildAdsLink(ch.apiKey, dateRange)}
                    className="rounded-lg border border-gray-100 p-4 hover:border-blue-200 hover:shadow-sm transition-all group"
                  >
                    <div className="flex items-center justify-between mb-3">
                      <span className={`inline-block px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        ch.platform === 'TikTok' ? 'bg-sky-50 text-sky-600' : 'bg-indigo-50 text-indigo-600'
                      }`}>
                        {ch.platform}
                      </span>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400">占比 {ch.share}%</span>
                        <ArrowRight className="w-3.5 h-3.5 text-gray-300 group-hover:text-blue-400 transition-colors" />
                      </div>
                    </div>
                    <div className="grid grid-cols-3 gap-3 text-center">
                      <div>
                        <p className="text-xs text-gray-400 mb-1">消耗</p>
                        <p className="text-sm font-semibold">{fmtUsd(ch.spend)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-400 mb-1">收入</p>
                        <p className="text-sm font-semibold text-green-600">{fmtUsd(ch.revenue)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-400 mb-1">ROAS</p>
                        <p className="text-sm font-semibold text-blue-600">{ch.roi.toFixed(2)}</p>
                      </div>
                    </div>
                    <div className="mt-3 h-1.5 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${ch.platform === 'TikTok' ? 'bg-sky-400' : 'bg-indigo-400'}`}
                        style={{ width: `${ch.share}%` }}
                      />
                    </div>
                  </Link>
                ))}
              </div>
            </SectionCard>
          )}

          <InsightPanel insights={insights} loading={isLoading} />
        </>
      )}
    </div>
  )
}
