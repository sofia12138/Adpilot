import { useState, useMemo } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { Loader2, AlertCircle, CheckCircle, AlertTriangle } from 'lucide-react'
import { useBizOverview } from '@/hooks/use-biz'
import { usePrdSummary } from '@/hooks/use-bizdata'

const fmtUsd = (n: number) => `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`
const fmt = (n: number) => n.toLocaleString()

interface CompareRow {
  metric: string; prd: string; biz: string
  prdNum: number | null; bizNum: number | null; hasDiff: boolean
}

export default function DataComparePage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('all'))

  const { data: bizOv, isLoading: bizL, isError: bizE } = useBizOverview(dateRange)
  const { data: prdOv, isLoading: prdL, isError: prdE } = usePrdSummary(dateRange.startDate, dateRange.endDate)

  const isLoading = bizL || prdL
  const hasPrd = !!prdOv
  const hasBiz = !!bizOv

  const rows = useMemo<CompareRow[]>(() => {
    const r: CompareRow[] = []
    const add = (metric: string, prdVal: number | undefined, bizVal: number | undefined, formatter: (n: number) => string = fmt) => {
      const pStr = prdVal != null ? formatter(prdVal) : '-'
      const bStr = bizVal != null ? formatter(bizVal) : '-'
      r.push({ metric, prd: pStr, biz: bStr, prdNum: prdVal ?? null, bizNum: bizVal ?? null,
        hasDiff: pStr !== bStr && pStr !== '-' && bStr !== '-' })
    }

    add('广告消耗', prdOv?.ad_cost_amount, bizOv?.total_spend, fmtUsd)
    add('总收入', prdOv?.recharge_total_amount, bizOv?.total_revenue, fmtUsd)
    add('注册/转化数', prdOv?.register_count, bizOv?.total_conversions)
    add('安装数', undefined, bizOv?.total_installs)
    add('点击数', undefined, bizOv?.total_clicks)
    add('展示数', undefined, bizOv?.total_impressions)
    add('首充人数', prdOv?.first_subscribe_count, undefined)
    add('首充金额', prdOv?.first_subscribe_amount, undefined, fmtUsd)
    add('复充人数', prdOv?.repeat_subscribe_count, undefined)
    add('复充金额', prdOv?.repeat_subscribe_amount, undefined, fmtUsd)
    add('D1 ROI', prdOv?.day1_roi, undefined)
    add('D7 ROI', prdOv?.day7_roi, undefined)
    add('ROAS (BIZ)', undefined, bizOv?.avg_roas != null ? bizOv.avg_roas : undefined, (n) => n.toFixed(2))
    return r
  }, [prdOv, bizOv])

  const diffRows = rows.filter(r => r.prdNum !== null && r.bizNum !== null)
  const warnCount = diffRows.filter(r => r.hasDiff).length
  const okCount = diffRows.filter(r => !r.hasDiff).length

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="数据对比" description="PRD 与 BIZ 数据对比及差异分析" />

      <div className="flex items-center gap-4 mb-6">
        <DateRangeFilter value={dateRange} onChange={setDateRange} />
      </div>

      <div className="flex items-center gap-3 mb-4">
        <div className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs ${hasPrd ? 'bg-green-50 text-green-600' : 'bg-gray-100 text-gray-400'}`}>
          <span className={`w-2 h-2 rounded-full ${hasPrd ? 'bg-green-400' : 'bg-gray-300'}`} /> PRD（产研数据库）
        </div>
        <div className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs ${hasBiz ? 'bg-green-50 text-green-600' : 'bg-gray-100 text-gray-400'}`}>
          <span className={`w-2 h-2 rounded-full ${hasBiz ? 'bg-green-400' : 'bg-gray-300'}`} /> BIZ（业务数据库）
        </div>
      </div>

      {isLoading && <div className="flex items-center justify-center py-32 text-gray-400"><Loader2 className="w-6 h-6 animate-spin mr-2" /><span className="text-sm">加载中...</span></div>}
      {(bizE && prdE) && <div className="flex flex-col items-center justify-center py-24 text-red-400"><AlertCircle className="w-8 h-8 mb-2" /><p className="text-sm font-medium">两个数据源均加载失败</p></div>}

      {!isLoading && !(bizE && prdE) && (
        <>
          <SectionCard title="数据源对比" extra="PRD vs BIZ" noPadding className="mb-6">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left py-3 px-5 text-xs font-medium text-gray-500">指标</th>
                  <th className="text-right py-3 px-5 text-xs font-medium text-gray-500">PRD 产研</th>
                  <th className="text-right py-3 px-5 text-xs font-medium text-gray-500">BIZ 业务</th>
                  <th className="text-center py-3 px-5 text-xs font-medium text-gray-500 w-16">差异</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(r => (
                  <tr key={r.metric} className="border-b border-gray-50 hover:bg-gray-50/50 transition-colors">
                    <td className="py-3 px-5 text-gray-700">{r.metric}</td>
                    <td className="py-3 px-5 text-right font-mono text-gray-600">{r.prd}</td>
                    <td className="py-3 px-5 text-right font-mono text-gray-600">{r.biz}</td>
                    <td className="py-3 px-5 text-center">
                      {r.hasDiff ? <span className="inline-block w-2 h-2 rounded-full bg-amber-400" />
                        : r.prd === '-' || r.biz === '-' ? <span className="text-gray-300 text-xs">—</span>
                        : <span className="inline-block w-2 h-2 rounded-full bg-green-400" />}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </SectionCard>

          <SectionCard title="差异分析" extra={
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-1 text-xs text-green-600"><CheckCircle className="w-3.5 h-3.5" /> {okCount} 项一致</div>
              {warnCount > 0 && <div className="flex items-center gap-1 text-xs text-amber-600"><AlertTriangle className="w-3.5 h-3.5" /> {warnCount} 项有差异</div>}
            </div>
          }>
            {diffRows.length === 0 ? (
              <p className="text-sm text-gray-400 text-center py-8">两个数据源暂无可对比指标</p>
            ) : (
              <div className="space-y-3">
                {diffRows.map(r => {
                  const diff = r.bizNum !== null && r.prdNum !== null ? r.bizNum - r.prdNum : 0
                  const pct = r.prdNum && r.prdNum !== 0 ? ((diff / Math.abs(r.prdNum)) * 100).toFixed(1) : '-'
                  return (
                    <div key={r.metric} className={`flex items-center justify-between p-3 rounded-lg ${r.hasDiff ? 'bg-amber-50/50 border border-amber-100' : 'bg-gray-50'}`}>
                      <span className="text-sm text-gray-700">{r.metric}</span>
                      <div className="flex items-center gap-6 text-xs">
                        <span className="text-gray-500">PRD: <span className="font-mono font-medium">{r.prd}</span></span>
                        <span className="text-gray-500">BIZ: <span className="font-mono font-medium">{r.biz}</span></span>
                        <span className={`font-mono font-medium ${r.hasDiff ? 'text-amber-600' : 'text-green-600'}`}>{r.hasDiff ? `偏差 ${pct}%` : '一致'}</span>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </SectionCard>
        </>
      )}
    </div>
  )
}
