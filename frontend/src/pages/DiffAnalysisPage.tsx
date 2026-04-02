import { useState, useMemo } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import { Loader2, AlertTriangle, CheckCircle } from 'lucide-react'
import { useBizOverview } from '@/hooks/use-biz'
import { usePrdSummary } from '@/hooks/use-bizdata'

const fmtUsd = (n: number) => `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`

interface DiffRow {
  metric: string
  prd: number | null
  biz: number | null
  diff: number | null
  diffPct: string
  status: 'ok' | 'warn' | 'na'
}

export default function DiffAnalysisPage() {
  const [dateRange, setDateRange] = useState<DateRange>(() => getDefaultDateRange('all'))

  const { data: bizOv, isLoading: bizL } = useBizOverview(dateRange)
  const { data: prdOv, isLoading: prdL } = usePrdSummary(dateRange.startDate, dateRange.endDate)

  const isLoading = bizL || prdL

  const rows = useMemo<DiffRow[]>(() => {
    const result: DiffRow[] = []
    const prdSpend = prdOv?.ad_cost_amount ?? null
    const bizSpend = bizOv?.total_spend ?? null

    function addRow(metric: string, p: number | null, b: number | null) {
      if (p === null && b === null) return
      const diff = (p !== null && b !== null) ? b - p : null
      const diffPct = (p !== null && b !== null && p !== 0) ? `${((diff! / p) * 100).toFixed(1)}%` : '-'
      const status: DiffRow['status'] = (diff === null) ? 'na' : (Math.abs(diff) / Math.max(Math.abs(p ?? 1), 1) > 0.1) ? 'warn' : 'ok'
      result.push({ metric, prd: p, biz: b, diff, diffPct, status })
    }

    addRow('广告消耗', prdSpend, bizSpend)
    addRow('充值/收入', prdOv?.recharge_total_amount ?? null, bizOv?.total_revenue ?? null)
    addRow('注册/转化', prdOv?.register_count ?? null, bizOv?.total_conversions ?? null)

    return result
  }, [prdOv, bizOv])

  const warnCount = rows.filter(r => r.status === 'warn').length
  const okCount = rows.filter(r => r.status === 'ok').length

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="差异分析" description="分析 PRD 与 BIZ 数据之间的差异与偏差原因" />

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
          <div className="flex items-center gap-4 mb-6">
            <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs bg-green-50 text-green-600">
              <CheckCircle className="w-3.5 h-3.5" /> {okCount} 项一致
            </div>
            {warnCount > 0 && (
              <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs bg-amber-50 text-amber-600">
                <AlertTriangle className="w-3.5 h-3.5" /> {warnCount} 项偏差 &gt;10%
              </div>
            )}
          </div>

          <SectionCard title="差异明细" noPadding>
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50/80 border-b border-gray-100">
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500">指标</th>
                  <th className="text-right px-5 py-3 text-xs font-medium text-gray-500">PRD</th>
                  <th className="text-right px-5 py-3 text-xs font-medium text-gray-500">BIZ</th>
                  <th className="text-right px-5 py-3 text-xs font-medium text-gray-500">差值</th>
                  <th className="text-right px-5 py-3 text-xs font-medium text-gray-500">偏差</th>
                  <th className="text-center px-5 py-3 text-xs font-medium text-gray-500 w-16">状态</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(r => (
                  <tr key={r.metric} className="border-b border-gray-50 hover:bg-gray-50/50 transition-colors">
                    <td className="px-5 py-3 text-gray-700">{r.metric}</td>
                    <td className="px-5 py-3 text-right font-mono text-gray-600">{r.prd !== null ? fmtUsd(r.prd) : '-'}</td>
                    <td className="px-5 py-3 text-right font-mono text-gray-600">{r.biz !== null ? fmtUsd(r.biz) : '-'}</td>
                    <td className="px-5 py-3 text-right font-mono text-gray-600">{r.diff !== null ? fmtUsd(r.diff) : '-'}</td>
                    <td className={`px-5 py-3 text-right font-mono ${r.status === 'warn' ? 'text-amber-600 font-medium' : 'text-gray-500'}`}>{r.diffPct}</td>
                    <td className="px-5 py-3 text-center">
                      {r.status === 'ok' && <span className="inline-block w-2 h-2 rounded-full bg-green-400" />}
                      {r.status === 'warn' && <span className="inline-block w-2 h-2 rounded-full bg-amber-400" />}
                      {r.status === 'na' && <span className="text-gray-300 text-xs">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </SectionCard>
        </>
      )}
    </div>
  )
}
