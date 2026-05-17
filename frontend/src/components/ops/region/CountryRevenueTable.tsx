import { useMemo, useState } from 'react'
import { ChevronUp, ChevronDown } from 'lucide-react'
import type { CountryRevenueRow } from './reshape'
import { fmtUsd } from '../formatters'
import { cn } from '@/utils/cn'

interface Props {
  rows: CountryRevenueRow[]
  pageSize?: number
}

type SortKey = 'totalUsd' | 'organicUsd' | 'tiktokUsd' | 'metaUsd' | 'otherUsd'
              | 'payerUv' | 'orderCnt' | 'arpu' | 'organicShare'
type SortDir = 'asc' | 'desc'

/**
 * 各国家充值情况表（分页 + 列排序 + 总计行）
 *
 * 由父组件控制 OS Tab（all / iOS / Android），传入对应过滤后的 rows
 */
export function CountryRevenueTable({ rows, pageSize = 20 }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>('totalUsd')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [page, setPage] = useState(1)

  const sorted = useMemo(() => {
    const out = [...rows]
    out.sort((a, b) => {
      const av = a[sortKey] || 0
      const bv = b[sortKey] || 0
      return sortDir === 'desc' ? bv - av : av - bv
    })
    return out
  }, [rows, sortKey, sortDir])

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const safePage = Math.min(page, totalPages)
  const pageRows = sorted.slice((safePage - 1) * pageSize, safePage * pageSize)

  const totals = useMemo(() => {
    let totalUsd = 0, organicUsd = 0, tiktokUsd = 0, metaUsd = 0, otherUsd = 0
    let payerUv = 0, orderCnt = 0
    for (const r of rows) {
      totalUsd += r.totalUsd; organicUsd += r.organicUsd
      tiktokUsd += r.tiktokUsd; metaUsd += r.metaUsd; otherUsd += r.otherUsd
      payerUv += r.payerUv; orderCnt += r.orderCnt
    }
    return {
      totalUsd, organicUsd, tiktokUsd, metaUsd, otherUsd, payerUv, orderCnt,
      arpu: payerUv > 0 ? totalUsd / payerUv : 0,
      organicShare: totalUsd > 0 ? (organicUsd / totalUsd) * 100 : 0,
    }
  }, [rows])

  const onSort = (k: SortKey) => {
    if (k === sortKey) setSortDir(sortDir === 'desc' ? 'asc' : 'desc')
    else { setSortKey(k); setSortDir('desc') }
    setPage(1)
  }

  return (
    <div className="overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-muted text-muted-foreground">
            <tr>
              <th className="px-3 py-2 text-left font-medium sticky left-0 bg-muted z-10">国家</th>
              <ThSort label="总充值" k="totalUsd" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="自然量" k="organicUsd" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="TikTok" k="tiktokUsd" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="Meta" k="metaUsd" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="其它" k="otherUsd" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="付费UV" k="payerUv" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="订单数" k="orderCnt" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="ARPU" k="arpu" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="自然占比" k="organicShare" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
            </tr>
          </thead>
          <tbody className="divide-y divide-card-border">
            <tr className="bg-blue-50/40 font-medium text-gray-800">
              <td className="px-3 py-2 sticky left-0 bg-blue-50/40">合计</td>
              <td className="px-3 py-2 text-right tabular-nums">{fmtUsd(totals.totalUsd)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{fmtUsd(totals.organicUsd)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{fmtUsd(totals.tiktokUsd)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{fmtUsd(totals.metaUsd)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{fmtUsd(totals.otherUsd)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.payerUv.toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.orderCnt.toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums">{fmtUsd(totals.arpu)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.organicShare.toFixed(1)}%</td>
            </tr>
            {pageRows.map(r => (
              <tr key={r.region} className="hover:bg-muted/40">
                <td className="px-3 py-1.5 font-medium sticky left-0 bg-card">{r.region}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{fmtUsd(r.totalUsd)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums text-emerald-600">{fmtUsd(r.organicUsd)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{fmtUsd(r.tiktokUsd)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{fmtUsd(r.metaUsd)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{fmtUsd(r.otherUsd)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.payerUv.toLocaleString()}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.orderCnt.toLocaleString()}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{fmtUsd(r.arpu)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.organicShare.toFixed(1)}%</td>
              </tr>
            ))}
            {pageRows.length === 0 && (
              <tr>
                <td colSpan={10} className="px-3 py-8 text-center text-muted-foreground">无数据</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <Pagination page={safePage} totalPages={totalPages} onChange={setPage} />
      )}
    </div>
  )
}


interface ThSortProps {
  label: string
  k: SortKey
  sortKey: SortKey
  sortDir: SortDir
  onSort: (k: SortKey) => void
}

function ThSort({ label, k, sortKey, sortDir, onSort }: ThSortProps) {
  const active = k === sortKey
  return (
    <th className="px-3 py-2 text-right font-medium select-none">
      <button
        onClick={() => onSort(k)}
        className={cn(
          'inline-flex items-center gap-0.5 hover:text-gray-900',
          active && 'text-gray-900',
        )}
      >
        {label}
        {active && (sortDir === 'desc'
          ? <ChevronDown className="w-3 h-3" />
          : <ChevronUp className="w-3 h-3" />)}
      </button>
    </th>
  )
}

interface PaginationProps {
  page: number
  totalPages: number
  onChange: (p: number) => void
}

function Pagination({ page, totalPages, onChange }: PaginationProps) {
  return (
    <div className="flex items-center justify-end gap-2 px-3 py-2 text-xs text-muted-foreground border-t border-card-border">
      <button
        disabled={page <= 1}
        onClick={() => onChange(page - 1)}
        className="px-2 py-1 rounded hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
      >
        上一页
      </button>
      <span className="tabular-nums">{page} / {totalPages}</span>
      <button
        disabled={page >= totalPages}
        onClick={() => onChange(page + 1)}
        className="px-2 py-1 rounded hover:bg-muted disabled:opacity-30 disabled:cursor-not-allowed"
      >
        下一页
      </button>
    </div>
  )
}
