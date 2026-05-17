import { useMemo, useState } from 'react'
import { ChevronUp, ChevronDown } from 'lucide-react'
import type { CountryRegisterRow } from './reshape'
import { cn } from '@/utils/cn'

interface Props {
  rows: CountryRegisterRow[]
  pageSize?: number
}

type SortKey = 'total' | 'organic' | 'tiktok' | 'meta' | 'other' | 'organicShare'
type SortDir = 'asc' | 'desc'

/**
 * 各国家注册情况表（分页 + 列排序 + 总计行）
 */
export function CountryRegisterTable({ rows, pageSize = 20 }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>('total')
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
    let total = 0, organic = 0, tiktok = 0, meta = 0, other = 0
    for (const r of rows) {
      total += r.total; organic += r.organic; tiktok += r.tiktok
      meta += r.meta; other += r.other
    }
    return { total, organic, tiktok, meta, other,
      organicShare: total > 0 ? (organic / total) * 100 : 0 }
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
              <Th label="国家" sticky />
              <ThSort label="总注册" k="total" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="自然量" k="organic" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="TikTok" k="tiktok" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="Meta" k="meta" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="其它" k="other" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
              <ThSort label="自然量占比" k="organicShare" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />
            </tr>
          </thead>
          <tbody className="divide-y divide-card-border">
            <tr className="bg-blue-50/40 font-medium text-gray-800">
              <td className="px-3 py-2 sticky left-0 bg-blue-50/40">合计</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.total.toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.organic.toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.tiktok.toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.meta.toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.other.toLocaleString()}</td>
              <td className="px-3 py-2 text-right tabular-nums">{totals.organicShare.toFixed(1)}%</td>
            </tr>
            {pageRows.map(r => (
              <tr key={r.region} className="hover:bg-muted/40">
                <td className="px-3 py-1.5 font-medium sticky left-0 bg-card">{r.region}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.total.toLocaleString()}</td>
                <td className="px-3 py-1.5 text-right tabular-nums text-emerald-600">{r.organic.toLocaleString()}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.tiktok.toLocaleString()}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.meta.toLocaleString()}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.other.toLocaleString()}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{r.organicShare.toFixed(1)}%</td>
              </tr>
            ))}
            {pageRows.length === 0 && (
              <tr>
                <td colSpan={7} className="px-3 py-8 text-center text-muted-foreground">无数据</td>
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


function Th({ label, sticky }: { label: string; sticky?: boolean }) {
  return (
    <th
      className={cn(
        'px-3 py-2 text-left font-medium',
        sticky && 'sticky left-0 bg-muted z-10',
      )}
    >
      {label}
    </th>
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
