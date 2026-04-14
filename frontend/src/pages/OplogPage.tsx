import { useState } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { DataTable, type Column } from '@/components/common/DataTable'
import { Loader2, AlertCircle, ChevronLeft, ChevronRight, LogIn, X, Eye } from 'lucide-react'
import { useOplog } from '@/hooks/use-oplog'
import { AuthError } from '@/services/api'
import type { OplogEntry } from '@/services/oplog'

const PAGE_SIZE = 20

const statusBadge = (s: string) => {
  const cls = s === 'success'
    ? 'bg-green-50 text-green-600'
    : s === 'fail' ? 'bg-red-50 text-red-500' : 'bg-gray-100 text-gray-500'
  return <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>{s === 'success' ? '成功' : s === 'fail' ? '失败' : s}</span>
}

function DetailModal({ entry, onClose }: { entry: OplogEntry; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[80vh] overflow-hidden" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h3 className="text-sm font-semibold text-gray-800">操作日志详情</h3>
          <button onClick={onClose} className="p-1 rounded-lg hover:bg-gray-100 transition text-gray-400 hover:text-gray-600">
            <X className="w-4 h-4" />
          </button>
        </div>
        <div className="px-6 py-4 overflow-y-auto max-h-[calc(80vh-60px)] space-y-4">
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div><span className="text-gray-400">时间</span><p className="text-gray-700 mt-0.5">{entry.time}</p></div>
            <div><span className="text-gray-400">操作人</span><p className="text-gray-700 mt-0.5">{entry.user || '-'}</p></div>
            <div><span className="text-gray-400">操作</span><p className="text-gray-700 mt-0.5">{entry.action}</p></div>
            <div><span className="text-gray-400">状态</span><div className="mt-0.5">{statusBadge(entry.status)}</div></div>
            <div className="col-span-2"><span className="text-gray-400">目标</span><p className="text-gray-700 mt-0.5">{entry.target || '-'}</p></div>
            <div className="col-span-2"><span className="text-gray-400">详情</span><p className="text-gray-700 mt-0.5 break-all">{entry.detail || '-'}</p></div>
          </div>

          {entry.before_data && (
            <div>
              <p className="text-xs font-medium text-gray-500 mb-1.5">操作前数据 (before)</p>
              <pre className="text-[11px] text-gray-600 bg-gray-50 rounded-xl p-3 overflow-x-auto max-h-48 whitespace-pre-wrap break-all">
                {JSON.stringify(entry.before_data, null, 2)}
              </pre>
            </div>
          )}

          {entry.after_data && (
            <div>
              <p className="text-xs font-medium text-gray-500 mb-1.5">操作后数据 (after)</p>
              <pre className="text-[11px] text-gray-600 bg-blue-50 rounded-xl p-3 overflow-x-auto max-h-48 whitespace-pre-wrap break-all">
                {JSON.stringify(entry.after_data, null, 2)}
              </pre>
            </div>
          )}

          {!entry.before_data && !entry.after_data && (
            <p className="text-xs text-gray-300 text-center py-4">暂无 before/after 数据</p>
          )}
        </div>
      </div>
    </div>
  )
}

export default function OplogPage() {
  const [page, setPage] = useState(1)
  const { data, isLoading, isError, error } = useOplog(page, PAGE_SIZE)
  const [selectedEntry, setSelectedEntry] = useState<OplogEntry | null>(null)

  const isAuthError = error instanceof AuthError
  const list = data?.list ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))

  const hasExtra = (r: OplogEntry) => !!(r.before_data || r.after_data)

  const columns: Column<OplogEntry>[] = [
    { key: 'time', title: '时间', render: (r) => <span className="text-xs text-gray-500 whitespace-nowrap">{r.time}</span> },
    { key: 'user', title: '操作人', render: (r) => <span className="font-medium text-gray-800">{r.user || '-'}</span> },
    { key: 'action', title: '操作', render: (r) => <span className="text-gray-700">{r.action}</span> },
    { key: 'target', title: '目标', render: (r) => <span className="text-xs text-gray-500 truncate max-w-[200px] block">{r.target || '-'}</span> },
    { key: 'platform', title: '平台', render: (r) => {
      if (!r.platform) return <span className="text-xs text-gray-300">-</span>
      const cls = r.platform.toLowerCase() === 'tiktok' ? 'bg-sky-50 text-sky-600' : 'bg-indigo-50 text-indigo-600'
      return <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>{r.platform}</span>
    }},
    { key: 'status', title: '状态', render: (r) => statusBadge(r.status) },
    { key: 'detail', title: '详情', render: (r) => (
      <div className="flex items-center gap-1.5 max-w-[220px]">
        <span className="text-xs text-gray-400 truncate flex-1" title={r.detail}>{r.detail || '-'}</span>
        {hasExtra(r) && (
          <button
            onClick={(e) => { e.stopPropagation(); setSelectedEntry(r) }}
            className="shrink-0 flex items-center gap-0.5 px-1.5 py-0.5 rounded-md text-[10px] font-medium text-blue-500 bg-blue-50 hover:bg-blue-100 transition"
          >
            <Eye className="w-3 h-3" />
            详情
          </button>
        )}
      </div>
    )},
  ]

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="操作日志" description="查看系统操作记录" />

      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中…</span>
        </div>
      )}

      {isError && isAuthError && (
        <div className="flex flex-col items-center justify-center py-24 text-gray-400">
          <LogIn className="w-10 h-10 mb-3 text-gray-300" />
          <p className="text-sm font-medium text-gray-600">需要登录后查看</p>
          <p className="text-xs mt-1 text-gray-400">操作日志需要登录权限</p>
        </div>
      )}

      {isError && !isAuthError && (
        <div className="flex flex-col items-center justify-center py-24 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" />
          <p className="text-sm font-medium">数据加载失败</p>
        </div>
      )}

      {!isLoading && !isError && (
        <>
          <SectionCard title={`操作日志（共 ${total} 条）`} noPadding>
            <DataTable
              columns={columns}
              data={list}
              rowKey={(r) => String(r.id)}
            />
          </SectionCard>

          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-4 px-1">
              <span className="text-xs text-gray-400">
                第 {page}/{totalPages} 页，共 {total} 条
              </span>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page <= 1}
                  className="p-1.5 rounded-lg border border-gray-200 text-gray-500 hover:bg-gray-50 disabled:opacity-30 transition"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>
                {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                  const start = Math.max(1, Math.min(page - 2, totalPages - 4))
                  const p = start + i
                  if (p > totalPages) return null
                  return (
                    <button
                      key={p}
                      onClick={() => setPage(p)}
                      className={`w-8 h-8 rounded-lg text-xs transition ${
                        p === page ? 'bg-blue-500 text-white font-medium' : 'border border-gray-200 text-gray-500 hover:bg-gray-50'
                      }`}
                    >
                      {p}
                    </button>
                  )
                })}
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages}
                  className="p-1.5 rounded-lg border border-gray-200 text-gray-500 hover:bg-gray-50 disabled:opacity-30 transition"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            </div>
          )}
        </>
      )}

      {selectedEntry && <DetailModal entry={selectedEntry} onClose={() => setSelectedEntry(null)} />}
    </div>
  )
}
