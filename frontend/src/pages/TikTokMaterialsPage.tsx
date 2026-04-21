import { useState, useRef, useCallback, useEffect, useMemo, useSyncExternalStore } from 'react'
import { useQueryClient, useQuery } from '@tanstack/react-query'
import {
  Upload, Loader2, AlertCircle, CheckCircle2, XCircle, Clock,
  Film, Search, Copy, AlertTriangle, Info, FileVideo, RotateCcw, Trash2, X,
} from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { useTikTokMaterials } from '@/hooks/use-tiktok-materials'
import {
  validateVideoFile, getVideoDuration, isDurationOverLimit, DURATION_THRESHOLD,
  deleteMaterial, type TikTokMaterialRecord,
} from '@/services/tiktok-materials'
import { UploadQueue, type UploadTask } from '@/services/tiktok-upload-queue'
import { fetchTikTokAdvertisers, type Advertiser } from '@/services/advertisers'

const STATUS_MAP: Record<string, { label: string; color: string; Icon: typeof CheckCircle2 }> = {
  pending:   { label: '待上传', color: 'text-gray-500 bg-gray-50',   Icon: Clock },
  uploading: { label: '上传中', color: 'text-blue-600 bg-blue-50',   Icon: Loader2 },
  success:   { label: '成功',   color: 'text-green-600 bg-green-50', Icon: CheckCircle2 },
  failed:    { label: '失败',   color: 'text-red-600 bg-red-50',     Icon: XCircle },
}

function StatusBadge({ status }: { status: string }) {
  const cfg = STATUS_MAP[status] ?? STATUS_MAP.pending
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cfg.color}`}>
      <cfg.Icon className={`w-3 h-3 ${status === 'uploading' ? 'animate-spin' : ''}`} />
      {cfg.label}
    </span>
  )
}

function fmtSize(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)}MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)}GB`
}

function fmtDuration(sec: number | null): string {
  if (sec == null) return '--'
  const m = Math.floor(sec / 60)
  const s = Math.floor(sec % 60)
  return `${m}:${s.toString().padStart(2, '0')}`
}

function copyText(text: string) {
  navigator.clipboard.writeText(text).catch(() => {})
}

const CONCURRENCY_KEY = 'tiktok_upload_concurrency'
function loadConcurrency(): number {
  const v = parseInt(localStorage.getItem(CONCURRENCY_KEY) ?? '1', 10)
  if (!Number.isFinite(v) || v < 1 || v > 3) return 1
  return v
}

interface PendingTask {
  id: string
  file: File
  durationSec: number | null
  validationError?: string
}

let _pendingSeq = 0
function pendingId() { _pendingSeq += 1; return `p-${Date.now().toString(36)}-${_pendingSeq.toString(36)}` }

export default function TikTokMaterialsPage() {
  const qc = useQueryClient()
  const fileRef = useRef<HTMLInputElement>(null)

  const [advertiserId, setAdvertiserId] = useState('')
  const [keyword, setKeyword] = useState('')
  const [page, setPage] = useState(1)
  const pageSize = 20

  // ── upload queue (单实例，绑定到组件生命周期) ──
  const queueRef = useRef<UploadQueue | null>(null)
  if (queueRef.current === null) {
    queueRef.current = new UploadQueue(loadConcurrency())
  }
  const queue = queueRef.current

  // 订阅队列快照，自动 re-render
  const snapshot = useSyncExternalStore(
    useCallback((cb) => queue.subscribe(cb), [queue]),
    useCallback(() => queue.snapshot(), [queue]),
  )

  const [concurrency, setConcurrencyState] = useState<number>(loadConcurrency())
  const onConcurrencyChange = useCallback((n: number) => {
    setConcurrencyState(n)
    queue.setConcurrency(n)
    localStorage.setItem(CONCURRENCY_KEY, String(n))
  }, [queue])

  // ── 待确认任务（已选好文件，尚未点「全部上传」） ──
  const [pending, setPending] = useState<PendingTask[]>([])
  const [pageError, setPageError] = useState('')
  const [isDragging, setIsDragging] = useState(false)

  const { data: advData } = useQuery({
    queryKey: ['tiktok-advertisers'],
    queryFn: fetchTikTokAdvertisers,
  })
  const advertisers: Advertiser[] = advData?.data ?? []

  const { data: listData, isLoading } = useTikTokMaterials({
    advertiser_id: advertiserId || undefined,
    keyword: keyword || undefined,
    page,
    page_size: pageSize,
  }, snapshot.active)
  const items: TikTokMaterialRecord[] = listData?.data?.items ?? []
  const total = listData?.data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / pageSize))

  // 批量上传任意一个完成时，刷一下列表
  const lastFinishedRef = useRef(0)
  useEffect(() => {
    const finished = snapshot.succeeded + snapshot.failed + snapshot.canceled
    if (finished !== lastFinishedRef.current) {
      lastFinishedRef.current = finished
      qc.invalidateQueries({ queryKey: ['tiktok-materials'] })
    }
  }, [snapshot.succeeded, snapshot.failed, snapshot.canceled, qc])

  // beforeunload：批量进行中时阻止关闭
  useEffect(() => {
    if (!snapshot.active) return
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault()
      e.returnValue = '上传任务正在进行中，离开将中断上传'
      return e.returnValue
    }
    window.addEventListener('beforeunload', handler)
    return () => window.removeEventListener('beforeunload', handler)
  }, [snapshot.active])

  // ── 文件选择/拖拽：批量入预览队列 ──
  const ingestFiles = useCallback(async (files: File[]) => {
    if (!files.length) return
    if (!advertiserId) {
      setPageError('请先选择广告主再上传')
      return
    }
    setPageError('')

    const drafts: PendingTask[] = files.map(f => ({
      id: pendingId(), file: f, durationSec: null,
      validationError: validateVideoFile(f) ?? undefined,
    }))
    // 立即插入，先展示文件名/大小
    setPending(prev => [...prev, ...drafts])

    // 并行读时长（验证失败的不读）
    await Promise.all(drafts.map(async d => {
      if (d.validationError) return
      try {
        const dur = await getVideoDuration(d.file)
        setPending(prev => prev.map(p => p.id === d.id ? { ...p, durationSec: dur } : p))
      } catch {
        // 读不到时长（编解码不兼容），视为合法，duration 留空，由后端兜底
        setPending(prev => prev.map(p => p.id === d.id ? { ...p, durationSec: null } : p))
      }
    }))
  }, [advertiserId])

  const handleFileInput = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files ?? [])
    e.target.value = ''
    void ingestFiles(files)
  }, [ingestFiles])

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)
    const files = Array.from(e.dataTransfer?.files ?? []).filter(f => f.type.startsWith('video/') || /\.(mp4|mov|avi|mkv|webm|m4v)$/i.test(f.name))
    void ingestFiles(files)
  }, [ingestFiles])

  const removePending = useCallback((id: string) => {
    setPending(prev => prev.filter(p => p.id !== id))
  }, [])
  const clearPending = useCallback(() => setPending([]), [])

  const startAll = useCallback(() => {
    if (!advertiserId || pending.length === 0) return
    queue.enqueue(pending.map(p => ({
      file: p.file,
      advertiserId,
      durationSec: p.durationSec,
      validationError: p.validationError,
    })))
    setPending([])
  }, [advertiserId, pending, queue])

  // ── 失败重试：弹文件选择器拿新文件，删旧记录后入队 ──
  const retryRef = useRef<HTMLInputElement>(null)
  const retryRecordRef = useRef<TikTokMaterialRecord | null>(null)

  const handleRetryRecord = useCallback((record: TikTokMaterialRecord) => {
    retryRecordRef.current = record
    if (record.advertiser_id) setAdvertiserId(record.advertiser_id)
    retryRef.current?.click()
  }, [])

  const handleRetryFileChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    const record = retryRecordRef.current
    retryRecordRef.current = null
    if (!record) return
    const err = validateVideoFile(file)
    if (err) { setPageError(err); return }
    try { await deleteMaterial(record.id) } catch { /* ignore */ }
    let dur: number | null = record.duration_sec
    try { dur = await getVideoDuration(file) } catch { /* keep old */ }
    queue.enqueue([{ file, advertiserId: record.advertiser_id, durationSec: dur }])
    qc.invalidateQueries({ queryKey: ['tiktok-materials'] })
  }, [queue, qc])

  // ── 队列内任务：失败重试（重新入队 / 已是 queued/uploading 时不响应） ──
  const handleQueueRetry = useCallback((taskId: string) => {
    queue.retry(taskId)
  }, [queue])
  const handleQueueCancel = useCallback((taskId: string) => {
    queue.cancelTask(taskId)
  }, [queue])

  // ── 删除已落库的失败记录 ──
  const deletingRef = useRef<Set<number>>(new Set())
  const handleDelete = useCallback(async (id: number) => {
    if (deletingRef.current.has(id)) return
    deletingRef.current.add(id)
    try {
      await deleteMaterial(id)
      qc.invalidateQueries({ queryKey: ['tiktok-materials'] })
    } catch {
      setPageError('删除失败')
    } finally {
      deletingRef.current.delete(id)
    }
  }, [qc])

  // 结合：把后端列表行的 tiktok_progress 映射给队列任务（按 record_id）
  const tiktokProgressByRecord = useMemo(() => {
    const m = new Map<number, { sent: number; total: number; pct: number; phase: string }>()
    for (const r of items) {
      if (r.tiktok_progress) m.set(r.id, r.tiktok_progress)
    }
    return m
  }, [items])

  const pendingValid = pending.filter(p => !p.validationError).length
  const pendingInvalid = pending.length - pendingValid

  return (
    <div className="space-y-4">
      <input ref={retryRef} type="file" accept="video/*" className="hidden" onChange={handleRetryFileChange} />
      <input ref={fileRef} type="file" accept="video/*" multiple className="hidden" onChange={handleFileInput} />

      <PageHeader
        title="TikTok 素材上传"
        description="支持批量上传视频到 TikTok Asset Library，长视频自动走 API 入库"
      />

      {/* ── toolbar ── */}
      <div className="flex items-center gap-3 flex-wrap">
        <select
          className="h-9 px-3 rounded-lg border border-gray-200 text-sm bg-white"
          value={advertiserId}
          onChange={e => { setAdvertiserId(e.target.value); setPage(1) }}
        >
          <option value="">全部广告主</option>
          {advertisers.map(a => (
            <option key={a.advertiser_id} value={a.advertiser_id}>
              {a.advertiser_name || a.advertiser_id}
            </option>
          ))}
        </select>

        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            className="h-9 pl-8 pr-3 rounded-lg border border-gray-200 text-sm w-56"
            placeholder="搜索文件名 / video_id"
            value={keyword}
            onChange={e => { setKeyword(e.target.value); setPage(1) }}
          />
        </div>

        <div className="flex-1" />

        <button
          onClick={() => fileRef.current?.click()}
          disabled={!advertiserId}
          className="inline-flex items-center gap-2 h-9 px-4 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition"
          title={advertiserId ? '可一次选择多个文件' : '请先选择广告主'}
        >
          <Upload className="w-4 h-4" />
          选择视频（支持多选）
        </button>
      </div>

      {/* ── 拖拽区（仅在没待确认/无进行中时显示，避免遮挡列表） ── */}
      {pending.length === 0 && !snapshot.active && advertiserId && (
        <div
          onDragOver={e => { e.preventDefault(); setIsDragging(true) }}
          onDragLeave={() => setIsDragging(false)}
          onDrop={handleDrop}
          className={`border-2 border-dashed rounded-xl py-8 text-center text-sm transition ${
            isDragging
              ? 'border-blue-400 bg-blue-50/60 text-blue-700'
              : 'border-gray-200 bg-gray-50/40 text-gray-500 hover:border-blue-300 hover:bg-blue-50/30'
          }`}
        >
          <FileVideo className="w-8 h-8 mx-auto mb-2 text-gray-400" />
          将视频文件拖到此处，或点击上方「选择视频」按钮
          <div className="mt-1 text-xs text-gray-400">
            支持 mp4/mov/avi/mkv/webm/m4v，单文件不超过 1GB
          </div>
        </div>
      )}

      {/* ── pending 任务确认面板 ── */}
      {pending.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm space-y-3">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="font-medium text-gray-800">
              准备上传 {pending.length} 个文件
              {pendingInvalid > 0 && (
                <span className="ml-2 text-xs text-red-500">（其中 {pendingInvalid} 个不合法）</span>
              )}
            </div>
            <div className="flex-1" />
            <label className="flex items-center gap-2 text-xs text-gray-500">
              并发数
              <select
                value={concurrency}
                onChange={e => onConcurrencyChange(parseInt(e.target.value, 10))}
                className="h-7 px-2 rounded border border-gray-200 text-xs bg-white"
              >
                <option value={1}>1（推荐 / 大文件）</option>
                <option value={2}>2</option>
                <option value={3}>3（最快）</option>
              </select>
            </label>
            <button
              onClick={clearPending}
              className="px-3 h-8 text-xs rounded border border-gray-200 hover:bg-gray-50"
            >
              全部移除
            </button>
            <button
              onClick={startAll}
              disabled={pendingValid === 0}
              className="inline-flex items-center gap-1 px-4 h-8 text-xs rounded bg-blue-600 text-white font-medium hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <Upload className="w-3 h-3" />
              开始上传 {pendingValid > 0 ? `（${pendingValid}）` : ''}
            </button>
          </div>

          {concurrency > 1 && (
            <div className="text-[11px] text-gray-400">
              并发越大越占用服务器内存盘，大文件建议保持 1。
            </div>
          )}

          <div className="divide-y divide-gray-50 border-t border-gray-100 pt-2 max-h-72 overflow-auto">
            {pending.map(p => {
              const overLimit = isDurationOverLimit(p.durationSec)
              return (
                <div key={p.id} className="flex items-center gap-3 py-2">
                  <FileVideo className={`w-5 h-5 flex-shrink-0 ${p.validationError ? 'text-red-400' : 'text-blue-500'}`} />
                  <div className="flex-1 min-w-0">
                    <div className="text-sm text-gray-800 truncate" title={p.file.name}>{p.file.name}</div>
                    <div className="text-xs text-gray-500 mt-0.5 flex items-center gap-3">
                      <span>{fmtSize(p.file.size)}</span>
                      <span>{p.durationSec != null ? fmtDuration(p.durationSec) : '时长读取中...'}</span>
                      {overLimit && (
                        <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded bg-amber-50 text-amber-700 text-[10px]">
                          <AlertTriangle className="w-3 h-3" />长视频 API 入库
                        </span>
                      )}
                      {p.validationError && (
                        <span className="text-red-500 text-[11px]">{p.validationError}</span>
                      )}
                    </div>
                  </div>
                  <button
                    onClick={() => removePending(p.id)}
                    className="p-1 text-gray-400 hover:text-red-500 transition"
                    title="移除"
                  >
                    <X className="w-4 h-4" />
                  </button>
                </div>
              )
            })}
          </div>

          {pending.some(p => isDurationOverLimit(p.durationSec)) && (
            <div className="flex items-start gap-2 p-2.5 rounded-lg bg-amber-50 border border-amber-100 text-xs text-amber-800">
              <Info className="w-4 h-4 flex-shrink-0 mt-0.5" />
              含超过 {DURATION_THRESHOLD / 60} 分钟的视频，将通过 API 入库（Non-Spark Ads 投放上限为 10 分钟，是否可直接投放以 TikTok 账户能力为准）。
            </div>
          )}
        </div>
      )}

      {/* ── 批量上传进度面板 ── */}
      {snapshot.tasks.length > 0 && (
        <BatchProgressPanel
          tasks={snapshot.tasks}
          summary={snapshot}
          tiktokProgressByRecord={tiktokProgressByRecord}
          onCancel={handleQueueCancel}
          onRetry={handleQueueRetry}
          onClearFinished={() => queue.clearFinished()}
        />
      )}

      {/* ── error ── */}
      {pageError && (
        <div className="flex items-center gap-2 p-3 rounded-lg bg-red-50 border border-red-200 text-sm text-red-700">
          <AlertCircle className="w-4 h-4 flex-shrink-0" />
          {pageError}
          <button onClick={() => setPageError('')} className="ml-auto text-red-400 hover:text-red-600">✕</button>
        </div>
      )}

      {/* ── list ── */}
      <div className="bg-white rounded-xl border border-gray-100 overflow-hidden">
        <table className="w-full text-left">
          <thead>
            <tr className="border-b border-gray-100 bg-gray-50/60">
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase">文件名</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase w-20">时长</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase w-20">大小</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase w-20">上传方式</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase w-40">TikTok Video ID</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase w-20">状态</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase w-20">可投放</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase">信息</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase w-24">操作</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <tr><td colSpan={9} className="py-16 text-center text-gray-400">
                <Loader2 className="w-6 h-6 animate-spin mx-auto mb-2" />加载中...
              </td></tr>
            ) : items.length === 0 ? (
              <tr><td colSpan={9} className="py-16 text-center text-gray-400">
                <Film className="w-8 h-8 mx-auto mb-2 text-gray-300" />暂无上传记录
              </td></tr>
            ) : items.map(r => (
              <tr key={r.id} className="border-b border-gray-50 hover:bg-blue-50/30 transition-colors">
                <td className="px-4 py-3">
                  <div className="text-sm font-medium text-gray-800 max-w-xs break-all line-clamp-2" title={r.local_file_name}>
                    {r.local_file_name}
                  </div>
                  <div className="text-[11px] text-gray-400 mt-0.5">{r.created_at?.slice(0, 16)}</div>
                </td>
                <td className="px-4 py-3 text-xs text-gray-600">{fmtDuration(r.duration_sec)}</td>
                <td className="px-4 py-3 text-xs text-gray-600">{fmtSize(r.file_size_bytes)}</td>
                <td className="px-4 py-3">
                  <span className={`text-xs px-1.5 py-0.5 rounded ${
                    r.upload_channel === 'api' ? 'bg-violet-50 text-violet-700' : 'bg-blue-50 text-blue-600'
                  }`}>
                    {r.upload_channel === 'api' ? 'API 长视频' : '标准上传'}
                  </span>
                </td>
                <td className="px-4 py-3">
                  {r.tiktok_video_id ? (
                    <button onClick={() => copyText(r.tiktok_video_id!)}
                      className="inline-flex items-center gap-1 text-xs font-mono text-gray-600 hover:text-blue-600 transition"
                      title="点击复制">
                      <Copy className="w-3 h-3" />
                      {r.tiktok_video_id.length > 16
                        ? r.tiktok_video_id.slice(0, 8) + '...' + r.tiktok_video_id.slice(-6)
                        : r.tiktok_video_id}
                    </button>
                  ) : <span className="text-xs text-gray-300">--</span>}
                </td>
                <td className="px-4 py-3">
                  <StatusBadge status={r.upload_status} />
                  {r.upload_status === 'uploading' && r.tiktok_progress && (
                    <div className="mt-1.5 space-y-0.5">
                      <div className="w-20 bg-gray-100 rounded-full h-1.5 overflow-hidden">
                        <div className="bg-blue-500 h-full rounded-full transition-all duration-500"
                          style={{ width: `${r.tiktok_progress.pct}%` }} />
                      </div>
                      <div className="text-[10px] text-gray-400">
                        TikTok {r.tiktok_progress.pct}%
                      </div>
                    </div>
                  )}
                </td>
                <td className="px-4 py-3">
                  {r.can_use_for_ad ? (
                    <span className="text-xs text-green-600">可投放</span>
                  ) : (
                    <span className="text-xs text-amber-600" title={r.ad_usage_note || '不可直接投放'}>受限</span>
                  )}
                </td>
                <td className="px-4 py-3">
                  {r.upload_status === 'failed' && r.error_message ? (
                    <span className="text-xs text-red-500 truncate block max-w-xs" title={r.error_message}>
                      {r.error_message.slice(0, 60)}{r.error_message.length > 60 ? '...' : ''}
                    </span>
                  ) : r.ad_usage_note ? (
                    <span className="text-xs text-gray-500 truncate block max-w-xs" title={r.ad_usage_note}>
                      {r.ad_usage_note.slice(0, 40)}
                    </span>
                  ) : <span className="text-xs text-gray-300">--</span>}
                </td>
                <td className="px-4 py-3">
                  {r.upload_status === 'failed' && (
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => handleRetryRecord(r)}
                        className="inline-flex items-center gap-1 px-2 py-1 text-xs text-blue-600 bg-blue-50 rounded hover:bg-blue-100 transition"
                        title="重新选择文件上传"
                      >
                        <RotateCcw className="w-3 h-3" />重试
                      </button>
                      <button
                        onClick={() => handleDelete(r.id)}
                        className="inline-flex items-center gap-1 px-2 py-1 text-xs text-red-500 bg-red-50 rounded hover:bg-red-100 transition"
                        title="删除该记录"
                      >
                        <Trash2 className="w-3 h-3" />
                      </button>
                    </div>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {/* pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
            <span>共 {total} 条</span>
            <div className="flex items-center gap-1">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page <= 1}
                className="px-2 py-1 rounded hover:bg-gray-100 disabled:opacity-30">上一页</button>
              <span className="px-2">{page} / {totalPages}</span>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}
                className="px-2 py-1 rounded hover:bg-gray-100 disabled:opacity-30">下一页</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────
//  批量进度面板（独立组件，避免主组件 props 列表过长）
// ─────────────────────────────────────────────────────────────────────

interface BatchProgressPanelProps {
  tasks: UploadTask[]
  summary: { running: number; queued: number; succeeded: number; failed: number; canceled: number; total: number; active: boolean }
  tiktokProgressByRecord: Map<number, { pct: number }>
  onCancel: (taskId: string) => void
  onRetry: (taskId: string) => void
  onClearFinished: () => void
}

function BatchProgressPanel({
  tasks, summary, tiktokProgressByRecord, onCancel, onRetry, onClearFinished,
}: BatchProgressPanelProps) {
  // 总体进度：所有任务 browserPct 平均（success 视为 100）
  const overall = tasks.length === 0 ? 0 :
    Math.round(tasks.reduce((acc, t) => {
      if (t.status === 'success') return acc + 100
      if (t.status === 'failed' || t.status === 'canceled') return acc + 100
      return acc + t.browserPct
    }, 0) / tasks.length)

  const finishedCount = summary.succeeded + summary.failed + summary.canceled

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm space-y-3">
      <div className="flex items-center gap-3 text-sm">
        {summary.active ? (
          <Loader2 className="w-4 h-4 animate-spin text-blue-500" />
        ) : (
          <CheckCircle2 className="w-4 h-4 text-green-500" />
        )}
        <div className="font-medium text-gray-800">
          批量上传：完成 {finishedCount} / {summary.total}
          {summary.failed > 0 && <span className="ml-2 text-red-500">失败 {summary.failed}</span>}
          {summary.canceled > 0 && <span className="ml-2 text-gray-400">取消 {summary.canceled}</span>}
          {summary.running > 0 && <span className="ml-2 text-blue-500">进行中 {summary.running}</span>}
          {summary.queued > 0 && <span className="ml-2 text-gray-500">排队 {summary.queued}</span>}
        </div>
        <div className="flex-1" />
        {!summary.active && (
          <button
            onClick={onClearFinished}
            className="px-2 py-1 text-xs text-gray-500 hover:text-gray-800 hover:bg-gray-100 rounded transition"
          >
            清除完成
          </button>
        )}
      </div>

      <div className="w-full bg-gray-100 rounded-full h-1.5 overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-300 ${
            summary.failed > 0 && !summary.active ? 'bg-amber-500' : 'bg-blue-500'
          }`}
          style={{ width: `${overall}%` }}
        />
      </div>

      <div className="divide-y divide-gray-50 max-h-80 overflow-auto -mx-1">
        {tasks.map(t => {
          const recordProg = t.recordId ? tiktokProgressByRecord.get(t.recordId) : undefined
          const phase = t.status === 'uploading'
            ? (t.browserPct < 100 ? 'browser' : 'tiktok')
            : 'done'
          return (
            <div key={t.id} className="px-1 py-2 flex items-center gap-3">
              <TaskStatusIcon status={t.status} />
              <div className="flex-1 min-w-0">
                <div className="text-sm text-gray-800 truncate" title={t.file.name}>
                  {t.file.name}
                  <span className="ml-2 text-[11px] text-gray-400">{fmtSize(t.file.size)}</span>
                </div>
                <div className="mt-1 flex items-center gap-2">
                  {t.status === 'uploading' && (
                    <>
                      <div className="flex-1 h-1 bg-gray-100 rounded overflow-hidden">
                        {phase === 'browser' ? (
                          <div className="h-full bg-blue-500 transition-all"
                            style={{ width: `${t.browserPct}%` }} />
                        ) : recordProg ? (
                          <div className="h-full bg-emerald-500 transition-all"
                            style={{ width: `${recordProg.pct}%` }} />
                        ) : (
                          <div className="h-full bg-gradient-to-r from-blue-400 via-blue-500 to-blue-400 animate-pulse" />
                        )}
                      </div>
                      <span className="text-[11px] text-gray-500 w-28 text-right">
                        {phase === 'browser'
                          ? `上传至服务器 ${t.browserPct}%`
                          : recordProg
                            ? `转发 TikTok ${recordProg.pct}%`
                            : '等待 TikTok...'}
                      </span>
                    </>
                  )}
                  {t.status === 'queued' && (
                    <span className="text-[11px] text-gray-400">排队中</span>
                  )}
                  {t.status === 'success' && (
                    <span className="text-[11px] text-green-600">上传成功</span>
                  )}
                  {t.status === 'failed' && (
                    <span className="text-[11px] text-red-500 truncate" title={t.error}>
                      失败：{t.error}
                    </span>
                  )}
                  {t.status === 'canceled' && (
                    <span className="text-[11px] text-gray-400">已取消</span>
                  )}
                </div>
              </div>

              {(t.status === 'uploading' || t.status === 'queued') && (
                <button
                  onClick={() => onCancel(t.id)}
                  className="p-1 text-gray-400 hover:text-red-500 transition"
                  title="取消该任务"
                >
                  <X className="w-4 h-4" />
                </button>
              )}
              {(t.status === 'failed' || t.status === 'canceled') && !t.validationError && (
                <button
                  onClick={() => onRetry(t.id)}
                  className="inline-flex items-center gap-1 px-2 py-0.5 text-[11px] text-blue-600 bg-blue-50 rounded hover:bg-blue-100 transition"
                  title="重新上传"
                >
                  <RotateCcw className="w-3 h-3" />重试
                </button>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function TaskStatusIcon({ status }: { status: UploadTask['status'] }) {
  if (status === 'uploading') return <Loader2 className="w-4 h-4 text-blue-500 animate-spin flex-shrink-0" />
  if (status === 'success')   return <CheckCircle2 className="w-4 h-4 text-green-500 flex-shrink-0" />
  if (status === 'failed')    return <XCircle className="w-4 h-4 text-red-500 flex-shrink-0" />
  if (status === 'canceled')  return <XCircle className="w-4 h-4 text-gray-400 flex-shrink-0" />
  return <Clock className="w-4 h-4 text-gray-400 flex-shrink-0" />
}
