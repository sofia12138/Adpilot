import { useState, useRef, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  Upload, Loader2, AlertCircle, CheckCircle2, XCircle, Clock,
  Film, Search, Copy, AlertTriangle, Info, FileVideo, RotateCcw, Trash2,
} from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { useTikTokMaterials } from '@/hooks/use-tiktok-materials'
import {
  validateVideoFile, getVideoDuration, uploadTikTokVideo,
  isDurationOverLimit, DURATION_THRESHOLD, deleteMaterial,
  type TikTokMaterialRecord,
} from '@/services/tiktok-materials'
import { fetchTikTokAdvertisers, type Advertiser } from '@/services/advertisers'
import { useQuery } from '@tanstack/react-query'

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

export default function TikTokMaterialsPage() {
  const qc = useQueryClient()
  const fileRef = useRef<HTMLInputElement>(null)

  const [advertiserId, setAdvertiserId] = useState('')
  const [keyword, setKeyword] = useState('')
  const [page, setPage] = useState(1)
  const pageSize = 20

  // upload state
  const [uploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState(0)
  const [uploadError, setUploadError] = useState('')
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [fileDuration, setFileDuration] = useState<number | null>(null)
  const [showConfirm, setShowConfirm] = useState(false)
  const abortRef = useRef<(() => void) | null>(null)

  const { data: advData } = useQuery({
    queryKey: ['tiktok-advertisers'],
    queryFn: fetchTikTokAdvertisers,
  })
  const advertisers: Advertiser[] = advData?.data ?? []

  const effectiveAdvId = advertiserId

  const { data: listData, isLoading } = useTikTokMaterials({
    advertiser_id: effectiveAdvId || undefined,
    keyword: keyword || undefined,
    page,
    page_size: pageSize,
  }, uploading)
  const items: TikTokMaterialRecord[] = listData?.data?.items ?? []
  const total = listData?.data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / pageSize))

  // ── file select ──
  const handleFileSelect = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''

    const err = validateVideoFile(file)
    if (err) { setUploadError(err); return }

    setUploadError('')
    setSelectedFile(file)

    try {
      const dur = await getVideoDuration(file)
      setFileDuration(dur)
    } catch {
      setFileDuration(null)
    }
    setShowConfirm(true)
  }, [])

  // ── do upload ──
  const doUpload = useCallback(async () => {
    if (!selectedFile || !effectiveAdvId) return
    setShowConfirm(false)
    setUploading(true)
    setUploadProgress(0)
    setUploadError('')

    const { promise, abort } = uploadTikTokVideo(
      effectiveAdvId, selectedFile, fileDuration,
      (pct) => setUploadProgress(pct),
    )
    abortRef.current = abort

    try {
      const result = await promise
      if (!result.success) {
        setUploadError(result.error ?? '上传失败')
      }
      qc.invalidateQueries({ queryKey: ['tiktok-materials'] })
    } finally {
      setUploading(false)
      setSelectedFile(null)
      setFileDuration(null)
      abortRef.current = null
    }
  }, [selectedFile, effectiveAdvId, fileDuration, qc])

  const cancelUpload = useCallback(() => {
    setShowConfirm(false)
    setSelectedFile(null)
    setFileDuration(null)
  }, [])

  const retryRef = useRef<HTMLInputElement>(null)
  const retryRecordRef = useRef<TikTokMaterialRecord | null>(null)

  const handleRetry = useCallback((record: TikTokMaterialRecord) => {
    retryRecordRef.current = record
    if (record.advertiser_id) setAdvertiserId(record.advertiser_id)
    retryRef.current?.click()
  }, [])

  const handleRetryFileChange = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''

    const err = validateVideoFile(file)
    if (err) { setUploadError(err); return }

    const record = retryRecordRef.current
    if (!record) return

    // 删除旧的失败记录
    try { await deleteMaterial(record.id) } catch { /* ignore */ }

    setUploadError('')
    setSelectedFile(file)

    try {
      const dur = await getVideoDuration(file)
      setFileDuration(dur)
    } catch {
      setFileDuration(record.duration_sec)
    }
    setShowConfirm(true)
    retryRecordRef.current = null
  }, [])

  const deletingRef = useRef(false)
  const handleDelete = useCallback(async (id: number) => {
    if (deletingRef.current) return
    deletingRef.current = true
    try {
      await deleteMaterial(id)
      qc.invalidateQueries({ queryKey: ['tiktok-materials'] })
    } catch {
      setUploadError('删除失败')
    } finally {
      deletingRef.current = false
    }
  }, [qc])

  const isOverLimit = isDurationOverLimit(fileDuration)

  return (
    <div className="space-y-4">
      <input ref={retryRef} type="file" accept="video/*" className="hidden" onChange={handleRetryFileChange} />
      <PageHeader title="TikTok 素材上传" description="上传视频到 TikTok Asset Library，支持长视频 API 入库" />

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

        <input ref={fileRef} type="file" accept="video/*" className="hidden" onChange={handleFileSelect} />
        <button
          onClick={() => fileRef.current?.click()}
          disabled={uploading || !effectiveAdvId}
          className="inline-flex items-center gap-2 h-9 px-4 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition"
        >
          {uploading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />}
          {uploading ? `上传中 ${uploadProgress}%` : '上传视频'}
        </button>
      </div>

      {/* ── upload confirm dialog ── */}
      {showConfirm && selectedFile && (
        <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm space-y-3">
          <div className="flex items-start gap-3">
            <FileVideo className="w-10 h-10 text-blue-500 flex-shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <div className="font-medium text-gray-800 truncate">{selectedFile.name}</div>
              <div className="text-sm text-gray-500 mt-1 flex items-center gap-3">
                <span>{fmtSize(selectedFile.size)}</span>
                <span>{fileDuration != null ? fmtDuration(fileDuration) : '时长读取中...'}</span>
              </div>
            </div>
          </div>

          {isOverLimit && (
            <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-50 border border-amber-200">
              <AlertTriangle className="w-5 h-5 text-amber-600 flex-shrink-0 mt-0.5" />
              <div className="text-sm text-amber-800">
                <div className="font-medium">该视频时长超过 {DURATION_THRESHOLD / 60} 分钟，仅支持 API 入库模式</div>
                <div className="mt-1 text-amber-700">
                  是否可直接用于广告投放，以 TikTok 广告规格和账户能力为准。
                  Non-Spark Ads 视频时长上限为 10 分钟。
                </div>
              </div>
            </div>
          )}

          {!isOverLimit && fileDuration != null && (
            <div className="flex items-start gap-2 p-3 rounded-lg bg-blue-50 border border-blue-100">
              <Info className="w-4 h-4 text-blue-500 flex-shrink-0 mt-0.5" />
              <div className="text-sm text-blue-700">
                视频时长在 {DURATION_THRESHOLD / 60} 分钟以内，将通过 API 上传至 TikTok Asset Library。
              </div>
            </div>
          )}

          <div className="flex items-center gap-2 justify-end">
            <button onClick={cancelUpload}
              className="px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 transition">
              取消
            </button>
            <button onClick={doUpload}
              className="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition font-medium">
              确认上传
            </button>
          </div>
        </div>
      )}

      {/* ── progress bar ── */}
      {uploading && (() => {
        const tiktokProg = items.find(r => r.upload_status === 'uploading' && r.tiktok_progress)?.tiktok_progress
        const phase2 = uploadProgress >= 100
        const phase2pct = tiktokProg?.pct ?? 0
        return (
          <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm space-y-2">
            <div className="flex items-center justify-between text-sm">
              <div className="flex items-center gap-2 text-gray-700 font-medium">
                <Loader2 className="w-4 h-4 animate-spin text-blue-500" />
                {!phase2
                  ? `正在上传至服务器... ${uploadProgress}%`
                  : tiktokProg
                    ? `正在转发至 TikTok API... ${phase2pct}%`
                    : '文件已到达服务器，正在转发至 TikTok API...'}
              </div>
              {!phase2 && (
                <button
                  onClick={() => { abortRef.current?.(); setUploading(false); setUploadError('已取消上传') }}
                  className="text-xs text-gray-400 hover:text-red-500 transition"
                >取消</button>
              )}
            </div>
            <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
              {!phase2 ? (
                <div className="bg-blue-500 h-full rounded-full transition-all duration-300"
                  style={{ width: `${uploadProgress}%` }} />
              ) : tiktokProg ? (
                <div className="bg-emerald-500 h-full rounded-full transition-all duration-500"
                  style={{ width: `${phase2pct}%` }} />
              ) : (
                <div className="h-full rounded-full bg-gradient-to-r from-blue-400 via-blue-500 to-blue-400 animate-pulse" />
              )}
            </div>
            <div className="text-xs text-gray-400">
              {!phase2
                ? '大文件上传可能需要数分钟，请勿关闭页面'
                : tiktokProg
                  ? `已传输 ${(phase2pct).toFixed(0)}%，大文件可能需要 2-5 分钟`
                  : '等待 TikTok 处理中，大文件可能需要 2-5 分钟'}
            </div>
          </div>
        )
      })()}

      {/* ── error ── */}
      {uploadError && (
        <div className="flex items-center gap-2 p-3 rounded-lg bg-red-50 border border-red-200 text-sm text-red-700">
          <AlertCircle className="w-4 h-4 flex-shrink-0" />
          {uploadError}
          <button onClick={() => setUploadError('')} className="ml-auto text-red-400 hover:text-red-600">✕</button>
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
                        onClick={() => handleRetry(r)}
                        disabled={uploading}
                        className="inline-flex items-center gap-1 px-2 py-1 text-xs text-blue-600 bg-blue-50 rounded hover:bg-blue-100 transition disabled:opacity-40"
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
