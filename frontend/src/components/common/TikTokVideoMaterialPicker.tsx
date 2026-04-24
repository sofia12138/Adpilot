/**
 * 统一视频素材选择器（W2A 批量建广告专用）
 *
 * 三个 Tab：
 *  A. 系统素材：本系统已上传的 TikTok 素材（fetchMaterialList）
 *  B. 本地批量上传：拖拽/多文件选择 → 顺序上传到 TikTok（uploadTikTokVideo）→ 自动加入已选
 *  C. 账户素材：当前 advertiser_id 在 TikTok 上的所有视频素材（含手动上传，分页）
 *
 * 多选；统一返回 PickedVideo[]：{video_id, file_name, source}
 */
import { useState, useRef, useEffect, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2, X, Search, Upload, CheckCircle, FileVideo, Trash2, AlertCircle } from 'lucide-react'
import {
  fetchMaterialList,
  fetchAccountLibraryVideos,
  uploadTikTokVideo,
  validateVideoFile,
  getVideoDuration,
  type TikTokMaterialRecord,
  type AccountLibraryVideo,
} from '@/services/tiktok-materials'

export interface PickedVideo {
  video_id: string
  file_name: string
  source: 'system' | 'local' | 'account'
  /** 系统素材专属 */
  material_id?: number
  /** 预览封面（可选，仅账户素材有） */
  cover_url?: string
}

interface Props {
  open: boolean
  advertiserId: string
  /** 已选素材（受控） */
  value: PickedVideo[]
  onChange: (next: PickedVideo[]) => void
  onClose: () => void
}

type TabKey = 'system' | 'local' | 'account'

interface LocalUploadItem {
  id: string  // 本地临时 id
  file: File
  status: 'pending' | 'uploading' | 'success' | 'failed'
  progress: number
  video_id?: string
  error?: string
}

const tabBtnCls = (active: boolean) =>
  `px-4 py-2 text-sm font-medium border-b-2 transition ${
    active
      ? 'border-pink-500 text-pink-600'
      : 'border-transparent text-gray-500 hover:text-gray-700'
  }`

export function TikTokVideoMaterialPicker({ open, advertiserId, value, onChange, onClose }: Props) {
  const [tab, setTab] = useState<TabKey>('system')

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-xl w-full max-w-4xl mx-4 overflow-hidden flex flex-col" style={{ maxHeight: '85vh' }} onClick={e => e.stopPropagation()}>
        {/* 头部 */}
        <div className="px-6 py-4 border-b border-gray-100 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-800">选择视频素材</h3>
            <p className="text-xs text-gray-400 mt-0.5">已选 {value.length} 个 · 一个素材将创建一个 Ad</p>
          </div>
          <button onClick={onClose} className="p-1 hover:bg-gray-100 rounded-lg"><X className="w-4 h-4 text-gray-400" /></button>
        </div>

        {/* Tab 切换 */}
        <div className="px-6 border-b border-gray-100 flex gap-2">
          <button className={tabBtnCls(tab === 'system')} onClick={() => setTab('system')}>系统素材</button>
          <button className={tabBtnCls(tab === 'local')} onClick={() => setTab('local')}>本地批量上传</button>
          <button className={tabBtnCls(tab === 'account')} onClick={() => setTab('account')}>账户素材</button>
        </div>

        {/* 内容区 */}
        <div className="flex-1 overflow-hidden">
          {!advertiserId ? (
            <div className="py-16 text-center text-sm text-gray-400">请先在表单中选择广告主</div>
          ) : tab === 'system' ? (
            <SystemTab advertiserId={advertiserId} value={value} onChange={onChange} />
          ) : tab === 'local' ? (
            <LocalTab advertiserId={advertiserId} value={value} onChange={onChange} />
          ) : (
            <AccountTab advertiserId={advertiserId} value={value} onChange={onChange} />
          )}
        </div>

        {/* 已选预览（始终显示） */}
        {value.length > 0 && (
          <div className="px-6 py-3 border-t border-gray-100 bg-gray-50">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-gray-600">已选 {value.length} 个素材</span>
              <button
                onClick={() => onChange([])}
                className="text-[11px] text-gray-400 hover:text-red-500"
              >全部清除</button>
            </div>
            <div className="flex flex-wrap gap-2 max-h-24 overflow-y-auto">
              {value.map(v => (
                <div key={`${v.source}-${v.video_id}`} className="flex items-center gap-1 px-2 py-1 bg-white border border-gray-200 rounded-md text-[11px]">
                  <FileVideo className="w-3 h-3 text-blue-400" />
                  <span className="max-w-[180px] truncate" title={v.file_name}>{v.file_name || v.video_id}</span>
                  <button
                    onClick={() => onChange(value.filter(x => !(x.video_id === v.video_id && x.source === v.source)))}
                    className="ml-1 text-gray-300 hover:text-red-500"
                  ><X className="w-3 h-3" /></button>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* 底部 */}
        <div className="px-6 py-3 border-t border-gray-100 flex items-center justify-end gap-3">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50">取消</button>
          <button
            onClick={onClose}
            disabled={value.length === 0}
            className="px-4 py-2 text-sm text-white bg-pink-500 rounded-lg hover:bg-pink-600 disabled:opacity-40"
          >确认（{value.length} 个）</button>
        </div>
      </div>
    </div>
  )
}

/* ───── Tab A: 系统素材 ───── */

function SystemTab({ advertiserId, value, onChange }: { advertiserId: string; value: PickedVideo[]; onChange: (n: PickedVideo[]) => void }) {
  const [keyword, setKeyword] = useState('')
  const { data, isLoading, error } = useQuery({
    queryKey: ['picker-system-materials', advertiserId, keyword],
    queryFn: () => fetchMaterialList({ advertiser_id: advertiserId, status: 'success', page_size: 100, keyword: keyword || undefined }),
    enabled: !!advertiserId,
    staleTime: 30_000,
  })
  const items: TikTokMaterialRecord[] = (data?.data?.items ?? []).filter(m => !!m.tiktok_video_id)

  const isPicked = (vid: string) => value.some(v => v.video_id === vid && v.source === 'system')
  function toggle(m: TikTokMaterialRecord) {
    if (!m.tiktok_video_id) return
    if (isPicked(m.tiktok_video_id)) {
      onChange(value.filter(v => !(v.video_id === m.tiktok_video_id && v.source === 'system')))
    } else {
      onChange([...value, {
        video_id: m.tiktok_video_id,
        file_name: m.local_file_name,
        source: 'system',
        material_id: m.id,
      }])
    }
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-6 py-3 border-b border-gray-50">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            value={keyword}
            onChange={e => setKeyword(e.target.value)}
            placeholder="搜索文件名 / video_id..."
            className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-pink-100"
          />
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-6 py-2" style={{ maxHeight: '40vh' }}>
        {isLoading && <CenterLoading />}
        {!isLoading && error && <CenterError msg={(error as Error).message} />}
        {!isLoading && !error && items.length === 0 && <CenterEmpty msg="该广告主下暂无成功上传的系统素材" />}
        {!isLoading && items.map(m => (
          <ItemRow
            key={m.id}
            picked={isPicked(m.tiktok_video_id!)}
            file_name={m.local_file_name}
            sub={`${m.tiktok_video_id} · ${m.duration_sec ?? '?'}s`}
            onClick={() => toggle(m)}
          />
        ))}
      </div>
    </div>
  )
}

/* ───── Tab B: 本地批量上传 ───── */

function LocalTab({ advertiserId, value, onChange }: { advertiserId: string; value: PickedVideo[]; onChange: (n: PickedVideo[]) => void }) {
  const [items, setItems] = useState<LocalUploadItem[]>([])
  const inputRef = useRef<HTMLInputElement>(null)
  const [dragOver, setDragOver] = useState(false)

  const isPicked = (vid: string) => value.some(v => v.video_id === vid && v.source === 'local')

  const handleFiles = useCallback((files: FileList | File[]) => {
    const arr = Array.from(files)
    const next: LocalUploadItem[] = []
    for (const f of arr) {
      const err = validateVideoFile(f)
      next.push({
        id: `${f.name}-${f.size}-${Date.now()}-${Math.random().toString(16).slice(2, 6)}`,
        file: f,
        status: err ? 'failed' : 'pending',
        progress: 0,
        error: err || undefined,
      })
    }
    setItems(prev => [...prev, ...next])
  }, [])

  // 顺序上传：避免并发占满浏览器/网络
  useEffect(() => {
    let cancelled = false
    async function pump() {
      while (!cancelled) {
        const next = items.find(it => it.status === 'pending')
        if (!next) return
        // 标记为 uploading
        setItems(prev => prev.map(it => it.id === next.id ? { ...it, status: 'uploading' } : it))
        let duration: number | null = null
        try {
          duration = await getVideoDuration(next.file)
        } catch {
          duration = null
        }
        const { promise } = uploadTikTokVideo(advertiserId, next.file, duration, (pct) => {
          setItems(prev => prev.map(it => it.id === next.id ? { ...it, progress: pct } : it))
        })
        const res = await promise
        if (cancelled) return
        if (res.success && res.data?.tiktok_video_id) {
          const vid = res.data.tiktok_video_id
          const fname = res.data.local_file_name || next.file.name
          setItems(prev => prev.map(it => it.id === next.id ? { ...it, status: 'success', progress: 100, video_id: vid } : it))
          // 自动加入已选（去重）
          if (!isPicked(vid)) {
            onChange([...value, { video_id: vid, file_name: fname, source: 'local' }])
          }
        } else {
          setItems(prev => prev.map(it => it.id === next.id ? { ...it, status: 'failed', error: res.error || '上传失败' } : it))
        }
      }
    }
    pump()
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items.map(i => i.id + i.status).join(',')])

  function removeItem(id: string) {
    const item = items.find(i => i.id === id)
    setItems(prev => prev.filter(i => i.id !== id))
    if (item?.video_id) {
      onChange(value.filter(v => !(v.video_id === item.video_id && v.source === 'local')))
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* 拖拽/选择区 */}
      <div className="px-6 pt-4">
        <div
          onDragOver={e => { e.preventDefault(); setDragOver(true) }}
          onDragLeave={() => setDragOver(false)}
          onDrop={e => {
            e.preventDefault()
            setDragOver(false)
            if (e.dataTransfer.files?.length) handleFiles(e.dataTransfer.files)
          }}
          onClick={() => inputRef.current?.click()}
          className={`flex flex-col items-center justify-center py-8 border-2 border-dashed rounded-xl cursor-pointer transition ${
            dragOver ? 'border-pink-400 bg-pink-50/50' : 'border-gray-200 hover:border-pink-300 hover:bg-pink-50/30'
          }`}
        >
          <Upload className="w-6 h-6 text-gray-400 mb-2" />
          <p className="text-sm text-gray-600">拖拽视频文件到此处，或点击选择多文件</p>
          <p className="text-xs text-gray-400 mt-1">支持 mp4 / mov / avi / mkv / webm，单文件 ≤ 1GB</p>
          <input
            ref={inputRef}
            type="file"
            multiple
            accept=".mp4,.mov,.avi,.mkv,.webm,.m4v"
            className="hidden"
            onChange={e => { if (e.target.files?.length) handleFiles(e.target.files); e.target.value = '' }}
          />
        </div>
      </div>

      {/* 上传队列 */}
      <div className="flex-1 overflow-y-auto px-6 py-3" style={{ maxHeight: '32vh' }}>
        {items.length === 0 && <p className="text-xs text-center text-gray-400 py-4">暂无上传任务</p>}
        {items.map(it => (
          <div key={it.id} className="flex items-center gap-3 px-3 py-2 mb-1 bg-gray-50 rounded-lg text-xs">
            <FileVideo className="w-4 h-4 text-blue-400 shrink-0" />
            <div className="flex-1 min-w-0">
              <div className="text-gray-800 truncate">{it.file.name}</div>
              <div className="mt-1">
                {it.status === 'pending' && <span className="text-gray-400">等待上传...</span>}
                {it.status === 'uploading' && (
                  <div className="flex items-center gap-2">
                    <div className="flex-1 bg-gray-200 rounded-full h-1.5 overflow-hidden">
                      <div className="bg-blue-400 h-full transition-all" style={{ width: `${it.progress}%` }} />
                    </div>
                    <span className="text-blue-500">{it.progress}%</span>
                  </div>
                )}
                {it.status === 'success' && <span className="text-green-600 flex items-center gap-1"><CheckCircle className="w-3 h-3" /> 已上传 → {it.video_id}</span>}
                {it.status === 'failed' && <span className="text-red-500 flex items-center gap-1"><AlertCircle className="w-3 h-3" /> {it.error || '失败'}</span>}
              </div>
            </div>
            <button onClick={() => removeItem(it.id)} className="text-gray-300 hover:text-red-500 p-1" title="移除">
              <Trash2 className="w-3.5 h-3.5" />
            </button>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ───── Tab C: 账户素材（TikTok 远端） ───── */

function AccountTab({ advertiserId, value, onChange }: { advertiserId: string; value: PickedVideo[]; onChange: (n: PickedVideo[]) => void }) {
  const [page, setPage] = useState(1)
  const [keyword, setKeyword] = useState('')
  const { data, isLoading, error } = useQuery({
    queryKey: ['picker-account-library', advertiserId, page, keyword],
    queryFn: () => fetchAccountLibraryVideos({ advertiser_id: advertiserId, page, page_size: 20, keyword: keyword || undefined }),
    enabled: !!advertiserId,
    staleTime: 30_000,
    retry: 1,
  })

  const items: AccountLibraryVideo[] = data?.data?.items ?? []
  const totalPage = data?.data?.total_page ?? 0
  const total = data?.data?.total ?? 0

  const isPicked = (vid: string) => value.some(v => v.video_id === vid && v.source === 'account')
  function toggle(it: AccountLibraryVideo) {
    if (isPicked(it.video_id)) {
      onChange(value.filter(v => !(v.video_id === it.video_id && v.source === 'account')))
    } else {
      onChange([...value, {
        video_id: it.video_id,
        file_name: it.file_name || it.video_id,
        source: 'account',
        cover_url: it.video_cover_url,
      }])
    }
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-6 py-3 border-b border-gray-50 flex items-center gap-3">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            value={keyword}
            onChange={e => { setKeyword(e.target.value); setPage(1) }}
            placeholder="按文件名 / video_id 模糊搜索（仅当前页）..."
            className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-pink-100"
          />
        </div>
        <div className="text-xs text-gray-400">
          共 {total} 个 · 第 {page} / {totalPage || '?'} 页
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-6 py-2" style={{ maxHeight: '36vh' }}>
        {isLoading && <CenterLoading />}
        {!isLoading && error && <CenterError msg={(error as Error).message} />}
        {!isLoading && !error && items.length === 0 && <CenterEmpty msg="账户内未查询到视频素材" />}
        {!isLoading && items.map(it => (
          <ItemRow
            key={it.video_id}
            picked={isPicked(it.video_id)}
            file_name={it.file_name || it.video_id}
            sub={`${it.video_id} · ${it.duration ? Math.round(it.duration) + 's' : '?s'} · ${it.width}x${it.height}`}
            cover={it.video_cover_url}
            onClick={() => toggle(it)}
          />
        ))}
      </div>
      {/* 分页 */}
      <div className="px-6 py-2 border-t border-gray-50 flex items-center justify-between">
        <button
          onClick={() => setPage(p => Math.max(1, p - 1))}
          disabled={page <= 1 || isLoading}
          className="px-3 py-1 text-xs text-gray-600 border border-gray-200 rounded-md disabled:opacity-40 hover:bg-gray-50"
        >上一页</button>
        <span className="text-xs text-gray-400">{page} / {totalPage || '?'}</span>
        <button
          onClick={() => setPage(p => p + 1)}
          disabled={!data?.data?.has_more || isLoading}
          className="px-3 py-1 text-xs text-gray-600 border border-gray-200 rounded-md disabled:opacity-40 hover:bg-gray-50"
        >下一页</button>
      </div>
    </div>
  )
}

/* ───── 通用子组件 ───── */

function ItemRow({ picked, file_name, sub, cover, onClick }: {
  picked: boolean
  file_name: string
  sub: string
  cover?: string
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={`w-full text-left px-3 py-2 mb-1 rounded-lg flex items-center gap-3 transition ${
        picked ? 'bg-pink-50/50 border border-pink-200' : 'border border-transparent hover:bg-gray-50'
      }`}
    >
      <div className="w-12 h-12 rounded bg-gray-100 flex items-center justify-center overflow-hidden shrink-0">
        {cover ? <img src={cover} alt="" className="w-full h-full object-cover" /> : <FileVideo className="w-5 h-5 text-gray-300" />}
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-sm text-gray-800 truncate">{file_name}</div>
        <div className="text-xs text-gray-400 truncate mt-0.5">{sub}</div>
      </div>
      <div className={`w-5 h-5 rounded-full border-2 flex items-center justify-center shrink-0 ${picked ? 'bg-pink-500 border-pink-500' : 'border-gray-300'}`}>
        {picked && <CheckCircle className="w-4 h-4 text-white" />}
      </div>
    </button>
  )
}

function CenterLoading() {
  return <div className="flex items-center justify-center py-12 text-gray-400"><Loader2 className="w-5 h-5 animate-spin mr-2" /><span className="text-sm">加载中...</span></div>
}
function CenterError({ msg }: { msg: string }) {
  return <div className="py-12 text-center text-sm text-red-500">{msg}</div>
}
function CenterEmpty({ msg }: { msg: string }) {
  return <div className="py-12 text-center text-sm text-gray-300">{msg}</div>
}
