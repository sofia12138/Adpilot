/**
 * Meta 账户素材选择器（Modal）
 *
 * 仅展示当前 ad_account_id 内已上传的素材（含运营手动在 Meta 平台上传的素材），
 * 通过 GET /api/meta/assets/list 拉取。本地批量上传仍走原有 uploadMetaImage / uploadMetaVideo 流程，
 * 不在本组件内重复实现。
 *
 * - Tab：视频 / 图片 / 全部
 * - 多选；统一返回标准化资产列表，由父组件转换成 MaterialItem 加入素材池
 * - 按当前页关键词过滤；分页通过 video_cursor / image_cursor 续翻
 */
import { useState, useEffect, useCallback } from 'react'
import { Loader2, Search, X, FileVideo, ImageIcon, CheckCircle } from 'lucide-react'
import { fetchMetaAccountAssets, type MetaAccountAsset } from '@/services/meta-assets'

type AssetType = 'video' | 'image' | 'all'

interface Props {
  open: boolean
  adAccountId: string
  /** 已选素材标识（{type, meta_asset_id} 唯一），用于打勾态 */
  pickedKeys: Set<string>
  /** 每次确认时把新增勾选回传给父组件（父组件负责合并到 materials 池） */
  onConfirm: (assets: MetaAccountAsset[]) => void
  onClose: () => void
}

export function buildMetaAssetKey(a: { type: string; meta_asset_id?: string; image_hash?: string; video_id?: string }): string {
  const id = a.meta_asset_id || a.video_id || a.image_hash || ''
  return `${a.type}:${id}`
}

const tabBtnCls = (active: boolean) =>
  `px-4 py-2 text-sm font-medium border-b-2 transition ${
    active ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-500 hover:text-gray-700'
  }`

export function MetaAccountAssetPicker({ open, adAccountId, pickedKeys, onConfirm, onClose }: Props) {
  const [type, setType] = useState<AssetType>('video')
  const [items, setItems] = useState<MetaAccountAsset[]>([])
  const [videoCursor, setVideoCursor] = useState<string | null>(null)
  const [imageCursor, setImageCursor] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState('')
  const [keyword, setKeyword] = useState('')
  const [stagedKeys, setStagedKeys] = useState<Set<string>>(new Set())
  const [stagedItems, setStagedItems] = useState<Map<string, MetaAccountAsset>>(new Map())

  const reset = useCallback(() => {
    setItems([]); setVideoCursor(null); setImageCursor(null); setError('')
  }, [])

  const loadFirstPage = useCallback(async () => {
    if (!adAccountId) return
    setLoading(true); reset()
    try {
      const res = await fetchMetaAccountAssets({ ad_account_id: adAccountId, type, keyword: keyword || undefined, limit: 25 })
      if (res.error && (!res.data?.items || res.data.items.length === 0)) {
        setError(res.error)
      } else {
        setItems(res.data?.items ?? [])
        setVideoCursor(res.data?.video_cursor ?? null)
        setImageCursor(res.data?.image_cursor ?? null)
      }
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [adAccountId, type, keyword, reset])

  // 打开 / 切类型时，重置 staged + 拉首屏
  useEffect(() => {
    if (!open) return
    setStagedKeys(new Set())
    setStagedItems(new Map())
    loadFirstPage()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, type, adAccountId])

  async function loadMore() {
    if (loadingMore) return
    // type=all 时无法续翻（cursor 不属单一类型）；提示用户切换 Tab
    if (type === 'all') return
    const cursor = type === 'video' ? videoCursor : imageCursor
    if (!cursor) return
    setLoadingMore(true)
    try {
      const res = await fetchMetaAccountAssets({ ad_account_id: adAccountId, type, cursor, keyword: keyword || undefined, limit: 25 })
      const more = res.data?.items ?? []
      setItems(prev => [...prev, ...more])
      setVideoCursor(res.data?.video_cursor ?? null)
      setImageCursor(res.data?.image_cursor ?? null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoadingMore(false)
    }
  }

  function toggle(item: MetaAccountAsset) {
    const k = buildMetaAssetKey(item)
    if (pickedKeys.has(k)) return  // 已经在素材池里就不允许重复勾选
    setStagedKeys(prev => {
      const next = new Set(prev)
      if (next.has(k)) next.delete(k); else next.add(k)
      return next
    })
    setStagedItems(prev => {
      const next = new Map(prev)
      if (next.has(k)) next.delete(k); else next.set(k, item)
      return next
    })
  }

  function confirm() {
    const arr = Array.from(stagedItems.values())
    if (arr.length > 0) onConfirm(arr)
    onClose()
  }

  if (!open) return null

  const showLoadMoreCursor = type === 'video' ? videoCursor : type === 'image' ? imageCursor : null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden flex flex-col" style={{ maxHeight: '85vh' }} onClick={e => e.stopPropagation()}>
        <div className="px-6 py-4 border-b border-gray-100 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-800">从账户素材库选择</h3>
            <p className="text-xs text-gray-400 mt-0.5">{adAccountId} · 含 Meta 平台手动上传素材 · 本次将新增 {stagedKeys.size} 个</p>
          </div>
          <button onClick={onClose} className="p-1 hover:bg-gray-100 rounded-lg"><X className="w-4 h-4 text-gray-400" /></button>
        </div>

        <div className="px-6 border-b border-gray-100 flex gap-2">
          <button className={tabBtnCls(type === 'video')} onClick={() => setType('video')}>视频</button>
          <button className={tabBtnCls(type === 'image')} onClick={() => setType('image')}>图片</button>
          <button className={tabBtnCls(type === 'all')} onClick={() => setType('all')}>全部</button>
        </div>

        <div className="px-6 py-3 border-b border-gray-50 flex items-center gap-3">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={keyword}
              onChange={e => setKeyword(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') loadFirstPage() }}
              placeholder="按 name / id 模糊搜索（仅当前页）"
              className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-100"
            />
          </div>
          <button
            onClick={loadFirstPage}
            disabled={loading}
            className="px-3 py-2 text-xs text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 disabled:opacity-40"
          >刷新</button>
        </div>

        <div className="flex-1 overflow-y-auto px-6 py-3" style={{ maxHeight: '50vh' }}>
          {!adAccountId && (
            <p className="py-12 text-center text-sm text-gray-400">请先选择 Meta 广告账户</p>
          )}
          {adAccountId && loading && (
            <div className="flex items-center justify-center py-12 text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin mr-2" /><span className="text-sm">加载中...</span>
            </div>
          )}
          {adAccountId && !loading && error && (
            <div className="py-12 text-center text-sm text-red-500">{error}</div>
          )}
          {adAccountId && !loading && !error && items.length === 0 && (
            <div className="py-12 text-center text-sm text-gray-300">该账户内未查询到{type === 'all' ? '' : type === 'video' ? '视频' : '图片'}素材</div>
          )}
          {adAccountId && !loading && items.map(it => {
            const k = buildMetaAssetKey(it)
            const isStaged = stagedKeys.has(k)
            const alreadyInPool = pickedKeys.has(k)
            return (
              <button
                key={k}
                onClick={() => toggle(it)}
                disabled={alreadyInPool}
                className={`w-full text-left px-3 py-2 mb-1 rounded-lg flex items-center gap-3 transition border ${
                  alreadyInPool ? 'border-gray-100 bg-gray-50 opacity-60 cursor-not-allowed' :
                  isStaged ? 'border-blue-200 bg-blue-50/50' : 'border-transparent hover:bg-gray-50'
                }`}
              >
                <div className="w-12 h-12 rounded bg-gray-100 flex items-center justify-center overflow-hidden shrink-0">
                  {it.thumbnail_url || it.preview_url ? (
                    <img src={it.thumbnail_url || it.preview_url} alt="" className="w-full h-full object-cover" />
                  ) : it.type === 'video' ? (
                    <FileVideo className="w-5 h-5 text-gray-300" />
                  ) : (
                    <ImageIcon className="w-5 h-5 text-gray-300" />
                  )}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="text-sm text-gray-800 truncate">{it.name}</div>
                  <div className="text-xs text-gray-400 truncate mt-0.5">
                    {it.type === 'video' ? `视频 · ${it.duration_sec ? Math.round(it.duration_sec / 1000) + 's' : '?s'}` : `图片 · ${it.width || '?'}x${it.height || '?'}`}
                    <span className="mx-1">·</span>
                    {it.meta_asset_id}
                  </div>
                </div>
                {alreadyInPool ? (
                  <span className="text-[10px] text-gray-400">已在素材池</span>
                ) : (
                  <div className={`w-5 h-5 rounded-full border-2 flex items-center justify-center shrink-0 ${isStaged ? 'bg-blue-500 border-blue-500' : 'border-gray-300'}`}>
                    {isStaged && <CheckCircle className="w-4 h-4 text-white" />}
                  </div>
                )}
              </button>
            )
          })}
        </div>

        <div className="px-6 py-3 border-t border-gray-100 flex items-center justify-between">
          <div className="text-xs text-gray-400">
            {type === 'all' ? '提示：要分页加载请切换至「视频」或「图片」Tab' :
              showLoadMoreCursor ? '还有更多素材' : '已全部加载'}
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={loadMore}
              disabled={!showLoadMoreCursor || loadingMore || loading}
              className="px-3 py-2 text-xs text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-40"
            >{loadingMore ? '加载中...' : '加载更多'}</button>
            <button onClick={onClose} className="px-4 py-2 text-sm text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50">取消</button>
            <button
              onClick={confirm}
              disabled={stagedKeys.size === 0}
              className="px-4 py-2 text-sm text-white bg-blue-500 rounded-lg hover:bg-blue-600 disabled:opacity-40"
            >添加到素材池（{stagedKeys.size}）</button>
          </div>
        </div>
      </div>
    </div>
  )
}
