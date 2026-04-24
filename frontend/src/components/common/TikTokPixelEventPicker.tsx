/**
 * TikTok Pixel + 优化事件 两级联动选择器
 *
 * 联动规则：
 *  1. advertiserId 为空 → Pixel 下拉禁用
 *  2. advertiserId 变更 → 重新拉取 pixel 列表，清空已选 pixel/event
 *  3. pixelId 为空 → 优化事件下拉禁用
 *  4. pixelId 变更 → 自动清空已选 optimization_event
 *  5. 拉取失败 / pixel 列表为空 → 仍以下拉形式呈现，并提供"手动输入 Pixel ID"兜底，
 *     优化事件兜底用一份常见标准事件，确保前端绝不退化为纯文本输入
 */
import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2, RefreshCw, AlertCircle, Pencil } from 'lucide-react'
import { apiFetch } from '@/services/api'

export interface TikTokPixelEvent {
  event_code: string
  event_name: string
  event_type: string
}

export interface TikTokPixel {
  pixel_id: string
  pixel_name: string
  pixel_mode?: string
  pixel_category?: string
  events: TikTokPixelEvent[]
}

interface PixelResp {
  data?: {
    pixel_list: TikTokPixel[]
    errors?: { error: string }[]
  }
}

/**
 * 业务白名单：调用方可指定"只允许选这些 event_code"，并提供前端展示名映射。
 *  - 不传 allowedEvents → 维持原有动态拉取行为，展示 Pixel 全量事件
 *  - 传了 allowedEvents → 二级下拉只展示白名单里的事件（不论 Pixel 实际配置如何），
 *    并使用 label 作为显示名；提交值仍是 event_code
 */
export interface AllowedEvent {
  code: string
  label: string
}

interface Props {
  advertiserId: string
  pixelId: string
  optimizationEvent: string
  onChange: (next: { pixel_id: string; optimization_event: string }) => void
  disabled?: boolean
  className?: string
  allowedEvents?: AllowedEvent[]
}

// ── 拉取失败/无 pixel 时的标准事件兜底（仅 fallback，不参与正常路径） ──
// 来源：TikTok 官方常用 web event_code，避免在前端"硬编码业务列表"
const FALLBACK_EVENTS: TikTokPixelEvent[] = [
  { event_code: 'COMPLETE_PAYMENT', event_name: 'Complete Payment（完成支付）', event_type: 'STANDARD' },
  { event_code: 'SHOPPING', event_name: 'Shopping（购物）', event_type: 'STANDARD' },
  { event_code: 'PLACE_AN_ORDER', event_name: 'Place an Order（下单）', event_type: 'STANDARD' },
  { event_code: 'INITIATE_CHECKOUT', event_name: 'Initiate Checkout（开始结账）', event_type: 'STANDARD' },
  { event_code: 'ADD_TO_CART', event_name: 'Add to Cart（加入购物车）', event_type: 'STANDARD' },
  { event_code: 'ADD_BILLING', event_name: 'Add Billing（添加支付信息）', event_type: 'STANDARD' },
  { event_code: 'COMPLETE_REGISTRATION', event_name: 'Complete Registration（完成注册）', event_type: 'STANDARD' },
  { event_code: 'FORM', event_name: 'Form Submit（表单提交）', event_type: 'STANDARD' },
  { event_code: 'SUBSCRIBE', event_name: 'Subscribe（订阅）', event_type: 'STANDARD' },
  { event_code: 'VIEW_CONTENT', event_name: 'View Content（查看内容）', event_type: 'STANDARD' },
  { event_code: 'CLICK_BUTTON', event_name: 'Click Button（点击按钮）', event_type: 'STANDARD' },
  { event_code: 'DOWNLOAD', event_name: 'Download（下载）', event_type: 'STANDARD' },
  { event_code: 'LANDING_PAGE_VIEW', event_name: 'Landing Page View（落地页浏览）', event_type: 'STANDARD' },
]

async function fetchPixels(advertiserId: string): Promise<{ list: TikTokPixel[]; errorMsg: string }> {
  if (!advertiserId) return { list: [], errorMsg: '' }
  const r = await apiFetch<PixelResp>(`/api/creatives/pixels?advertiser_id=${encodeURIComponent(advertiserId)}`)
  const list = r.data?.pixel_list ?? []
  const errs = r.data?.errors ?? []
  return { list, errorMsg: errs.length > 0 ? errs.map(e => e.error).join('; ') : '' }
}

const inputCls = 'w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-pink-500/20 focus:border-pink-400 transition'

export function TikTokPixelEventPicker({
  advertiserId,
  pixelId,
  optimizationEvent,
  onChange,
  disabled = false,
  className = '',
  allowedEvents,
}: Props) {
  const enabled = !!advertiserId
  const { data, isLoading, isFetching, isError, error, refetch } = useQuery({
    queryKey: ['tiktok-pixels', advertiserId],
    queryFn: () => fetchPixels(advertiserId),
    enabled,
    staleTime: 30_000,
    // backend reload / 网络抖动会让本地 fetch 收到 "Server disconnected"。
    // 这类瞬时错误自动重试 2 次（指数退避 0.5s/1s/2s），避免要求用户手动刷新。
    retry: 2,
    retryDelay: attempt => Math.min(500 * 2 ** attempt, 2000),
  })
  const pixelList = useMemo(() => data?.list ?? [], [data])
  const apiErrorMsg = data?.errorMsg ?? ''

  // ── 当前选中的 pixel ──
  const selectedPixel = useMemo(
    () => pixelList.find(p => p.pixel_id === pixelId) ?? null,
    [pixelList, pixelId],
  )

  // 优化事件来源：
  //  · 优先选中 pixel 自带的 events
  //  · 当 API 失败 / pixel 列表为空 / 选中 pixel 没有 events 时，回落到 FALLBACK_EVENTS
  //  · 若调用方传入 allowedEvents（业务白名单），最后再 filter+remap：
  //    - 只保留白名单中的 event_code
  //    - 用 label 替换显示名（event_name），这样下拉显示业务名而非平台原始名
  //    - 即使 Pixel 上没有这些 event_code，也注入为可选项（保证业务能力始终可用）
  const eventOptions: TikTokPixelEvent[] = useMemo(() => {
    let base: TikTokPixelEvent[]
    if (selectedPixel && selectedPixel.events.length > 0) base = selectedPixel.events
    else if (pixelId && (apiErrorMsg || pixelList.length === 0 || !selectedPixel)) base = FALLBACK_EVENTS
    else base = []

    if (!allowedEvents || allowedEvents.length === 0) return base

    const baseMap = new Map(base.map(e => [e.event_code, e]))
    return allowedEvents.map(allow => {
      const hit = baseMap.get(allow.code)
      return {
        event_code: allow.code,
        event_name: allow.label,
        event_type: hit?.event_type || 'STANDARD',
      }
    })
  }, [selectedPixel, pixelId, apiErrorMsg, pixelList.length, allowedEvents])

  const eventInList = useMemo(
    () => eventOptions.some(e => e.event_code === optimizationEvent),
    [eventOptions, optimizationEvent],
  )

  // ── 切换 advertiser 时清空 pixel + event ──
  // ESLint 警告关掉：onChange 是父组件函数，不需要进依赖列表
  useEffect(() => {
    if (!advertiserId && (pixelId || optimizationEvent)) {
      onChange({ pixel_id: '', optimization_event: '' })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [advertiserId])

  // ── 手动输入 Pixel ID 兜底切换 ──
  const [showManualPixel, setShowManualPixel] = useState(false)

  function handlePixelSelect(nextPid: string) {
    // 切换 pixel 时强制清空已选事件，由用户重新选
    onChange({ pixel_id: nextPid, optimization_event: '' })
  }

  function handleEventSelect(nextEvent: string) {
    onChange({ pixel_id: pixelId, optimization_event: nextEvent })
  }

  // pixel 下拉选项；当模板带的 pixelId 不在 API 列表中（API 失败或 pixel 已被清理）时，
  // 把它作为一个临时 option 保留，避免 select 显示成空白
  const pixelSelectOptions = useMemo(() => {
    const opts = pixelList.map(p => ({
      pixel_id: p.pixel_id,
      label: `${p.pixel_name} · ${p.pixel_id}`,
    }))
    if (pixelId && !pixelList.some(p => p.pixel_id === pixelId)) {
      opts.unshift({ pixel_id: pixelId, label: `（手动/历史值）${pixelId}` })
    }
    return opts
  }, [pixelList, pixelId])

  // 同样地：optimizationEvent 不在新 options 里时保留为临时选项
  // 当调用方传 allowedEvents 时，事件名已经是业务名（如 "Purchase"），就不再追加 (event_code)
  const useBusinessLabel = !!(allowedEvents && allowedEvents.length > 0)
  const eventSelectOptions = useMemo(() => {
    const opts = eventOptions.map(e => ({
      event_code: e.event_code,
      label: useBusinessLabel ? e.event_name : `${e.event_name} (${e.event_code})`,
    }))
    if (optimizationEvent && !eventInList) {
      opts.unshift({ event_code: optimizationEvent, label: `（手动/历史值）${optimizationEvent}` })
    }
    return opts
  }, [eventOptions, optimizationEvent, eventInList, useBusinessLabel])

  return (
    <div className={`grid grid-cols-2 gap-4 ${className}`}>
      {/* ── Pixel 下拉 ── */}
      <div>
        <div className="flex items-center justify-between mb-1">
          <label className="block text-xs font-medium text-gray-600">Pixel ID</label>
          <button
            type="button"
            onClick={() => setShowManualPixel(s => !s)}
            className="text-[11px] text-blue-500 hover:text-blue-600 flex items-center gap-1"
            disabled={disabled || !enabled}
            title="手动输入 Pixel ID（用于兜底/老数据）"
          >
            <Pencil className="w-3 h-3" />
            {showManualPixel ? '收起手动输入' : '手动输入'}
          </button>
        </div>

        {!enabled ? (
          <div className="text-xs text-gray-400 py-2">请先选择广告主</div>
        ) : isLoading ? (
          <div className="flex items-center gap-2 text-xs text-gray-400 py-2">
            <Loader2 className="w-3.5 h-3.5 animate-spin" /> 加载 Pixel 列表...
          </div>
        ) : (
          <div className="flex items-center gap-2">
            <select
              value={pixelId}
              onChange={e => handlePixelSelect(e.target.value)}
              className={`${inputCls} bg-white flex-1`}
              disabled={disabled}
            >
              <option value="">请选择 Pixel</option>
              {pixelSelectOptions.map(o => (
                <option key={o.pixel_id} value={o.pixel_id}>{o.label}</option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => refetch()}
              disabled={isFetching || disabled}
              className="p-2 rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-500 disabled:opacity-50"
              title="刷新 Pixel 列表"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? 'animate-spin' : ''}`} />
            </button>
          </div>
        )}

        {/* 错误/空态提示 */}
        {enabled && !isLoading && (isError || apiErrorMsg) && (
          <div className="flex items-start gap-1 mt-1 text-[11px] text-amber-700">
            <AlertCircle className="w-3 h-3 shrink-0 mt-0.5" />
            <span>
              拉取 Pixel 失败：{(error as Error)?.message || apiErrorMsg}。
              可点上方"手动输入"或重试。
            </span>
          </div>
        )}
        {enabled && !isLoading && !isError && !apiErrorMsg && pixelList.length === 0 && (
          <p className="text-[11px] text-amber-700 mt-1">
            该广告主下未查询到 Pixel。可点上方"手动输入"。
          </p>
        )}

        {/* 手动输入兜底 */}
        {showManualPixel && enabled && (
          <input
            value={pixelId}
            onChange={e => handlePixelSelect(e.target.value.trim())}
            placeholder="手动输入 Pixel ID"
            className={`${inputCls} mt-2`}
            disabled={disabled}
          />
        )}
      </div>

      {/* ── 优化事件下拉（联动） ── */}
      <div>
        <label className="block text-xs font-medium text-gray-600 mb-1">优化事件</label>
        {!enabled ? (
          <div className="text-xs text-gray-400 py-2">请先选择广告主</div>
        ) : !pixelId ? (
          <div className="text-xs text-gray-400 py-2">请先选择 Pixel</div>
        ) : (
          <select
            value={optimizationEvent}
            onChange={e => handleEventSelect(e.target.value)}
            className={`${inputCls} bg-white`}
            disabled={disabled}
          >
            <option value="">请选择优化事件</option>
            {eventSelectOptions.map(o => (
              <option key={o.event_code} value={o.event_code}>{o.label}</option>
            ))}
          </select>
        )}

        {/* 数据来源提示，便于排查 */}
        {enabled && pixelId && eventOptions.length > 0 && (
          <p className="text-[11px] text-gray-400 mt-1">
            {useBusinessLabel
              ? `事件来源：业务白名单（${eventOptions.length} 个）`
              : selectedPixel && selectedPixel.events.length > 0
              ? `事件来源：Pixel "${selectedPixel.pixel_name}" 配置（${selectedPixel.events.length} 个）`
              : `事件来源：兜底标准事件（API 暂未返回 events，${eventOptions.length} 个）`}
          </p>
        )}
      </div>
    </div>
  )
}
