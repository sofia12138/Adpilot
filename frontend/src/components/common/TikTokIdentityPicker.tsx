import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2, RefreshCw, AlertCircle, User } from 'lucide-react'
import { apiFetch } from '@/services/api'

export interface TikTokIdentity {
  identity_id: string
  identity_type: string
  display_name: string
  profile_image?: string
}

interface IdentityResp {
  data?: {
    identity_list: TikTokIdentity[]
    errors?: { identity_type: string; error: string }[]
  }
}

interface Props {
  advertiserId: string
  value: string
  /** 选择后回调；type 可能为空字符串 */
  onChange: (next: { identity_id: string; identity_type: string }) => void
  disabled?: boolean
  className?: string
}

async function fetchIdentities(advertiserId: string): Promise<TikTokIdentity[]> {
  if (!advertiserId) return []
  const r = await apiFetch<IdentityResp>(
    `/api/creatives/identities?advertiser_id=${encodeURIComponent(advertiserId)}`,
  )
  return r.data?.identity_list ?? []
}

export function TikTokIdentityPicker({ advertiserId, value, onChange, disabled = false, className = '' }: Props) {
  const enabled = !!advertiserId
  const { data, isLoading, isFetching, isError, error, refetch } = useQuery({
    queryKey: ['tiktok-identities', advertiserId],
    queryFn: () => fetchIdentities(advertiserId),
    enabled,
    staleTime: 30_000,
  })
  const list = useMemo(() => data ?? [], [data])

  // 当 value 在新列表里时，回填一下 identity_type
  useEffect(() => {
    if (!value) return
    const found = list.find(i => i.identity_id === value)
    if (found) onChange({ identity_id: found.identity_id, identity_type: found.identity_type })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [list.length])

  // 手动输入兜底（兼容老模板里的 identity_id）
  const [showManual, setShowManual] = useState(false)

  function handleSelect(id: string) {
    if (!id) {
      onChange({ identity_id: '', identity_type: '' })
      return
    }
    const item = list.find(i => i.identity_id === id)
    onChange({
      identity_id: id,
      identity_type: item?.identity_type || '',
    })
  }

  const inputCls = 'w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-pink-500/20 focus:border-pink-400 transition'

  return (
    <div className={`space-y-1.5 ${className}`}>
      {!enabled ? (
        <div className="text-xs text-gray-400 py-2">请先选择广告主</div>
      ) : isLoading ? (
        <div className="flex items-center gap-2 text-xs text-gray-400 py-2">
          <Loader2 className="w-3.5 h-3.5 animate-spin" /> 加载 Identity 列表...
        </div>
      ) : isError ? (
        <div className="flex items-start gap-2 p-2 text-xs text-red-600 bg-red-50 border border-red-200 rounded-lg">
          <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
          <div>
            <div>拉取 Identity 失败：{(error as Error)?.message || '未知错误'}</div>
            <button onClick={() => refetch()} className="mt-1 text-red-700 underline">重试</button>
          </div>
        </div>
      ) : list.length === 0 ? (
        <div className="flex items-start gap-2 p-2 text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded-lg">
          <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
          <div className="flex-1">
            <div>该广告主下未查询到可用 Identity。</div>
            <div className="text-amber-600 mt-0.5">
              请前往 TikTok Ads Manager → Identity 中授权或新建身份；或下方手动输入 ID。
            </div>
            <button
              onClick={() => setShowManual(s => !s)}
              className="mt-1 text-amber-800 underline"
            >
              {showManual ? '收起手动输入' : '手动输入 Identity ID'}
            </button>
          </div>
        </div>
      ) : (
        <div className="flex items-center gap-2">
          <select
            value={value}
            onChange={e => handleSelect(e.target.value)}
            className={`${inputCls} bg-white flex-1`}
            disabled={disabled}
          >
            <option value="">请选择 Identity</option>
            {list.map(i => (
              <option key={`${i.identity_id}_${i.identity_type}`} value={i.identity_id}>
                {i.display_name} · {i.identity_type}
              </option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => refetch()}
            disabled={isFetching}
            className="p-2 rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-500 disabled:opacity-50"
            title="刷新 Identity 列表"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? 'animate-spin' : ''}`} />
          </button>
          <button
            type="button"
            onClick={() => setShowManual(s => !s)}
            className="p-2 rounded-lg border border-gray-200 hover:bg-gray-50 text-gray-500"
            title="手动输入"
          >
            <User className="w-3.5 h-3.5" />
          </button>
        </div>
      )}

      {/* 手动输入兜底（兼容回填老模板里的 identity_id） */}
      {showManual && (
        <div className="grid grid-cols-2 gap-2">
          <input
            value={value}
            onChange={e => onChange({ identity_id: e.target.value.trim(), identity_type: '' })}
            placeholder="手动输入 Identity ID"
            className={inputCls}
            disabled={disabled}
          />
          <select
            value={(list.find(i => i.identity_id === value)?.identity_type) || ''}
            onChange={e => onChange({ identity_id: value, identity_type: e.target.value })}
            className={`${inputCls} bg-white`}
            disabled={disabled}
          >
            <option value="">identity_type（自动）</option>
            <option value="CUSTOMIZED_USER">CUSTOMIZED_USER</option>
            <option value="AUTH_CODE">AUTH_CODE</option>
            <option value="TT_USER">TT_USER</option>
            <option value="BC_AUTH_TT">BC_AUTH_TT</option>
          </select>
        </div>
      )}

      {/* 调试条：当前 identity_id / type */}
      {value && (
        <div className="text-[11px] text-gray-400 font-mono bg-gray-50 px-2.5 py-1 rounded-md">
          identity_id = "{value}"
          {(() => {
            const cur = list.find(i => i.identity_id === value)
            return cur ? <span className="ml-2 text-pink-500">· type={cur.identity_type}</span> : null
          })()}
        </div>
      )}
    </div>
  )
}
