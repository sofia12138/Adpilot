import { useMemo } from 'react'
import { Check, X } from 'lucide-react'
import {
  TIKTOK_COUNTRIES,
  TIKTOK_REGION_GROUPS,
  codesToLocationIds,
  matchRegionGroup,
  type LocationSelection,
} from '@/constants/tiktok-locations'

interface Props {
  /** 当前已选国家代码（受控） */
  value: string[]
  /** 选择变更时回调；同时返回派生的 location_ids 与 group_key */
  onChange: (next: { country_codes: string[]; location_ids: string[]; group_key: string | null }) => void
  /** 是否禁用（系统母版只读模式可用） */
  disabled?: boolean
  /** 是否展示底部 location_ids 调试条（默认 true） */
  showDebug?: boolean
}

export function TikTokLocationPicker({ value, onChange, disabled = false, showDebug = true }: Props) {
  const selected = useMemo(() => new Set(value), [value])
  const matchedGroup = useMemo(() => matchRegionGroup(value), [value])
  const locationIds = useMemo(() => codesToLocationIds(value), [value])

  function emit(nextCodes: string[]) {
    if (disabled) return
    const ids = codesToLocationIds(nextCodes)
    const grp = matchRegionGroup(nextCodes)
    onChange({
      country_codes: nextCodes,
      location_ids: ids,
      group_key: grp ? grp.key : null,
    })
  }

  function toggleCountry(code: string) {
    if (selected.has(code)) emit(value.filter(c => c !== code))
    else emit([...value, code])
  }

  function applyGroup(groupKey: string) {
    const grp = TIKTOK_REGION_GROUPS.find(g => g.key === groupKey)
    if (!grp) return
    emit([...grp.country_codes])
  }

  function clearAll() {
    emit([])
  }

  return (
    <div className="space-y-2">
      {/* 地区组快捷区 */}
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="text-[11px] text-gray-400 mr-1">地区组：</span>
        {TIKTOK_REGION_GROUPS.map(g => {
          const active = matchedGroup?.key === g.key
          return (
            <button
              key={g.key}
              type="button"
              disabled={disabled}
              onClick={() => applyGroup(g.key)}
              className={`px-2.5 py-1 text-xs rounded-md border transition ${
                active
                  ? 'bg-pink-50 border-pink-300 text-pink-600 font-medium'
                  : 'bg-white border-gray-200 text-gray-600 hover:border-pink-300 hover:text-pink-500'
              } ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
              title={g.country_codes.join(', ')}
            >
              {g.name_zh}
              <span className="ml-1 text-[10px] text-gray-400">({g.country_codes.length})</span>
            </button>
          )
        })}
        {value.length > 0 && !disabled && (
          <button
            type="button"
            onClick={clearAll}
            className="ml-auto px-2 py-1 text-[11px] text-gray-400 hover:text-red-500 inline-flex items-center gap-1"
          >
            <X className="w-3 h-3" /> 清空
          </button>
        )}
      </div>

      {/* 国家多选 chips */}
      <div className="border border-gray-200 rounded-xl p-2.5 bg-white">
        <div className="flex flex-wrap gap-1.5">
          {TIKTOK_COUNTRIES.map(c => {
            const active = selected.has(c.code)
            return (
              <button
                key={c.code}
                type="button"
                disabled={disabled}
                onClick={() => toggleCountry(c.code)}
                className={`inline-flex items-center gap-1.5 px-2.5 py-1.5 text-xs rounded-lg border transition ${
                  active
                    ? 'bg-blue-50 border-blue-300 text-blue-600 font-medium'
                    : 'bg-gray-50 border-gray-200 text-gray-600 hover:border-blue-300'
                } ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
                title={`${c.name_en} · location_id=${c.location_id}`}
              >
                {active && <Check className="w-3 h-3" />}
                <span>{c.name_zh}</span>
                <span className="text-[10px] text-gray-400 font-mono">{c.code}</span>
              </button>
            )
          })}
        </div>
        {value.length === 0 && (
          <div className="mt-1.5 text-[11px] text-gray-400">请至少选择 1 个国家</div>
        )}
      </div>

      {/* 调试条：最终 location_ids */}
      {showDebug && (
        <div className="text-[11px] text-gray-400 font-mono bg-gray-50 px-2.5 py-1.5 rounded-md break-all">
          location_ids = [{locationIds.map(id => `"${id}"`).join(', ')}]
          {matchedGroup && (
            <span className="ml-2 text-pink-500">· group: {matchedGroup.key}</span>
          )}
        </div>
      )}
    </div>
  )
}

/** 工具：把模板里读出的 selection / location_ids 归一化为 country_codes */
export function resolveCountryCodesFromTemplate(
  selection: LocationSelection | undefined | null,
  fallbackLocationIds: string[] | undefined | null,
): string[] {
  if (selection?.country_codes && selection.country_codes.length > 0) {
    return [...selection.country_codes]
  }
  if (fallbackLocationIds && fallbackLocationIds.length > 0) {
    // 用 ID → code 反查；未识别的 ID 会被忽略
    const ids = fallbackLocationIds.map(String)
    const codes: string[] = []
    const seen = new Set<string>()
    for (const id of ids) {
      const c = TIKTOK_COUNTRIES.find(co => co.location_id === id)
      if (c && !seen.has(c.code)) {
        seen.add(c.code)
        codes.push(c.code)
      }
    }
    return codes
  }
  return []
}
