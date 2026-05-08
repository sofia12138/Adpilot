/**
 * 创建广告时的「投放语种」选择器
 *
 * 数据源 = template.delivery_languages（白名单），默认 = template.default_delivery_language。
 * 后端会强制启用平台语言定向，所以本控件不允许"不选"——若白名单为空则按 ['en'] 兜底。
 */
import { useEffect, useMemo } from 'react'
import { Globe } from 'lucide-react'
import {
  DELIVERY_LANGUAGE_OPTIONS,
  DEFAULT_DELIVERY_LANGUAGE,
  deliveryLanguageLabel,
  normalizeTemplateDeliveryLanguages,
} from '@/constants/deliveryLanguages'

interface Props {
  /** 当前选中的投放语种（受控）；首次为空时本组件会自动写入 template 默认值 */
  value: string
  /** 输出回调；调用方负责存到 state 中 */
  onChange: (code: string) => void
  /** 当前所选模板的允许语种与默认语种（来自 Template 行） */
  deliveryLanguages?: string[]
  defaultDeliveryLanguage?: string
  /** 已知模板 id，用于在切换模板时强制重置默认值 */
  templateId?: string | null
  /** 输入控件 className（沿用外层风格） */
  inputClassName?: string
  /** 顶部小标题颜色风格：默认蓝色（Meta），TikTok 链路传 'pink' */
  accent?: 'blue' | 'pink'
}

export default function DeliveryLanguageSelect({
  value, onChange,
  deliveryLanguages, defaultDeliveryLanguage,
  templateId,
  inputClassName,
  accent = 'blue',
}: Props) {
  const dl = useMemo(() => normalizeTemplateDeliveryLanguages({
    delivery_languages: deliveryLanguages,
    default_delivery_language: defaultDeliveryLanguage,
  }), [deliveryLanguages, defaultDeliveryLanguage])

  const allowedSet = useMemo(() => new Set(dl.delivery_languages), [dl.delivery_languages])
  const allowedOptions = useMemo(
    () => DELIVERY_LANGUAGE_OPTIONS.filter(o => allowedSet.has(o.code)),
    [allowedSet],
  )

  // 模板切换 / 当前选择不在白名单时，回落到模板默认语种
  useEffect(() => {
    if (!value || !allowedSet.has(value)) {
      onChange(dl.default_delivery_language || DEFAULT_DELIVERY_LANGUAGE)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [templateId, dl.default_delivery_language, dl.delivery_languages.join('|')])

  const focusRing = accent === 'pink'
    ? 'focus:ring-pink-500/20 focus:border-pink-400'
    : 'focus:ring-blue-500/20 focus:border-blue-400'
  const cls = inputClassName
    || `w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none ${focusRing} transition`

  const safeValue = allowedSet.has(value) ? value : (dl.default_delivery_language || DEFAULT_DELIVERY_LANGUAGE)

  return (
    <div>
      <div className="flex items-center gap-1.5 mb-1">
        <Globe className="w-3.5 h-3.5 text-gray-400" />
        <label className="block text-xs font-medium text-gray-600">
          投放语种 <span className="text-red-400">*</span>
        </label>
      </div>
      <select
        value={safeValue}
        onChange={e => onChange(e.target.value)}
        className={`${cls} bg-white`}
      >
        {allowedOptions.length === 0 ? (
          <option value={safeValue}>{deliveryLanguageLabel(safeValue)}</option>
        ) : (
          allowedOptions.map(o => (
            <option key={o.code} value={o.code}>
              {o.label}{o.code === dl.default_delivery_language ? ' · 模板默认' : ''}
            </option>
          ))
        )}
      </select>
      <p className="text-[11px] text-gray-400 mt-1">
        选择语种后，广告将仅投放到该语种人群。
      </p>
    </div>
  )
}
