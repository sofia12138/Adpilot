/**
 * 投放语种常量（与后端 backend/services/delivery_language.py 保持一致）
 *
 * 顺序即下拉/多选框中的展示顺序；新增语种请同步更新后端 SUPPORTED_LANGUAGE_CODES、
 * META_LOCALES、TIKTOK_LANGUAGES。
 */
export interface DeliveryLanguageOption {
  code: string
  label: string
}

export const DELIVERY_LANGUAGE_OPTIONS: DeliveryLanguageOption[] = [
  { code: 'en',      label: '英语（全部）' },
  { code: 'en-US',   label: '英语（美国）' },
  { code: 'en-GB',   label: '英语（英国）' },
  { code: 'es',      label: '西班牙语' },
  { code: 'es-ES',   label: '西班牙语（西班牙）' },
  { code: 'pt',      label: '葡萄牙语（全部）' },
  { code: 'pt-BR',   label: '葡萄牙语（巴西）' },
  { code: 'pt-PT',   label: '葡萄牙语（葡萄牙）' },
  { code: 'fr',      label: '法语（全部）' },
  { code: 'fr-FR',   label: '法语（法国）' },
  { code: 'fr-CA',   label: '法语（加拿大）' },
  { code: 'de',      label: '德语' },
  { code: 'it',      label: '意大利语' },
  { code: 'nl',      label: '荷兰语' },
  { code: 'ru',      label: '俄语' },
  { code: 'ar',      label: '阿拉伯语' },
  { code: 'ja',      label: '日语' },
  { code: 'ko',      label: '韩语' },
  { code: 'id',      label: '印尼语' },
  { code: 'th',      label: '泰语' },
  { code: 'vi',      label: '越南语' },
  { code: 'zh-Hant', label: '繁体中文（台湾）' },
  { code: 'zh-HK',   label: '繁体中文（香港）' },
  { code: 'zh-Hans', label: '简体中文' },
]

export const SUPPORTED_DELIVERY_LANGUAGE_CODES: string[] = DELIVERY_LANGUAGE_OPTIONS.map(o => o.code)

export const DEFAULT_DELIVERY_LANGUAGES: string[] = ['en']
export const DEFAULT_DELIVERY_LANGUAGE: string = 'en'

const LABEL_MAP: Record<string, string> = Object.fromEntries(
  DELIVERY_LANGUAGE_OPTIONS.map(o => [o.code, o.label]),
)

export function deliveryLanguageLabel(code: string): string {
  return LABEL_MAP[code] ?? code
}

/**
 * 规范化模板里的 delivery_languages / default_delivery_language。
 *
 * 输入可能来自旧模板（缺字段）或被人工编辑过的脏数据，本函数始终返回合法值组合：
 *   - allowed 至少含 1 项；
 *   - 默认语种必须 ∈ allowed，否则取 allowed[0]。
 */
export function normalizeTemplateDeliveryLanguages(input: {
  delivery_languages?: unknown
  default_delivery_language?: unknown
}): { delivery_languages: string[]; default_delivery_language: string } {
  const raw = Array.isArray(input.delivery_languages) ? input.delivery_languages : []
  const filtered = raw
    .filter((c): c is string => typeof c === 'string' && c.trim().length > 0)
    .map(c => c.trim())
  const seen = new Set<string>()
  const allowed: string[] = []
  for (const c of filtered) {
    if (!seen.has(c)) { seen.add(c); allowed.push(c) }
  }
  const finalAllowed = allowed.length > 0 ? allowed : [...DEFAULT_DELIVERY_LANGUAGES]
  const candidate = typeof input.default_delivery_language === 'string'
    ? input.default_delivery_language.trim()
    : ''
  const finalDefault = candidate && finalAllowed.includes(candidate)
    ? candidate
    : finalAllowed[0]
  return {
    delivery_languages: finalAllowed,
    default_delivery_language: finalDefault,
  }
}
