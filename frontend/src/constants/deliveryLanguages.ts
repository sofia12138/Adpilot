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
  { code: 'en',      label: '英语 (English)' },
  { code: 'es',      label: '西班牙语 (Español)' },
  { code: 'pt',      label: '葡萄牙语 (Português)' },
  { code: 'fr',      label: '法语 (Français)' },
  { code: 'de',      label: '德语 (Deutsch)' },
  { code: 'it',      label: '意大利语 (Italiano)' },
  { code: 'nl',      label: '荷兰语 (Nederlands)' },
  { code: 'ru',      label: '俄语 (Русский)' },
  { code: 'ar',      label: '阿拉伯语 (العربية)' },
  { code: 'ja',      label: '日语 (日本語)' },
  { code: 'ko',      label: '韩语 (한국어)' },
  { code: 'id',      label: '印尼语 (Bahasa Indonesia)' },
  { code: 'th',      label: '泰语 (ไทย)' },
  { code: 'vi',      label: '越南语 (Tiếng Việt)' },
  { code: 'zh-Hant', label: '繁体中文 (繁體中文)' },
  { code: 'zh-Hans', label: '简体中文 (简体中文)' },
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
