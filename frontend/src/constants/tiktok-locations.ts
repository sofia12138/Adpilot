/**
 * TikTok 投放地区配置
 * ─────────────────────────────────────────────
 * TikTok Marketing API 的 location_ids 来自 TikTok 自有的 Geo 字典，
 * 与 GeoNames / Meta 的 location_id 不通用，必须独立维护一份映射。
 *
 * 内置 4 个核心英语国家；后续如需扩展，直接往 TIKTOK_COUNTRIES 追加即可。
 * 如果用户后续要走"地区组资产库"（ad_assets/region-groups），可以在
 * 选择器里再加一个 source = 'asset'，按相同结构注入。
 */

export interface TikTokCountry {
  /** ISO 国家代码，如 US */
  code: string
  /** 中文名 */
  name_zh: string
  /** 英文名 */
  name_en: string
  /** TikTok location_id（字符串） */
  location_id: string
}

export interface TikTokRegionGroup {
  key: string
  name_zh: string
  name_en: string
  country_codes: string[]
}

export const TIKTOK_COUNTRIES: TikTokCountry[] = [
  { code: 'US', name_zh: '美国', name_en: 'United States', location_id: '2077456' },
  { code: 'GB', name_zh: '英国', name_en: 'United Kingdom', location_id: '2635167' },
  { code: 'CA', name_zh: '加拿大', name_en: 'Canada', location_id: '2186224' },
  { code: 'AU', name_zh: '澳大利亚', name_en: 'Australia', location_id: '6251999' },
]

export const TIKTOK_REGION_GROUPS: TikTokRegionGroup[] = [
  {
    key: 'north_america',
    name_zh: '北美',
    name_en: 'North America',
    country_codes: ['US', 'CA'],
  },
  {
    key: 'english_core',
    name_zh: '英语核心',
    name_en: 'English Core',
    country_codes: ['US', 'CA', 'GB', 'AU'],
  },
  {
    key: 'tier1',
    name_zh: 'Tier 1',
    name_en: 'Tier 1',
    country_codes: ['US', 'CA', 'GB', 'AU'],
  },
]

const CODE_TO_COUNTRY: Record<string, TikTokCountry> = Object.fromEntries(
  TIKTOK_COUNTRIES.map(c => [c.code, c]),
)
const ID_TO_COUNTRY: Record<string, TikTokCountry> = Object.fromEntries(
  TIKTOK_COUNTRIES.map(c => [c.location_id, c]),
)

/** 根据国家代码列表生成 location_ids（保持顺序、去重） */
export function codesToLocationIds(codes: string[]): string[] {
  const seen = new Set<string>()
  const ids: string[] = []
  for (const c of codes) {
    const entry = CODE_TO_COUNTRY[c]
    if (entry && !seen.has(entry.location_id)) {
      seen.add(entry.location_id)
      ids.push(entry.location_id)
    }
  }
  return ids
}

/** 反向：根据 location_ids 推回国家代码（未识别的 ID 会被丢弃） */
export function locationIdsToCodes(ids: string[]): string[] {
  const seen = new Set<string>()
  const codes: string[] = []
  for (const id of ids) {
    const entry = ID_TO_COUNTRY[String(id)]
    if (entry && !seen.has(entry.code)) {
      seen.add(entry.code)
      codes.push(entry.code)
    }
  }
  return codes
}

export function getCountryByCode(code: string): TikTokCountry | undefined {
  return CODE_TO_COUNTRY[code]
}

export function getRegionGroupByKey(key: string): TikTokRegionGroup | undefined {
  return TIKTOK_REGION_GROUPS.find(g => g.key === key)
}

/**
 * 已选国家代码集合是否完全等于某个地区组（用于回显时高亮组按钮）。
 * 顺序无关，必须严格相等。
 */
export function matchRegionGroup(codes: string[]): TikTokRegionGroup | undefined {
  const set = new Set(codes)
  return TIKTOK_REGION_GROUPS.find(g => {
    if (g.country_codes.length !== set.size) return false
    return g.country_codes.every(c => set.has(c))
  })
}

/** 模板中保存的结构化选择 */
export interface LocationSelection {
  group_key?: string | null
  country_codes: string[]
}
