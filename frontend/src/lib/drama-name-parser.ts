/**
 * drama-name-parser.ts
 *
 * 剧名解析工具库：三层分离架构
 *   parseDramaName()              — 从单个剧名字符串中解析语言尾缀
 *   parseMiniProgramCampaignName() — 解析小程序推广活动名称（'-' 分隔，第10字段=剧名）
 *   parseAppCampaignName()         — 解析 APP 推广活动名称（'-' 分隔，第10字段=剧名）
 *
 * 核心约束（不可违反）：
 *   - 剧名固定来自第 10 字段（1-based），是唯一合法剧名来源
 *   - 第 11 字段及以后全部归入 remarkRaw
 *   - remarkRaw 不参与语言识别、剧名解析、content_key 生成
 *   - 即使 remarkRaw 中出现括号语言标记，也不影响解析结果
 */

// ─────────────────────────────────────────────────────────────
// 类型定义
// ─────────────────────────────────────────────────────────────

export interface ParsedDramaName {
  /** 原始剧名字段（未做任何修改） */
  dramaNameRaw: string
  /** 去掉语言尾缀后的本地化剧名（trim 后） */
  localizedDramaName: string
  /** 标准化语言代码，未识别时为 'unknown' */
  languageCode: string
  /** 原始语言标记字符串，如 "(EN)"、"（ES）"、"[PT]"，未识别时为 undefined */
  languageTagRaw?: string
}

export interface ParsedMiniProgramCampaign {
  /** 原始活动名称 */
  campaignNameRaw: string
  /** 解析状态 */
  parseStatus: 'ok' | 'partial' | 'failed'
  /** 解析失败或异常时的错误描述 */
  parseError?: string

  // ── 定位字段（1~9 字段）──
  dramaId?: string
  dramaType?: string
  country?: string
  sourceType?: string
  mediaChannel?: string
  buyerName?: string
  optimizationType?: string
  bidType?: string
  publishDate?: string

  // ── 剧名字段（第 10 字段）──
  dramaNameRaw?: string
  localizedDramaName?: string
  languageCode?: string
  languageTagRaw?: string

  // ── 备注（第 11 字段及以后，仅原样保留）──
  remarkRaw?: string
}

export interface ParsedAppCampaign {
  /** 原始活动名称 */
  campaignNameRaw: string
  /** 解析状态 */
  parseStatus: 'ok' | 'partial' | 'failed'
  /** 解析失败或异常时的错误描述 */
  parseError?: string

  // ── 定位字段（1~9 字段）──
  field1?: string
  field2?: string
  field3?: string
  field4?: string
  field5?: string
  field6?: string
  field7?: string
  field8?: string
  field9?: string

  // ── 剧名字段（第 10 字段）──
  dramaNameRaw?: string
  localizedDramaName?: string
  languageCode?: string
  languageTagRaw?: string

  // ── 第 11 字段：投手简称 ──
  buyerShortName?: string

  // ── 备注（第 12 字段及以后，仅原样保留）──
  remarkRaw?: string
}

// ─────────────────────────────────────────────────────────────
// 语言尾缀映射表
// ─────────────────────────────────────────────────────────────

/**
 * 支持的语言标记（大写 key -> 标准 language_code）
 * 必须在此列表内才会被识别，其他任何括号内容均忽略。
 */
const LANG_CODE_MAP: Record<string, string> = {
  EN: 'en',
  ES: 'es',
  PT: 'pt',
  FR: 'fr',
  DE: 'de',
  ID: 'id',
  TH: 'th',
  JP: 'ja',
  JA: 'ja',
  KR: 'ko',
  KO: 'ko',
  AR: 'ar',
}

/**
 * 匹配剧名结尾的语言尾缀。
 *
 * 支持三种括号：半角 ()、全角 （）、方括号 []
 * 仅匹配已知语言代码（2~3 字母），大小写不敏感。
 * 使用 $ 锚定结尾，确保只匹配剧名末尾的标记。
 *
 * 捕获组 1：括号内的内容（不含括号本身），用于 code 查找
 * 完整匹配：作为 languageTagRaw 返回
 */
const LANG_SUFFIX_RE = /[\s]*(?:\(|（|\[)([A-Za-z]{2,3})(?:\)|）|\])$/

// ─────────────────────────────────────────────────────────────
// 核心函数
// ─────────────────────────────────────────────────────────────

/**
 * 从剧名原始字符串解析语言尾缀。
 *
 * @param raw 第 10 字段的原始内容（仅此字段，不含备注）
 */
export function parseDramaName(raw: string): ParsedDramaName {
  const dramaNameRaw = raw

  if (!raw || raw.trim() === '') {
    return {
      dramaNameRaw,
      localizedDramaName: '',
      languageCode: 'unknown',
    }
  }

  const match = raw.match(LANG_SUFFIX_RE)

  if (match) {
    const tagContent = match[1].toUpperCase()
    const code = LANG_CODE_MAP[tagContent]

    if (code) {
      // 去掉完整匹配（含前导空格）后 trim
      const localizedDramaName = raw.slice(0, match.index).trim()
      return {
        dramaNameRaw,
        localizedDramaName,
        languageCode: code,
        languageTagRaw: match[0].trim(),
      }
    }
  }

  // 未识别到合法语言尾缀
  return {
    dramaNameRaw,
    localizedDramaName: raw.trim(),
    languageCode: 'unknown',
  }
}

// ─────────────────────────────────────────────────────────────
// 小程序活动名称解析器
// ─────────────────────────────────────────────────────────────

/**
 * 解析小程序推广活动名称。
 *
 * 固定字段（按 '-' 分隔，1-based）：
 *   1  drama_id
 *   2  drama_type
 *   3  country
 *   4  source_type
 *   5  media_channel
 *   6  buyer_name
 *   7  optimization_type
 *   8  bid_type
 *   9  publish_date
 *   10 drama_name_raw        ← 唯一合法剧名来源
 *   11+ remark_raw（可多段，重新用 '-' 拼接）
 *
 * 少于 10 段 → parseStatus='failed'
 */
export function parseMiniProgramCampaignName(name: string): ParsedMiniProgramCampaign {
  const campaignNameRaw = name

  try {
    const parts = name.split('-')

    if (parts.length < 10) {
      return {
        campaignNameRaw,
        parseStatus: 'failed',
        parseError: `字段数不足，期望 ≥10 段，实际 ${parts.length} 段`,
      }
    }

    // 第 10 字段（index 9）是唯一合法剧名来源
    const dramaField = parts[9]
    const parsed = parseDramaName(dramaField)

    // 第 11 字段及以后 → remarkRaw（原样拼接，不做任何解析）
    const remarkRaw = parts.length > 10 ? parts.slice(10).join('-') : undefined

    return {
      campaignNameRaw,
      parseStatus: 'ok',

      dramaId: parts[0].trim(),
      dramaType: parts[1].trim(),
      country: parts[2].trim(),
      sourceType: parts[3].trim(),
      mediaChannel: parts[4].trim(),
      buyerName: parts[5].trim(),
      optimizationType: parts[6].trim(),
      bidType: parts[7].trim(),
      publishDate: parts[8].trim(),

      dramaNameRaw: parsed.dramaNameRaw,
      localizedDramaName: parsed.localizedDramaName,
      languageCode: parsed.languageCode,
      languageTagRaw: parsed.languageTagRaw,

      remarkRaw,
    }
  } catch (e) {
    return {
      campaignNameRaw,
      parseStatus: 'failed',
      parseError: e instanceof Error ? e.message : String(e),
    }
  }
}

// ─────────────────────────────────────────────────────────────
// APP 活动名称解析器
// ─────────────────────────────────────────────────────────────

/**
 * 解析 APP 推广活动名称。
 *
 * 当前字段约定（按 '-' 分隔，1-based）：
 *   1~9  定位字段（结构可能变化，预留为 field1~field9）
 *   10   drama_name_raw        ← 唯一合法剧名来源
 *   11   buyer_short_name（若存在）
 *   12+  remark_raw（可多段，重新用 '-' 拼接）
 *
 * 少于 10 段 → parseStatus='failed'
 * 解析逻辑集中封装，便于后续调整字段位置。
 */
export function parseAppCampaignName(name: string): ParsedAppCampaign {
  const campaignNameRaw = name

  try {
    const parts = name.split('-')

    if (parts.length < 10) {
      return {
        campaignNameRaw,
        parseStatus: 'failed',
        parseError: `字段数不足，期望 ≥10 段，实际 ${parts.length} 段`,
      }
    }

    // 第 10 字段（index 9）是唯一合法剧名来源
    const dramaField = parts[9]
    const parsed = parseDramaName(dramaField)

    // 第 11 字段：buyerShortName（若存在）
    const buyerShortName = parts.length > 10 ? parts[10].trim() || undefined : undefined

    // 第 12 字段及以后 → remarkRaw（原样拼接，不做任何解析）
    const remarkRaw = parts.length > 11 ? parts.slice(11).join('-') : undefined

    return {
      campaignNameRaw,
      parseStatus: 'ok',

      field1: parts[0].trim(),
      field2: parts[1].trim(),
      field3: parts[2].trim(),
      field4: parts[3].trim(),
      field5: parts[4].trim(),
      field6: parts[5].trim(),
      field7: parts[6].trim(),
      field8: parts[7].trim(),
      field9: parts[8].trim(),

      dramaNameRaw: parsed.dramaNameRaw,
      localizedDramaName: parsed.localizedDramaName,
      languageCode: parsed.languageCode,
      languageTagRaw: parsed.languageTagRaw,

      buyerShortName,
      remarkRaw,
    }
  } catch (e) {
    return {
      campaignNameRaw,
      parseStatus: 'failed',
      parseError: e instanceof Error ? e.message : String(e),
    }
  }
}

// ─────────────────────────────────────────────────────────────
// content_key 生成
// ─────────────────────────────────────────────────────────────

/**
 * 对 localizedDramaName 做标准化处理，生成 content_key。
 * 去除首尾空格，转小写，压缩空白字符为单个空格。
 * 注意：remark_raw 绝对不能传入此函数。
 */
export function normalizeContentKey(localizedDramaName: string): string {
  return localizedDramaName.trim().toLowerCase().replace(/\s+/g, ' ')
}
