/**
 * 优化师名称解析工具
 *
 * 解析规则：
 *   小程序投放：campaign_name 按 '-' 切分，第 6 位（index 5）为 optimizer
 *   APP 投放：  campaign_name 按 '-' 切分，第 11 位（index 10）为 optimizer
 *
 * 标准化规则：trim → 压缩多空格 → 大写。空值返回 "未识别"。
 */

export const UNKNOWN_OPTIMIZER = '未识别'

export function normalizeOptimizerName(name: string | null | undefined): string {
  if (!name || typeof name !== 'string') return UNKNOWN_OPTIMIZER
  const s = name.trim()
  if (!s) return UNKNOWN_OPTIMIZER
  return s.replace(/\s+/g, ' ').toUpperCase()
}

export function parseMiniProgramOptimizerName(campaignName: string): string {
  if (!campaignName) return UNKNOWN_OPTIMIZER
  const parts = campaignName.split('-')
  if (parts.length < 6) return UNKNOWN_OPTIMIZER
  return normalizeOptimizerName(parts[5])
}

export function parseAppOptimizerName(campaignName: string): string {
  if (!campaignName) return UNKNOWN_OPTIMIZER
  const parts = campaignName.split('-')
  if (parts.length < 11) return UNKNOWN_OPTIMIZER
  return normalizeOptimizerName(parts[10])
}
