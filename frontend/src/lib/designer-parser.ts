/**
 * 设计师名称解析工具
 *
 * 素材命名规则：各字段以 '-' 分隔，第一个字段为设计师名称。
 * 示例：
 *   "徐晓杰-01-20260413-我穿成炮灰..." → designer_name = "徐晓杰"
 *   "" / null / undefined             → designer_name = "未识别"
 *   "无分隔符素材名"                   → designer_name = "未识别"
 */

export const UNKNOWN_DESIGNER = '未识别'

/**
 * 从素材名称解析设计师名称。
 *
 * @param adName - 素材名称，可能为空/null/undefined
 * @returns 设计师名称；无法解析时返回 "未识别"
 */
export function parseDesignerName(adName: string | null | undefined): string {
  if (!adName || typeof adName !== 'string') return UNKNOWN_DESIGNER

  const trimmed = adName.trim()
  if (!trimmed) return UNKNOWN_DESIGNER
  if (!trimmed.includes('-')) return UNKNOWN_DESIGNER

  const first = trimmed.split('-')[0]?.trim()
  return first || UNKNOWN_DESIGNER
}
