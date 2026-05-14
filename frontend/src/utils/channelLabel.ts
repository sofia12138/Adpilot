/**
 * channel_id → 可读名称工具
 *
 * 规则：
 * 1. 优先使用后端字典里的 label（含 ad_platform 和 advertiser_id）
 * 2. 字典里查不到时按本地兜底规则：
 *    - '0' / ''  → "自然量"
 *    - 其它      → "渠道-{channel_id}"
 */
import type { ChannelInfo } from '@/types/userPayment'

export function channelLabel(
  channel_id: string | null | undefined,
  dict?: Record<string, ChannelInfo> | null,
): string {
  const cid = (channel_id ?? '').trim()
  if (dict && dict[cid]?.label) return dict[cid].label
  if (cid === '' || cid === '0') return '自然量'
  return '渠道'
}

/**
 * 鼠标悬停时显示的详细信息（tooltip）
 */
export function channelTooltip(
  channel_id: string | null | undefined,
  dict?: Record<string, ChannelInfo> | null,
): string {
  const cid = (channel_id ?? '').trim() || '(空)'
  const info = dict?.[cid]
  const parts = [`channel_id = ${cid}`]
  if (info) {
    if (info.ad_platform != null) {
      const platformName =
        info.ad_platform === 0 ? '其它'
        : info.ad_platform === 1 ? 'TikTok'
        : info.ad_platform === 2 ? 'Meta'
        : `未知(${info.ad_platform})`
      parts.push(`ad_platform = ${info.ad_platform} (${platformName})`)
    }
    if (info.advertiser_id) parts.push(`advertiser_id = ${info.advertiser_id}`)
  }
  return parts.join('\n')
}
