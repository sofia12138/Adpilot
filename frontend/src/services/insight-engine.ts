/**
 * 规则引擎 — 基于数据 + 可配置阈值生成结论
 *
 * 每个 build* 函数返回 Insight[]，由调用方传入页面已有数据 + thresholds。
 * 不额外发请求、不引入 AI。
 */

import type { InsightThresholds, RoiThresholds } from '@/config/insight-thresholds'
import type { BizOverview, BizTopCampaign } from '@/services/biz'

// ─── 公共类型 ────────────────────────────────────────────

export type InsightSeverity = 'danger' | 'warning' | 'success'

export interface Insight {
  type: InsightSeverity
  title: string
  desc: string
}

// ─── 工具 ────────────────────────────────────────────────

function roiLevel(roas: number, roi: RoiThresholds): InsightSeverity {
  if (roas < roi.low) return 'danger'
  if (roas < roi.target) return 'warning'
  return 'success'
}

function pct(n: number): string { return `${(n * 100).toFixed(1)}%` }
function usd(n: number): string { return `$${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}` }

// ─── OverviewPage ────────────────────────────────────────

export interface OverviewInput {
  overview: BizOverview | null
  tiktokOverview: BizOverview | null
  metaOverview: BizOverview | null
}

export function buildOverviewInsights(
  data: OverviewInput,
  thresholds: InsightThresholds,
): Insight[] {
  const insights: Insight[] = []
  const ov = data.overview
  const roi = thresholds.roi
  if (!ov) return [{ type: 'warning', title: '数据缺失', desc: '未获取到总览数据，无法生成结论' }]

  // 1. 整体 ROAS 判断
  if (ov.avg_roas != null && ov.avg_roas > 0) {
    const level = roiLevel(ov.avg_roas, roi)
    if (level === 'danger') {
      insights.push({ type: 'danger', title: `整体 ROAS 偏低 (${ov.avg_roas.toFixed(2)})`, desc: `低于预警线 ${roi.low}，需关注投放效率并考虑优化素材或定向` })
    } else if (level === 'warning') {
      insights.push({ type: 'warning', title: `整体 ROAS 未达标 (${ov.avg_roas.toFixed(2)})`, desc: `介于 ${roi.low}~${roi.target} 之间，建议持续优化以达到目标 ${roi.target}` })
    } else {
      insights.push({ type: 'success', title: `整体 ROAS 表现良好 (${ov.avg_roas.toFixed(2)})`, desc: ov.avg_roas >= roi.high ? `已超越优秀线 ${roi.high}，可考虑适度放量` : `已达标，保持当前投放策略` })
    }
  }

  // 2. 数据异常检测 — spend > 0 但 conversion = 0
  if (ov.total_spend > 0 && ov.total_conversions === 0) {
    insights.push({ type: 'danger', title: '有消耗但零转化', desc: `已消耗 ${usd(ov.total_spend)} 但转化数为 0，需立即排查广告链路` })
  }

  // 3. CTR 检查（近似判断，基于行业经验 1% 基线）
  if (ov.avg_ctr != null) {
    if (ov.avg_ctr < 0.005) {
      insights.push({ type: 'warning', title: `CTR 偏低 (${pct(ov.avg_ctr)})`, desc: '点击率低于 0.5%，素材吸引力不足，建议更换素材或调整受众' })
    } else if (ov.avg_ctr > 0.03) {
      insights.push({ type: 'success', title: `CTR 表现优秀 (${pct(ov.avg_ctr)})`, desc: '点击率超过 3%，素材与受众匹配度高' })
    }
  }

  // 4. 渠道对比（如果同时有两个渠道数据）
  const tt = data.tiktokOverview
  const meta = data.metaOverview
  if (tt && meta && tt.total_spend > 0 && meta.total_spend > 0) {
    const ttRoas = tt.avg_roas ?? 0
    const metaRoas = meta.avg_roas ?? 0
    if (ttRoas > 0 && metaRoas > 0) {
      const better = ttRoas > metaRoas ? 'TikTok' : 'Meta'
      const worse = ttRoas > metaRoas ? 'Meta' : 'TikTok'
      const ratio = Math.max(ttRoas, metaRoas) / Math.min(ttRoas, metaRoas)
      if (ratio > 1.5) {
        insights.push({ type: 'warning', title: `${better} ROAS 明显优于 ${worse}`, desc: `${better} ROAS ${Math.max(ttRoas, metaRoas).toFixed(2)} vs ${worse} ${Math.min(ttRoas, metaRoas).toFixed(2)}，考虑调整预算分配` })
      }
    }
  }

  // 5. 兜底正向结论
  if (insights.length < 3 && ov.total_revenue > 0) {
    insights.push({ type: 'success', title: '投放整体正常运转', desc: `当前周期内产生收入 ${usd(ov.total_revenue)}，系统运行正常` })
  }

  return insights.slice(0, 5)
}

// ─── ChannelAnalysisPage ─────────────────────────────────

export interface ChannelInput {
  totalOverview: BizOverview | null
  tiktokOverview: BizOverview | null
  metaOverview: BizOverview | null
  tiktokTopCampaigns: BizTopCampaign[]
  metaTopCampaigns: BizTopCampaign[]
}

export function buildChannelInsights(
  data: ChannelInput,
  thresholds: InsightThresholds,
): Insight[] {
  const insights: Insight[] = []
  const roi = thresholds.roi
  const tt = data.tiktokOverview
  const meta = data.metaOverview

  if (!tt && !meta) return [{ type: 'warning', title: '渠道数据缺失', desc: '未获取到渠道数据，无法生成对比结论' }]

  const totalSpend = (tt?.total_spend ?? 0) + (meta?.total_spend ?? 0)
  const totalRev = (tt?.total_revenue ?? 0) + (meta?.total_revenue ?? 0)

  // 1. 各渠道 ROAS
  for (const ch of [{ name: 'TikTok', ov: tt }, { name: 'Meta', ov: meta }]) {
    if (!ch.ov || ch.ov.total_spend === 0) continue
    const roas = ch.ov.avg_roas ?? 0
    if (roas > 0 && roas < roi.low) {
      insights.push({ type: 'danger', title: `${ch.name} ROAS 低于预警线 (${roas.toFixed(2)})`, desc: `低于 ${roi.low}，该渠道投放效率需重点关注` })
    } else if (roas >= roi.high) {
      insights.push({ type: 'success', title: `${ch.name} ROAS 优秀 (${roas.toFixed(2)})`, desc: `超过 ${roi.high}，可考虑在该渠道加大投放` })
    }
  }

  // 2. 消耗占比 vs 收入占比不匹配
  if (tt && meta && totalSpend > 0 && totalRev > 0) {
    const ttSpendShare = tt.total_spend / totalSpend
    const ttRevShare = totalRev > 0 ? tt.total_revenue / totalRev : 0
    const gap = ttSpendShare - ttRevShare
    if (Math.abs(gap) > 0.15) {
      const overSpender = gap > 0 ? 'TikTok' : 'Meta'
      insights.push({ type: 'warning', title: `${overSpender} 消耗占比高于收入占比`, desc: `消耗与收入占比差距超过 15%，预算分配可能不够高效` })
    }
  }

  // 3. 某渠道投放过于集中
  if (totalSpend > 0) {
    const ttShare = (tt?.total_spend ?? 0) / totalSpend
    if (ttShare > 0.85 || ttShare < 0.15) {
      const dominant = ttShare > 0.85 ? 'TikTok' : 'Meta'
      insights.push({ type: 'warning', title: `投放过于集中在 ${dominant}`, desc: `${dominant} 占消耗 ${(Math.max(ttShare, 1 - ttShare) * 100).toFixed(0)}%，建议适当分散以降低风险` })
    }
  }

  // 4. Top Campaign 中低 ROI 占比
  const allTop = [...data.tiktokTopCampaigns, ...data.metaTopCampaigns]
  const lowRoiCampaigns = allTop.filter(c => c.avg_roas != null && c.avg_roas > 0 && c.avg_roas < roi.low)
  if (lowRoiCampaigns.length > 0 && allTop.length > 0) {
    insights.push({ type: 'warning', title: `${lowRoiCampaigns.length} 个 Top Campaign ROAS 低于 ${roi.low}`, desc: '头部 Campaign 中存在低效投放，建议逐个优化或暂停' })
  }

  // 5. 兜底
  if (insights.length < 2) {
    insights.push({ type: 'success', title: '渠道整体表现正常', desc: '两个渠道指标无明显异常，保持当前投放策略' })
  }

  return insights.slice(0, 5)
}

// ─── BizAnalysisPage ─────────────────────────────────────

export interface BizInput {
  overview: BizOverview | null
  topCampaigns: BizTopCampaign[]
}

export function buildBizInsights(
  data: BizInput,
  thresholds: InsightThresholds,
): Insight[] {
  const insights: Insight[] = []
  const roi = thresholds.roi
  const ov = data.overview
  const tops = data.topCampaigns

  if (!ov) return [{ type: 'warning', title: '业务数据缺失', desc: '未获取到业务数据，无法生成结论' }]

  // 1. 整体 ROAS
  if (ov.avg_roas != null && ov.avg_roas > 0) {
    const level = roiLevel(ov.avg_roas, roi)
    if (level === 'danger') {
      insights.push({ type: 'danger', title: `业务 ROAS 不达标 (${ov.avg_roas.toFixed(2)})`, desc: `低于 ${roi.low}，整体业务盈利能力堪忧` })
    } else if (level === 'success') {
      insights.push({ type: 'success', title: `业务 ROAS 健康 (${ov.avg_roas.toFixed(2)})`, desc: `达到或超过目标 ${roi.target}，业务处于良好状态` })
    }
  }

  // 2. 头部集中度 — Top1 消耗占比
  if (tops.length >= 2) {
    const totalSpend = tops.reduce((s, c) => s + c.total_spend, 0)
    if (totalSpend > 0) {
      const top1Share = tops[0].total_spend / totalSpend
      if (top1Share > 0.5) {
        insights.push({ type: 'warning', title: `Top1 Campaign 消耗占比 ${(top1Share * 100).toFixed(0)}%`, desc: `"${tops[0].campaign_name || tops[0].campaign_id}" 占据过半消耗，存在单点风险` })
      }
    }
  }

  // 3. 放量机会 — 高 ROAS 的 Campaign
  const highRoasCampaigns = tops.filter(c => c.avg_roas != null && c.avg_roas >= roi.high && c.total_spend > 50)
  if (highRoasCampaigns.length > 0) {
    const names = highRoasCampaigns.slice(0, 2).map(c => c.campaign_name || c.campaign_id).join('、')
    insights.push({ type: 'success', title: `${highRoasCampaigns.length} 个 Campaign 具备放量机会`, desc: `${names} ROAS 超过 ${roi.high}，量级稳定，可考虑增加预算` })
  }

  // 4. 低效 Campaign 识别
  const lowRoaCampaigns = tops.filter(c => c.avg_roas != null && c.avg_roas > 0 && c.avg_roas < roi.low && c.total_spend > 30)
  if (lowRoaCampaigns.length > 0) {
    insights.push({ type: 'danger', title: `${lowRoaCampaigns.length} 个 Campaign ROAS 低于 ${roi.low}`, desc: '建议暂停或优化这些低效 Campaign 以减少浪费' })
  }

  // 5. 兜底
  if (insights.length < 2) {
    insights.push({ type: 'success', title: '业务指标整体正常', desc: `当前周期消耗 ${usd(ov.total_spend)}，收入 ${usd(ov.total_revenue)}，无明显异常` })
  }

  return insights.slice(0, 5)
}
