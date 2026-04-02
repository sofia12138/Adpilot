import { apiFetch } from './api'
import type { Template } from './templates'

const TIKTOK_LOCATION_IDS: Record<string, string> = {
  US: '6252001', JP: '1861060', KR: '1835841', TW: '1668284',
  TH: '1605651', ID: '1643084', VN: '1562822', BR: '3469034',
  MX: '3996063', DE: '2921044',
}

// ─── Types ───────────────────────────────────────────────

export interface CreateAdsParams {
  mode: 'blank' | 'template'
  platform: 'tiktok' | 'meta'
  campaignName: string
  country: string
  budget: number
  templateId?: string
  template?: Template | null
}

export interface CreateResult {
  success: boolean
  message: string
  details?: Record<string, unknown>
}

// ─── 统一入口 ────────────────────────────────────────────

export async function createAds(params: CreateAdsParams): Promise<CreateResult> {
  if (params.mode === 'template' && params.templateId) {
    return launchFromTemplate(params)
  }
  return params.platform === 'tiktok' ? createTikTok(params) : createMeta(params)
}

// ─── 模板投放：POST /api/templates/launch ────────────────

async function launchFromTemplate(p: CreateAdsParams): Promise<CreateResult> {
  try {
    const locId = TIKTOK_LOCATION_IDS[p.country]

    const res = await apiFetch<{ data: Record<string, unknown> }>('/api/templates/launch', {
      method: 'POST',
      body: JSON.stringify({
        template_id: p.templateId,
        advertiser_id: '',
        campaign_name: p.campaignName,
        budget: p.budget || 50,
        location_ids: locId ? [locId] : [],
      }),
    })

    const d = res.data ?? {}
    const campOk = (d.campaign as Record<string, unknown>)?.success
    if (!campOk) {
      const err = (d.campaign as Record<string, unknown>)?.error ?? '未知错误'
      return { success: false, message: `模板投放失败: ${err}`, details: d }
    }
    return { success: true, message: '模板投放创建成功', details: d }
  } catch (e) {
    return { success: false, message: `模板投放失败: ${(e as Error).message}` }
  }
}

// ─── TikTok 空白创建：Campaign → AdGroup → Ad ───────────

async function createTikTok(p: CreateAdsParams): Promise<CreateResult> {
  const steps: Record<string, unknown> = {}
  try {
    const camp = await apiFetch<{ data: Record<string, unknown> }>('/api/campaigns/', {
      method: 'POST',
      body: JSON.stringify({
        campaign_name: p.campaignName,
        objective_type: 'APP_PROMOTION',
        budget_mode: p.budget ? 'BUDGET_MODE_DAY' : 'BUDGET_MODE_INFINITE',
        budget: p.budget || undefined,
      }),
    })
    steps.campaign = camp
    const cid = (camp.data as Record<string, unknown>)?.campaign_id
      ?? ((camp.data as Record<string, unknown>)?.campaign_ids as string[] | undefined)?.[0]
    if (!cid) {
      return { success: false, message: 'Campaign 已创建但未返回 ID，请在 TikTok 后台确认', details: steps }
    }

    const locId = TIKTOK_LOCATION_IDS[p.country]
    const adg = await apiFetch<{ data: Record<string, unknown> }>('/api/adgroups/', {
      method: 'POST',
      body: JSON.stringify({
        campaign_id: cid,
        adgroup_name: `${p.campaignName}_adgroup`,
        budget: p.budget || 50,
        location_ids: locId ? [locId] : [],
      }),
    })
    steps.adgroup = adg
    const agid = (adg.data as Record<string, unknown>)?.adgroup_id
      ?? ((adg.data as Record<string, unknown>)?.adgroup_ids as string[] | undefined)?.[0]
    if (!agid) {
      return { success: true, message: 'Campaign + AdGroup 创建成功（Ad 跳过：未返回 AdGroup ID）', details: steps }
    }

    // TODO: 接入素材选择后，传入 video_id / image_ids
    const ad = await apiFetch<{ data: Record<string, unknown> }>('/api/ads/', {
      method: 'POST',
      body: JSON.stringify({
        adgroup_id: agid,
        ad_name: `${p.campaignName}_ad`,
      }),
    })
    steps.ad = ad

    return { success: true, message: 'TikTok 广告创建成功（Campaign → AdGroup → Ad）', details: steps }
  } catch (e) {
    return { success: false, message: `TikTok 创建失败: ${(e as Error).message}`, details: steps }
  }
}

// ─── Meta 空白创建：Campaign → AdSet ─────────────────────

async function createMeta(p: CreateAdsParams): Promise<CreateResult> {
  const steps: Record<string, unknown> = {}
  try {
    const camp = await apiFetch<{ data: Record<string, unknown> }>('/api/meta/campaigns/', {
      method: 'POST',
      body: JSON.stringify({
        name: p.campaignName,
        objective: 'OUTCOME_APP_PROMOTION',
        status: 'PAUSED',
        special_ad_categories: [],
        daily_budget: Math.round((p.budget || 50) * 100),
      }),
    })
    steps.campaign = camp
    const cid = (camp.data as Record<string, unknown>)?.id
    if (!cid) {
      return { success: true, message: 'Meta Campaign 创建请求已发送（未返回 ID，跳过后续步骤）', details: steps }
    }

    // TODO: 完善 targeting、optimization_goal 等配置
    const adset = await apiFetch<{ data: Record<string, unknown> }>('/api/meta/adsets/', {
      method: 'POST',
      body: JSON.stringify({
        name: `${p.campaignName}_adset`,
        campaign_id: cid,
        status: 'PAUSED',
        daily_budget: Math.round((p.budget || 50) * 100),
        billing_event: 'IMPRESSIONS',
        optimization_goal: 'APP_INSTALLS',
        targeting: { geo_locations: { countries: [p.country] } },
      }),
    })
    steps.adset = adset

    return { success: true, message: 'Meta Campaign + AdSet 创建成功（Ad 需绑定素材，暂跳过）', details: steps }
  } catch (e) {
    return { success: false, message: `Meta 创建失败: ${(e as Error).message}`, details: steps }
  }
}
