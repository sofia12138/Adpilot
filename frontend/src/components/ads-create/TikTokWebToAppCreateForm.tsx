/**
 * TikTok Web to App 新建广告子表单（嵌入到统一新建广告页中使用）
 *
 * 与 TikTokMinisCreateForm 保持相同的结构和交互风格；区别在于：
 *  - 不需要 App ID / Minis ID
 *  - 必填 Landing Page URL
 *  - 支持 Ad Title / Call to Action / Tracking URL
 *  - 使用普通 budget（非 ROAS）
 *
 * 模板的选择 (selectedTpl) 由父组件维护并通过 props 注入。
 */
import { useState, useEffect, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2, AlertCircle, CheckCircle, Send, Shield } from 'lucide-react'
import { SectionCard } from '@/components/common/SectionCard'
import type { Template } from '@/services/templates'
import { fetchTikTokAdvertisers, type Advertiser } from '@/services/advertisers'
import { fetchMaterialList, type TikTokMaterialRecord } from '@/services/tiktok-materials'
import { apiFetch } from '@/services/api'
import { TikTokLocationPicker, resolveCountryCodesFromTemplate } from '@/components/common/TikTokLocationPicker'
import { TikTokIdentityPicker } from '@/components/common/TikTokIdentityPicker'
import { TikTokPixelEventPicker } from '@/components/common/TikTokPixelEventPicker'
import { codesToLocationIds, type LocationSelection } from '@/constants/tiktok-locations'

const inputCls = 'w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-pink-500/20 focus:border-pink-400 transition'

export function isTikTokWebToAppTpl(t: Template | null | undefined): boolean {
  if (!t) return false
  return t.platform === 'tiktok' && t.template_type === 'tiktok_web_to_app'
}

const CTA_OPTIONS = [
  'LEARN_MORE', 'DOWNLOAD_NOW', 'SHOP_NOW', 'SIGN_UP', 'INSTALL_NOW',
  'GET_QUOTE', 'APPLY_NOW', 'BOOK_NOW', 'VIEW_NOW', 'ORDER_NOW',
]

interface LaunchResult {
  data?: {
    platform?: string
    template_type?: string
    campaign?: { success: boolean; campaign_id?: string; error?: string; skipped?: boolean; reason?: string; hint?: string }
    adgroup?: { success: boolean; adgroup_id?: string; error?: string; skipped?: boolean; reason?: string; hint?: string }
    ad?: { success: boolean; ad_id?: string; ad_name?: string; error?: string; skipped?: boolean; reason?: string; hint?: string }
    summary?: { total: number; success: number; fail: number }
  }
  error?: string
}

interface Props {
  /** 当前选中的 W2A 模板（必须是 tiktok_web_to_app） */
  tpl: Template
}

type StepResult = {
  success: boolean
  error?: string
  skipped?: boolean
  reason?: string
  hint?: string
} & Record<string, unknown>

function renderStepResult(step: StepResult | undefined, idKey: string): string {
  if (!step) return '未执行'
  if (step.success) return `成功 → ${(step[idKey] as string) ?? ''}`
  if (step.skipped) return step.reason || '未执行（上一步失败）'
  if (step.hint) return `失败 → ${step.hint}（原始: ${step.error ?? ''}）`
  return `失败 → ${step.error ?? '未知错误'}`
}

export default function TikTokWebToAppCreateForm({ tpl }: Props) {
  const { data: advResp } = useQuery({ queryKey: ['tiktok-advertisers'], queryFn: fetchTikTokAdvertisers })
  const advertisers: Advertiser[] = advResp?.data ?? []

  const [advertiserId, setAdvertiserId] = useState('')
  const [campaignName, setCampaignName] = useState('')
  const [adgroupName, setAdgroupName] = useState('')
  const [adName, setAdName] = useState('')
  const [budget, setBudget] = useState('50')
  const [bidPrice, setBidPrice] = useState('')
  const [scheduleStartTime, setScheduleStartTime] = useState('')
  const [scheduleEndTime, setScheduleEndTime] = useState('')
  const [identityId, setIdentityId] = useState('')
  const [identityType, setIdentityType] = useState('CUSTOMIZED_USER')
  const [countryCodes, setCountryCodes] = useState<string[]>([])
  const [landingPageUrl, setLandingPageUrl] = useState('')
  const [trackingUrl, setTrackingUrl] = useState('')
  const [pixelId, setPixelId] = useState('')
  const [optimizationEvent, setOptimizationEvent] = useState('SHOPPING')
  const [adText, setAdText] = useState('')
  const [adTitle, setAdTitle] = useState('')
  const [callToAction, setCallToAction] = useState('LEARN_MORE')
  const [videoMaterialId, setVideoMaterialId] = useState<number | null>(null)

  // 切换模板时回填默认值
  useEffect(() => {
    if (!tpl) return
    const defaults = (tpl.defaults as Record<string, unknown>) ?? {}
    const adgroup = (tpl.adgroup as Record<string, unknown>) ?? {}
    const ad = (tpl.ad as Record<string, unknown>) ?? {}

    const targeting = (defaults.targeting as Record<string, unknown>) ?? {}
    const selection = (targeting.location_selection as LocationSelection | undefined) ?? null
    const fallbackIds = Array.isArray(targeting.location_ids)
      ? (targeting.location_ids as unknown[]).map(String)
      : []
    setCountryCodes(resolveCountryCodesFromTemplate(selection, fallbackIds))

    const identity = (defaults.identity as Record<string, unknown>) ?? {}
    const tplIdentityId = (identity.identity_id as string | undefined) || ''
    const tplIdentityType = (identity.identity_type as string | undefined) || ''
    setIdentityId(tplIdentityId)
    if (tplIdentityType) setIdentityType(tplIdentityType)

    const tracking = (defaults.tracking as Record<string, unknown>) ?? {}
    if (tracking.pixel_id) setPixelId(String(tracking.pixel_id))
    if (tracking.optimization_event) setOptimizationEvent(String(tracking.optimization_event))

    const tplCta = (defaults.call_to_action as string | undefined) || (ad.call_to_action as string | undefined) || ''
    if (tplCta) setCallToAction(tplCta)

    if (defaults.landing_page_template) setLandingPageUrl(String(defaults.landing_page_template))
    if (adgroup.default_budget) setBudget(String(adgroup.default_budget))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tpl?.id])

  // ── 拉取所选广告主下的 TikTok 视频素材 ──
  const { data: matsResp } = useQuery({
    queryKey: ['tiktok-materials', advertiserId, 'success'],
    queryFn: () => fetchMaterialList({ advertiser_id: advertiserId, status: 'success', page_size: 50 }),
    enabled: !!advertiserId,
  })
  const materials: TikTokMaterialRecord[] = matsResp?.data?.items ?? []

  const [submitting, setSubmitting] = useState(false)
  const [result, setResult] = useState<LaunchResult | null>(null)
  const [errorMsg, setErrorMsg] = useState('')

  function validate(): string | null {
    if (!advertiserId) return '请选择广告主'
    if (!campaignName.trim()) return 'Campaign Name 不能为空'
    if (!identityId.trim()) return '请选择 Identity（TikTok 广告必填）'
    if (countryCodes.length === 0) return '请至少选择 1 个投放国家'
    if (!landingPageUrl.trim()) return 'Landing Page URL 不能为空（Web to App 必填）'
    if (!videoMaterialId) return '请选择视频素材'
    const mat = materials.find(m => m.id === videoMaterialId)
    if (!mat?.tiktok_video_id) return '所选素材没有 tiktok_video_id（请确认素材已上传成功）'
    const b = Number(budget)
    if (!Number.isFinite(b) || b <= 0) return '日预算必须为正数'
    return null
  }

  async function handleSubmit() {
    setErrorMsg(''); setResult(null)
    const err = validate()
    if (err) { setErrorMsg(err); return }
    const mat = materials.find(m => m.id === videoMaterialId)!

    const payload: Record<string, unknown> = {
      template_id: tpl.id,
      advertiser_id: advertiserId,
      campaign_name: campaignName.trim(),
      adgroup_name: adgroupName.trim() || campaignName.trim(),
      ad_name: adName.trim() || adgroupName.trim() || campaignName.trim(),
      budget: Number(budget),
      identity_id: identityId.trim(),
      identity_type: identityType,
      video_id: mat.tiktok_video_id,
      ad_text: adText,
      ad_title: adTitle.trim(),
      call_to_action: callToAction,
      landing_page_url: landingPageUrl.trim(),
      location_ids: codesToLocationIds(countryCodes),
    }
    if (bidPrice) payload.bid = Number(bidPrice)
    if (trackingUrl.trim()) payload.tracking_url = trackingUrl.trim()
    if (pixelId.trim()) payload.pixel_id = pixelId.trim()
    if (optimizationEvent) payload.optimization_event = optimizationEvent
    if (scheduleStartTime) payload.schedule_start_time = scheduleStartTime
    if (scheduleEndTime) payload.schedule_end_time = scheduleEndTime

    setSubmitting(true)
    try {
      const res = await apiFetch<LaunchResult>('/api/templates/launch', {
        method: 'POST',
        body: JSON.stringify(payload),
      })
      setResult(res)
      if (res.error) setErrorMsg(res.error)
    } catch (e) {
      setErrorMsg(`投放失败: ${(e as Error).message}`)
    } finally {
      setSubmitting(false)
    }
  }

  const isSystemTpl = Boolean(tpl.is_system)
  const summary = result?.data?.summary
  const created = result?.data
  const overallSuccess = !!created && created.campaign?.success && created.adgroup?.success && created.ad?.success

  return (
    <>
      {isSystemTpl && (
        <div className="flex items-start gap-2 p-3 mb-5 bg-amber-50 border border-amber-200 rounded-xl">
          <Shield className="w-4 h-4 text-amber-500 shrink-0 mt-0.5" />
          <p className="text-xs text-amber-700">
            当前选择的是 TikTok Web to App 系统母版（只读）。母版提供策略默认值，只会根据下方白名单字段生成最终投放参数；如需保存自定义配置，请先在「模板管理」页另存为业务模板再使用。
          </p>
        </div>
      )}

      {/* 账户与命名 */}
      <SectionCard title="账户与命名" className="mb-5">
        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">广告主 <span className="text-red-400">*</span></label>
            <select value={advertiserId} onChange={e => setAdvertiserId(e.target.value)} className={`${inputCls} bg-white`}>
              <option value="">请选择广告主</option>
              {advertisers.map(a => (
                <option key={a.advertiser_id} value={a.advertiser_id}>{a.advertiser_name} ({a.advertiser_id})</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Campaign Name <span className="text-red-400">*</span></label>
            <input value={campaignName} onChange={e => setCampaignName(e.target.value)} className={inputCls} placeholder="例：102-W2A-US-LandingXX-20260422-01" />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">AdGroup Name</label>
              <input value={adgroupName} onChange={e => setAdgroupName(e.target.value)} className={inputCls} placeholder="留空则使用 Campaign Name" />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Ad Name</label>
              <input value={adName} onChange={e => setAdName(e.target.value)} className={inputCls} placeholder="留空则使用 AdGroup Name" />
            </div>
          </div>
        </div>
      </SectionCard>

      {/* 预算与排期 */}
      <SectionCard title="预算与排期" className="mb-5">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">日预算 (USD) <span className="text-red-400">*</span></label>
            <input type="number" value={budget} onChange={e => setBudget(e.target.value)} className={inputCls} min="1" step="1" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">出价 (Bid, 可选)</label>
            <input type="number" value={bidPrice} onChange={e => setBidPrice(e.target.value)} className={inputCls} min="0.01" step="0.01" placeholder="留空则使用 NO_BID" />
            <p className="text-xs text-gray-400 mt-1">填入数值会自动切换到 BID_TYPE_CUSTOM</p>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">投放开始时间</label>
            <input type="datetime-local" value={scheduleStartTime} onChange={e => setScheduleStartTime(e.target.value.replace('T', ' '))} className={inputCls} />
            <p className="text-xs text-gray-400 mt-1">留空则使用 SCHEDULE_FROM_NOW</p>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">投放结束时间</label>
            <input type="datetime-local" value={scheduleEndTime} onChange={e => setScheduleEndTime(e.target.value.replace('T', ' '))} className={inputCls} />
          </div>
        </div>
      </SectionCard>

      {/* 定向与落地 */}
      <SectionCard title="定向与落地" className="mb-5">
        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">投放地区 <span className="text-red-400">*</span></label>
            <TikTokLocationPicker
              value={countryCodes}
              onChange={({ country_codes }) => setCountryCodes(country_codes)}
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Landing Page URL <span className="text-red-400">*</span></label>
            <input value={landingPageUrl} onChange={e => setLandingPageUrl(e.target.value)} className={inputCls} placeholder="https://your.landing.page/xxx" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Tracking URL（可选）</label>
            <input value={trackingUrl} onChange={e => setTrackingUrl(e.target.value)} className={inputCls} placeholder="第三方监测链接" />
          </div>
          <TikTokPixelEventPicker
            advertiserId={advertiserId}
            pixelId={pixelId}
            optimizationEvent={optimizationEvent}
            onChange={({ pixel_id, optimization_event }) => {
              setPixelId(pixel_id)
              setOptimizationEvent(optimization_event)
            }}
          />
        </div>
      </SectionCard>

      {/* 创意（Identity + 视频 + 文案 + CTA） */}
      <SectionCard title="创意" className="mb-5">
        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Identity（身份） <span className="text-red-400">*</span></label>
            <TikTokIdentityPicker
              advertiserId={advertiserId}
              value={identityId}
              onChange={({ identity_id, identity_type }) => {
                setIdentityId(identity_id)
                if (identity_type) setIdentityType(identity_type)
              }}
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">视频素材 <span className="text-red-400">*</span></label>
            {!advertiserId ? (
              <p className="text-xs text-gray-400">请先选择广告主</p>
            ) : materials.length === 0 ? (
              <p className="text-xs text-gray-400">该广告主下暂无成功上传的视频素材，请先在「TikTok 素材上传」页上传</p>
            ) : (
              <select value={videoMaterialId ?? ''} onChange={e => setVideoMaterialId(e.target.value ? Number(e.target.value) : null)} className={`${inputCls} bg-white`}>
                <option value="">请选择视频</option>
                {materials.filter(m => m.tiktok_video_id).map(m => (
                  <option key={m.id} value={m.id}>{m.local_file_name} · {m.tiktok_video_id}</option>
                ))}
              </select>
            )}
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Ad Title (Display Name)</label>
              <input value={adTitle} onChange={e => setAdTitle(e.target.value)} className={inputCls} placeholder="广告标题" />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Call to Action</label>
              <select value={callToAction} onChange={e => setCallToAction(e.target.value)} className={`${inputCls} bg-white`}>
                {CTA_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">广告文案 (Ad Text)</label>
            <textarea value={adText} onChange={e => setAdText(e.target.value)} className={inputCls} rows={3} placeholder="广告描述文案" />
          </div>
        </div>
      </SectionCard>

      {/* 提交 */}
      <div className="flex items-center gap-3 mb-6">
        <button
          type="button"
          onClick={handleSubmit}
          disabled={submitting}
          className="px-5 py-2.5 bg-pink-500 text-white text-sm rounded-xl hover:bg-pink-600 disabled:opacity-50 transition font-medium flex items-center gap-1.5"
        >
          {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
          {submitting ? '创建中...' : '创建广告'}
        </button>
      </div>

      {errorMsg && (
        <div className="flex items-start gap-2 p-3 mb-4 bg-red-50 border border-red-200 rounded-xl text-sm text-red-600">
          <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
          <span>{errorMsg}</span>
        </div>
      )}

      {result?.data && (
        <SectionCard title={overallSuccess ? '创建成功' : '创建结果（含失败）'} className="mb-5">
          {overallSuccess && (
            <div className="flex items-center gap-2 p-3 mb-3 bg-green-50 border border-green-200 rounded-xl text-sm text-green-700">
              <CheckCircle className="w-4 h-4" />
              <span>已成功创建 1 个 Campaign / 1 个 AdGroup / 1 个 Ad</span>
            </div>
          )}
          <div className="text-xs space-y-1 font-mono text-gray-600">
            <div>Campaign: {renderStepResult(created?.campaign, 'campaign_id')}</div>
            <div>AdGroup:  {renderStepResult(created?.adgroup, 'adgroup_id')}</div>
            <div>Ad:       {renderStepResult(created?.ad, 'ad_id')}</div>
            {summary && <div className="text-gray-400">summary: total={summary.total} success={summary.success} fail={summary.fail}</div>}
          </div>
        </SectionCard>
      )}
    </>
  )
}

/** 派生：把全量模板列表里的 W2A 模板筛出来 */
export function useW2aTemplates(allTemplates: Template[] | undefined): Template[] {
  return useMemo(() => {
    if (!allTemplates) return []
    return allTemplates.filter(isTikTokWebToAppTpl)
  }, [allTemplates])
}
