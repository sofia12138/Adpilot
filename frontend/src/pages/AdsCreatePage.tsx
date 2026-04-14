import { useState, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { CountryMultiSelect } from '@/components/common/CountryMultiSelect'
import { Send, Loader2, CheckCircle, AlertCircle, Image, Film } from 'lucide-react'
import { useTemplates } from '@/hooks/use-templates'
import { createAds, type CreateResult, type W2aFields } from '@/services/ads-create'
import { fetchMetaAccounts, type MetaAccount } from '@/services/advertisers'
import {
  fetchMetaPages, fetchMetaPixels,
  uploadMetaImage, uploadMetaVideo,
  validateImageFile, validateVideoFile,
  type MetaPageOption, type MetaPixelOption,
} from '@/services/meta-assets'

const CTA_OPTIONS = [
  'LEARN_MORE', 'SHOP_NOW', 'INSTALL_NOW', 'SIGN_UP',
  'WATCH_MORE', 'DOWNLOAD', 'GET_OFFER', 'ORDER_NOW',
] as const

const EVENT_TYPE_OPTIONS = [
  { value: 'PURCHASE', label: 'PURCHASE (购买)' },
  { value: 'COMPLETE_REGISTRATION', label: 'COMPLETE_REGISTRATION (完成注册)' },
  { value: 'LEAD', label: 'LEAD (线索)' },
  { value: 'INITIATE_CHECKOUT', label: 'INITIATE_CHECKOUT (发起结账)' },
  { value: 'ADD_TO_CART', label: 'ADD_TO_CART (加入购物车)' },
  { value: 'VIEW_CONTENT', label: 'VIEW_CONTENT (查看内容)' },
  { value: 'SEARCH', label: 'SEARCH (搜索)' },
  { value: 'CONTACT', label: 'CONTACT (联系)' },
  { value: 'SUBSCRIBE', label: 'SUBSCRIBE (订阅)' },
  { value: 'START_TRIAL', label: 'START_TRIAL (开始试用)' },
] as const

const inputCls = 'w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 transition'

function emptyW2a(): W2aFields {
  return {
    pageId: '', landingPageUrl: '', primaryText: '', headline: '',
    description: '', callToAction: 'LEARN_MORE', imageHash: '', videoId: '',
    pixelId: '', customEventType: '',
  }
}

export default function AdsCreatePage() {
  const navigate = useNavigate()
  const { data: templates } = useTemplates()
  const { data: metaAccountsResp } = useQuery({ queryKey: ['meta-accounts'], queryFn: fetchMetaAccounts })
  const metaAccounts: MetaAccount[] = metaAccountsResp?.data ?? []

  const [selectedTpl, setSelectedTpl] = useState('')
  const [metaCountries, setMetaCountries] = useState<string[]>(['US'])
  const [budget, setBudget] = useState('')
  const [campaignName, setCampaignName] = useState('')
  const [adAccountId, setAdAccountId] = useState('')
  const [w2a, setW2a] = useState<W2aFields>(emptyW2a)

  const [submitting, setSubmitting] = useState(false)
  const [result, setResult] = useState<CreateResult | null>(null)

  const [pages, setPages] = useState<MetaPageOption[]>([])
  const [pagesLoading, setPagesLoading] = useState(false)
  const [pagesError, setPagesError] = useState('')
  const [pageManual, setPageManual] = useState(false)

  const [pixels, setPixels] = useState<MetaPixelOption[]>([])
  const [pixelsLoading, setPixelsLoading] = useState(false)
  const [pixelsError, setPixelsError] = useState('')
  const [pixelManual, setPixelManual] = useState(false)

  const [imgUploading, setImgUploading] = useState(false)
  const [imgUploadName, setImgUploadName] = useState('')
  const [imgUploadError, setImgUploadError] = useState('')

  const [vidUploading, setVidUploading] = useState(false)
  const [vidUploadName, setVidUploadName] = useState('')
  const [vidUploadError, setVidUploadError] = useState('')
  const [vidProgress, setVidProgress] = useState(0)
  const [vidPhase, setVidPhase] = useState<'upload' | 'processing'>('upload')
  const vidAbortRef = useRef<(() => void) | null>(null)

  const imgInputRef = useRef<HTMLInputElement>(null)
  const vidInputRef = useRef<HTMLInputElement>(null)

  const currentTpl = (templates ?? []).find(t => t.id === selectedTpl)
  const tplPlatform = currentTpl?.platform
  const isMeta = tplPlatform === 'meta'
  const isW2a = isMeta && currentTpl?.template_type === 'web_to_app'
  const isW2aConversion = isW2a && (currentTpl?.template_subtype === 'conversion'
    || currentTpl?.id === 'tpl_meta_web_to_app_conv_abo')

  useEffect(() => {
    if (!adAccountId || !isW2a) { setPages([]); setPixels([]); return }
    setPagesLoading(true); setPagesError('')
    fetchMetaPages(adAccountId)
      .then(r => { setPages(r.data ?? []); if (r.error) setPagesError(r.error) })
      .catch(e => setPagesError(String(e)))
      .finally(() => setPagesLoading(false))
    setPixelsLoading(true); setPixelsError('')
    fetchMetaPixels(adAccountId)
      .then(r => { setPixels(r.data ?? []); if (r.error) setPixelsError(r.error) })
      .catch(e => setPixelsError(String(e)))
      .finally(() => setPixelsLoading(false))
  }, [adAccountId, isW2a])

  useEffect(() => {
    if (selectedTpl && templates) {
      const tpl = templates.find(t => t.id === selectedTpl)
      if (tpl) {
        if (tpl.budget) setBudget(String(tpl.budget))
        setCampaignName(`${tpl.name}_${new Date().toISOString().slice(5, 10)}`)
        const tplAdset = (tpl as Record<string, unknown>).adset as Record<string, unknown> | undefined
        const tplCountries = (tplAdset?.targeting as Record<string, unknown>)?.geo_locations as Record<string, unknown> | undefined
        if (tplCountries?.countries) setMetaCountries(tplCountries.countries as string[])
        const tplBudget = tplAdset?.daily_budget as number | undefined
        if (tplBudget) setBudget(String(tplBudget / 100))

        if (tpl.template_type === 'web_to_app') {
          const c = (tpl.creative ?? {}) as Record<string, string>
          const po = (tplAdset?.promoted_object ?? {}) as Record<string, string>
          setW2a(prev => ({
            ...prev,
            primaryText: c.primary_text || prev.primaryText,
            headline: c.headline || prev.headline,
            callToAction: c.call_to_action || 'LEARN_MORE',
            pageId: c.page_id || '',
            landingPageUrl: c.link || '',
            pixelId: po.pixel_id || '',
            customEventType: po.custom_event_type || '',
          }))
        } else {
          setW2a(emptyW2a())
        }

        const tplAdAccount = tpl.default_ad_account_id as string | undefined
        if (tplAdAccount) setAdAccountId(tplAdAccount)
      }
    }
  }, [selectedTpl, templates])

  useEffect(() => {
    if (result?.success) {
      const timer = setTimeout(() => navigate('/ads'), 2000)
      return () => clearTimeout(timer)
    }
  }, [result, navigate])

  function w2aValidation(): string | null {
    if (!isW2a) return null
    if (!w2a.pageId.trim()) return '请选择或填写 Page ID'
    if (!w2a.landingPageUrl.trim()) return '请填写 Landing Page URL'
    if (!w2a.primaryText.trim()) return '请填写 Primary Text'
    if (!w2a.headline.trim()) return '请填写 Headline'
    if (!w2a.imageHash.trim() && !w2a.videoId.trim()) return '请上传素材或填写 Image Hash / Video ID (至少一个)'
    if (isW2aConversion) {
      if (!w2a.pixelId.trim()) return '转化优化版模板必须选择 Pixel ID'
      if (!w2a.customEventType.trim()) return '转化优化版模板必须选择 Custom Event Type'
    } else {
      if (w2a.pixelId.trim() && !w2a.customEventType.trim()) return 'Pixel ID 已填写，请同时选择 Custom Event Type'
      if (w2a.customEventType.trim() && !w2a.pixelId.trim()) return 'Custom Event Type 已选择，请同时填写 Pixel ID'
    }
    return null
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!selectedTpl) { setResult({ success: false, message: '请先选择模板' }); return }
    if (!campaignName.trim()) { setResult({ success: false, message: '请输入广告系列名称' }); return }
    if (isMeta && !adAccountId) { setResult({ success: false, message: '请选择 Meta 广告账户' }); return }
    if (isW2a && metaCountries.length === 0) { setResult({ success: false, message: '请至少选择一个投放国家' }); return }
    const w2aErr = w2aValidation()
    if (w2aErr) { setResult({ success: false, message: w2aErr }); return }

    setSubmitting(true); setResult(null)
    const res = await createAds({
      mode: 'template',
      platform: (tplPlatform as 'tiktok' | 'meta') || 'meta',
      campaignName: campaignName.trim(),
      country: metaCountries[0] || 'US',
      countries: metaCountries,
      budget: Number(budget) || 0,
      templateId: selectedTpl,
      template: currentTpl ?? null,
      adAccountId: isMeta ? adAccountId : undefined,
      w2a: isW2a ? w2a : undefined,
    })
    setSubmitting(false); setResult(res)
  }

  const isUploading = imgUploading || vidUploading
  const canSubmit = !submitting && !isUploading
    && !!selectedTpl && campaignName.trim().length > 0
    && !(isMeta && !adAccountId)
    && !(isW2a && metaCountries.length === 0)
    && !w2aValidation()

  const setField = (field: keyof W2aFields, value: string) => {
    setW2a(prev => ({ ...prev, [field]: value })); setResult(null)
  }

  async function handleImageUpload(file: globalThis.File) {
    const err = validateImageFile(file)
    if (err) { setImgUploadError(err); return }
    if (!adAccountId) return
    setImgUploading(true); setImgUploadError(''); setImgUploadName(file.name)
    const r = await uploadMetaImage(adAccountId, file)
    setImgUploading(false)
    if (r.success && r.image_hash) {
      setField('imageHash', r.image_hash)
      setImgUploadName(`${file.name} (${r.image_hash.slice(0, 8)}...)`)
    } else {
      setImgUploadError(r.error || '上传失败')
    }
  }

  function handleVideoUpload(file: globalThis.File) {
    const err = validateVideoFile(file)
    if (err) { setVidUploadError(err); return }
    if (!adAccountId) return
    setVidUploading(true); setVidUploadError(''); setVidUploadName(file.name)
    setVidProgress(0); setVidPhase('upload')
    const { promise, abort } = uploadMetaVideo(adAccountId, file, pct => {
      setVidProgress(pct)
      if (pct >= 100) setVidPhase('processing')
    })
    vidAbortRef.current = abort
    promise.then(r => {
      setVidUploading(false); vidAbortRef.current = null; setVidPhase('upload')
      if (r.success && r.video_id) {
        setField('videoId', r.video_id)
        const secs = r.upload_time_ms ? `${(r.upload_time_ms / 1000).toFixed(1)}s` : ''
        const mode = r.upload_mode ? ` [${r.upload_mode}]` : ''
        setVidUploadName(`${file.name} (${r.video_id}) ${secs}${mode}`)
      } else {
        const mode = r.upload_mode || ''
        const stage = r.stage || ''
        const retries = r.retry_count ? `已重试${r.retry_count}次` : ''
        const prefix = [mode, stage, retries].filter(Boolean).join('/')
        setVidUploadError(`${prefix ? `[${prefix}] ` : ''}${r.error || '上传失败'}`)
      }
    })
  }

  return (
    <div className="max-w-3xl mx-auto">
      <PageHeader title="新建广告" description="基于模板快速创建广告投放计划"
        action={<button onClick={() => navigate('/ads')} className="text-sm text-gray-500 hover:text-gray-700 transition">返回广告数据</button>}
      />

      <form onSubmit={handleSubmit}>
        {/* ── 选择模板 ── */}
        <SectionCard title="选择模板" className="mb-6">
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">投放模板 <span className="text-red-400">*</span></label>
              <select value={selectedTpl} onChange={e => { setSelectedTpl(e.target.value); setResult(null) }} className={`${inputCls} bg-white`}>
                <option value="">请选择模板</option>
                {(templates ?? []).map(t => <option key={t.id} value={t.id}>{t.name} ({t.platform})</option>)}
              </select>
            </div>
            {currentTpl && (
              <div className="flex items-center gap-2 text-xs text-gray-400">
                <span className={`inline-block px-2 py-0.5 rounded-full font-medium ${isMeta ? 'bg-indigo-50 text-indigo-600' : 'bg-sky-50 text-sky-600'}`}>
                  {tplPlatform}
                </span>
                {currentTpl.template_type && (
                  <span className="inline-block px-2 py-0.5 rounded-full bg-gray-100 text-gray-500">{currentTpl.template_type as string}</span>
                )}
                {currentTpl.template_subtype && (
                  <span className="inline-block px-2 py-0.5 rounded-full bg-gray-100 text-gray-500">{currentTpl.template_subtype as string}</span>
                )}
              </div>
            )}
          </div>
        </SectionCard>

        {/* ── 模板驱动表单区域（选中模板后才展示） ── */}
        {currentTpl && (
          <>
            {/* 投放配置 */}
            <SectionCard title="投放配置" className="mb-6">
              <div className="space-y-5">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">Campaign Name <span className="text-red-400">*</span></label>
                  <input type="text" value={campaignName} onChange={e => { setCampaignName(e.target.value); setResult(null) }}
                    placeholder="例如：US_iOS_Summer_Campaign" className={inputCls} />
                </div>

                {isMeta && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1.5">Meta 广告账户 <span className="text-red-400">*</span></label>
                    <select value={adAccountId} onChange={e => { setAdAccountId(e.target.value); setResult(null) }} className={`${inputCls} bg-white`}>
                      <option value="">请选择 Meta 广告账户</option>
                      {metaAccounts.map(a => <option key={a.id} value={a.id}>{a.name} ({a.id})</option>)}
                    </select>
                  </div>
                )}

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1.5">
                      投放国家 {isW2a && <span className="text-red-400">*</span>}
                    </label>
                    <CountryMultiSelect value={metaCountries} onChange={setMetaCountries} />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1.5">日预算 (USD)</label>
                    <input type="number" value={budget} onChange={e => setBudget(e.target.value)} placeholder="50" min="0" step="1" className={inputCls} />
                  </div>
                </div>
              </div>
            </SectionCard>

            {/* Web-to-App 创意与文案 */}
            {isW2a && (
              <>
                <SectionCard title="Creative 素材与文案" className="mb-6">
                  <div className="space-y-4">
                    <div>
                      <div className="flex items-center justify-between mb-1">
                        <label className="block text-xs font-medium text-gray-600">主页 (Page) <span className="text-red-400">*</span></label>
                        <button type="button" onClick={() => setPageManual(!pageManual)} className="text-[11px] text-blue-500 hover:text-blue-600">
                          {pageManual ? '切换为下拉选择' : '手动输入 Page ID'}
                        </button>
                      </div>
                      {pageManual ? (
                        <input type="text" value={w2a.pageId} onChange={e => setField('pageId', e.target.value)} placeholder="手动输入 Facebook Page ID" className={inputCls} />
                      ) : (
                        <>
                          <select value={w2a.pageId} onChange={e => setField('pageId', e.target.value)} disabled={!adAccountId || pagesLoading} className={`${inputCls} bg-white`}>
                            <option value="">{!adAccountId ? '请先选择广告账户' : pagesLoading ? '加载中...' : '请选择主页'}</option>
                            {pages.map(p => <option key={p.id} value={p.id}>{p.name} ({p.id})</option>)}
                          </select>
                          {pagesError && <p className="text-xs text-red-400 mt-1">拉取失败: {pagesError}</p>}
                          {!pagesLoading && adAccountId && pages.length === 0 && !pagesError && (
                            <p className="text-xs text-gray-400 mt-1">未找到可用主页，<button type="button" onClick={() => setPageManual(true)} className="text-blue-500 hover:underline">手动输入</button></p>
                          )}
                        </>
                      )}
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-gray-600 mb-1">Landing Page URL <span className="text-red-400">*</span></label>
                      <input type="url" value={w2a.landingPageUrl} onChange={e => setField('landingPageUrl', e.target.value)} placeholder="https://example.com/landing" className={inputCls} />
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 mb-1">Primary Text <span className="text-red-400">*</span></label>
                      <textarea value={w2a.primaryText} onChange={e => setField('primaryText', e.target.value)} rows={2} placeholder="广告主文案" className={inputCls} />
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-xs font-medium text-gray-600 mb-1">Headline <span className="text-red-400">*</span></label>
                        <input type="text" value={w2a.headline} onChange={e => setField('headline', e.target.value)} placeholder="标题" className={inputCls} />
                      </div>
                      <div>
                        <label className="block text-xs font-medium text-gray-600 mb-1">Call To Action</label>
                        <select value={w2a.callToAction} onChange={e => setField('callToAction', e.target.value)} className={`${inputCls} bg-white`}>
                          {CTA_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
                        </select>
                      </div>
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 mb-1">Description</label>
                      <input type="text" value={w2a.description} onChange={e => setField('description', e.target.value)} placeholder="可选描述" className={inputCls} />
                    </div>

                    {/* 素材上传 */}
                    <div className="border border-gray-100 rounded-xl p-4 bg-gray-50/50 space-y-3">
                      <p className="text-xs font-medium text-gray-600">广告素材 <span className="text-gray-400 font-normal">(图片或视频至少一个)</span></p>
                      <div className="flex items-center gap-3 flex-wrap">
                        <input ref={imgInputRef} type="file" accept=".jpg,.jpeg,.png,.gif,.bmp,.webp" className="hidden"
                          onChange={e => { const f = e.target.files?.[0]; if (f) handleImageUpload(f); e.target.value = '' }} />
                        <button type="button" onClick={() => imgInputRef.current?.click()} disabled={!adAccountId || imgUploading}
                          className="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 text-xs text-gray-600 hover:bg-white hover:border-gray-300 transition disabled:opacity-50">
                          {imgUploading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Image className="w-3.5 h-3.5" />}
                          上传图片
                        </button>
                        {imgUploadName && !imgUploadError && (
                          <span className="text-xs text-gray-500">{imgUploading ? `上传中: ${imgUploadName}` : imgUploadName}</span>
                        )}
                        {w2a.imageHash && !imgUploading && <span className="text-xs text-green-500">hash: {w2a.imageHash.slice(0, 12)}...</span>}
                        {imgUploadError && <span className="text-xs text-red-400">{imgUploadError}</span>}
                      </div>

                      <div className="space-y-1.5">
                        <div className="flex items-center gap-3 flex-wrap">
                          <input ref={vidInputRef} type="file" accept=".mp4,.mov,.avi,.mkv,.webm,.m4v" className="hidden"
                            onChange={e => { const f = e.target.files?.[0]; if (f) handleVideoUpload(f); e.target.value = '' }} />
                          <button type="button" onClick={() => vidInputRef.current?.click()} disabled={!adAccountId || vidUploading}
                            className="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 text-xs text-gray-600 hover:bg-white hover:border-gray-300 transition disabled:opacity-50">
                            {vidUploading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Film className="w-3.5 h-3.5" />}
                            上传视频
                          </button>
                          {vidUploading && (
                            <button type="button" onClick={() => vidAbortRef.current?.()} className="text-xs text-red-400 hover:text-red-500">取消</button>
                          )}
                          {vidUploadName && !vidUploadError && !vidUploading && (
                            <span className="text-xs text-gray-500">{vidUploadName}</span>
                          )}
                          {w2a.videoId && !vidUploading && <span className="text-xs text-green-500">id: {w2a.videoId}</span>}
                          {vidUploadError && <span className="text-xs text-red-400">{vidUploadError}</span>}
                        </div>
                        {vidUploading && (
                          <div className="space-y-1">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 h-2 bg-gray-200 rounded-full overflow-hidden">
                                <div className={`h-full rounded-full transition-all duration-300 ${vidPhase === 'processing' ? 'bg-amber-500 animate-pulse' : 'bg-blue-500'}`}
                                  style={{ width: vidPhase === 'processing' ? '100%' : `${vidProgress}%` }} />
                              </div>
                              <span className="text-xs text-gray-500 w-10 text-right">
                                {vidPhase === 'processing' ? '...' : `${vidProgress}%`}
                              </span>
                            </div>
                            <p className="text-[11px] text-gray-400">
                              {vidPhase === 'processing'
                                ? '文件已上传到服务器，正在分片转发到 Meta（大视频需要更长时间）...'
                                : '正在上传到服务器，请勿关闭页面'}
                            </p>
                          </div>
                        )}
                      </div>

                      <div className="grid grid-cols-2 gap-4 pt-2 border-t border-gray-200">
                        <div>
                          <label className="block text-[11px] text-gray-400 mb-1">Image Hash {w2a.imageHash && <span className="text-green-500">(已填)</span>}</label>
                          <input type="text" value={w2a.imageHash} onChange={e => setField('imageHash', e.target.value)} placeholder="上传后自动填入，或手动输入" className={`${inputCls} text-xs`} />
                        </div>
                        <div>
                          <label className="block text-[11px] text-gray-400 mb-1">Video ID {w2a.videoId && <span className="text-green-500">(已填)</span>}</label>
                          <input type="text" value={w2a.videoId} onChange={e => setField('videoId', e.target.value)} placeholder="上传后自动填入，或手动输入" className={`${inputCls} text-xs`} />
                        </div>
                      </div>
                      {!w2a.imageHash && !w2a.videoId && <p className="text-xs text-amber-500">Image Hash 或 Video ID 至少填写一个</p>}
                    </div>
                  </div>
                </SectionCard>

                {/* Pixel 转化追踪 */}
                <SectionCard title={isW2aConversion ? 'Pixel 转化追踪（必填）' : 'Pixel 与转化追踪'} className="mb-6">
                  <div className="space-y-4">
                    {isW2aConversion && (
                      <p className="text-xs text-blue-500 bg-blue-50 rounded-lg px-3 py-2">转化优化版模板要求 Pixel ID 和 Custom Event Type 必填</p>
                    )}
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <div className="flex items-center justify-between mb-1">
                          <label className="text-xs font-medium text-gray-600">Pixel ID {isW2aConversion && <span className="text-red-400">*</span>}</label>
                          <button type="button" onClick={() => setPixelManual(!pixelManual)} className="text-[11px] text-blue-500 hover:text-blue-600">
                            {pixelManual ? '切换为下拉选择' : '手动输入'}
                          </button>
                        </div>
                        {pixelManual ? (
                          <input type="text" value={w2a.pixelId} onChange={e => setField('pixelId', e.target.value)} placeholder="手动输入 Pixel ID" className={inputCls} />
                        ) : (
                          <>
                            <select value={w2a.pixelId} onChange={e => setField('pixelId', e.target.value)} disabled={!adAccountId || pixelsLoading} className={`${inputCls} bg-white`}>
                              <option value="">{!adAccountId ? '请先选择广告账户' : pixelsLoading ? '加载中...' : '请选择 Pixel'}</option>
                              {pixels.map(p => <option key={p.id} value={p.id}>{p.name} ({p.id})</option>)}
                            </select>
                            {pixelsError && <p className="text-xs text-red-400 mt-1">拉取失败: {pixelsError}</p>}
                            {!pixelsLoading && adAccountId && pixels.length === 0 && !pixelsError && (
                              <p className="text-xs text-gray-400 mt-1">未找到可用 Pixel，<button type="button" onClick={() => setPixelManual(true)} className="text-blue-500 hover:underline">手动输入</button></p>
                            )}
                          </>
                        )}
                      </div>
                      <div>
                        <label className="block text-xs font-medium text-gray-600 mb-1">Custom Event Type {isW2aConversion && <span className="text-red-400">*</span>}</label>
                        <select value={w2a.customEventType} onChange={e => setField('customEventType', e.target.value)} className={`${inputCls} bg-white`}>
                          <option value="">请选择转化事件</option>
                          {EVENT_TYPE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                        </select>
                      </div>
                    </div>
                    {!isW2aConversion && ((w2a.pixelId && !w2a.customEventType) || (!w2a.pixelId && w2a.customEventType)) && (
                      <p className="text-xs text-amber-500">Pixel ID 和 Custom Event Type 必须成对填写</p>
                    )}
                    {isW2aConversion && (!w2a.pixelId || !w2a.customEventType) && (
                      <p className="text-xs text-red-400">转化优化模板的 Pixel ID 和 Custom Event Type 均为必填</p>
                    )}
                  </div>
                </SectionCard>
              </>
            )}
          </>
        )}

        {/* ── 结果反馈 ── */}
        {result && (
          <div className={`mb-6 p-4 rounded-xl border ${result.success ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
            <div className="flex items-start gap-3">
              {result.success ? <CheckCircle className="w-5 h-5 text-green-500 shrink-0 mt-0.5" /> : <AlertCircle className="w-5 h-5 text-red-500 shrink-0 mt-0.5" />}
              <div className="min-w-0 flex-1">
                <p className={`text-sm font-medium ${result.success ? 'text-green-700' : 'text-red-700'}`}>{result.success ? '创建成功' : '创建失败'}</p>
                <p className={`text-xs mt-1 ${result.success ? 'text-green-600' : 'text-red-600'}`}>{result.message}</p>
                {result.success && <p className="text-xs text-green-500 mt-1">2 秒后自动跳转到广告数据页...</p>}
                {result.details && (
                  <details className="mt-2">
                    <summary className="text-xs text-gray-400 cursor-pointer hover:text-gray-600">查看详情</summary>
                    <pre className="mt-1 text-[11px] text-gray-500 bg-white/60 rounded-lg p-2 overflow-x-auto max-h-40">{JSON.stringify(result.details, null, 2)}</pre>
                  </details>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ── 提交按钮 ── */}
        <div className="flex justify-end">
          <button type="submit" disabled={!canSubmit}
            className="px-6 py-2.5 bg-blue-500 hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-medium rounded-xl shadow-sm shadow-blue-500/20 transition-all flex items-center gap-2">
            {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
            {submitting ? '创建中...' : isUploading ? '素材上传中...' : '提交创建'}
          </button>
        </div>
      </form>
    </div>
  )
}
