import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { CountryMultiSelect } from '@/components/common/CountryMultiSelect'
import {
  Send, Loader2, CheckCircle, AlertCircle,
  Image, Film, Trash2, Plus, Copy, ChevronDown, ChevronUp, Shield,
} from 'lucide-react'
import { useTemplates } from '@/hooks/use-templates'
import {
  createAds,
  fileNameToAdName,
  type CreateResult,
  type W2aFields,
  type MaterialItem,
  type AdSetConfig,
  type BatchLaunchResult,
} from '@/services/ads-create'
import { fetchMetaAccounts, type MetaAccount } from '@/services/advertisers'
import {
  fetchMetaPages, fetchMetaPixels,
  uploadMetaImage, uploadMetaVideo,
  validateImageFile, validateVideoFile,
  type MetaPageOption, type MetaPixelOption,
} from '@/services/meta-assets'

const MAX_MATERIALS = 20
const MAX_ADSETS = 20

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
    description: '', callToAction: 'LEARN_MORE',
    pixelId: '', customEventType: '',
  }
}

// ─── 素材上传状态 ──
interface UploadingMaterial {
  id: string
  file: File
  type: 'image' | 'video'
  original_name: string
  ad_name: string
  status: 'uploading' | 'success' | 'error'
  progress: number
  image_hash?: string
  video_id?: string
  picture_url?: string
  error?: string
  abort?: () => void
  thumbnail_uploading?: boolean
  thumbnail_error?: string
}

// ─── AdSet 本地状态 ──
interface AdSetLocal {
  key: string
  name: string
  daily_budget: string
  countries: string[]
  pixel_id: string
  custom_event_type: string
  material_ids: string[]
  collapsed: boolean
}

let _matIdCounter = 0
function nextMatId() { return `mat_${Date.now()}_${++_matIdCounter}` }
let _adsetKeyCounter = 0
function nextAdSetKey() { return `as_${Date.now()}_${++_adsetKeyCounter}` }

export default function AdsCreatePage() {
  const navigate = useNavigate()
  const { data: allTemplates } = useTemplates()
  const { data: metaAccountsResp } = useQuery({ queryKey: ['meta-accounts'], queryFn: fetchMetaAccounts })
  const metaAccounts: MetaAccount[] = metaAccountsResp?.data ?? []

  const MASTER_TPL_ID = 'tpl_meta_web_to_app_conv_abo'
  const templates = useMemo(() => {
    if (!allTemplates) return []
    return allTemplates.filter(t =>
      t.platform === 'meta'
      && t.template_type === 'web_to_app'
      && (t.template_subtype === 'conversion' || t.id === MASTER_TPL_ID),
    )
  }, [allTemplates])

  const defaultTpls = useMemo(() => templates.filter(t => t.id === MASTER_TPL_ID || Boolean(t.is_builtin)), [templates])
  const businessTpls = useMemo(() => templates.filter(t => t.id !== MASTER_TPL_ID && !t.is_builtin), [templates])

  // ── 模板 & 全局配置 ──
  const [selectedTpl, setSelectedTpl] = useState('')
  const [campaignName, setCampaignName] = useState('')
  const [adAccountId, setAdAccountId] = useState('')
  const [w2a, setW2a] = useState<W2aFields>(emptyW2a)

  // ── 素材池 ──
  const [materials, setMaterials] = useState<UploadingMaterial[]>([])

  // ── AdSet 列表 ──
  const [adSets, setAdSets] = useState<AdSetLocal[]>([])

  // ── 提交状态 ──
  const [submitting, setSubmitting] = useState(false)
  const [result, setResult] = useState<CreateResult | null>(null)

  // ── Page / Pixel 联动 ──
  const [pages, setPages] = useState<MetaPageOption[]>([])
  const [pagesLoading, setPagesLoading] = useState(false)
  const [pagesError, setPagesError] = useState('')
  const [pageManual, setPageManual] = useState(false)
  const [pixels, setPixels] = useState<MetaPixelOption[]>([])
  const [pixelsLoading, setPixelsLoading] = useState(false)
  const [pixelsError, setPixelsError] = useState('')
  const [pixelManual, setPixelManual] = useState(false)

  const fileInputRef = useRef<HTMLInputElement>(null)

  const currentTpl = (templates ?? []).find(t => t.id === selectedTpl)
  const tplPlatform = currentTpl?.platform
  const isMeta = tplPlatform === 'meta'
  const isW2a = isMeta && currentTpl?.template_type === 'web_to_app'
  const isW2aConversion = isW2a && (currentTpl?.template_subtype === 'conversion'
    || currentTpl?.id === 'tpl_meta_web_to_app_conv_abo')

  const readyMaterials = useMemo(() =>
    materials.filter(m => m.status === 'success'),
    [materials],
  )

  // ── 初始化默认 adset ──
  function createDefaultAdSet(cName: string, countries: string[], budget: string, pixelId: string, eventType: string): AdSetLocal {
    return {
      key: nextAdSetKey(),
      name: cName || 'AdSet_01',
      daily_budget: budget || '50',
      countries: countries.length > 0 ? countries : ['US'],
      pixel_id: pixelId,
      custom_event_type: eventType,
      material_ids: [],
      collapsed: false,
    }
  }

  // ── Page/Pixel 联动 ──
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

  // ── 模板选择后预填 ──
  useEffect(() => {
    if (!selectedTpl || !templates) return
    const tpl = templates.find(t => t.id === selectedTpl)
    if (!tpl) return

    setCampaignName(`${tpl.name}_${new Date().toISOString().slice(5, 10)}`)

    const tplAdset = (tpl as Record<string, unknown>).adset as Record<string, unknown> | undefined
    const tplCountries = (tplAdset?.targeting as Record<string, unknown>)?.geo_locations as Record<string, unknown> | undefined
    const countries = (tplCountries?.countries as string[]) ?? ['US']
    const tplBudget = tplAdset?.daily_budget as number | undefined
    const budgetStr = tplBudget ? String(tplBudget / 100) : '50'

    let pixelId = ''
    let eventType = ''

    if (tpl.template_type === 'web_to_app') {
      const c = (tpl.creative ?? {}) as Record<string, string>
      const po = (tplAdset?.promoted_object ?? {}) as Record<string, string>
      pixelId = po.pixel_id || ''
      eventType = po.custom_event_type || ''
      setW2a(prev => ({
        ...prev,
        primaryText: c.primary_text || prev.primaryText,
        headline: c.headline || prev.headline,
        callToAction: c.call_to_action || 'LEARN_MORE',
        pageId: c.page_id || '',
        landingPageUrl: c.link || '',
        pixelId,
        customEventType: eventType,
      }))
    } else {
      setW2a(emptyW2a())
    }

    const tplAdAccount = tpl.default_ad_account_id as string | undefined
    if (tplAdAccount) setAdAccountId(tplAdAccount)

    const newName = `${tpl.name}_${new Date().toISOString().slice(5, 10)}`
    setAdSets([createDefaultAdSet(newName, countries, budgetStr, pixelId, eventType)])
    setMaterials([])
    setResult(null)
  }, [selectedTpl, templates])

  // ── 素材批量上传 ──
  const handleFileSelect = useCallback(async (fileList: FileList) => {
    if (!adAccountId) return
    const files = Array.from(fileList)
    const currentCount = materials.length
    const remaining = MAX_MATERIALS - currentCount
    if (remaining <= 0) {
      alert(`素材池已满（最多 ${MAX_MATERIALS} 个）`)
      return
    }
    const toUpload = files.slice(0, remaining)
    if (toUpload.length < files.length) {
      alert(`只能再上传 ${remaining} 个素材，已自动截取前 ${remaining} 个`)
    }

    const newItems: UploadingMaterial[] = toUpload.map(f => {
      const ext = f.name.split('.').pop()?.toLowerCase() ?? ''
      const isVideo = ['mp4', 'mov', 'avi', 'mkv', 'webm', 'm4v'].includes(ext)
      return {
        id: nextMatId(),
        file: f,
        type: isVideo ? 'video' : 'image',
        original_name: f.name,
        ad_name: fileNameToAdName(f.name),
        status: 'uploading' as const,
        progress: 0,
      }
    })

    setMaterials(prev => [...prev, ...newItems])

    for (const item of newItems) {
      if (item.type === 'image') {
        const err = validateImageFile(item.file)
        if (err) {
          setMaterials(prev => prev.map(m => m.id === item.id ? { ...m, status: 'error', error: err } : m))
          continue
        }
        const r = await uploadMetaImage(adAccountId, item.file)
        setMaterials(prev => prev.map(m => m.id === item.id
          ? r.success && r.image_hash
            ? { ...m, status: 'success', image_hash: r.image_hash, progress: 100 }
            : { ...m, status: 'error', error: r.error || '上传失败' }
          : m))
      } else {
        const err = validateVideoFile(item.file)
        if (err) {
          setMaterials(prev => prev.map(m => m.id === item.id ? { ...m, status: 'error', error: err } : m))
          continue
        }
        const { promise, abort } = uploadMetaVideo(adAccountId, item.file, pct => {
          setMaterials(prev => prev.map(m => m.id === item.id ? { ...m, progress: pct } : m))
        })
        setMaterials(prev => prev.map(m => m.id === item.id ? { ...m, abort } : m))
        const r = await promise
        setMaterials(prev => prev.map(m => m.id === item.id
          ? r.success && r.video_id
            ? { ...m, status: 'success', video_id: r.video_id, picture_url: r.picture_url || undefined, progress: 100, abort: undefined }
            : { ...m, status: 'error', error: r.error || '上传失败', abort: undefined }
          : m))
      }
    }
  }, [adAccountId, materials.length])

  const removeMaterial = useCallback((matId: string) => {
    const mat = materials.find(m => m.id === matId)
    if (mat?.abort) mat.abort()
    setMaterials(prev => prev.filter(m => m.id !== matId))
    setAdSets(prev => prev.map(a => ({
      ...a,
      material_ids: a.material_ids.filter(id => id !== matId),
    })))
  }, [materials])

  const uploadThumbnail = useCallback(async (matId: string, file: File) => {
    if (!adAccountId) return
    const err = validateImageFile(file)
    if (err) {
      setMaterials(prev => prev.map(m => m.id === matId ? { ...m, thumbnail_error: err } : m))
      return
    }
    setMaterials(prev => prev.map(m => m.id === matId ? { ...m, thumbnail_uploading: true, thumbnail_error: undefined } : m))
    const r = await uploadMetaImage(adAccountId, file)
    if (r.success && r.image_hash) {
      setMaterials(prev => prev.map(m => m.id === matId ? { ...m, image_hash: r.image_hash, thumbnail_uploading: false } : m))
    } else {
      setMaterials(prev => prev.map(m => m.id === matId ? { ...m, thumbnail_uploading: false, thumbnail_error: r.error || '封面上传失败' } : m))
    }
  }, [adAccountId])

  // ── AdSet 操作 ──
  const addAdSet = useCallback(() => {
    if (adSets.length >= MAX_ADSETS) return
    const idx = adSets.length + 1
    setAdSets(prev => [...prev, {
      key: nextAdSetKey(),
      name: `${campaignName}_${String(idx).padStart(2, '0')}`,
      daily_budget: prev[0]?.daily_budget || '50',
      countries: prev[0]?.countries ?? ['US'],
      pixel_id: w2a.pixelId,
      custom_event_type: w2a.customEventType,
      material_ids: [],
      collapsed: false,
    }])
  }, [adSets, campaignName, w2a.pixelId, w2a.customEventType])

  const removeAdSet = useCallback((key: string) => {
    if (adSets.length <= 1) return
    setAdSets(prev => prev.filter(a => a.key !== key))
  }, [adSets.length])

  const duplicateAdSet = useCallback((key: string) => {
    if (adSets.length >= MAX_ADSETS) return
    const src = adSets.find(a => a.key === key)
    if (!src) return
    setAdSets(prev => [...prev, { ...src, key: nextAdSetKey(), name: `${src.name}_copy` }])
  }, [adSets])

  const updateAdSet = useCallback((key: string, patch: Partial<AdSetLocal>) => {
    setAdSets(prev => prev.map(a => a.key === key ? { ...a, ...patch } : a))
  }, [])

  const toggleMaterial = useCallback((adSetKey: string, matId: string) => {
    setAdSets(prev => prev.map(a => {
      if (a.key !== adSetKey) return a
      const has = a.material_ids.includes(matId)
      return { ...a, material_ids: has ? a.material_ids.filter(id => id !== matId) : [...a.material_ids, matId] }
    }))
  }, [])

  const selectAllMaterials = useCallback((adSetKey: string) => {
    const allIds = readyMaterials.map(m => m.id)
    setAdSets(prev => prev.map(a => a.key === adSetKey ? { ...a, material_ids: allIds } : a))
  }, [readyMaterials])

  const clearMaterials = useCallback((adSetKey: string) => {
    setAdSets(prev => prev.map(a => a.key === adSetKey ? { ...a, material_ids: [] } : a))
  }, [])

  // ── 自动为只有 1 个 adset 时同步 campaign name ──
  useEffect(() => {
    if (adSets.length === 1) {
      setAdSets(prev => prev.map((a, i) => i === 0 ? { ...a, name: campaignName } : a))
    }
  }, [campaignName, adSets.length])

  // ── 校验 ──
  function validate(): string | null {
    if (!selectedTpl) return '请先选择模板'
    if (!campaignName.trim()) return '请输入广告系列名称'
    if (isMeta && !adAccountId) return '请选择 Meta 广告账户'
    if (isW2a) {
      if (!w2a.pageId.trim()) return '请选择或填写 Page ID'
      if (!w2a.landingPageUrl.trim()) return '请填写 Landing Page URL'
      if (!w2a.primaryText.trim()) return '请填写 Primary Text'
      if (!w2a.headline.trim()) return '请填写 Headline'
    }
    const videosWithoutCover = readyMaterials.filter(m => m.type === 'video' && !m.image_hash && !m.picture_url)
    if (videosWithoutCover.length > 0) {
      return `视频素材 "${videosWithoutCover[0].ad_name}" 缺少封面图，请上传封面或等待自动获取`
    }
    if (readyMaterials.length === 0) return '至少上传 1 个素材'
    if (adSets.length === 0) return '至少需要 1 个 AdSet'
    for (let i = 0; i < adSets.length; i++) {
      const a = adSets[i]
      if (!a.name.trim()) return `AdSet #${i + 1} 名称不能为空`
      if (!a.daily_budget || Number(a.daily_budget) <= 0) return `AdSet #${i + 1} 日预算必须大于 0`
      if (a.countries.length === 0) return `AdSet #${i + 1} 至少选择一个国家`
      if (a.material_ids.length === 0) return `AdSet #${i + 1} 至少选择 1 个素材`
      if (isW2aConversion) {
        if (!a.pixel_id.trim()) return `AdSet #${i + 1} Pixel ID 必填`
        if (!a.custom_event_type.trim()) return `AdSet #${i + 1} Custom Event Type 必填`
      }
    }
    return null
  }

  const isUploading = materials.some(m => m.status === 'uploading' || m.thumbnail_uploading)
  const validationError = validate()
  const canSubmit = !submitting && !isUploading && !validationError

  const setField = (field: keyof W2aFields, value: string) => {
    setW2a(prev => ({ ...prev, [field]: value })); setResult(null)
  }

  // ── 提交 ──
  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const err = validate()
    if (err) { setResult({ success: false, message: err }); return }

    const mats: MaterialItem[] = readyMaterials.map(m => ({
      id: m.id,
      type: m.type,
      image_hash: m.image_hash,
      video_id: m.video_id,
      picture_url: m.picture_url,
      original_name: m.original_name,
      ad_name: m.ad_name,
    }))

    const adsetConfigs: AdSetConfig[] = adSets.map(a => ({
      name: a.name,
      daily_budget: Math.round(Number(a.daily_budget) * 100),
      countries: a.countries,
      pixel_id: a.pixel_id || w2a.pixelId,
      custom_event_type: a.custom_event_type || w2a.customEventType,
      material_ids: a.material_ids,
    }))

    setSubmitting(true); setResult(null)
    const res = await createAds({
      mode: 'template',
      platform: (tplPlatform as 'tiktok' | 'meta') || 'meta',
      campaignName: campaignName.trim(),
      country: adSets[0]?.countries[0] || 'US',
      countries: adSets[0]?.countries ?? ['US'],
      budget: Number(adSets[0]?.daily_budget) || 50,
      templateId: selectedTpl,
      template: currentTpl ?? null,
      adAccountId: isMeta ? adAccountId : undefined,
      w2a: isW2a ? w2a : undefined,
      materials: mats,
      adsets: adsetConfigs,
    })
    setSubmitting(false); setResult(res)
  }

  return (
    <div className="max-w-4xl mx-auto">
      <PageHeader title="新建广告" description="基于模板快速创建广告投放计划"
        action={<button onClick={() => navigate('/ads')} className="text-sm text-gray-500 hover:text-gray-700 transition">返回广告数据</button>}
      />

      <form onSubmit={handleSubmit}>
        {/* ══ 选择模板 ══ */}
        <SectionCard title="选择模板" className="mb-6">
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">投放模板 <span className="text-red-400">*</span></label>
              <select value={selectedTpl} onChange={e => { setSelectedTpl(e.target.value); setResult(null) }} className={`${inputCls} bg-white`}>
                <option value="">请选择模板</option>
                {defaultTpls.length > 0 && (
                  <optgroup label="系统默认模板">
                    {defaultTpls.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                  </optgroup>
                )}
                {businessTpls.length > 0 && (
                  <optgroup label="我的业务模板">
                    {businessTpls.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                  </optgroup>
                )}
              </select>
            </div>
            {currentTpl && (
              <div className="flex items-center gap-2 text-xs text-gray-400">
                <span className="inline-block px-2 py-0.5 rounded-full font-medium bg-indigo-50 text-indigo-600">Meta</span>
                <span className="inline-block px-2 py-0.5 rounded-full bg-gray-100 text-gray-500">W2A Conversion (ABO)</span>
                {(currentTpl.id === MASTER_TPL_ID || currentTpl.is_builtin) && (
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-amber-50 text-amber-600 font-medium">
                    <Shield className="w-3 h-3" /> 系统默认
                  </span>
                )}
              </div>
            )}
          </div>
        </SectionCard>

        {currentTpl && (
          <>
            {/* ══ 投放配置（全局） ══ */}
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
              </div>
            </SectionCard>

            {/* ══ W2A: 创意与文案（全局共享） ══ */}
            {isW2a && (
              <SectionCard title="创意与文案（全局共享）" className="mb-6">
                <div className="space-y-4">
                  {/* Page */}
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
                  {/* URL */}
                  <div>
                    <label className="block text-xs font-medium text-gray-600 mb-1">Landing Page URL <span className="text-red-400">*</span></label>
                    <input type="url" value={w2a.landingPageUrl} onChange={e => setField('landingPageUrl', e.target.value)} placeholder="https://example.com/landing" className={inputCls} />
                  </div>
                  {/* Texts */}
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
                </div>
              </SectionCard>
            )}

            {/* ══ 素材池 ══ */}
            <SectionCard title={`素材池 (${readyMaterials.length}/${MAX_MATERIALS})`} className="mb-6">
              <div className="space-y-4">
                <div className="flex items-center gap-3">
                  <input ref={fileInputRef} type="file" multiple
                    accept=".jpg,.jpeg,.png,.gif,.bmp,.webp,.mp4,.mov,.avi,.mkv,.webm,.m4v"
                    className="hidden"
                    onChange={e => { if (e.target.files?.length) handleFileSelect(e.target.files); e.target.value = '' }}
                  />
                  <button type="button" onClick={() => fileInputRef.current?.click()}
                    disabled={!adAccountId || materials.length >= MAX_MATERIALS}
                    className="flex items-center gap-2 px-4 py-2 rounded-xl border border-gray-200 text-sm text-gray-600 hover:bg-white hover:border-gray-300 transition disabled:opacity-50">
                    <Plus className="w-4 h-4" /> 上传素材（图片/视频）
                  </button>
                  <span className="text-xs text-gray-400">支持批量选择，图片和视频可混合，最多 {MAX_MATERIALS} 个</span>
                </div>

                {materials.length > 0 && (
                  <div className="border border-gray-100 rounded-xl overflow-hidden">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="text-left px-3 py-2 font-medium text-gray-500 w-8">#</th>
                          <th className="text-left px-3 py-2 font-medium text-gray-500">文件名</th>
                          <th className="text-left px-3 py-2 font-medium text-gray-500 w-16">类型</th>
                          <th className="text-left px-3 py-2 font-medium text-gray-500 w-20">状态</th>
                          <th className="text-left px-3 py-2 font-medium text-gray-500">Hash / ID</th>
                          <th className="text-left px-3 py-2 font-medium text-gray-500">封面图</th>
                          <th className="text-left px-3 py-2 font-medium text-gray-500">Ad 名称</th>
                          <th className="text-left px-3 py-2 font-medium text-gray-500 w-10"></th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-50">
                        {materials.map((m, i) => (
                          <tr key={m.id} className={m.status === 'error' ? 'bg-red-50/50' : ''}>
                            <td className="px-3 py-2 text-gray-400">{i + 1}</td>
                            <td className="px-3 py-2 text-gray-700 max-w-[160px] truncate" title={m.original_name}>{m.original_name}</td>
                            <td className="px-3 py-2">
                              <span className={`inline-flex items-center gap-1 ${m.type === 'video' ? 'text-purple-600' : 'text-blue-600'}`}>
                                {m.type === 'video' ? <Film className="w-3 h-3" /> : <Image className="w-3 h-3" />}
                                {m.type}
                              </span>
                            </td>
                            <td className="px-3 py-2">
                              {m.status === 'uploading' && (
                                <span className="flex items-center gap-1 text-amber-500">
                                  <Loader2 className="w-3 h-3 animate-spin" />
                                  {m.progress < 100 ? `${m.progress}%` : '处理中...'}
                                </span>
                              )}
                              {m.status === 'success' && <span className="text-green-600 flex items-center gap-1"><CheckCircle className="w-3 h-3" /> 就绪</span>}
                              {m.status === 'error' && <span className="text-red-500" title={m.error}>失败</span>}
                            </td>
                            <td className="px-3 py-2 text-gray-500 font-mono text-[11px] max-w-[100px] truncate">
                              {m.type === 'video' ? (m.video_id || '-') : (m.image_hash || '-')}
                              {m.error && !m.video_id && !m.image_hash && <span className="text-red-400 font-sans">{m.error}</span>}
                            </td>
                            {/* 封面图列 */}
                            <td className="px-3 py-2">
                              {m.type === 'video' ? (
                                m.image_hash ? (
                                  <span className="text-green-600 text-[11px] font-mono" title={`hash: ${m.image_hash}`}>
                                    <CheckCircle className="w-3 h-3 inline mr-0.5" />{m.image_hash.slice(0, 8)}...
                                  </span>
                                ) : m.picture_url ? (
                                  <span className="text-green-600 text-[11px]" title={m.picture_url}>
                                    <CheckCircle className="w-3 h-3 inline mr-0.5" />自动封面
                                  </span>
                                ) : m.status === 'uploading' ? (
                                  <span className="text-gray-400 text-[11px]">
                                    <Loader2 className="w-3 h-3 inline mr-0.5 animate-spin" />等待上传完成
                                  </span>
                                ) : m.thumbnail_uploading ? (
                                  <span className="flex items-center gap-1 text-amber-500">
                                    <Loader2 className="w-3 h-3 animate-spin" /> 上传中
                                  </span>
                                ) : m.status === 'success' ? (
                                  <div className="flex flex-col gap-0.5">
                                    <label className="cursor-pointer text-blue-500 hover:text-blue-600 whitespace-nowrap">
                                      <Image className="w-3 h-3 inline mr-0.5" />上传封面
                                      <input type="file" accept=".jpg,.jpeg,.png" className="hidden"
                                        onChange={e => {
                                          const f = e.target.files?.[0]
                                          if (f) uploadThumbnail(m.id, f)
                                          e.target.value = ''
                                        }} />
                                    </label>
                                    {m.thumbnail_error && <span className="text-red-400 text-[10px]">{m.thumbnail_error}</span>}
                                    <input type="text" placeholder="或输入 hash"
                                      className="w-full px-1 py-0.5 border border-gray-200 rounded text-[11px] focus:outline-none focus:ring-1 focus:ring-blue-400 mt-0.5"
                                      onBlur={e => {
                                        const v = e.target.value.trim()
                                        if (v) setMaterials(prev => prev.map(x => x.id === m.id ? { ...x, image_hash: v } : x))
                                      }} />
                                  </div>
                                ) : (
                                  <span className="text-gray-300">—</span>
                                )
                              ) : (
                                <span className="text-gray-300">—</span>
                              )}
                            </td>
                            <td className="px-3 py-2">
                              <input type="text" value={m.ad_name}
                                onChange={e => setMaterials(prev => prev.map(x => x.id === m.id ? { ...x, ad_name: e.target.value } : x))}
                                className="w-full px-2 py-1 border border-gray-200 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-400"
                              />
                            </td>
                            <td className="px-3 py-2">
                              <button type="button" onClick={() => removeMaterial(m.id)} className="text-gray-400 hover:text-red-500 transition">
                                <Trash2 className="w-3.5 h-3.5" />
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                {materials.length === 0 && (
                  <p className="text-sm text-gray-400 text-center py-6">暂无素材，请点击上方按钮上传</p>
                )}
                {readyMaterials.some(m => m.type === 'video' && !m.image_hash && !m.picture_url) && (
                  <p className="text-xs text-amber-600 bg-amber-50 rounded-lg px-3 py-2">
                    <AlertCircle className="w-3.5 h-3.5 inline mr-1" />
                    有视频素材缺少封面图 — 请手动上传封面，或等待 Meta 自动生成
                  </p>
                )}
              </div>
            </SectionCard>

            {/* ══ AdSet 配置区 ══ */}
            <SectionCard title={`AdSet 配置 (${adSets.length})`} className="mb-6">
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-gray-400">每个 AdSet 可独立配置预算、国家和素材，最多 {MAX_ADSETS} 个</span>
                  <button type="button" onClick={addAdSet} disabled={adSets.length >= MAX_ADSETS}
                    className="flex items-center gap-1 text-sm text-blue-500 hover:text-blue-600 disabled:opacity-40 transition">
                    <Plus className="w-4 h-4" /> 添加 AdSet
                  </button>
                </div>

                {adSets.map((adSet, idx) => (
                  <div key={adSet.key} className="border border-gray-200 rounded-xl overflow-hidden">
                    {/* header */}
                    <div className="flex items-center justify-between px-4 py-2.5 bg-gray-50 border-b border-gray-100">
                      <button type="button" onClick={() => updateAdSet(adSet.key, { collapsed: !adSet.collapsed })}
                        className="flex items-center gap-2 text-sm font-medium text-gray-700 hover:text-blue-600 transition">
                        {adSet.collapsed ? <ChevronDown className="w-4 h-4" /> : <ChevronUp className="w-4 h-4" />}
                        AdSet #{idx + 1}: {adSet.name || '未命名'}
                        <span className="text-xs font-normal text-gray-400">
                          ({adSet.material_ids.length} 素材 / ${adSet.daily_budget}/天 / {adSet.countries.join(',')})
                        </span>
                      </button>
                      <div className="flex items-center gap-2">
                        <button type="button" onClick={() => duplicateAdSet(adSet.key)} title="复制"
                          className="text-gray-400 hover:text-blue-500 transition"><Copy className="w-3.5 h-3.5" /></button>
                        {adSets.length > 1 && (
                          <button type="button" onClick={() => removeAdSet(adSet.key)} title="删除"
                            className="text-gray-400 hover:text-red-500 transition"><Trash2 className="w-3.5 h-3.5" /></button>
                        )}
                      </div>
                    </div>

                    {!adSet.collapsed && (
                      <div className="p-4 space-y-4">
                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <label className="block text-xs font-medium text-gray-600 mb-1">AdSet 名称 <span className="text-red-400">*</span></label>
                            <input type="text" value={adSet.name}
                              onChange={e => updateAdSet(adSet.key, { name: e.target.value })}
                              className={inputCls} placeholder="AdSet 名称" />
                          </div>
                          <div>
                            <label className="block text-xs font-medium text-gray-600 mb-1">日预算 (USD) <span className="text-red-400">*</span></label>
                            <input type="number" value={adSet.daily_budget}
                              onChange={e => updateAdSet(adSet.key, { daily_budget: e.target.value })}
                              min="1" step="1" className={inputCls} placeholder="50" />
                          </div>
                        </div>

                        <div>
                          <label className="block text-xs font-medium text-gray-600 mb-1">投放国家 <span className="text-red-400">*</span></label>
                          <CountryMultiSelect value={adSet.countries}
                            onChange={c => updateAdSet(adSet.key, { countries: c })} />
                        </div>

                        {/* Pixel / Event per adset (for conversion templates) */}
                        {isW2aConversion && (
                          <div className="grid grid-cols-2 gap-4">
                            <div>
                              <label className="block text-xs font-medium text-gray-600 mb-1">Pixel ID {isW2aConversion && <span className="text-red-400">*</span>}</label>
                              {pixelManual ? (
                                <input type="text" value={adSet.pixel_id}
                                  onChange={e => updateAdSet(adSet.key, { pixel_id: e.target.value })}
                                  placeholder="Pixel ID" className={inputCls} />
                              ) : (
                                <select value={adSet.pixel_id}
                                  onChange={e => updateAdSet(adSet.key, { pixel_id: e.target.value })}
                                  disabled={!adAccountId || pixelsLoading} className={`${inputCls} bg-white`}>
                                  <option value="">{pixelsLoading ? '加载中...' : '请选择 Pixel'}</option>
                                  {pixels.map(p => <option key={p.id} value={p.id}>{p.name} ({p.id})</option>)}
                                </select>
                              )}
                            </div>
                            <div>
                              <label className="block text-xs font-medium text-gray-600 mb-1">Custom Event Type {isW2aConversion && <span className="text-red-400">*</span>}</label>
                              <select value={adSet.custom_event_type}
                                onChange={e => updateAdSet(adSet.key, { custom_event_type: e.target.value })}
                                className={`${inputCls} bg-white`}>
                                <option value="">请选择</option>
                                {EVENT_TYPE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                              </select>
                            </div>
                          </div>
                        )}

                        {/* 素材勾选 */}
                        <div>
                          <div className="flex items-center justify-between mb-2">
                            <label className="text-xs font-medium text-gray-600">选择素材 <span className="text-red-400">*</span></label>
                            <div className="flex gap-2">
                              <button type="button" onClick={() => selectAllMaterials(adSet.key)}
                                className="text-[11px] text-blue-500 hover:text-blue-600">全选</button>
                              <button type="button" onClick={() => clearMaterials(adSet.key)}
                                className="text-[11px] text-gray-400 hover:text-gray-600">清空</button>
                            </div>
                          </div>
                          {readyMaterials.length === 0 ? (
                            <p className="text-xs text-gray-400 py-2">请先在素材池中上传素材</p>
                          ) : (
                            <div className="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto pr-1">
                              {readyMaterials.map(m => {
                                const checked = adSet.material_ids.includes(m.id)
                                return (
                                  <label key={m.id}
                                    className={`flex items-center gap-2 px-3 py-2 rounded-lg border cursor-pointer transition text-xs
                                      ${checked ? 'border-blue-300 bg-blue-50' : 'border-gray-100 hover:border-gray-200'}`}>
                                    <input type="checkbox" checked={checked}
                                      onChange={() => toggleMaterial(adSet.key, m.id)}
                                      className="w-3.5 h-3.5 rounded border-gray-300 text-blue-500 focus:ring-blue-400" />
                                    <span className={`${m.type === 'video' ? 'text-purple-600' : 'text-blue-600'}`}>
                                      {m.type === 'video' ? <Film className="w-3 h-3 inline" /> : <Image className="w-3 h-3 inline" />}
                                    </span>
                                    <span className="truncate flex-1" title={m.original_name}>{m.ad_name}</span>
                                  </label>
                                )
                              })}
                            </div>
                          )}
                          <p className="text-[11px] text-gray-400 mt-1">已选 {adSet.material_ids.length} / {readyMaterials.length} 个素材</p>
                        </div>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </SectionCard>

            {/* ══ Pixel 转化追踪（非 conversion 模板时全局配置） ══ */}
            {isW2a && !isW2aConversion && (
              <SectionCard title="Pixel 与转化追踪" className="mb-6">
                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <div className="flex items-center justify-between mb-1">
                        <label className="text-xs font-medium text-gray-600">Pixel ID</label>
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
                          {pixelsError && <p className="text-xs text-red-400 mt-1">{pixelsError}</p>}
                        </>
                      )}
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-gray-600 mb-1">Custom Event Type</label>
                      <select value={w2a.customEventType} onChange={e => setField('customEventType', e.target.value)} className={`${inputCls} bg-white`}>
                        <option value="">请选择转化事件</option>
                        {EVENT_TYPE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                      </select>
                    </div>
                  </div>
                </div>
              </SectionCard>
            )}
          </>
        )}

        {/* ══ 创建预览 ══ */}
        {currentTpl && readyMaterials.length > 0 && adSets.length > 0 && (
          <div className="mb-6 p-4 bg-blue-50/50 border border-blue-100 rounded-xl">
            <p className="text-sm font-medium text-blue-700 mb-2">创建预览</p>
            <div className="text-xs text-blue-600 space-y-1">
              <p>Campaign: <strong>{campaignName}</strong></p>
              <p>AdSet 数量: <strong>{adSets.length}</strong></p>
              <p>素材总数: <strong>{readyMaterials.length}</strong></p>
              <p>预计 Ad 总数: <strong>{adSets.reduce((s, a) => s + a.material_ids.length, 0)}</strong></p>
              {adSets.map((a, i) => (
                <p key={a.key} className="pl-4">
                  AdSet #{i + 1} ({a.name}): {a.material_ids.length} 个 Ad · ${a.daily_budget}/天 · {a.countries.join(',')}
                </p>
              ))}
            </div>
          </div>
        )}

        {/* ══ 结果反馈 ══ */}
        {result && (
          <BatchResultDisplay result={result} />
        )}

        {/* ══ 校验提示 ══ */}
        {validationError && !result && (
          <p className="text-xs text-amber-500 mb-4">{validationError}</p>
        )}

        {/* ══ 提交按钮 ══ */}
        <div className="flex justify-end">
          <button type="submit" disabled={!canSubmit}
            className="px-6 py-2.5 bg-blue-500 hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-medium rounded-xl shadow-sm shadow-blue-500/20 transition-all flex items-center gap-2">
            {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
            {submitting ? '批量创建中...' : isUploading ? '素材上传中...' : '提交创建'}
          </button>
        </div>
      </form>
    </div>
  )
}

// ─── 批量结果展示组件 ───
function BatchResultDisplay({ result }: { result: CreateResult }) {
  const details = result.details as BatchLaunchResult | undefined
  const isPartial = result.success && result.message.includes('部分')

  return (
    <div className={`mb-6 p-4 rounded-xl border ${
      !result.success ? 'bg-red-50 border-red-200'
        : isPartial ? 'bg-amber-50 border-amber-200'
          : 'bg-green-50 border-green-200'
    }`}>
      <div className="flex items-start gap-3">
        {!result.success
          ? <AlertCircle className="w-5 h-5 text-red-500 shrink-0 mt-0.5" />
          : isPartial
            ? <AlertCircle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
            : <CheckCircle className="w-5 h-5 text-green-500 shrink-0 mt-0.5" />}
        <div className="min-w-0 flex-1">
          <p className={`text-sm font-medium ${!result.success ? 'text-red-700' : isPartial ? 'text-amber-700' : 'text-green-700'}`}>
            {!result.success ? '创建失败' : isPartial ? '部分创建成功' : '创建成功'}
          </p>
          <p className={`text-xs mt-1 ${!result.success ? 'text-red-600' : isPartial ? 'text-amber-600' : 'text-green-600'}`}>{result.message}</p>

          {details && (
            <div className="mt-3 space-y-2">
              {details.campaign && (
                <div className="text-xs">
                  <span className="font-medium text-gray-600">Campaign: </span>
                  {details.campaign.success
                    ? <span className="text-green-600">成功 (ID: {details.campaign.campaign_id})</span>
                    : <span className="text-red-600">失败 - {details.campaign.error}</span>}
                </div>
              )}

              {details.adsets?.map((as, i) => (
                <div key={i} className="border border-gray-200 rounded-lg p-3 bg-white/60">
                  <div className="flex items-center gap-2 text-xs mb-1">
                    {as.success
                      ? <CheckCircle className="w-3 h-3 text-green-500" />
                      : <AlertCircle className="w-3 h-3 text-red-500" />}
                    <span className="font-medium text-gray-700">AdSet #{i + 1}: {as.adset_name}</span>
                    {as.success && <span className="text-green-600 text-[11px]">ID: {as.adset_id}</span>}
                    {!as.success && <span className="text-red-500 text-[11px]">{as.error}</span>}
                  </div>
                  {as.ads && as.ads.length > 0 && (
                    <div className="ml-5 space-y-0.5">
                      {as.ads.map((ad, j) => (
                        <div key={j} className="text-[11px] flex items-center gap-1">
                          {ad.success
                            ? <span className="text-green-600">OK</span>
                            : <span className="text-red-500">FAIL</span>}
                          <span className="text-gray-600">{ad.ad_name}</span>
                          <span className="text-gray-400">({ad.material_name})</span>
                          {ad.ad_id && <span className="text-gray-400 font-mono">ID:{ad.ad_id}</span>}
                          {ad.error && <span className="text-red-400">{ad.error}</span>}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          {!details && result.details && (
            <details className="mt-2">
              <summary className="text-xs text-gray-400 cursor-pointer hover:text-gray-600">查看原始详情</summary>
              <pre className="mt-1 text-[11px] text-gray-500 bg-white/60 rounded-lg p-2 overflow-x-auto max-h-40">
                {JSON.stringify(result.details, null, 2)}
              </pre>
            </details>
          )}
        </div>
      </div>
    </div>
  )
}
