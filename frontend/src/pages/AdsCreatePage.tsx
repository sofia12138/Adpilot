import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { Send, FileText, File, Loader2, CheckCircle, AlertCircle } from 'lucide-react'
import { useTemplates } from '@/hooks/use-templates'
import { createAds, type CreateResult } from '@/services/ads-create'

const platforms = ['TikTok', 'Meta'] as const
const countries = ['US', 'JP', 'KR', 'TW', 'TH', 'ID', 'VN', 'BR', 'MX', 'DE'] as const

type CreateMode = 'blank' | 'template'

export default function AdsCreatePage() {
  const navigate = useNavigate()
  const { data: templates } = useTemplates()

  const [createMode, setCreateMode] = useState<CreateMode>('blank')
  const [selectedTpl, setSelectedTpl] = useState<string>('')
  const [platform, setPlatform] = useState<string>(platforms[0])
  const [country, setCountry] = useState<string>(countries[0])
  const [budget, setBudget] = useState('')
  const [campaignName, setCampaignName] = useState('')

  const [submitting, setSubmitting] = useState(false)
  const [result, setResult] = useState<CreateResult | null>(null)

  useEffect(() => {
    if (createMode === 'template' && selectedTpl && templates) {
      const tpl = templates.find(t => t.id === selectedTpl)
      if (tpl) {
        setPlatform(tpl.platform === 'tiktok' ? 'TikTok' : tpl.platform === 'meta' ? 'Meta' : platforms[0])
        if (tpl.country) setCountry(String(tpl.country))
        if (tpl.budget) setBudget(String(tpl.budget))
        setCampaignName(`${tpl.name}_${new Date().toISOString().slice(5, 10)}`)
      }
    }
  }, [selectedTpl, createMode, templates])

  // 成功后 2 秒自动跳转
  useEffect(() => {
    if (result?.success) {
      const timer = setTimeout(() => navigate('/ads'), 2000)
      return () => clearTimeout(timer)
    }
  }, [result, navigate])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()

    if (!campaignName.trim()) {
      setResult({ success: false, message: '请输入广告系列名称' })
      return
    }

    setSubmitting(true)
    setResult(null)

    const res = await createAds({
      mode: createMode,
      platform: platform.toLowerCase() as 'tiktok' | 'meta',
      campaignName: campaignName.trim(),
      country,
      budget: Number(budget) || 0,
      templateId: createMode === 'template' ? selectedTpl : undefined,
      template: createMode === 'template' && selectedTpl
        ? (templates ?? []).find(t => t.id === selectedTpl) ?? null
        : null,
    })

    setSubmitting(false)
    setResult(res)
  }

  const canSubmit = !submitting && campaignName.trim().length > 0

  return (
    <div className="max-w-3xl mx-auto">
      <PageHeader title="新建广告" description="创建新的广告投放计划"
        action={<button onClick={() => navigate('/ads')} className="text-sm text-gray-500 hover:text-gray-700 transition">返回广告数据</button>}
      />

      <form onSubmit={handleSubmit}>
        {/* ── 创建方式 ── */}
        <SectionCard title="创建方式" className="mb-6">
          <div className="flex gap-3">
            <button type="button" onClick={() => { setCreateMode('blank'); setSelectedTpl(''); setResult(null) }}
              className={`flex-1 flex items-center gap-3 p-4 rounded-xl border-2 transition ${createMode === 'blank' ? 'border-blue-400 bg-blue-50/50' : 'border-gray-200 hover:border-gray-300'}`}>
              <File className={`w-5 h-5 ${createMode === 'blank' ? 'text-blue-500' : 'text-gray-400'}`} />
              <div className="text-left">
                <p className={`text-sm font-medium ${createMode === 'blank' ? 'text-blue-600' : 'text-gray-700'}`}>空白创建</p>
                <p className="text-xs text-gray-400">从零开始配置广告</p>
              </div>
            </button>
            <button type="button" onClick={() => { setCreateMode('template'); setResult(null) }}
              className={`flex-1 flex items-center gap-3 p-4 rounded-xl border-2 transition ${createMode === 'template' ? 'border-blue-400 bg-blue-50/50' : 'border-gray-200 hover:border-gray-300'}`}>
              <FileText className={`w-5 h-5 ${createMode === 'template' ? 'text-blue-500' : 'text-gray-400'}`} />
              <div className="text-left">
                <p className={`text-sm font-medium ${createMode === 'template' ? 'text-blue-600' : 'text-gray-700'}`}>使用模板</p>
                <p className="text-xs text-gray-400">基于已有模板快速创建</p>
              </div>
            </button>
          </div>

          {createMode === 'template' && (
            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-1.5">选择模板</label>
              <select value={selectedTpl} onChange={e => { setSelectedTpl(e.target.value); setResult(null) }}
                className="w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 transition bg-white">
                <option value="">请选择模板</option>
                {(templates ?? []).map(t => <option key={t.id} value={t.id}>{t.name} ({t.platform})</option>)}
              </select>
            </div>
          )}
        </SectionCard>

        {/* ── 基础信息 ── */}
        <SectionCard title="基础信息" className="mb-6">
          <div className="space-y-5">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">广告系列名称 <span className="text-red-400">*</span></label>
              <input type="text" value={campaignName} onChange={e => { setCampaignName(e.target.value); setResult(null) }}
                placeholder="例如：US_iOS_Summer_Campaign"
                className="w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 transition" />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">投放渠道</label>
              <div className="flex gap-2">
                {platforms.map(p => (
                  <button key={p} type="button" onClick={() => setPlatform(p)}
                    className={`px-4 py-2 rounded-xl text-sm border transition ${platform === p ? 'border-blue-400 bg-blue-50 text-blue-600 font-medium' : 'border-gray-200 text-gray-600 hover:bg-gray-50'}`}>{p}</button>
                ))}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">投放国家</label>
              <div className="flex flex-wrap gap-2">
                {countries.map(c => (
                  <button key={c} type="button" onClick={() => setCountry(c)}
                    className={`px-3 py-1.5 rounded-lg text-xs border transition ${country === c ? 'border-blue-400 bg-blue-50 text-blue-600 font-medium' : 'border-gray-200 text-gray-500 hover:bg-gray-50'}`}>{c}</button>
                ))}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">日预算（USD）</label>
              <input type="number" value={budget} onChange={e => setBudget(e.target.value)}
                placeholder="例如：500" min="0" step="1"
                className="w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 transition" />
            </div>
          </div>
        </SectionCard>

        {/* ── 结果反馈 ── */}
        {result && (
          <div className={`mb-6 p-4 rounded-xl border ${result.success ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
            <div className="flex items-start gap-3">
              {result.success
                ? <CheckCircle className="w-5 h-5 text-green-500 shrink-0 mt-0.5" />
                : <AlertCircle className="w-5 h-5 text-red-500 shrink-0 mt-0.5" />}
              <div className="min-w-0 flex-1">
                <p className={`text-sm font-medium ${result.success ? 'text-green-700' : 'text-red-700'}`}>
                  {result.success ? '创建成功' : '创建失败'}
                </p>
                <p className={`text-xs mt-1 ${result.success ? 'text-green-600' : 'text-red-600'}`}>
                  {result.message}
                </p>
                {result.success && (
                  <p className="text-xs text-green-500 mt-1">2 秒后自动跳转到广告数据页...</p>
                )}
                {result.details && (
                  <details className="mt-2">
                    <summary className="text-xs text-gray-400 cursor-pointer hover:text-gray-600">查看详情</summary>
                    <pre className="mt-1 text-[11px] text-gray-500 bg-white/60 rounded-lg p-2 overflow-x-auto max-h-40">
                      {JSON.stringify(result.details, null, 2)}
                    </pre>
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
            {submitting ? '创建中...' : '提交创建'}
          </button>
        </div>
      </form>
    </div>
  )
}
