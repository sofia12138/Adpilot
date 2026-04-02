import { useState, useEffect } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { Save, Loader2, RotateCcw } from 'lucide-react'
import { useInsightConfig, useUpdateInsightConfig } from '@/hooks/use-insight'
import { DEFAULT_INSIGHT_THRESHOLDS } from '@/config/insight-thresholds'

const FIELDS = [
  { key: 'min' as const, label: '最低线 (min)', hint: '低于此值表示严重亏损' },
  { key: 'low' as const, label: '预警线 (low)', hint: '低于此值触发 danger 结论' },
  { key: 'target' as const, label: '目标线 (target)', hint: '达到此值视为合格' },
  { key: 'high' as const, label: '优秀线 (high)', hint: '超过此值触发 success / 放量建议' },
]

export default function InsightConfigPage() {
  const { data: config, isLoading } = useInsightConfig()
  const mutation = useUpdateInsightConfig()

  const [form, setForm] = useState({ min: 0.1, low: 0.8, target: 1.2, high: 2.0 })
  const [err, setErr] = useState('')
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    if (config?.roi) {
      setForm({ ...config.roi })
    }
  }, [config])

  const update = (key: keyof typeof form, val: string) => {
    setErr('')
    setSaved(false)
    setForm(prev => ({ ...prev, [key]: val === '' ? '' as unknown as number : parseFloat(val) }))
  }

  const validate = (): string | null => {
    for (const f of FIELDS) {
      const v = form[f.key]
      if (v === undefined || v === null || isNaN(v)) return `${f.label} 必须是有效数字`
      if (v < 0) return `${f.label} 不能为负数`
    }
    if (form.high > 10) return 'high 不能超过 10'
    if (!(form.min <= form.low && form.low <= form.target && form.target <= form.high))
      return '必须满足 min ≤ low ≤ target ≤ high'
    return null
  }

  const handleSave = () => {
    const e = validate()
    if (e) { setErr(e); return }
    mutation.mutate(
      { roi: form },
      {
        onSuccess: () => { setSaved(true); setErr('') },
        onError: (error: Error) => setErr(error.message || '保存失败'),
      },
    )
  }

  const handleReset = () => {
    setForm({ ...DEFAULT_INSIGHT_THRESHOLDS.roi })
    setErr('')
    setSaved(false)
  }

  if (isLoading) {
    return (
      <div className="max-w-3xl mx-auto flex items-center justify-center py-32 text-gray-400">
        <Loader2 className="w-6 h-6 animate-spin mr-2" />
        <span className="text-sm">加载配置…</span>
      </div>
    )
  }

  return (
    <div className="max-w-3xl mx-auto">
      <PageHeader
        title="ROI 阈值配置"
        description="设定系统结论引擎使用的 ROI 分层标准，影响所有分析页面的结论生成"
      />

      <SectionCard title="ROI 阈值参数">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
          {FIELDS.map(f => (
            <div key={f.key}>
              <label className="block text-sm font-medium text-gray-700 mb-1">{f.label}</label>
              <input
                type="number"
                step="0.01"
                min="0"
                max="10"
                value={form[f.key]}
                onChange={e => update(f.key, e.target.value)}
                className="w-full rounded-lg border border-gray-200 px-3 py-2 text-sm focus:border-blue-400 focus:ring-1 focus:ring-blue-400 outline-none"
              />
              <p className="text-xs text-gray-400 mt-1">{f.hint}</p>
            </div>
          ))}
        </div>

        {/* 可视化标尺 */}
        <div className="mt-6 px-1">
          <p className="text-xs text-gray-400 mb-2">阈值分布预览</p>
          <div className="relative h-6 bg-gradient-to-r from-red-200 via-amber-200 via-60% to-green-200 rounded-full">
            {FIELDS.map(f => {
              const pos = Math.min(form[f.key] / (form.high || 2) * 100, 100)
              return (
                <div
                  key={f.key}
                  className="absolute top-full mt-1 flex flex-col items-center"
                  style={{ left: `${pos}%`, transform: 'translateX(-50%)' }}
                >
                  <div className="w-0.5 h-2 bg-gray-400" />
                  <span className="text-[10px] text-gray-500 whitespace-nowrap">{f.key}={form[f.key]}</span>
                </div>
              )
            })}
          </div>
        </div>

        {err && <p className="mt-4 text-sm text-red-600">{err}</p>}
        {saved && <p className="mt-4 text-sm text-green-600">保存成功，新配置已生效</p>}

        <div className="flex items-center gap-3 mt-6">
          <button
            onClick={handleSave}
            disabled={mutation.isPending}
            className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {mutation.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            保存配置
          </button>
          <button
            onClick={handleReset}
            className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200"
          >
            <RotateCcw className="w-4 h-4" />
            恢复默认值
          </button>
        </div>
      </SectionCard>

      <SectionCard title="配置说明" className="mt-6">
        <div className="text-sm text-gray-600 leading-relaxed space-y-2">
          <p>结论引擎根据以下规则生成 ROI 相关结论：</p>
          <ul className="list-disc pl-5 space-y-1">
            <li><span className="text-red-600 font-medium">Danger</span>：ROAS &lt; low（{form.low}）— 效率不达标，需重点关注</li>
            <li><span className="text-amber-600 font-medium">Warning</span>：low ≤ ROAS &lt; target（{form.low}~{form.target}）— 接近合格但仍有提升空间</li>
            <li><span className="text-green-600 font-medium">Success</span>：ROAS ≥ high（{form.high}）— 表现优秀，可考虑放量</li>
            <li><span className="text-gray-500 font-medium">Normal</span>：target ≤ ROAS &lt; high — 达标但未达优秀，不额外提示</li>
          </ul>
          <p className="text-xs text-gray-400 mt-3">提示：不同业务阶段（冷启动 / 放量 / 盈利）可能需要不同标准，请根据实际情况调整。</p>
        </div>
      </SectionCard>
    </div>
  )
}
