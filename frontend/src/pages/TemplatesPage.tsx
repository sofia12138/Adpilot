import { useState } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { DataTable, type Column } from '@/components/common/DataTable'
import { Loader2, AlertCircle, Plus, Trash2, Pencil, Copy, X, ToggleLeft, ToggleRight } from 'lucide-react'
import { useTemplates, useCreateTemplate, useUpdateTemplate, useDeleteTemplate } from '@/hooks/use-templates'
import type { Template } from '@/services/templates'

const platformBadge = (p: string) => {
  const cls = p === 'tiktok' ? 'bg-sky-50 text-sky-600' : p === 'meta' ? 'bg-indigo-50 text-indigo-600' : 'bg-gray-100 text-gray-600'
  return <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>{p}</span>
}

function extractTags(t: Template): string[] {
  const tags: string[] = []
  if (t.objective_type) tags.push(String(t.objective_type))
  if (t.optimization_goal) tags.push(String(t.optimization_goal))
  if (t.billing_event) tags.push(String(t.billing_event))
  return tags.slice(0, 3)
}

interface FormState {
  mode: 'create' | 'edit'
  id: string
  name: string
  platform: string
  country: string
  budget: string
  bidding_strategy: string
  status: string
}

const emptyForm: FormState = {
  mode: 'create', id: '', name: '', platform: 'tiktok',
  country: '', budget: '', bidding_strategy: '', status: 'active',
}

export default function TemplatesPage() {
  const { data: templates, isLoading, isError } = useTemplates()
  const createMutation = useCreateTemplate()
  const updateMutation = useUpdateTemplate()
  const deleteMutation = useDeleteTemplate()

  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState<FormState>(emptyForm)
  const [platformFilter, setPlatformFilter] = useState<string | null>(null)

  const filtered = (templates ?? []).filter(t => !platformFilter || t.platform === platformFilter)

  const isBuiltin = (t: Template) => String(t.id).startsWith('tpl_')
    && ['tpl_tiktok_android_purchase', 'tpl_web_to_app', 'tpl_miniapp_troas'].includes(t.id)

  function openCreate() { setForm(emptyForm); setShowForm(true) }

  function openEdit(t: Template) {
    setForm({
      mode: 'edit', id: t.id, name: t.name, platform: t.platform,
      country: String(t.country ?? ''), budget: String(t.budget ?? ''),
      bidding_strategy: String(t.bidding_strategy ?? ''),
      status: String(t.status ?? 'active'),
    })
    setShowForm(true)
  }

  function handleCopy(t: Template) {
    setForm({
      mode: 'create', id: '', name: `${t.name}_copy`, platform: t.platform,
      country: String(t.country ?? ''), budget: String(t.budget ?? ''),
      bidding_strategy: String(t.bidding_strategy ?? ''), status: 'active',
    })
    setShowForm(true)
  }

  function handleToggleStatus(t: Template) {
    const newStatus = String(t.status ?? 'active') === 'active' ? 'disabled' : 'active'
    updateMutation.mutate({ tplId: t.id, body: { status: newStatus } })
  }

  function handleSubmit() {
    const body: Record<string, unknown> = { name: form.name.trim(), platform: form.platform }
    if (form.country) body.country = form.country
    if (form.budget) body.budget = Number(form.budget)
    if (form.bidding_strategy) body.bidding_strategy = form.bidding_strategy
    if (form.status) body.status = form.status

    if (form.mode === 'create') {
      createMutation.mutate(body, { onSuccess: () => setShowForm(false) })
    } else {
      updateMutation.mutate({ tplId: form.id, body }, { onSuccess: () => setShowForm(false) })
    }
  }

  function handleDelete(tplId: string) {
    if (!confirm('确定删除此模板？')) return
    deleteMutation.mutate(tplId)
  }

  const isPending = createMutation.isPending || updateMutation.isPending

  const columns: Column<Template>[] = [
    { key: 'name', title: '模板名称', render: (r) => <span className="font-medium text-gray-800">{r.name}</span> },
    { key: 'platform', title: '平台', render: (r) => platformBadge(r.platform) },
    { key: 'country', title: '国家', render: (r) => <span className="text-xs text-gray-500">{String(r.country || '-')}</span> },
    { key: 'budget', title: '预算', align: 'right', render: (r) => <span className="text-xs text-gray-500">{r.budget ? `$${r.budget}` : '-'}</span> },
    { key: 'tags', title: '标签', render: (r) => {
      const tags = extractTags(r)
      return (
        <div className="flex gap-1 flex-wrap">
          {tags.map(t => <span key={t} className="inline-block px-2 py-0.5 rounded-full text-xs bg-gray-100 text-gray-500">{t}</span>)}
          {tags.length === 0 && <span className="text-xs text-gray-300">-</span>}
        </div>
      )
    }},
    { key: 'status', title: '状态', render: (r) => {
      const st = String(r.status ?? 'active')
      return <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${st === 'active' ? 'bg-green-50 text-green-600' : 'bg-gray-100 text-gray-400'}`}>
        {st === 'active' ? '启用' : '停用'}
      </span>
    }},
    { key: 'actions', title: '', render: (r) => (
      <div className="flex items-center gap-1.5">
        <button onClick={() => openEdit(r)} className="p-1 text-gray-400 hover:text-blue-500 transition" title="编辑"><Pencil className="w-3.5 h-3.5" /></button>
        <button onClick={() => handleCopy(r)} className="p-1 text-gray-400 hover:text-blue-500 transition" title="复制"><Copy className="w-3.5 h-3.5" /></button>
        <button onClick={() => handleToggleStatus(r)} className="p-1 text-gray-400 hover:text-amber-500 transition" title={String(r.status ?? 'active') === 'active' ? '停用' : '启用'}>
          {String(r.status ?? 'active') === 'active' ? <ToggleRight className="w-3.5 h-3.5" /> : <ToggleLeft className="w-3.5 h-3.5" />}
        </button>
        {!isBuiltin(r) && <button onClick={() => handleDelete(r.id)} className="p-1 text-gray-400 hover:text-red-500 transition" title="删除"><Trash2 className="w-3.5 h-3.5" /></button>}
      </div>
    )},
  ]

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="模板管理" description="沉淀投放策略，快速复用"
        action={<button onClick={openCreate} className="flex items-center gap-1.5 px-4 py-2 bg-blue-500 text-white text-sm rounded-lg hover:bg-blue-600 transition-colors"><Plus className="w-4 h-4" /> 新建模板</button>}
      />

      <div className="flex items-center gap-2 mb-4">
        <span className="text-xs text-gray-400">平台</span>
        {[null, 'tiktok', 'meta'].map(p => (
          <button key={p ?? 'all'} onClick={() => setPlatformFilter(p)}
            className={`px-3 py-1 rounded-full text-xs transition ${platformFilter === p ? 'bg-blue-500 text-white font-medium' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'}`}>
            {p === null ? '全部' : p === 'tiktok' ? 'TikTok' : 'Meta'}
          </button>
        ))}
      </div>

      {showForm && (
        <div className="bg-white rounded-xl border border-blue-200 p-5 mb-4 relative">
          <button onClick={() => setShowForm(false)} className="absolute top-3 right-3 text-gray-400 hover:text-gray-600"><X className="w-4 h-4" /></button>
          <h3 className="text-sm font-medium text-gray-800 mb-3">{form.mode === 'create' ? '新建模板' : `编辑模板：${form.name}`}</h3>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <div><label className="text-xs text-gray-500 block mb-1">模板名称</label><input value={form.name} onChange={e => setForm(f => ({...f, name: e.target.value}))} placeholder="例：US-iOS-放量" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300" /></div>
            <div><label className="text-xs text-gray-500 block mb-1">平台</label><select value={form.platform} onChange={e => setForm(f => ({...f, platform: e.target.value}))} className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"><option value="tiktok">TikTok</option><option value="meta">Meta</option></select></div>
            <div><label className="text-xs text-gray-500 block mb-1">国家</label><input value={form.country} onChange={e => setForm(f => ({...f, country: e.target.value}))} placeholder="US, JP" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300" /></div>
            <div><label className="text-xs text-gray-500 block mb-1">日预算</label><input type="number" value={form.budget} onChange={e => setForm(f => ({...f, budget: e.target.value}))} placeholder="500" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300" /></div>
            <div><label className="text-xs text-gray-500 block mb-1">出价策略</label><select value={form.bidding_strategy} onChange={e => setForm(f => ({...f, bidding_strategy: e.target.value}))} className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"><option value="">不限</option><option value="LOWEST_COST">最低成本</option><option value="COST_CAP">成本上限</option><option value="BID_CAP">出价上限</option></select></div>
            <div><label className="text-xs text-gray-500 block mb-1">状态</label><select value={form.status} onChange={e => setForm(f => ({...f, status: e.target.value}))} className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"><option value="active">启用</option><option value="disabled">停用</option></select></div>
          </div>
          <div className="mt-4 flex gap-2">
            <button onClick={handleSubmit} disabled={isPending || !form.name.trim()} className="px-4 py-2 bg-blue-500 text-white text-sm rounded-lg hover:bg-blue-600 disabled:opacity-50 transition">{isPending ? '提交中...' : form.mode === 'create' ? '创建' : '保存'}</button>
            <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-100 text-gray-600 text-sm rounded-lg hover:bg-gray-200 transition">取消</button>
          </div>
        </div>
      )}

      {isLoading && <div className="flex items-center justify-center py-32 text-gray-400"><Loader2 className="w-6 h-6 animate-spin mr-2" /><span className="text-sm">加载中...</span></div>}
      {isError && <div className="flex flex-col items-center justify-center py-24 text-red-400"><AlertCircle className="w-8 h-8 mb-2" /><p className="text-sm font-medium">数据加载失败</p></div>}
      {!isLoading && !isError && (
        <SectionCard title={`模板列表（${filtered.length}）`} noPadding>
          <DataTable columns={columns} data={filtered} rowKey={(r) => r.id} />
        </SectionCard>
      )}
    </div>
  )
}
