import { useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Loader2, AlertCircle, Search, X, Plus, Pencil, Trash2,
  CheckCircle, XCircle, Globe,
} from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import {
  fetchRegionGroups, createRegionGroup, updateRegionGroup,
  deleteRegionGroup, toggleRegionGroup,
  type RegionGroupAsset,
} from '@/services/ad-assets'

/* ── 表单弹窗 ── */

interface FormDialogProps {
  item?: RegionGroupAsset | null
  onClose: () => void
  onSave: (data: Partial<RegionGroupAsset>) => void
  saving: boolean
}

function FormDialog({ item, onClose, onSave, saving }: FormDialogProps) {
  const [name, setName] = useState(item?.name || '')
  const [countryCodes, setCountryCodes] = useState(item?.country_codes?.join(', ') || '')
  const [languageHint, setLanguageHint] = useState(item?.language_hint || '')
  const [remark, setRemark] = useState(item?.remark || '')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const codes = countryCodes.split(',').map(s => s.trim().toUpperCase()).filter(Boolean)
    onSave({
      ...(item ? { id: item.id } : {}),
      name: name.trim(),
      country_codes: codes,
      language_hint: languageHint.trim(),
      remark: remark.trim(),
    })
  }

  const parsedCount = countryCodes.split(',').map(s => s.trim()).filter(Boolean).length

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 overflow-hidden" onClick={e => e.stopPropagation()}>
        <div className="px-6 py-4 border-b border-gray-100">
          <h3 className="text-lg font-semibold text-gray-800">{item ? '编辑地区组' : '新建地区组'}</h3>
        </div>
        <form onSubmit={handleSubmit} className="px-6 py-5 space-y-4 max-h-[70vh] overflow-y-auto">
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">名称 <span className="text-red-400">*</span></label>
            <input type="text" required className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" value={name} onChange={e => setName(e.target.value)} placeholder="如：东南亚核心 / 北美T1" />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">国家代码 <span className="text-red-400">*</span></label>
            <textarea className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none font-mono" rows={3} value={countryCodes} onChange={e => setCountryCodes(e.target.value)} placeholder="US, CA, GB, AU（ISO 3166-1 alpha-2，逗号分隔）" />
            <p className="text-xs text-gray-400 mt-1">当前 {parsedCount} 个国家</p>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">语言提示</label>
            <input type="text" className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" value={languageHint} onChange={e => setLanguageHint(e.target.value)} placeholder="en / zh / multi" />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">备注</label>
            <textarea className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" rows={2} value={remark} onChange={e => setRemark(e.target.value)} />
          </div>
          <div className="flex justify-end gap-3 pt-2">
            <button type="button" className="px-4 py-2 text-sm text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50" onClick={onClose}>取消</button>
            <button type="submit" disabled={saving} className="px-4 py-2 text-sm text-white bg-blue-500 rounded-lg hover:bg-blue-600 disabled:opacity-50 flex items-center gap-1.5">
              {saving && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
              {item ? '保存' : '创建'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

/* ── 主页面 ── */

export default function AssetRegionGroupsPage() {
  const [keyword, setKeyword] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [editItem, setEditItem] = useState<RegionGroupAsset | null | undefined>(undefined)
  const queryClient = useQueryClient()

  const { data: list = [], isLoading, isError } = useQuery({
    queryKey: ['asset-region-groups', keyword, statusFilter],
    queryFn: () => fetchRegionGroups({ keyword: keyword || undefined, status: statusFilter || undefined }),
    staleTime: 30_000,
  })

  const createMut = useMutation({
    mutationFn: (d: Partial<RegionGroupAsset>) => createRegionGroup(d),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['asset-region-groups'] }); setEditItem(undefined) },
  })

  const updateMut = useMutation({
    mutationFn: (d: Partial<RegionGroupAsset> & { id: number }) => updateRegionGroup(d.id, d),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['asset-region-groups'] }); setEditItem(undefined) },
  })

  const toggleMut = useMutation({
    mutationFn: (id: number) => toggleRegionGroup(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['asset-region-groups'] }),
  })

  const deleteMut = useMutation({
    mutationFn: (id: number) => deleteRegionGroup(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['asset-region-groups'] }),
  })

  const handleSave = useCallback((data: Partial<RegionGroupAsset>) => {
    if (data.id) {
      updateMut.mutate(data as Partial<RegionGroupAsset> & { id: number })
    } else {
      createMut.mutate(data)
    }
  }, [createMut, updateMut])

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="地区组库" description="预定义投放国家组合，广告创建时可一键选择地区组快速填充投放国家" />

      <div className="flex flex-wrap items-center gap-3 mb-4">
        <button className="text-sm px-3 py-1.5 bg-blue-500 text-white rounded-lg hover:bg-blue-600 flex items-center gap-1.5" onClick={() => setEditItem(null)}>
          <Plus className="w-3.5 h-3.5" />新建地区组
        </button>
        <div className="relative flex items-center">
          <Search className="absolute left-2.5 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
          <input type="text" placeholder="搜索名称..." className="text-sm border border-gray-200 rounded-lg pl-8 pr-8 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100 w-48" value={keyword} onChange={e => setKeyword(e.target.value)} />
          {keyword && <button className="absolute right-2 text-gray-300 hover:text-gray-500" onClick={() => setKeyword('')}><X className="w-3.5 h-3.5" /></button>}
        </div>
        <select className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100" value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
          <option value="">全部状态</option>
          <option value="active">启用</option>
          <option value="inactive">停用</option>
        </select>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-20 text-gray-400">
          <Loader2 className="w-5 h-5 animate-spin mr-2" /><span className="text-sm">加载中...</span>
        </div>
      )}

      {isError && (
        <div className="flex flex-col items-center justify-center py-16 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" /><p className="text-sm">数据加载失败</p>
        </div>
      )}

      {!isLoading && !isError && (
        <SectionCard title={`地区组（${list.length}）`} noPadding>
          {list.length === 0 ? (
            <div className="px-5 py-12 text-center text-sm text-gray-300">暂无地区组，请点击"新建地区组"</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100">
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">名称</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">国家代码</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-400">国家数</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">语言</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-400">状态</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">创建者</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">更新时间</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-400">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {list.map(item => (
                    <tr key={item.id} className="border-b border-gray-50 hover:bg-blue-50/20 transition-colors">
                      <td className="px-4 py-3 font-medium text-gray-800 flex items-center gap-1.5">
                        <Globe className="w-3.5 h-3.5 text-gray-300 flex-shrink-0" />
                        {item.name}
                      </td>
                      <td className="px-4 py-3 text-gray-500 text-xs max-w-[300px]">
                        <div className="flex flex-wrap gap-1">
                          {item.country_codes.slice(0, 10).map(c => (
                            <span key={c} className="inline-block px-1.5 py-0.5 rounded bg-blue-50 text-blue-600 font-mono text-[10px]">{c}</span>
                          ))}
                          {item.country_codes.length > 10 && (
                            <span className="inline-block px-1.5 py-0.5 rounded bg-gray-50 text-gray-400 text-[10px]">+{item.country_codes.length - 10}</span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-3 text-center text-gray-600 tabular-nums">{item.country_count}</td>
                      <td className="px-4 py-3 text-gray-600 text-xs">{item.language_hint || '-'}</td>
                      <td className="px-4 py-3 text-center">
                        {item.status === 'active' ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs bg-green-50 text-green-600"><CheckCircle className="w-3 h-3" />启用</span>
                        ) : (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs bg-gray-100 text-gray-400"><XCircle className="w-3 h-3" />停用</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-gray-400 text-xs">{item.created_by || '-'}</td>
                      <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">{item.updated_at?.slice(0, 16)}</td>
                      <td className="px-4 py-3 text-center">
                        <div className="flex items-center justify-center gap-1">
                          <button className="p-1.5 text-gray-400 hover:text-blue-500 hover:bg-blue-50 rounded-lg transition-colors" title="编辑" onClick={() => setEditItem(item)}><Pencil className="w-3.5 h-3.5" /></button>
                          <button className="p-1.5 text-gray-400 hover:text-amber-500 hover:bg-amber-50 rounded-lg transition-colors" title={item.status === 'active' ? '停用' : '启用'} onClick={() => toggleMut.mutate(item.id)}>
                            {item.status === 'active' ? <XCircle className="w-3.5 h-3.5" /> : <CheckCircle className="w-3.5 h-3.5" />}
                          </button>
                          <button className="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors" title="删除" onClick={() => { if (confirm(`确认删除地区组「${item.name}」？`)) deleteMut.mutate(item.id) }}>
                            <Trash2 className="w-3.5 h-3.5" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </SectionCard>
      )}

      {editItem !== undefined && (
        <FormDialog item={editItem} onClose={() => setEditItem(undefined)} onSave={handleSave} saving={createMut.isPending || updateMut.isPending} />
      )}
    </div>
  )
}
