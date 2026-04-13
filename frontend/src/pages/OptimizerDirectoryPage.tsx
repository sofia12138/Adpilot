import { useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Loader2, AlertCircle, Search, X, Plus, Pencil, Trash2,
  RefreshCw, UserPlus, CheckCircle, XCircle,
} from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { GlobalSyncBar } from '@/components/common/GlobalSyncBar'
import {
  fetchDirectoryList,
  createDirectory,
  updateDirectory,
  toggleDirectoryStatus,
  deleteDirectory,
  fetchUnassignedSamples,
  assignSample,
  rebuildMapping,
  type OptimizerDirectoryItem,
  type UnassignedSample,
  type DirectoryCreatePayload,
} from '@/services/optimizer-directory'

const fmtUsd = (n: number | null | undefined) =>
  n != null ? `$${n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}` : '--'

// ---------------------------------------------------------------------------
// 新增/编辑弹窗
// ---------------------------------------------------------------------------

interface FormDialogProps {
  item?: OptimizerDirectoryItem | null
  onClose: () => void
  onSave: (data: DirectoryCreatePayload & { id?: number }) => void
  saving: boolean
}

function FormDialog({ item, onClose, onSave, saving }: FormDialogProps) {
  const [name, setName] = useState(item?.optimizer_name || '')
  const [code, setCode] = useState(item?.optimizer_code || '')
  const [aliases, setAliases] = useState(item?.aliases || '')
  const [isActive, setIsActive] = useState(item?.is_active ?? 1)
  const [remark, setRemark] = useState(item?.remark || '')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    onSave({
      ...(item ? { id: item.id } : {}),
      optimizer_name: name.trim(),
      optimizer_code: code.trim(),
      aliases: aliases.trim(),
      is_active: isActive,
      remark: remark.trim(),
    })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 overflow-hidden" onClick={e => e.stopPropagation()}>
        <div className="px-6 py-4 border-b border-gray-100">
          <h3 className="text-lg font-semibold text-gray-800">{item ? '编辑优化师' : '新增优化师'}</h3>
        </div>
        <form onSubmit={handleSubmit} className="px-6 py-5 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">标准名称 <span className="text-red-400">*</span></label>
            <input type="text" required className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" value={name} onChange={e => setName(e.target.value)} placeholder="例如：张三" />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">编码 <span className="text-red-400">*</span></label>
            <input type="text" required className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" value={code} onChange={e => setCode(e.target.value)} placeholder="唯一编码，如 ZS" />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">别名</label>
            <input type="text" className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" value={aliases} onChange={e => setAliases(e.target.value)} placeholder="逗号分隔，如: 张3,zhangsan" />
            <p className="text-xs text-gray-400 mt-1">多个别名用逗号分隔，匹配时大小写不敏感</p>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex-1">
              <label className="block text-sm font-medium text-gray-600 mb-1">状态</label>
              <select className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" value={isActive} onChange={e => setIsActive(Number(e.target.value))}>
                <option value={1}>启用</option>
                <option value={0}>停用</option>
              </select>
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-600 mb-1">备注</label>
            <textarea className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none" rows={2} value={remark} onChange={e => setRemark(e.target.value)} placeholder="可选备注信息" />
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

// ---------------------------------------------------------------------------
// 匹配到现有优化师弹窗
// ---------------------------------------------------------------------------

interface AssignDialogProps {
  sample: UnassignedSample
  directory: OptimizerDirectoryItem[]
  onClose: () => void
  onAssign: (optimizer_name_raw: string, optimizer_id: number) => void
  assigning: boolean
}

function AssignDialog({ sample, directory, onClose, onAssign, assigning }: AssignDialogProps) {
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const activeList = directory.filter(d => d.is_active === 1)

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden" onClick={e => e.stopPropagation()}>
        <div className="px-6 py-4 border-b border-gray-100">
          <h3 className="text-lg font-semibold text-gray-800">匹配到现有优化师</h3>
          <p className="text-sm text-gray-400 mt-1">原始值：<span className="font-mono text-gray-600">{sample.optimizer_name_raw}</span></p>
        </div>
        <div className="px-6 py-4 space-y-3">
          <label className="block text-sm font-medium text-gray-600">选择目标优化师</label>
          <select
            className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-100 focus:outline-none"
            value={selectedId ?? ''}
            onChange={e => setSelectedId(e.target.value ? Number(e.target.value) : null)}
          >
            <option value="">请选择...</option>
            {activeList.map(d => (
              <option key={d.id} value={d.id}>{d.optimizer_name} ({d.optimizer_code})</option>
            ))}
          </select>
        </div>
        <div className="px-6 py-4 border-t border-gray-100 flex justify-end gap-3">
          <button type="button" className="px-4 py-2 text-sm text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50" onClick={onClose}>取消</button>
          <button
            disabled={!selectedId || assigning}
            onClick={() => selectedId && onAssign(sample.optimizer_name_raw, selectedId)}
            className="px-4 py-2 text-sm text-white bg-blue-500 rounded-lg hover:bg-blue-600 disabled:opacity-50 flex items-center gap-1.5"
          >
            {assigning && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
            确认匹配
          </button>
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Tab: 优化师名单
// ---------------------------------------------------------------------------

function DirectoryTab() {
  const [keyword, setKeyword] = useState('')
  const [statusFilter, setStatusFilter] = useState<number | undefined>(undefined)
  const [editItem, setEditItem] = useState<OptimizerDirectoryItem | null | undefined>(undefined)
  const queryClient = useQueryClient()

  const { data: list = [], isLoading, isError } = useQuery({
    queryKey: ['optimizer-directory', 'list', keyword, statusFilter],
    queryFn: () => fetchDirectoryList({ keyword: keyword || undefined, is_active: statusFilter }),
    staleTime: 30_000,
  })

  const createMut = useMutation({
    mutationFn: (d: DirectoryCreatePayload) => createDirectory(d),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['optimizer-directory'] }); setEditItem(undefined) },
  })

  const updateMut = useMutation({
    mutationFn: (d: DirectoryCreatePayload & { id?: number }) => {
      if (!d.id) throw new Error('missing id')
      return updateDirectory({ id: d.id, ...d })
    },
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['optimizer-directory'] }); setEditItem(undefined) },
  })

  const toggleMut = useMutation({
    mutationFn: ({ id, is_active }: { id: number; is_active: number }) => toggleDirectoryStatus(id, is_active),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['optimizer-directory'] }),
  })

  const deleteMut = useMutation({
    mutationFn: (id: number) => deleteDirectory(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['optimizer-directory'] }),
  })

  const rebuildMut = useMutation({
    mutationFn: rebuildMapping,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['optimizer-directory'] }),
  })

  const handleSave = useCallback((data: DirectoryCreatePayload & { id?: number }) => {
    if (data.id) {
      updateMut.mutate(data)
    } else {
      createMut.mutate(data)
    }
  }, [createMut, updateMut])

  return (
    <>
      {/* 操作区 */}
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <button className="text-sm px-3 py-1.5 bg-blue-500 text-white rounded-lg hover:bg-blue-600 flex items-center gap-1.5" onClick={() => setEditItem(null)}>
          <Plus className="w-3.5 h-3.5" />新增优化师
        </button>

        <div className="relative flex items-center">
          <Search className="absolute left-2.5 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
          <input
            type="text"
            placeholder="搜索名称/编码/别名..."
            className="text-sm border border-gray-200 rounded-lg pl-8 pr-8 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100 w-48"
            value={keyword}
            onChange={e => setKeyword(e.target.value)}
          />
          {keyword && (
            <button className="absolute right-2 text-gray-300 hover:text-gray-500" onClick={() => setKeyword('')}>
              <X className="w-3.5 h-3.5" />
            </button>
          )}
        </div>

        <select
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-100"
          value={statusFilter ?? ''}
          onChange={e => setStatusFilter(e.target.value === '' ? undefined : Number(e.target.value))}
        >
          <option value="">全部状态</option>
          <option value="1">启用</option>
          <option value="0">停用</option>
        </select>

        <button
          className="text-sm px-3 py-1.5 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 flex items-center gap-1.5 disabled:opacity-50 ml-auto"
          onClick={() => rebuildMut.mutate()}
          disabled={rebuildMut.isPending}
        >
          <RefreshCw className={`w-3.5 h-3.5 ${rebuildMut.isPending ? 'animate-spin' : ''}`} />
          {rebuildMut.isPending ? '重跑中...' : '重跑近30天映射'}
        </button>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-20 text-gray-400">
          <Loader2 className="w-5 h-5 animate-spin mr-2" /><span className="text-sm">加载中...</span>
        </div>
      )}

      {isError && (
        <div className="flex flex-col items-center justify-center py-16 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" />
          <p className="text-sm">数据加载失败</p>
        </div>
      )}

      {!isLoading && !isError && (
        <SectionCard title={`优化师名单（${list.length}）`} noPadding>
          {list.length === 0 ? (
            <div className="px-5 py-12 text-center text-sm text-gray-300">暂无优化师数据，请点击"新增优化师"</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100">
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">标准名称</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">编码</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">别名</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-400">状态</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">备注</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">更新时间</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-400">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {list.map(item => (
                    <tr key={item.id} className="border-b border-gray-50 hover:bg-blue-50/20 transition-colors">
                      <td className="px-4 py-3 font-medium text-gray-800">{item.optimizer_name}</td>
                      <td className="px-4 py-3 text-gray-600 font-mono text-xs">{item.optimizer_code}</td>
                      <td className="px-4 py-3 text-gray-500 text-xs max-w-[200px] truncate" title={item.aliases}>{item.aliases || '-'}</td>
                      <td className="px-4 py-3 text-center">
                        {item.is_active ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs bg-green-50 text-green-600">
                            <CheckCircle className="w-3 h-3" />启用
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs bg-gray-100 text-gray-400">
                            <XCircle className="w-3 h-3" />停用
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-gray-400 text-xs max-w-[150px] truncate" title={item.remark}>{item.remark || '-'}</td>
                      <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">{item.updated_at?.slice(0, 16)}</td>
                      <td className="px-4 py-3 text-center">
                        <div className="flex items-center justify-center gap-1">
                          <button className="p-1.5 text-gray-400 hover:text-blue-500 hover:bg-blue-50 rounded-lg transition-colors" title="编辑" onClick={() => setEditItem(item)}>
                            <Pencil className="w-3.5 h-3.5" />
                          </button>
                          <button
                            className="p-1.5 text-gray-400 hover:text-amber-500 hover:bg-amber-50 rounded-lg transition-colors"
                            title={item.is_active ? '停用' : '启用'}
                            onClick={() => toggleMut.mutate({ id: item.id, is_active: item.is_active ? 0 : 1 })}
                          >
                            {item.is_active ? <XCircle className="w-3.5 h-3.5" /> : <CheckCircle className="w-3.5 h-3.5" />}
                          </button>
                          <button
                            className="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors"
                            title="删除"
                            onClick={() => { if (confirm(`确认删除优化师「${item.optimizer_name}」？`)) deleteMut.mutate(item.id) }}
                          >
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
        <FormDialog
          item={editItem}
          onClose={() => setEditItem(undefined)}
          onSave={handleSave}
          saving={createMut.isPending || updateMut.isPending}
        />
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// Tab: 未识别样本
// ---------------------------------------------------------------------------

function UnassignedTab() {
  const [assignTarget, setAssignTarget] = useState<UnassignedSample | null>(null)
  const queryClient = useQueryClient()

  const { data: samples = [], isLoading, isError } = useQuery({
    queryKey: ['optimizer-directory', 'unassigned'],
    queryFn: () => fetchUnassignedSamples(),
    staleTime: 30_000,
  })

  const { data: directory = [] } = useQuery({
    queryKey: ['optimizer-directory', 'list-for-assign'],
    queryFn: () => fetchDirectoryList({ is_active: 1 }),
    staleTime: 60_000,
  })

  const assignMut = useMutation({
    mutationFn: ({ raw, optId }: { raw: string; optId: number }) => assignSample(raw, optId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['optimizer-directory'] })
      setAssignTarget(null)
    },
  })

  const rebuildMut = useMutation({
    mutationFn: rebuildMapping,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['optimizer-directory'] }),
  })

  return (
    <>
      <div className="flex items-center gap-3 mb-4">
        <p className="text-sm text-gray-500">以下是 campaign 中未在优化师名单中匹配到的原始值样本（近30天）</p>
        <button
          className="text-sm px-3 py-1.5 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 flex items-center gap-1.5 disabled:opacity-50 ml-auto"
          onClick={() => rebuildMut.mutate()}
          disabled={rebuildMut.isPending}
        >
          <RefreshCw className={`w-3.5 h-3.5 ${rebuildMut.isPending ? 'animate-spin' : ''}`} />
          {rebuildMut.isPending ? '重跑中...' : '重跑映射'}
        </button>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-20 text-gray-400">
          <Loader2 className="w-5 h-5 animate-spin mr-2" /><span className="text-sm">加载中...</span>
        </div>
      )}

      {isError && (
        <div className="flex flex-col items-center justify-center py-16 text-red-400">
          <AlertCircle className="w-8 h-8 mb-2" />
          <p className="text-sm">数据加载失败</p>
        </div>
      )}

      {!isLoading && !isError && (
        <SectionCard title={`未识别样本（${samples.length}）`} noPadding>
          {samples.length === 0 ? (
            <div className="px-5 py-12 text-center text-sm text-gray-300">暂无未识别样本</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100">
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">原始值</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-400">出现次数</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-400">对应消耗</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-400">最近出现</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-400">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {samples.map(s => (
                    <tr key={s.optimizer_name_raw} className="border-b border-gray-50 hover:bg-blue-50/20 transition-colors">
                      <td className="px-4 py-3 font-mono text-xs text-gray-700">{s.optimizer_name_raw || '(空)'}</td>
                      <td className="px-4 py-3 text-right tabular-nums text-gray-600">{s.occurrence_count}</td>
                      <td className="px-4 py-3 text-right tabular-nums text-gray-700 font-medium">{fmtUsd(s.total_spend)}</td>
                      <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">{s.last_seen_at?.slice(0, 16)}</td>
                      <td className="px-4 py-3 text-center">
                        <div className="flex items-center justify-center gap-1">
                          <button
                            className="p-1.5 text-gray-400 hover:text-blue-500 hover:bg-blue-50 rounded-lg transition-colors flex items-center gap-1 text-xs"
                            title="匹配到现有优化师"
                            onClick={() => setAssignTarget(s)}
                          >
                            <UserPlus className="w-3.5 h-3.5" />匹配
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

      {assignTarget && (
        <AssignDialog
          sample={assignTarget}
          directory={directory}
          onClose={() => setAssignTarget(null)}
          onAssign={(raw, optId) => assignMut.mutate({ raw, optId })}
          assigning={assignMut.isPending}
        />
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// 主页面
// ---------------------------------------------------------------------------

export default function OptimizerDirectoryPage() {
  const [activeTab, setActiveTab] = useState<'directory' | 'unassigned'>('directory')

  const tabs = [
    { key: 'directory' as const, label: '优化师名单' },
    { key: 'unassigned' as const, label: '未识别样本' },
  ]

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="优化师名单配置" description="管理优化师名单，用于 campaign 自动匹配优化师归属" />
      <GlobalSyncBar />

      {/* Tab 切换 */}
      <div className="flex border-b border-gray-200 mb-6">
        {tabs.map(tab => (
          <button
            key={tab.key}
            className={`px-5 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.key
                ? 'text-blue-600 border-blue-500'
                : 'text-gray-400 border-transparent hover:text-gray-600 hover:border-gray-200'
            }`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === 'directory' ? <DirectoryTab /> : <UnassignedTab />}
    </div>
  )
}
