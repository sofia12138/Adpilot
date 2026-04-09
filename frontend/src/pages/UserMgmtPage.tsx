import { useState } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { DataTable, type Column } from '@/components/common/DataTable'
import { Loader2, AlertCircle, Plus, Trash2, Pencil, X, LogIn, Shield, RotateCcw, CreditCard } from 'lucide-react'
import { useUsers, useCreateUser, useUpdateUser, useDeleteUser } from '@/hooks/use-users'
import { usePanels, useUserPanels, useUpdateUserPanels, useResetUserPanels } from '@/hooks/use-panels'
import { useAdAccounts } from '@/hooks/use-ad-accounts'
import { AuthError } from '@/services/api'
import type { UserInfo, CreateUserBody, UpdateUserBody } from '@/services/users'
import { ROLE_LABELS } from '@/types/menu'

const roleBadge = (role: string) => {
  const map: Record<string, string> = {
    super_admin: 'bg-purple-50 text-purple-600',
    admin: 'bg-red-50 text-red-600',
    optimizer: 'bg-blue-50 text-blue-600',
    designer: 'bg-pink-50 text-pink-600',
    analyst: 'bg-teal-50 text-teal-600',
    viewer: 'bg-gray-100 text-gray-500',
  }
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${map[role] ?? 'bg-gray-100 text-gray-500'}`}>
      {ROLE_LABELS[role] ?? role}
    </span>
  )
}

interface FormState {
  mode: 'create' | 'edit'
  username: string; password: string; role: string; display_name: string
}
const emptyForm: FormState = { mode: 'create', username: '', password: '', role: 'optimizer', display_name: '' }

export default function UserMgmtPage() {
  const { data: users, isLoading, isError, error } = useUsers()
  const createMutation = useCreateUser()
  const updateMutation = useUpdateUser()
  const deleteMutation = useDeleteUser()
  const isAuthError = error instanceof AuthError

  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState<FormState>(emptyForm)

  // Account authorization
  const [acctEditUser, setAcctEditUser] = useState('')
  const [selectedAccounts, setSelectedAccounts] = useState<string[]>([])
  const [acctDirty, setAcctDirty] = useState(false)
  const { data: allAccounts, isLoading: acctLoading } = useAdAccounts()

  function openAcctEdit(u: UserInfo) {
    setAcctEditUser(u.username)
    setSelectedAccounts(u.assigned_accounts ?? [])
    setAcctDirty(false)
  }

  function closeAcctEdit() {
    setAcctEditUser('')
    setSelectedAccounts([])
    setAcctDirty(false)
  }

  function toggleAccount(accountId: string) {
    setAcctDirty(true)
    setSelectedAccounts(prev =>
      prev.includes(accountId) ? prev.filter(id => id !== accountId) : [...prev, accountId]
    )
  }

  function saveAccounts() {
    const body: UpdateUserBody = { assigned_accounts: selectedAccounts }
    updateMutation.mutate({ username: acctEditUser, body }, {
      onSuccess: () => { setAcctDirty(false) },
    })
  }

  // Panel authorization
  const [panelEditUser, setPanelEditUser] = useState('')
  const { data: allPanels } = usePanels()
  const { data: userPanelData, isLoading: panelLoading } = useUserPanels(panelEditUser)
  const updateUserPanels = useUpdateUserPanels()
  const resetUserPanels = useResetUserPanels()
  const [selectedPanels, setSelectedPanels] = useState<string[]>([])
  const [panelDirty, setPanelDirty] = useState(false)

  function openPanelEdit(u: UserInfo) {
    setPanelEditUser(u.username)
    setPanelDirty(false)
  }

  function closePanelEdit() {
    setPanelEditUser('')
    setSelectedPanels([])
    setPanelDirty(false)
  }

  // sync selected panels when data loads
  if (userPanelData && !panelDirty && panelEditUser) {
    const current = userPanelData.allowed_panels ?? []
    if (JSON.stringify(current) !== JSON.stringify(selectedPanels)) {
      setSelectedPanels(current)
    }
  }

  function togglePanel(key: string) {
    setPanelDirty(true)
    setSelectedPanels(prev => prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key])
  }

  function savePanels() {
    updateUserPanels.mutate({ username: panelEditUser, panelKeys: selectedPanels }, {
      onSuccess: () => { setPanelDirty(false) },
    })
  }

  function resetPanels() {
    if (!confirm('确定重置为角色默认权限？')) return
    resetUserPanels.mutate(panelEditUser, {
      onSuccess: () => { setPanelDirty(false) },
    })
  }

  function openCreate() { setForm(emptyForm); setShowForm(true) }
  function openEdit(u: UserInfo) {
    setForm({ mode: 'edit', username: u.username, password: '', role: u.role, display_name: u.display_name })
    setShowForm(true)
  }

  function handleSubmit() {
    if (form.mode === 'create') {
      if (!form.username.trim() || !form.password.trim()) return
      const body: CreateUserBody = { username: form.username.trim(), password: form.password, role: form.role, display_name: form.display_name.trim() }
      createMutation.mutate(body, { onSuccess: () => setShowForm(false) })
    } else {
      const body: UpdateUserBody = { role: form.role, display_name: form.display_name.trim() }
      if (form.password) body.password = form.password
      updateMutation.mutate({ username: form.username, body }, { onSuccess: () => setShowForm(false) })
    }
  }

  function handleDelete(username: string) {
    if (!confirm(`确定删除用户 "${username}"？`)) return
    deleteMutation.mutate(username)
  }

  const isPending = createMutation.isPending || updateMutation.isPending

  const columns: Column<UserInfo>[] = [
    { key: 'username', title: '用户名', render: (r) => <span className="font-medium text-gray-800">{r.username}</span> },
    { key: 'display_name', title: '显示名', render: (r) => <span className="text-gray-600">{r.display_name || '-'}</span> },
    { key: 'role', title: '角色', render: (r) => roleBadge(r.role) },
    { key: 'accounts', title: '关联账户', render: (r) => (
      <div className="flex gap-1 flex-wrap">
        {(r.assigned_accounts ?? []).length > 0
          ? r.assigned_accounts.map(a => <span key={a} className="inline-block px-2 py-0.5 rounded text-xs bg-gray-50 text-gray-500 border border-gray-100">{a}</span>)
          : <span className="text-xs text-gray-300">-</span>}
      </div>
    )},
    { key: 'actions', title: '', render: (r) => (
      <div className="flex items-center gap-2">
        <button onClick={() => openAcctEdit(r)} className="text-gray-400 hover:text-green-500 transition-colors" title="账户授权"><CreditCard className="w-3.5 h-3.5" /></button>
        <button onClick={() => openPanelEdit(r)} className="text-gray-400 hover:text-purple-500 transition-colors" title="面板授权"><Shield className="w-3.5 h-3.5" /></button>
        <button onClick={() => openEdit(r)} className="text-gray-400 hover:text-blue-500 transition-colors" title="编辑"><Pencil className="w-3.5 h-3.5" /></button>
        <button onClick={() => handleDelete(r.username)} className="text-gray-400 hover:text-red-500 transition-colors" title="删除"><Trash2 className="w-3.5 h-3.5" /></button>
      </div>
    )},
  ]

  const panelGroups = allPanels
    ? [...new Set(allPanels.map(p => p.panel_group))].map(g => ({
        group: g,
        panels: allPanels.filter(p => p.panel_group === g).sort((a, b) => a.sort_order - b.sort_order),
      }))
    : []

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="用户权限" description="管理系统用户与面板授权"
        action={!isAuthError ? (
          <button onClick={openCreate} className="flex items-center gap-1.5 px-4 py-2 bg-blue-500 text-white text-sm rounded-lg hover:bg-blue-600 transition-colors"><Plus className="w-4 h-4" /> 新建用户</button>
        ) : undefined}
      />

      {/* 用户创建/编辑表单 */}
      {showForm && (
        <div className="bg-white rounded-xl border border-blue-200 p-5 mb-4 relative">
          <button onClick={() => setShowForm(false)} className="absolute top-3 right-3 text-gray-400 hover:text-gray-600"><X className="w-4 h-4" /></button>
          <h3 className="text-sm font-medium text-gray-800 mb-3">{form.mode === 'create' ? '新建用户' : `编辑用户：${form.username}`}</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div><label className="text-xs text-gray-500 block mb-1">用户名</label><input value={form.username} onChange={e => setForm(f => ({...f, username: e.target.value}))} disabled={form.mode === 'edit'} placeholder="用户名" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300 disabled:bg-gray-50 disabled:text-gray-400" /></div>
            <div><label className="text-xs text-gray-500 block mb-1">{form.mode === 'create' ? '密码' : '新密码（留空不改）'}</label><input type="password" value={form.password} onChange={e => setForm(f => ({...f, password: e.target.value}))} placeholder={form.mode === 'edit' ? '留空不修改' : '密码'} className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300" /></div>
            <div><label className="text-xs text-gray-500 block mb-1">显示名</label><input value={form.display_name} onChange={e => setForm(f => ({...f, display_name: e.target.value}))} placeholder="显示名" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300" /></div>
            <div><label className="text-xs text-gray-500 block mb-1">角色</label>
              <select value={form.role} onChange={e => setForm(f => ({...f, role: e.target.value}))} className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300">
                {Object.entries(ROLE_LABELS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}
              </select>
            </div>
          </div>
          <div className="mt-4 flex gap-2">
            <button onClick={handleSubmit} disabled={isPending} className="px-4 py-2 bg-blue-500 text-white text-sm rounded-lg hover:bg-blue-600 disabled:opacity-50 transition">{isPending ? '提交中...' : form.mode === 'create' ? '创建' : '保存'}</button>
            <button onClick={() => setShowForm(false)} className="px-4 py-2 bg-gray-100 text-gray-600 text-sm rounded-lg hover:bg-gray-200 transition">取消</button>
          </div>
        </div>
      )}

      {/* 账户授权编辑区 */}
      {acctEditUser && (
        <div className="bg-white rounded-xl border border-green-200 p-5 mb-4 relative">
          <button onClick={closeAcctEdit} className="absolute top-3 right-3 text-gray-400 hover:text-gray-600"><X className="w-4 h-4" /></button>
          <div className="flex items-center gap-3 mb-4">
            <CreditCard className="w-5 h-5 text-green-500" />
            <h3 className="text-sm font-medium text-gray-800">账户授权：{acctEditUser}</h3>
            <span className="text-xs text-gray-400">已选 {selectedAccounts.length} 个账户</span>
          </div>

          {acctLoading ? (
            <div className="flex items-center gap-2 py-6 text-gray-400"><Loader2 className="w-4 h-4 animate-spin" /><span className="text-sm">加载中...</span></div>
          ) : (
            <>
              {(!allAccounts || allAccounts.length === 0) ? (
                <p className="text-sm text-gray-400 py-4">暂无广告账号，请先在数据源中添加</p>
              ) : (
                <div className="space-y-4">
                  {[...new Set(allAccounts.map(a => a.platform))].map(platform => (
                    <div key={platform}>
                      <p className="text-xs font-medium text-gray-500 mb-2 uppercase">{platform}</p>
                      <div className="flex flex-wrap gap-2">
                        {allAccounts.filter(a => a.platform === platform).map(acct => {
                          const checked = selectedAccounts.includes(acct.account_id)
                          return (
                            <label key={acct.account_id} className={`flex items-center gap-2 px-3 py-2 rounded-lg border cursor-pointer transition ${
                              checked ? 'border-green-300 bg-green-50' : 'border-gray-200 hover:border-gray-300'
                            }`}>
                              <input type="checkbox" checked={checked} onChange={() => toggleAccount(acct.account_id)}
                                className="w-3.5 h-3.5 rounded border-gray-300 text-green-500 focus:ring-green-500/20" />
                              <span className="text-xs text-gray-700">{acct.account_name || acct.account_id}</span>
                              <span className="text-[10px] text-gray-400">{acct.account_id}</span>
                            </label>
                          )
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              )}
              <div className="mt-4 flex gap-2">
                <button onClick={saveAccounts} disabled={updateMutation.isPending || !acctDirty}
                  className="px-4 py-2 bg-green-500 text-white text-sm rounded-lg hover:bg-green-600 disabled:opacity-50 transition">
                  {updateMutation.isPending ? '保存中...' : '保存授权'}
                </button>
                <button onClick={closeAcctEdit} className="px-4 py-2 bg-gray-100 text-gray-600 text-sm rounded-lg hover:bg-gray-200 transition">关闭</button>
              </div>
            </>
          )}
        </div>
      )}

      {/* 面板授权编辑区 */}
      {panelEditUser && (
        <div className="bg-white rounded-xl border border-purple-200 p-5 mb-4 relative">
          <button onClick={closePanelEdit} className="absolute top-3 right-3 text-gray-400 hover:text-gray-600"><X className="w-4 h-4" /></button>
          <div className="flex items-center gap-3 mb-4">
            <Shield className="w-5 h-5 text-purple-500" />
            <h3 className="text-sm font-medium text-gray-800">面板授权：{panelEditUser}</h3>
            {userPanelData && (
              <span className="text-xs text-gray-400">
                角色：{roleBadge(userPanelData.role)}
                {userPanelData.has_override && <span className="ml-2 px-1.5 py-0.5 bg-amber-50 text-amber-600 rounded text-[10px]">已自定义</span>}
              </span>
            )}
          </div>

          {panelLoading ? (
            <div className="flex items-center gap-2 py-6 text-gray-400"><Loader2 className="w-4 h-4 animate-spin" /><span className="text-sm">加载中...</span></div>
          ) : (
            <>
              <div className="space-y-4">
                {panelGroups.map(g => (
                  <div key={g.group}>
                    <p className="text-xs font-medium text-gray-500 mb-2">{g.group}</p>
                    <div className="flex flex-wrap gap-2">
                      {g.panels.map(p => {
                        const checked = selectedPanels.includes(p.panel_key)
                        return (
                          <label key={p.panel_key} className={`flex items-center gap-2 px-3 py-2 rounded-lg border cursor-pointer transition ${
                            checked ? 'border-purple-300 bg-purple-50' : 'border-gray-200 hover:border-gray-300'
                          }`}>
                            <input type="checkbox" checked={checked} onChange={() => togglePanel(p.panel_key)}
                              className="w-3.5 h-3.5 rounded border-gray-300 text-purple-500 focus:ring-purple-500/20" />
                            <span className="text-xs text-gray-700">{p.panel_name}</span>
                          </label>
                        )
                      })}
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-4 flex gap-2">
                <button onClick={savePanels} disabled={updateUserPanels.isPending}
                  className="px-4 py-2 bg-purple-500 text-white text-sm rounded-lg hover:bg-purple-600 disabled:opacity-50 transition">
                  {updateUserPanels.isPending ? '保存中...' : '保存授权'}
                </button>
                <button onClick={resetPanels} disabled={resetUserPanels.isPending}
                  className="flex items-center gap-1.5 px-4 py-2 bg-gray-100 text-gray-600 text-sm rounded-lg hover:bg-gray-200 transition">
                  <RotateCcw className="w-3.5 h-3.5" /> 重置为角色默认
                </button>
                <button onClick={closePanelEdit} className="px-4 py-2 bg-gray-100 text-gray-600 text-sm rounded-lg hover:bg-gray-200 transition">关闭</button>
              </div>
            </>
          )}
        </div>
      )}

      {isLoading && <div className="flex items-center justify-center py-32 text-gray-400"><Loader2 className="w-6 h-6 animate-spin mr-2" /><span className="text-sm">加载中...</span></div>}
      {isError && isAuthError && (
        <div className="flex flex-col items-center justify-center py-24 text-gray-400"><LogIn className="w-10 h-10 mb-3 text-gray-300" /><p className="text-sm font-medium text-gray-600">需要管理员登录</p><p className="text-xs mt-1 text-gray-400">用户管理需要管理员权限</p></div>
      )}
      {isError && !isAuthError && <div className="flex flex-col items-center justify-center py-24 text-red-400"><AlertCircle className="w-8 h-8 mb-2" /><p className="text-sm font-medium">数据加载失败</p></div>}
      {!isLoading && !isError && (
        <SectionCard title={`用户列表（${(users ?? []).length}）`} noPadding>
          <DataTable columns={columns} data={users ?? []} rowKey={(r) => r.username} />
        </SectionCard>
      )}
    </div>
  )
}
