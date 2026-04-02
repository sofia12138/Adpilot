import { useState, useEffect } from 'react'
import { PageHeader } from '@/components/common/PageHeader'
import { SectionCard } from '@/components/common/SectionCard'
import { Loader2, Shield, Check } from 'lucide-react'
import { usePanels, useRolePanels, useUpdateRolePanels } from '@/hooks/use-panels'
import { ROLE_LABELS } from '@/types/menu'

const ROLE_KEYS = Object.keys(ROLE_LABELS)

export default function RolePermPage() {
  const { data: allPanels, isLoading: panelsLoading } = usePanels()
  const [activeRole, setActiveRole] = useState(ROLE_KEYS[0])
  const { data: rolePanels, isLoading: roleLoading } = useRolePanels(activeRole)
  const updateMutation = useUpdateRolePanels()

  const [selected, setSelected] = useState<string[]>([])
  const [dirty, setDirty] = useState(false)

  useEffect(() => {
    if (rolePanels && !dirty) {
      setSelected(rolePanels)
    }
  }, [rolePanels, dirty])

  useEffect(() => {
    setDirty(false)
  }, [activeRole])

  function togglePanel(key: string) {
    setDirty(true)
    setSelected(prev => prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key])
  }

  function selectAll() {
    if (!allPanels) return
    setDirty(true)
    setSelected(allPanels.map(p => p.panel_key))
  }

  function deselectAll() {
    setDirty(true)
    setSelected([])
  }

  function handleSave() {
    updateMutation.mutate({ roleKey: activeRole, panelKeys: selected }, {
      onSuccess: () => setDirty(false),
    })
  }

  const panelGroups = allPanels
    ? [...new Set(allPanels.map(p => p.panel_group))].map(g => ({
        group: g,
        panels: allPanels.filter(p => p.panel_group === g).sort((a, b) => a.sort_order - b.sort_order),
      }))
    : []

  const isLoading = panelsLoading || roleLoading

  return (
    <div className="max-w-7xl mx-auto">
      <PageHeader title="角色权限管理" description="配置各角色默认可访问的系统面板" />

      <div className="grid grid-cols-[240px_1fr] gap-6">
        {/* 角色列表 */}
        <SectionCard title="角色列表" noPadding>
          <div className="divide-y divide-gray-50">
            {ROLE_KEYS.map(key => (
              <button key={key} onClick={() => setActiveRole(key)}
                className={`w-full flex items-center gap-3 px-5 py-3 text-left transition ${
                  activeRole === key ? 'bg-purple-50 border-l-2 border-purple-500' : 'hover:bg-gray-50 border-l-2 border-transparent'
                }`}>
                <Shield className={`w-4 h-4 ${activeRole === key ? 'text-purple-500' : 'text-gray-400'}`} />
                <div>
                  <p className={`text-sm font-medium ${activeRole === key ? 'text-purple-700' : 'text-gray-700'}`}>{ROLE_LABELS[key]}</p>
                  <p className="text-[11px] text-gray-400">{key}</p>
                </div>
              </button>
            ))}
          </div>
        </SectionCard>

        {/* 面板权限配置 */}
        <SectionCard title={`${ROLE_LABELS[activeRole] ?? activeRole} — 面板权限`}
          extra={
            <div className="flex items-center gap-2">
              <button onClick={selectAll} className="text-xs text-blue-500 hover:text-blue-700 transition">全选</button>
              <span className="text-gray-200">|</span>
              <button onClick={deselectAll} className="text-xs text-gray-500 hover:text-gray-700 transition">清空</button>
            </div>
          }
        >
          {isLoading ? (
            <div className="flex items-center gap-2 py-12 justify-center text-gray-400">
              <Loader2 className="w-5 h-5 animate-spin" /><span className="text-sm">加载中...</span>
            </div>
          ) : (
            <>
              <div className="space-y-5">
                {panelGroups.map(g => (
                  <div key={g.group}>
                    <p className="text-xs font-medium text-gray-500 mb-2 uppercase tracking-wider">{g.group}</p>
                    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
                      {g.panels.map(p => {
                        const checked = selected.includes(p.panel_key)
                        return (
                          <label key={p.panel_key}
                            className={`flex items-center gap-2.5 px-3 py-2.5 rounded-lg border cursor-pointer transition ${
                              checked ? 'border-purple-300 bg-purple-50/60' : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
                            }`}>
                            <input type="checkbox" checked={checked} onChange={() => togglePanel(p.panel_key)}
                              className="w-3.5 h-3.5 rounded border-gray-300 text-purple-500 focus:ring-purple-500/20" />
                            <div className="min-w-0">
                              <p className="text-xs text-gray-700 font-medium">{p.panel_name}</p>
                              <p className="text-[10px] text-gray-400 truncate">{p.route_path}</p>
                            </div>
                          </label>
                        )
                      })}
                    </div>
                  </div>
                ))}
              </div>

              <div className="mt-6 flex items-center gap-3 pt-4 border-t border-gray-100">
                <button onClick={handleSave} disabled={!dirty || updateMutation.isPending}
                  className="flex items-center gap-1.5 px-5 py-2 bg-purple-500 text-white text-sm rounded-lg hover:bg-purple-600 disabled:opacity-50 transition">
                  {updateMutation.isPending ? (<><Loader2 className="w-3.5 h-3.5 animate-spin" />保存中...</>) : (<><Check className="w-3.5 h-3.5" />保存</>)}
                </button>
                {!dirty && updateMutation.isSuccess && (
                  <span className="text-xs text-green-600 flex items-center gap-1"><Check className="w-3 h-3" />已保存</span>
                )}
                <span className="text-xs text-gray-400 ml-auto">已选 {selected.length}/{allPanels?.length ?? 0} 个面板</span>
              </div>
            </>
          )}
        </SectionCard>
      </div>
    </div>
  )
}
