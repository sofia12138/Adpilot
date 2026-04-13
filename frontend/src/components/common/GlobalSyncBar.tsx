/**
 * 全局同步状态栏 — 可嵌入任意页面顶部
 *
 * 显示三个数据模块（结构数据 / 日报数据 / 回传数据）的同步时间和状态，
 * 并提供"立即同步"按钮触发全量数据刷新。
 *
 * 每 15 秒自动轮询状态，同步中时每 5 秒刷新一次。
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  RefreshCw, CheckCircle, Loader2, AlertCircle, Clock, ChevronDown, ChevronUp,
} from 'lucide-react'
import { useState } from 'react'
import {
  fetchSyncStatus, triggerSync,
  type AllSyncStatus, type ModuleSyncStatus,
} from '@/services/sync'

// ── 工具函数 ────────────────────────────────────────────────

function fmtRelTime(iso: string | null): string {
  if (!iso) return '从未同步'
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (diff < 60)    return `${diff}秒前`
  if (diff < 3600)  return `${Math.floor(diff / 60)}分钟前`
  if (diff < 86400) return `${Math.floor(diff / 3600)}小时前`
  return `${Math.floor(diff / 86400)}天前`
}

const MODULE_LABELS: Record<string, string> = {
  structure: '结构数据',
  reports:   '日报数据',
  returned:  '回传数据',
}

// ── 单模块行 ────────────────────────────────────────────────

function ModuleRow({ name, status }: { name: string; status: ModuleSyncStatus }) {
  const label  = MODULE_LABELS[name] ?? name
  const isRunning = status.is_running
  const hasError  = !!status.last_error

  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-16 text-gray-400 shrink-0">{label}</span>
      {isRunning ? (
        <Loader2 className="w-3 h-3 animate-spin text-blue-500 shrink-0" />
      ) : hasError ? (
        <AlertCircle className="w-3 h-3 text-red-400 shrink-0" />
      ) : (
        <CheckCircle className="w-3 h-3 text-emerald-400 shrink-0" />
      )}
      <span className={hasError ? 'text-red-500' : 'text-gray-500'}>
        {isRunning
          ? `同步中… ${status.last_range ?? ''}`
          : hasError
            ? status.last_error?.slice(0, 40)
            : fmtRelTime(status.last_synced_at)
        }
      </span>
      {status.last_range && !isRunning && (
        <span className="text-gray-300">（{status.last_range}）</span>
      )}
    </div>
  )
}

// ── 主组件 ──────────────────────────────────────────────────

interface GlobalSyncBarProps {
  /** 默认是否展开详细模块列表 */
  defaultExpanded?: boolean
  /** 自定义 class */
  className?: string
}

export function GlobalSyncBar({ defaultExpanded = false, className = '' }: GlobalSyncBarProps) {
  const queryClient = useQueryClient()
  const [expanded, setExpanded] = useState(defaultExpanded)

  const { data: allStatus, refetch: refetchStatus } = useQuery({
    queryKey: ['sync-status-all'],
    queryFn: fetchSyncStatus,
    refetchInterval: (query) => {
      const data = query.state.data as AllSyncStatus | undefined
      if (!data) return 15_000
      const anyRunning = Object.values(data).some(m => m.is_running)
      return anyRunning ? 5_000 : 15_000
    },
    staleTime: 5_000,
  })

  const mutation = useMutation({
    mutationFn: () => triggerSync(2),
    onSuccess: () => {
      refetchStatus()
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['returned-conversion'] })
        queryClient.invalidateQueries({ queryKey: ['biz'] })
        refetchStatus()
      }, 3000)
    },
  })

  const anyRunning = allStatus
    ? Object.values(allStatus).some(m => m.is_running)
    : false
  const anyError = allStatus
    ? Object.values(allStatus).some(m => !!m.last_error)
    : false

  // 取最近一次同步时间（各模块中最新的）
  const latestSync = allStatus
    ? Object.values(allStatus)
        .map(m => m.last_synced_at)
        .filter(Boolean)
        .sort()
        .at(-1) ?? null
    : null

  return (
    <div className={`rounded-lg border text-xs mb-4 overflow-hidden ${
      anyError
        ? 'bg-red-50 border-red-200'
        : anyRunning
          ? 'bg-blue-50 border-blue-200'
          : 'bg-gray-50 border-gray-200'
    } ${className}`}>

      {/* 主状态行 */}
      <div className="flex items-center gap-3 px-4 py-2.5">
        {anyRunning ? (
          <Loader2 className="w-3.5 h-3.5 animate-spin text-blue-500 shrink-0" />
        ) : anyError ? (
          <AlertCircle className="w-3.5 h-3.5 text-red-400 shrink-0" />
        ) : (
          <CheckCircle className="w-3.5 h-3.5 text-emerald-500 shrink-0" />
        )}

        <span className={`flex items-center gap-1.5 ${anyError ? 'text-red-700' : 'text-gray-500'}`}>
          <Clock className="w-3 h-3" />
          {anyRunning
            ? '数据同步中…'
            : anyError
              ? '部分模块同步出错'
              : `数据最近更新：${fmtRelTime(latestSync)}`
          }
        </span>

        <span className="text-gray-400">全量每 20 分钟自动同步</span>

        <div className="ml-auto flex items-center gap-2">
          {/* 展开/收起详情 */}
          <button
            onClick={() => setExpanded(v => !v)}
            className="flex items-center gap-1 text-gray-400 hover:text-gray-600 transition px-1"
          >
            {expanded ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
            详情
          </button>

          {/* 立即同步按钮 */}
          <button
            onClick={() => mutation.mutate()}
            disabled={anyRunning || mutation.isPending}
            className="flex items-center gap-1 px-3 py-1 rounded-md bg-white border border-gray-200 text-gray-600
                       hover:bg-gray-50 hover:border-gray-300 transition disabled:opacity-40 disabled:cursor-not-allowed"
          >
            <RefreshCw className={`w-3 h-3 ${mutation.isPending ? 'animate-spin' : ''}`} />
            立即同步
          </button>
        </div>
      </div>

      {/* 展开的模块详情 */}
      {expanded && allStatus && (
        <div className="border-t border-gray-200 px-4 py-3 space-y-1.5 bg-white">
          {Object.entries(allStatus).map(([name, status]) => (
            <ModuleRow key={name} name={name} status={status} />
          ))}
        </div>
      )}
    </div>
  )
}
