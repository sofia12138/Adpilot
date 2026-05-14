import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { Zap, RefreshCw, ChevronDown, ChevronUp } from 'lucide-react'
import { fetchToday } from '@/services/userPaymentService'
import { AnomalyChipList } from './AnomalyChip'

interface Props {
  onUserClick: (uid: number) => void
  onApply: (uid: number) => void
}

/**
 * 顶部实时今日异常用户条 — 仅展示带异常标签的用户，按 GMV 倒序前 10。
 * 数据来源：GET /api/ops/users/today（PolarDB 直查 + 60s cache）
 */
export function TodayRealtimeBanner({ onUserClick, onApply }: Props) {
  const [expanded, setExpanded] = useState(false)
  const { data, isLoading, refetch, isRefetching } = useQuery({
    queryKey: ['user-payment-today'],
    queryFn: () => fetchToday(),
    refetchInterval: 60000,
  })

  if (isLoading) {
    return (
      <div className="px-3 py-2 rounded-lg bg-amber-50 border border-amber-200 text-xs text-amber-700">
        <Zap className="w-3 h-3 inline mr-1.5" />实时数据加载中…
      </div>
    )
  }
  if (!data) return null

  const anomalies = data.items.filter(u => u.anomaly_tags.length > 0)
  const top = anomalies.slice(0, expanded ? anomalies.length : 10)

  return (
    <div className="rounded-lg bg-amber-50 border border-amber-200 text-xs overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2">
        <div className="flex items-center gap-2 text-amber-800">
          <Zap className="w-3.5 h-3.5" />
          <span className="font-medium">今日 LA ({data.la_ds}) 实时</span>
          <span className="text-amber-600">
            · 尝试付费 {data.total_users} 用户 · 其中 {anomalies.length} 个带异常标签
          </span>
          {data.truncated && (
            <span className="text-orange-500 text-[10px]">⚠ 数据已截断到前 5000</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => refetch()}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded hover:bg-amber-100 text-amber-700"
            disabled={isRefetching}
            title="刷新（每 60 秒自动刷新一次）"
          >
            <RefreshCw className={`w-3 h-3 ${isRefetching ? 'animate-spin' : ''}`} />
            刷新
          </button>
          {anomalies.length > 10 && (
            <button
              onClick={() => setExpanded(v => !v)}
              className="inline-flex items-center gap-1 px-2 py-0.5 rounded hover:bg-amber-100 text-amber-700"
            >
              {expanded ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
              {expanded ? '收起' : `展开全部 ${anomalies.length}`}
            </button>
          )}
        </div>
      </div>

      {anomalies.length === 0 && (
        <div className="px-3 pb-2 text-amber-600/70">今日暂无异常用户 ✓</div>
      )}

      {anomalies.length > 0 && (
        <div className="border-t border-amber-200 max-h-[260px] overflow-y-auto">
          <table className="w-full">
            <thead className="bg-amber-100/50 text-amber-700">
              <tr>
                <th className="px-2 py-1 text-left">user_id</th>
                <th className="px-2 py-1 text-right">创单</th>
                <th className="px-2 py-1 text-right">成单</th>
                <th className="px-2 py-1 text-right">GMV</th>
                <th className="px-2 py-1 text-right">尝试金额</th>
                <th className="px-2 py-1 text-left">最后下单</th>
                <th className="px-2 py-1 text-left">异常</th>
                <th className="px-2 py-1 text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {top.map(u => (
                <tr key={u.user_id} className="border-t border-amber-100/50 hover:bg-amber-100/30">
                  <td className="px-2 py-1">
                    <button
                      onClick={() => onUserClick(u.user_id)}
                      className="text-blue-600 hover:underline font-mono"
                    >
                      {u.user_id}
                    </button>
                  </td>
                  <td className="px-2 py-1 text-right tabular-nums">{u.total_orders}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{u.paid_orders}</td>
                  <td className="px-2 py-1 text-right tabular-nums font-medium">
                    ${u.total_gmv_usd.toFixed(2)}
                  </td>
                  <td className="px-2 py-1 text-right tabular-nums text-gray-500">
                    ${u.attempted_gmv_usd.toFixed(2)}
                  </td>
                  <td className="px-2 py-1 text-gray-600">
                    {u.last_action_la ? u.last_action_la.replace('T', ' ').slice(11, 19) : '—'}
                  </td>
                  <td className="px-2 py-1"><AnomalyChipList tags={u.anomaly_tags} /></td>
                  <td className="px-2 py-1 text-right">
                    <button
                      onClick={() => onApply(u.user_id)}
                      className="text-[10px] px-2 py-0.5 rounded border border-amber-300 hover:bg-amber-100 text-amber-800"
                    >
                      申请加白
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
