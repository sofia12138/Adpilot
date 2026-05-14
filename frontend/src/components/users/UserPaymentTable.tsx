import { useMemo } from 'react'
import type { ChannelInfo, UserPaymentSummaryRow } from '@/types/userPayment'
import { channelLabel, channelTooltip } from '@/utils/channelLabel'
import { AnomalyChipList } from './AnomalyChip'

interface Props {
  rows: UserPaymentSummaryRow[]
  total: number
  page: number
  pageSize: number
  onPageChange: (page: number) => void
  onSort: (orderBy: string) => void
  orderBy: string
  orderDesc: boolean
  onUserClick: (user_id: number) => void
  onApply: (user_id: number) => void
  /** channel_id → 渠道元信息字典；缺省时退化为本地 fallback（'0'→自然量） */
  channelDict?: Record<string, ChannelInfo> | null
}

const OAUTH_LABEL: Record<number, string> = {
  [-1]: '游客',
  1: 'Google',
  2: 'Facebook',
  3: 'Apple',
}

const OS_LABEL: Record<number, string> = { 1: 'Android', 2: 'iOS' }
const PAY_LABEL: Record<number, string> = { 1: 'ApplePay', 2: 'GooglePay' }

function fmtUsd(v: number) {
  if (!v) return '$0'
  if (Math.abs(v) >= 1000) return `$${(v / 1000).toFixed(1)}k`
  return `$${v.toFixed(2)}`
}

function fmtRate(v: number) {
  return `${(v * 100).toFixed(1)}%`
}

function fmtDateTime(s: string | null) {
  if (!s) return '—'
  return s.replace('T', ' ').slice(0, 16)
}

interface SortHeaderProps {
  field: string
  label: string
  align?: 'left' | 'right'
  orderBy: string
  orderDesc: boolean
  onSort: (f: string) => void
}

function SortHeader({ field, label, align = 'left', orderBy, orderDesc, onSort }: SortHeaderProps) {
  const active = orderBy === field
  return (
    <th
      onClick={() => onSort(field)}
      className={`px-2 py-2 cursor-pointer select-none ${align === 'right' ? 'text-right' : 'text-left'} hover:text-gray-900`}
    >
      <span className={active ? 'text-gray-900 font-medium' : ''}>
        {label}
        {active && <span className="ml-1 text-gray-400">{orderDesc ? '↓' : '↑'}</span>}
      </span>
    </th>
  )
}

export function UserPaymentTable({
  rows, total, page, pageSize, onPageChange, onSort, orderBy, orderDesc, onUserClick, onApply, channelDict,
}: Props) {
  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total, pageSize])

  return (
    <div className="bg-card border border-card-border rounded-xl overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-gray-50 text-gray-500 border-b border-card-border">
            <tr>
              <SortHeader field="user_id" label="user_id" orderBy={orderBy} orderDesc={orderDesc} onSort={onSort} />
              <th className="px-2 py-2 text-left">国家</th>
              <th className="px-2 py-2 text-left">登录</th>
              <th className="px-2 py-2 text-left">首单渠道</th>
              <th className="px-2 py-2 text-left">首单平台</th>
              <SortHeader field="total_orders" label="创单" align="right" orderBy={orderBy} orderDesc={orderDesc} onSort={onSort} />
              <SortHeader field="paid_orders" label="成单" align="right" orderBy={orderBy} orderDesc={orderDesc} onSort={onSort} />
              <th className="px-2 py-2 text-right">成功率</th>
              <SortHeader field="total_gmv_cents" label="GMV" align="right" orderBy={orderBy} orderDesc={orderDesc} onSort={onSort} />
              <SortHeader field="attempted_gmv_cents" label="尝试金额" align="right" orderBy={orderBy} orderDesc={orderDesc} onSort={onSort} />
              <th className="px-2 py-2 text-left">异常标签</th>
              <SortHeader field="last_action_time_utc" label="最后下单" orderBy={orderBy} orderDesc={orderDesc} onSort={onSort} />
              <th className="px-2 py-2 text-right">操作</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr>
                <td colSpan={13} className="py-10 text-center text-gray-400">无数据</td>
              </tr>
            )}
            {rows.map(r => (
              <tr key={r.user_id} className="border-b border-card-border/50 hover:bg-gray-50/50">
                <td className="px-2 py-2">
                  <button
                    onClick={() => onUserClick(r.user_id)}
                    className="text-blue-600 hover:underline font-mono"
                  >
                    {r.user_id}
                  </button>
                </td>
                <td className="px-2 py-2">
                  {r.region ? <span className="font-mono text-gray-700">{r.region}</span> : <span className="text-gray-300">—</span>}
                </td>
                <td className="px-2 py-2">
                  {r.oauth_platform != null && OAUTH_LABEL[r.oauth_platform] ? (
                    <span className={r.oauth_platform === -1 ? 'text-blue-600' : 'text-gray-700'}>
                      {OAUTH_LABEL[r.oauth_platform]}
                    </span>
                  ) : (
                    <span className="text-gray-300">—</span>
                  )}
                </td>
                <td className="px-2 py-2">
                  {(() => {
                    const label = channelLabel(r.first_channel_id, channelDict)
                    const tooltip = channelTooltip(r.first_channel_id, channelDict)
                    const isOrganic = label === '自然量'
                    return (
                      <span
                        title={tooltip}
                        className={isOrganic ? 'text-gray-400' : 'text-gray-700'}
                      >
                        {label}
                      </span>
                    )
                  })()}
                </td>
                <td className="px-2 py-2">
                  {OS_LABEL[r.first_os_type] || '—'} / {PAY_LABEL[r.first_pay_type] || '—'}
                </td>
                <td className="px-2 py-2 text-right tabular-nums">{r.total_orders}</td>
                <td className="px-2 py-2 text-right tabular-nums">{r.paid_orders}</td>
                <td className="px-2 py-2 text-right tabular-nums">
                  <span className={r.success_rate >= 0.5 ? 'text-green-600' : r.success_rate === 0 ? 'text-red-500' : 'text-orange-600'}>
                    {fmtRate(r.success_rate)}
                  </span>
                </td>
                <td className="px-2 py-2 text-right tabular-nums font-medium">{fmtUsd(r.total_gmv_usd)}</td>
                <td className="px-2 py-2 text-right tabular-nums text-gray-500">{fmtUsd(r.attempted_gmv_usd)}</td>
                <td className="px-2 py-2"><AnomalyChipList tags={r.anomaly_tags} /></td>
                <td className="px-2 py-2 text-gray-500">{fmtDateTime(r.last_action_time_utc)}</td>
                <td className="px-2 py-2 text-right">
                  <button
                    onClick={() => onApply(r.user_id)}
                    className="text-xs px-2 py-1 rounded border border-gray-200 hover:border-blue-300 hover:text-blue-600 transition-colors"
                    disabled={r.anomaly_tags.includes('whitelisted') || r.anomaly_tags.includes('pending_whitelist')}
                    title={
                      r.anomaly_tags.includes('whitelisted') ? '已在白名单'
                      : r.anomaly_tags.includes('pending_whitelist') ? '工单审批中'
                      : '申请加入白名单（需要 super_admin 审批）'
                    }
                  >
                    申请加白名单
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between px-3 py-2 border-t border-card-border text-xs text-gray-500">
        <div>
          共 <span className="font-medium text-gray-700">{total}</span> 条
        </div>
        <div className="flex items-center gap-2">
          <button
            disabled={page <= 1}
            onClick={() => onPageChange(page - 1)}
            className="px-2 py-1 rounded border border-gray-200 disabled:opacity-40"
          >
            上一页
          </button>
          <span>第 <span className="font-medium text-gray-700">{page}</span> / {totalPages} 页</span>
          <button
            disabled={page >= totalPages}
            onClick={() => onPageChange(page + 1)}
            className="px-2 py-1 rounded border border-gray-200 disabled:opacity-40"
          >
            下一页
          </button>
        </div>
      </div>
    </div>
  )
}
