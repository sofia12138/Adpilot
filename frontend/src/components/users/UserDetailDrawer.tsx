import { useQuery } from '@tanstack/react-query'
import { X } from 'lucide-react'
import { fetchChannelDict, fetchOrdersOfUser } from '@/services/userPaymentService'
import type { UserPaymentOrderRow } from '@/types/userPayment'
import { channelLabel, channelTooltip } from '@/utils/channelLabel'

interface Props {
  open: boolean
  user_id: number | null
  onClose: () => void
  onApply: (user_id: number) => void
}

const STATUS_LABEL: Record<number, { text: string; cls: string }> = {
  0: { text: '待支付',    cls: 'text-gray-500' },
  1: { text: '已支付',    cls: 'text-green-600' },
  2: { text: '全额退款',  cls: 'text-orange-500' },
  3: { text: '部分退款',  cls: 'text-orange-500' },
  4: { text: '用户取消',  cls: 'text-gray-400' },
  5: { text: '发起支付失败', cls: 'text-red-500' },
  6: { text: '支付超时',  cls: 'text-red-500' },
}

const OS_LABEL: Record<number, string> = { 1: 'Android', 2: 'iOS' }
const PAY_LABEL: Record<number, string> = { 1: 'ApplePay', 2: 'GooglePay' }

function fmtUsd(v: number) {
  if (!v) return '$0'
  return `$${v.toFixed(2)}`
}
function fmtDateTime(s: string | null) {
  if (!s) return '—'
  return s.replace('T', ' ').slice(0, 19)
}

export function UserDetailDrawer({ open, user_id, onClose, onApply }: Props) {
  const { data, isLoading } = useQuery({
    queryKey: ['user-orders', user_id],
    queryFn: () => fetchOrdersOfUser(user_id as number, 500),
    enabled: open && !!user_id,
  })

  const { data: channelDict } = useQuery({
    queryKey: ['user-payment-channel-dict'],
    queryFn: () => fetchChannelDict(),
    staleTime: 10 * 60_000,
    enabled: open,
  })

  if (!open || !user_id) return null

  const items = data?.items ?? []

  // 聚合
  const stats = computeStats(items)

  return (
    <div className="fixed inset-0 z-40 flex justify-end bg-black/30">
      <div className="bg-white w-[820px] max-w-full h-full shadow-xl flex flex-col">
        <div className="flex items-center justify-between px-4 py-3 border-b border-card-border shrink-0">
          <div>
            <h3 className="text-sm font-semibold text-gray-900">
              用户详情 <span className="font-mono ml-1 text-blue-600">{user_id}</span>
            </h3>
            <p className="text-xs text-gray-500 mt-0.5">
              全部订单 · 含创单 / 已支付 / 退款 / 失败
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => onApply(user_id)}
              className="px-3 py-1.5 rounded text-xs border border-blue-200 text-blue-600 hover:bg-blue-50"
            >
              申请加白名单
            </button>
            <button onClick={onClose} className="text-gray-400 hover:text-gray-700">
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* 头部聚合 */}
        <div className="grid grid-cols-4 gap-3 p-4 text-xs border-b border-card-border shrink-0">
          <Stat label="总创单" value={stats.total} />
          <Stat label="成功支付" value={stats.paid} sub={`${(stats.successRate * 100).toFixed(1)}%`} />
          <Stat label="GMV (USD)" value={fmtUsd(stats.gmv)} />
          <Stat label="退款 (USD)" value={fmtUsd(stats.refund)} sub={`${stats.refundOrders} 单`} />
        </div>

        <div className="flex-1 overflow-y-auto p-4">
          {isLoading && <div className="text-xs text-gray-400 text-center py-8">加载中…</div>}
          {!isLoading && items.length === 0 && (
            <div className="text-xs text-gray-400 text-center py-8">该用户尚无任何创单</div>
          )}
          {items.length > 0 && (
            <table className="w-full text-xs">
              <thead className="bg-gray-50 text-gray-500">
                <tr>
                  <th className="px-2 py-2 text-left">订单时间 (LA)</th>
                  <th className="px-2 py-2 text-left">order_no</th>
                  <th className="px-2 py-2 text-left">状态</th>
                  <th className="px-2 py-2 text-left">平台</th>
                  <th className="px-2 py-2 text-left">渠道</th>
                  <th className="px-2 py-2 text-left">商品</th>
                  <th className="px-2 py-2 text-right">金额</th>
                </tr>
              </thead>
              <tbody>
                {items.map(o => {
                  const meta = STATUS_LABEL[o.order_status] || { text: '?', cls: 'text-gray-400' }
                  return (
                    <tr key={o.order_id} className="border-b border-card-border/50">
                      <td className="px-2 py-1.5 font-mono text-gray-700">{fmtDateTime(o.created_at_la)}</td>
                      <td className="px-2 py-1.5 font-mono text-gray-500">{o.order_no}</td>
                      <td className={`px-2 py-1.5 ${meta.cls}`}>{meta.text}</td>
                      <td className="px-2 py-1.5">
                        {OS_LABEL[o.os_type] || '—'} / {PAY_LABEL[o.pay_type] || '—'}
                        {o.is_subscribe === 1 && <span className="ml-1 text-purple-500">订阅</span>}
                      </td>
                      <td
                        className="px-2 py-1.5 text-gray-700"
                        title={channelTooltip(o.channel_id, channelDict)}
                      >
                        {channelLabel(o.channel_id, channelDict)}
                      </td>
                      <td className="px-2 py-1.5 font-mono text-gray-500">{o.product_id}</td>
                      <td className="px-2 py-1.5 text-right tabular-nums">
                        {fmtUsd(o.pay_amount_usd)}
                        {o.refund_amount_usd > 0 && (
                          <span className="ml-1 text-orange-500 text-[10px]">
                            退 {fmtUsd(o.refund_amount_usd)}
                          </span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  )
}

function Stat({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-gray-50 rounded-lg p-2">
      <div className="text-[10px] text-gray-500">{label}</div>
      <div className="text-base font-semibold text-gray-800 tabular-nums">{value}</div>
      {sub && <div className="text-[10px] text-gray-500 mt-0.5">{sub}</div>}
    </div>
  )
}

function computeStats(items: UserPaymentOrderRow[]) {
  let total = items.length
  let paid = 0
  let gmv = 0
  let refund = 0
  let refundOrders = 0
  for (const o of items) {
    if (o.order_status === 1) {
      paid += 1
      gmv += o.pay_amount_usd
    }
    if (o.refund_amount_usd > 0) {
      refund += o.refund_amount_usd
      refundOrders += 1
    }
  }
  const successRate = total > 0 ? paid / total : 0
  return { total, paid, gmv, refund, refundOrders, successRate }
}
