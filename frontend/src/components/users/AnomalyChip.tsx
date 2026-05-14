import type { AnomalyTag } from '@/types/userPayment'
import { cn } from '@/utils/cn'

interface ChipMeta {
  label: string
  className: string
  title: string
}

const META: Record<AnomalyTag, ChipMeta> = {
  suspect_brush: {
    label: '刷单嫌疑',
    className: 'bg-red-50 text-red-700 border border-red-200',
    title: '单日下单 ≥10 且成单率 < 10%',
  },
  payment_loop: {
    label: '支付失败循环',
    className: 'bg-orange-50 text-orange-700 border border-orange-200',
    title: '单日下单 ≥5 且 0 成单',
  },
  instant_burst: {
    label: '注册即狂下',
    className: 'bg-yellow-50 text-yellow-800 border border-yellow-200',
    title: '注册后 30 分钟内下单 ≥5',
  },
  guest_payer: {
    label: '游客付费',
    className: 'bg-blue-50 text-blue-700 border border-blue-200',
    title: '游客（未绑账号）且累计下单 ≥3',
  },
  pending_whitelist: {
    label: '审批中',
    className: 'bg-gray-50 text-gray-600 border border-dashed border-gray-300',
    title: '白名单申请工单 pending 中',
  },
  whitelisted: {
    label: '已加白',
    className: 'bg-gray-100 text-gray-600 border border-gray-300',
    title: '已加入白名单，剔除口径下不计入大盘',
  },
}

export function AnomalyChip({ tag }: { tag: AnomalyTag }) {
  const meta = META[tag] || {
    label: tag,
    className: 'bg-gray-50 text-gray-600 border border-gray-200',
    title: tag,
  }
  return (
    <span
      title={meta.title}
      className={cn(
        'inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium leading-none',
        meta.className,
      )}
    >
      {meta.label}
    </span>
  )
}

export function AnomalyChipList({ tags }: { tags: AnomalyTag[] }) {
  if (!tags || tags.length === 0) {
    return <span className="text-[10px] text-gray-300">—</span>
  }
  return (
    <div className="flex flex-wrap gap-1">
      {tags.map(t => <AnomalyChip key={t} tag={t} />)}
    </div>
  )
}
