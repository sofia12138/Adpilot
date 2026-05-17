import { NavLink } from 'react-router-dom'
import { cn } from '@/utils/cn'
import { useQuery } from '@tanstack/react-query'
import { fetchApplications } from '@/services/userPaymentService'

/**
 * 运营数据面板顶部 Tab：在「运营总览」和「用户付费」之间切换。
 *
 * 「用户付费」Tab 上挂红点显示 pending 工单数（5s 间隔轮询）。
 */
export function OpsTabs() {
  const { data } = useQuery({
    queryKey: ['ops-users-pending-count'],
    queryFn: () => fetchApplications({ status: 'pending', page: 1, page_size: 1 }),
    refetchInterval: 15000,
  })
  const pending = data?.pending_count ?? 0

  return (
    <div className="inline-flex items-center bg-muted rounded-lg p-0.5 text-xs">
      <NavLink
        to="/dashboard/ops"
        end
        className={({ isActive }) =>
          cn(
            'px-4 py-1.5 rounded-md transition-colors',
            isActive
              ? 'bg-white text-gray-900 shadow-sm font-medium'
              : 'text-muted-foreground hover:text-gray-700',
          )
        }
      >
        运营总览
      </NavLink>
      <NavLink
        to="/dashboard/ops/users"
        className={({ isActive }) =>
          cn(
            'px-4 py-1.5 rounded-md transition-colors inline-flex items-center gap-1.5',
            isActive
              ? 'bg-white text-gray-900 shadow-sm font-medium'
              : 'text-muted-foreground hover:text-gray-700',
          )
        }
      >
        <span>用户付费</span>
        {pending > 0 && (
          <span className="inline-flex items-center justify-center min-w-[18px] h-[18px] rounded-full bg-red-500 text-white text-[10px] font-semibold px-1">
            {pending > 99 ? '99+' : pending}
          </span>
        )}
      </NavLink>
      <NavLink
        to="/dashboard/ops/region-channel"
        className={({ isActive }) =>
          cn(
            'px-4 py-1.5 rounded-md transition-colors',
            isActive
              ? 'bg-white text-gray-900 shadow-sm font-medium'
              : 'text-muted-foreground hover:text-gray-700',
          )
        }
      >
        区域渠道分析
      </NavLink>
    </div>
  )
}
