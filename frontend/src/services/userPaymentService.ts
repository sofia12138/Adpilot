import { apiFetch } from './api'
import type {
  AnomalyAction,
  AnomalyApplication,
  AnomalyTag,
  AnomalyTagKind,
  AnomalyWhitelistRow,
  ApplicationListResponse,
  ApplicationStatus,
  ChannelDictResponse,
  ChannelInfo,
  OrderListResponse,
  TodayResponse,
  UserPaymentKpiResponse,
  UserPaymentListResponse,
  UserPaymentOrderRow,
} from '@/types/userPayment'

/**
 * 用户付费面板 service 层
 *
 * 后端：/api/ops/users/* （ops_dashboard 面板权限）
 */

// ─── KPI / 列表 ─────────────────────────────────────────────

/**
 * 拉 KPI。
 * - 不传 startDs/endDs：返回 90 天累计快照
 * - 同时传 startDs & endDs：按窗口聚合（基于订单明细重新计算）
 */
export async function fetchKpi(params: { start_ds?: string; end_ds?: string } = {}): Promise<UserPaymentKpiResponse> {
  const qs = buildQuery(params)
  return apiFetch<UserPaymentKpiResponse>(`/api/ops/users/kpi${qs ? `?${qs}` : ''}`)
}

export interface ListUsersParams {
  start_ds?: string
  end_ds?: string
  region?: string
  oauth_platform?: number
  first_channel_id?: string
  /** 按平台分类过滤：organic / tiktok / meta / other */
  channel_kind?: string
  first_os_type?: number
  anomaly_tag?: AnomalyTag
  user_id?: number
  min_total_orders?: number
  /** 设为 1 即可过滤"只看成功付费用户"（paid_orders >= 1） */
  min_paid_orders?: number
  order_by?: string
  order_desc?: boolean
  page?: number
  page_size?: number
}

export async function fetchUsers(params: ListUsersParams = {}): Promise<UserPaymentListResponse> {
  const qs = buildQuery(params)
  return apiFetch<UserPaymentListResponse>(`/api/ops/users/summary?${qs}`)
}

// ─── 订单 ────────────────────────────────────────────────────

export interface ListOrdersParams {
  start_ds?: string
  end_ds?: string
  user_id?: number
  order_status?: number
  os_type?: number
  channel_id?: string
  is_subscribe?: number
  order_by?: string
  order_desc?: boolean
  page?: number
  page_size?: number
}

export async function fetchOrders(params: ListOrdersParams = {}): Promise<OrderListResponse> {
  const qs = buildQuery(params)
  return apiFetch<OrderListResponse>(`/api/ops/users/orders?${qs}`)
}

export async function fetchOrdersOfUser(
  user_id: number,
  limit = 500,
): Promise<{ items: UserPaymentOrderRow[] }> {
  return apiFetch<{ items: UserPaymentOrderRow[] }>(
    `/api/ops/users/${user_id}/orders?limit=${limit}`,
  )
}

// ─── 实时 today ──────────────────────────────────────────────

export interface TodayParams {
  la_ds?: string
  refresh?: boolean
}

export async function fetchToday(params: TodayParams = {}): Promise<TodayResponse> {
  const qs = buildQuery(params)
  return apiFetch<TodayResponse>(`/api/ops/users/today?${qs}`)
}

// ─── 渠道字典 ────────────────────────────────────────────────

/**
 * 拉 channel_id → 元信息字典（后端 10 分钟缓存）。
 * 前端进入面板时调一次，缓存到组件 state。
 */
export async function fetchChannelDict(): Promise<Record<string, ChannelInfo>> {
  const r = await apiFetch<ChannelDictResponse>('/api/ops/users/channel-dict')
  return r.items || {}
}

// ─── 白名单（只读） ──────────────────────────────────────────

export async function fetchWhitelist(): Promise<{ items: AnomalyWhitelistRow[] }> {
  return apiFetch<{ items: AnomalyWhitelistRow[] }>('/api/ops/users/anomaly/whitelist')
}

// ─── 审批工单 ────────────────────────────────────────────────

export interface ListApplicationsParams {
  status?: ApplicationStatus
  target_user_id?: number
  applicant_user?: string
  page?: number
  page_size?: number
}

export async function fetchApplications(
  params: ListApplicationsParams = {},
): Promise<ApplicationListResponse> {
  const qs = buildQuery(params)
  return apiFetch<ApplicationListResponse>(`/api/ops/users/anomaly/applications?${qs}`)
}

export interface SubmitApplicationBody {
  target_user_id: number
  requested_tag?: AnomalyTagKind
  action?: AnomalyAction
  reason: string
}

export async function submitApplication(body: SubmitApplicationBody): Promise<AnomalyApplication> {
  return apiFetch<AnomalyApplication>('/api/ops/users/anomaly/applications', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export async function approveApplication(
  id: number,
  review_note = '',
): Promise<AnomalyApplication> {
  return apiFetch<AnomalyApplication>(`/api/ops/users/anomaly/applications/${id}/approve`, {
    method: 'POST',
    body: JSON.stringify({ review_note }),
  })
}

export async function rejectApplication(
  id: number,
  review_note = '',
): Promise<AnomalyApplication> {
  return apiFetch<AnomalyApplication>(`/api/ops/users/anomaly/applications/${id}/reject`, {
    method: 'POST',
    body: JSON.stringify({ review_note }),
  })
}

export async function withdrawApplication(id: number): Promise<AnomalyApplication> {
  return apiFetch<AnomalyApplication>(`/api/ops/users/anomaly/applications/${id}/withdraw`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
}

// ─── 工具 ────────────────────────────────────────────────────

function buildQuery(params: Record<string, unknown>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    search.set(key, String(value))
  }
  return search.toString()
}
