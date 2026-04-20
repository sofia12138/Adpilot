/**
 * 广告回传分析 API 封装
 *
 * 数据口径说明：
 *   - 所有指标均为广告平台归因回传口径（data_label = 'returned'）
 *   - 不等同于后端订单真值，仅用于投放优化分析
 *   - 字段命名统一使用 returned 后缀，区分真实 revenue / ROI
 */
import { apiFetch } from './api'

export type GroupByDimension = 'date' | 'media' | 'campaign' | 'adset' | 'ad' | 'country' | 'platform'

export interface ReturnedConversionFilter {
  start_date: string
  end_date: string
  media_source?: string
  account_id?: string
  country?: string
  platform?: string
  campaign_id?: string
  adset_id?: string
  ad_id?: string
  search_keyword?: string
  group_by?: GroupByDimension
  order_dir?: 'asc' | 'desc'
}

/** 汇总指标卡数据（summary） */
export interface ReturnedSummary {
  spend: number
  impressions: number
  clicks: number
  installs: number
  /** 回传注册数（广告平台归因） */
  registrations_returned: number
  /** 回传充值价值（广告平台归因） */
  purchase_value_returned: number
  /** 回传订阅价值（平台不支持时为 0） */
  subscribe_value_returned: number
  /** 回传总价值 = purchase + subscribe */
  total_value_returned: number
  /** 累计回传ROI = total_value_returned / spend（所选时段内总回传价值 / 总花费） */
  cumulative_roi_returned: number
  /** D0 ROI（回传口径）= total_value / spend */
  d0_roi_returned: number
  /** D0 ROI 是否为 fallback（无 D0 cohort 时等同累计 ROI） */
  d0_roi_is_fallback?: boolean
  /** D1 回传价值（平台不支持 D1 cohort 时为 0） */
  d1_value_returned: number
  /** D1 ROI（回传口径）= d1_value / spend */
  d1_roi_returned: number
  /** 首日注册数（D0 Cohort，平台支持时有值） */
  d0_registrations_returned: number
  /** 首日充值金额（D0 Cohort，平台支持时有值） */
  d0_purchase_value_returned: number
  /** 首日订阅金额（D0 Cohort，平台支持时有值） */
  d0_subscribe_value_returned: number
}

/** 按 group_by 聚合的数据行 */
export interface ReturnedRow extends ReturnedSummary {
  dimension_key: string
  dimension_label: string
}

/**
 * 单字段可用性描述
 *
 * supported:        平台是否具备该字段能力（静态矩阵，与当前数值无关）
 *                   - true + 值=0  → 正常展示 0 / $0.00
 *                   - false        → 显示"暂不支持"提示，不展示任何数字
 *
 * has_nonzero_data: 当前筛选范围内是否存在 >0 的实际值（辅助信息，不决定是否支持）
 */
export interface ReturnedFieldAvailability {
  supported:        boolean
  has_nonzero_data: boolean
}

/** 所有 returned 字段的可用性集合 */
export interface ReturnedAvailability {
  registrations_returned:      ReturnedFieldAvailability
  purchase_value_returned:     ReturnedFieldAvailability
  subscribe_value_returned:    ReturnedFieldAvailability
  d1_value_returned:           ReturnedFieldAvailability
  d0_registrations_returned:   ReturnedFieldAvailability
  d0_purchase_value_returned:  ReturnedFieldAvailability
  d0_subscribe_value_returned: ReturnedFieldAvailability
}

export interface ReturnedConversionResponse {
  code: number
  message: string
  meta: {
    data_label: string
    disclaimer: string
    group_by: GroupByDimension
    db: string
  }
  summary: ReturnedSummary
  /**
   * 字段可用性：
   *   availability[field].supported = true  → 平台支持，正常展示数值（含 0）
   *   availability[field].supported = false → 平台不支持，显示"暂不支持"提示
   */
  availability: ReturnedAvailability
  rows: ReturnedRow[]
}

/**
 * 层级视图专用：基础筛选条件（不含定位用的 campaign_id / adset_id / ad_id / group_by）
 */
export type BaseReturnedFilter = Pick<
  ReturnedConversionFilter,
  'start_date' | 'end_date' | 'media_source' | 'country' | 'platform' | 'search_keyword'
>

/**
 * 完整层级行 — 来自 /hierarchy 接口。
 * 每行包含 campaign/adset/ad 三层的 id+name 以及指标聚合值，
 * 前端从同一批数据构建树，保证父子数据守恒。
 */
export interface HierarchyLeaf {
  campaign_id: string
  campaign_name: string
  adset_id: string
  adset_name: string
  ad_id: string
  ad_name: string
  // 指标
  spend: number
  impressions: number
  clicks: number
  installs: number
  registrations_returned: number
  purchase_value_returned: number
  subscribe_value_returned: number
  total_value_returned: number
  d1_value_returned: number
  d0_registrations_returned: number
  d0_purchase_value_returned: number
  d0_subscribe_value_returned: number
}

export interface HierarchyResponse {
  code: number
  message: string
  summary: ReturnedSummary
  availability: ReturnedAvailability
  rows: HierarchyLeaf[]
}

/**
 * 获取完整层级行（单次请求，前端构树）。
 * 替代原来的 fetchCampaignRows / fetchAdsetRows / fetchAdRows 三次独立请求。
 */
export async function fetchHierarchyRows(base: BaseReturnedFilter): Promise<HierarchyResponse> {
  const params = new URLSearchParams()
  params.set('start_date', base.start_date)
  params.set('end_date', base.end_date)
  if (base.media_source)   params.set('media_source', base.media_source)
  if (base.country)        params.set('country', base.country)
  if (base.platform)       params.set('platform', base.platform)
  if (base.search_keyword) params.set('search_keyword', base.search_keyword)
  return apiFetch<HierarchyResponse>(`/api/analysis/returned-conversion/hierarchy?${params}`)
}

/** @deprecated 使用 fetchHierarchyRows 替代 */
export function fetchCampaignRows(base: BaseReturnedFilter): Promise<ReturnedConversionResponse> {
  return fetchReturnedConversion({ ...base, group_by: 'campaign', order_dir: 'desc' })
}

/** @deprecated 使用 fetchHierarchyRows 替代 */
export function fetchAdsetRows(campaignId: string, base: BaseReturnedFilter): Promise<ReturnedConversionResponse> {
  return fetchReturnedConversion({ ...base, campaign_id: campaignId, group_by: 'adset', order_dir: 'desc' })
}

/** @deprecated 使用 fetchHierarchyRows 替代 */
export function fetchAdRows(adsetId: string, base: BaseReturnedFilter): Promise<ReturnedConversionResponse> {
  return fetchReturnedConversion({ ...base, adset_id: adsetId, group_by: 'ad', order_dir: 'desc' })
}

export async function fetchReturnedConversion(
  filter: ReturnedConversionFilter,
): Promise<ReturnedConversionResponse> {
  const params = new URLSearchParams()
  params.set('start_date', filter.start_date)
  params.set('end_date', filter.end_date)
  if (filter.media_source) params.set('media_source', filter.media_source)
  if (filter.account_id)   params.set('account_id', filter.account_id)
  if (filter.country)      params.set('country', filter.country)
  if (filter.platform)     params.set('platform', filter.platform)
  if (filter.campaign_id)  params.set('campaign_id', filter.campaign_id)
  if (filter.adset_id)     params.set('adset_id', filter.adset_id)
  if (filter.ad_id)        params.set('ad_id', filter.ad_id)
  if (filter.search_keyword) params.set('search_keyword', filter.search_keyword)
  if (filter.group_by)     params.set('group_by', filter.group_by)
  if (filter.order_dir)    params.set('order_dir', filter.order_dir)

  return apiFetch<ReturnedConversionResponse>(
    `/api/analysis/returned-conversion?${params.toString()}`,
  )
}
