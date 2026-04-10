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

/** Campaign 层汇总（group_by=campaign） */
export function fetchCampaignRows(base: BaseReturnedFilter): Promise<ReturnedConversionResponse> {
  return fetchReturnedConversion({ ...base, group_by: 'campaign', order_dir: 'desc' })
}

/** 某 Campaign 下的 Adset 列表（group_by=adset + campaign_id 筛选） */
export function fetchAdsetRows(campaignId: string, base: BaseReturnedFilter): Promise<ReturnedConversionResponse> {
  return fetchReturnedConversion({ ...base, campaign_id: campaignId, group_by: 'adset', order_dir: 'desc' })
}

/** 某 Adset 下的 Ad 列表（group_by=ad + adset_id 筛选） */
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
