export interface ApiResponse<T = unknown> {
  code?: number
  message?: string
  data?: T
  ok?: boolean
}

export interface PaginatedData<T> {
  total: number
  list: T[]
  page: number
  page_size: number
}

export interface ChannelBlock {
  platform: string
  summary: { spend: number; revenue: number; roas: number | null }
  campaigns: CampaignRow[]
}

export interface CampaignRow {
  platform: string
  campaign_id: string
  campaign_name: string
  status: string
  spend: number
  impressions: number
  clicks: number
  conversions: number
  revenue: number
  roas: number | null
}
