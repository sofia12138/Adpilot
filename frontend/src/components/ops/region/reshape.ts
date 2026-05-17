/**
 * 区域渠道分析数据 reshape 工具
 *
 * 后端原始行：
 *   register_rows: { ds, region, channel_kind, register_uv }
 *   revenue_rows:  { ds, region, channel_kind, os_type, payer_uv, order_cnt,
 *                    revenue_usd, sub_revenue_usd, iap_revenue_usd }
 *
 * 前端图表需要的形状：
 *   1) 按 channel_kind 分桶的 KPI 汇总
 *   2) 按日 × channel_kind 的趋势 (前端堆叠柱)
 *   3) 按 region 分组的国家表
 */
import type {
  ChannelKind,
  RegionRegisterRow,
  RegionRevenueRow,
  OsView,
} from '@/types/opsRegion'

// ─── 区间汇总（KPI 卡用） ─────────────────────────────────────
export interface RegisterSummary {
  total: number
  byKind: Record<ChannelKind, number>
}

export interface RevenueSummary {
  totalUsd: number
  payerUv: number     // 累计付费人次（多日按 ds 累加，跟运营总览一致）
  orderCnt: number
  byKind: Record<ChannelKind, number>      // USD
  byKindPayerUv: Record<ChannelKind, number>
}

const ZERO_KIND = (): Record<ChannelKind, number> => ({
  organic: 0, tiktok: 0, meta: 0, other: 0,
})

export function summarizeRegister(rows: RegionRegisterRow[]): RegisterSummary {
  const byKind = ZERO_KIND()
  let total = 0
  for (const r of rows) {
    byKind[r.channel_kind] += r.register_uv
    total += r.register_uv
  }
  return { total, byKind }
}

export function summarizeRevenue(
  rows: RegionRevenueRow[],
  os: OsView = 'all',
): RevenueSummary {
  const byKind = ZERO_KIND()
  const byKindPayerUv = ZERO_KIND()
  let totalUsd = 0
  let payerUv = 0
  let orderCnt = 0
  for (const r of rows) {
    if (os === 'ios' && r.os_type !== 2) continue
    if (os === 'android' && r.os_type !== 1) continue
    byKind[r.channel_kind] += r.revenue_usd
    byKindPayerUv[r.channel_kind] += r.payer_uv
    totalUsd += r.revenue_usd
    payerUv += r.payer_uv
    orderCnt += r.order_cnt
  }
  return { totalUsd, payerUv, orderCnt, byKind, byKindPayerUv }
}

// ─── 趋势图数据：每天一个对象 ─────────────────────────────────
export interface RegisterTrendPoint {
  date: string
  organic: number
  tiktok: number
  meta: number
  other: number
}

export interface RevenueTrendPoint {
  date: string
  organic: number
  tiktok: number
  meta: number
  other: number
}

/** 按日 × channel_kind 把注册数 reshape 成 recharts 友好结构（每天一个对象） */
export function buildRegisterTrend(
  rows: RegionRegisterRow[],
  allDates: string[],
): RegisterTrendPoint[] {
  const map = new Map<string, RegisterTrendPoint>()
  for (const ds of allDates) {
    map.set(ds, { date: ds, organic: 0, tiktok: 0, meta: 0, other: 0 })
  }
  for (const r of rows) {
    const p = map.get(r.ds)
    if (!p) continue
    p[r.channel_kind] += r.register_uv
  }
  return allDates.map(d => map.get(d)!)
}

/** 按日 × channel_kind 把充值 USD reshape；可按 OS 过滤 */
export function buildRevenueTrend(
  rows: RegionRevenueRow[],
  allDates: string[],
  os: OsView = 'all',
): RevenueTrendPoint[] {
  const map = new Map<string, RevenueTrendPoint>()
  for (const ds of allDates) {
    map.set(ds, { date: ds, organic: 0, tiktok: 0, meta: 0, other: 0 })
  }
  for (const r of rows) {
    if (os === 'ios' && r.os_type !== 2) continue
    if (os === 'android' && r.os_type !== 1) continue
    const p = map.get(r.ds)
    if (!p) continue
    p[r.channel_kind] += r.revenue_usd
  }
  return allDates.map(d => map.get(d)!)
}

// ─── 国家维度聚合 ──────────────────────────────────────────────
export interface CountryRegisterRow {
  region: string
  total: number
  organic: number
  tiktok: number
  meta: number
  other: number
  /** 自然量占比 0~100 */
  organicShare: number
}

export interface CountryRevenueRow {
  region: string
  totalUsd: number
  organicUsd: number
  tiktokUsd: number
  metaUsd: number
  otherUsd: number
  payerUv: number
  orderCnt: number
  /** ARPU = totalUsd / payerUv，payerUv=0 时为 0 */
  arpu: number
  organicShare: number
}

export function aggregateCountryRegister(rows: RegionRegisterRow[]): CountryRegisterRow[] {
  const map = new Map<string, CountryRegisterRow>()
  for (const r of rows) {
    let row = map.get(r.region)
    if (!row) {
      row = {
        region: r.region, total: 0,
        organic: 0, tiktok: 0, meta: 0, other: 0, organicShare: 0,
      }
      map.set(r.region, row)
    }
    row.total += r.register_uv
    row[r.channel_kind] += r.register_uv
  }
  for (const row of map.values()) {
    row.organicShare = row.total > 0 ? (row.organic / row.total) * 100 : 0
  }
  return [...map.values()].sort((a, b) => b.total - a.total)
}

export function aggregateCountryRevenue(
  rows: RegionRevenueRow[],
  os: OsView = 'all',
): CountryRevenueRow[] {
  const map = new Map<string, CountryRevenueRow>()
  for (const r of rows) {
    if (os === 'ios' && r.os_type !== 2) continue
    if (os === 'android' && r.os_type !== 1) continue
    let row = map.get(r.region)
    if (!row) {
      row = {
        region: r.region,
        totalUsd: 0,
        organicUsd: 0, tiktokUsd: 0, metaUsd: 0, otherUsd: 0,
        payerUv: 0, orderCnt: 0, arpu: 0, organicShare: 0,
      }
      map.set(r.region, row)
    }
    row.totalUsd += r.revenue_usd
    row.payerUv += r.payer_uv
    row.orderCnt += r.order_cnt
    if (r.channel_kind === 'organic') row.organicUsd += r.revenue_usd
    else if (r.channel_kind === 'tiktok') row.tiktokUsd += r.revenue_usd
    else if (r.channel_kind === 'meta') row.metaUsd += r.revenue_usd
    else row.otherUsd += r.revenue_usd
  }
  for (const row of map.values()) {
    row.arpu = row.payerUv > 0 ? row.totalUsd / row.payerUv : 0
    row.organicShare = row.totalUsd > 0 ? (row.organicUsd / row.totalUsd) * 100 : 0
  }
  return [...map.values()].sort((a, b) => b.totalUsd - a.totalUsd)
}

// ─── Top N + Other ─────────────────────────────────────────────
/** 把超过 topN 的国家行合并为一行 'OTHER'。用于 Top 10 双柱图。 */
export function mergeTopN<T extends { region: string }>(
  rows: T[],
  topN: number,
  collapser: (rows: T[]) => T,
): T[] {
  if (rows.length <= topN) return rows
  const top = rows.slice(0, topN)
  const rest = rows.slice(topN)
  if (rest.length === 0) return top
  return [...top, collapser(rest)]
}

export function collapseRegisterRows(rest: CountryRegisterRow[]): CountryRegisterRow {
  let total = 0, organic = 0, tiktok = 0, meta = 0, other = 0
  for (const r of rest) {
    total += r.total; organic += r.organic; tiktok += r.tiktok
    meta += r.meta; other += r.other
  }
  return {
    region: 'OTHER',
    total, organic, tiktok, meta, other,
    organicShare: total > 0 ? (organic / total) * 100 : 0,
  }
}

export function collapseRevenueRows(rest: CountryRevenueRow[]): CountryRevenueRow {
  let totalUsd = 0, organicUsd = 0, tiktokUsd = 0, metaUsd = 0, otherUsd = 0
  let payerUv = 0, orderCnt = 0
  for (const r of rest) {
    totalUsd += r.totalUsd; organicUsd += r.organicUsd
    tiktokUsd += r.tiktokUsd; metaUsd += r.metaUsd; otherUsd += r.otherUsd
    payerUv += r.payerUv; orderCnt += r.orderCnt
  }
  return {
    region: 'OTHER',
    totalUsd, organicUsd, tiktokUsd, metaUsd, otherUsd,
    payerUv, orderCnt,
    arpu: payerUv > 0 ? totalUsd / payerUv : 0,
    organicShare: totalUsd > 0 ? (organicUsd / totalUsd) * 100 : 0,
  }
}

// ─── 区间内的所有日期（含空 buckets） ─────────────────────────
export function collectDates(
  registerRows: RegionRegisterRow[],
  revenueRows: RegionRevenueRow[],
  fallbackStart: string,
  fallbackEnd: string,
): string[] {
  const set = new Set<string>()
  for (const r of registerRows) set.add(r.ds)
  for (const r of revenueRows) set.add(r.ds)
  if (set.size === 0) {
    // 没数据时也要画 X 轴：从 fallback 区间生成
    const out: string[] = []
    const s = new Date(fallbackStart)
    const e = new Date(fallbackEnd)
    const cur = new Date(s)
    while (cur <= e) {
      const yy = cur.getFullYear()
      const mm = String(cur.getMonth() + 1).padStart(2, '0')
      const dd = String(cur.getDate()).padStart(2, '0')
      out.push(`${yy}-${mm}-${dd}`)
      cur.setDate(cur.getDate() + 1)
    }
    return out
  }
  return [...set].sort()
}
