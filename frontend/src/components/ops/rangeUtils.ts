import type { DatePreset, DateRange } from '@/types/ops'

/**
 * 把 Date 对象格式化成 'YYYY-MM-DD'（按本地时区年月日字面值，避免 UTC 偏一天）
 *
 * 注：本文件中我们把"日期"当成 LA 时区下的字面字符串处理 —
 * 用 Date 对象只是为了方便算偏移。所有 Date 的 year/month/day 字段都已经
 * 通过 todayInLA() 锚定到 LA 当天的字面年月日，再用 fmtDate / addDays 做偏移
 * 时不会引入本地时区污染。
 */
export function fmtDate(d: Date): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

/**
 * 取「LA 时区的今天」对应的 Date（本地零点）
 *
 * 后端口径全部按 LA 日（biz_ops_daily.ds = LA 当地日）。前端必须用相同时区
 * 算"今天/昨天/近 N 天"，否则北京 0:00 ~ 16:00 期间会比 LA 早 1 天，导致
 * 用户点「今天」查到 LA 还未开始的日期，看上去像是数据缺失。
 *
 * 实现：用 Intl.DateTimeFormat 的 en-CA locale（输出 YYYY-MM-DD 格式）
 * 直接读出 LA 时区的当天日期字面值，再用本地零点构造 Date — 后续 fmtDate
 * / addDays 都基于这组本地字面年月日，跟时区已无关。
 */
function todayInLA(): Date {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric', month: '2-digit', day: '2-digit',
  })
  const [y, m, d] = fmt.format(new Date()).split('-').map(Number)
  return new Date(y, (m || 1) - 1, d || 1)
}

/** 在某天基础上偏移 N 天（正数往后，负数往前） */
function addDays(base: Date, n: number): Date {
  const d = new Date(base)
  d.setDate(d.getDate() + n)
  return d
}

/**
 * 把 preset 解析为具体的起止日期区间（全部按 LA 时区算）
 *
 * 数据延迟：后端按 LA 日同步，今天的数据要等明天才会出齐，因此：
 *   - "近 N 天"统一采用行业惯例 — 截止到昨天的过去 N 天，不含今天的不完整数据
 *   - "今天" preset 单独保留，方便观察当天累计（注意会显示偏低）
 *
 *   - yesterday: [昨天, 昨天]
 *   - today:     [今天, 今天]                ← 单日，数据尚在出
 *   - last7:     [昨天 - 6, 昨天]   共 7 天 ← 不含今天
 *   - last14:    [昨天 - 13, 昨天]
 *   - last30:    [昨天 - 29, 昨天]
 *   - custom:    透传 fallback
 */
export function presetToRange(preset: DatePreset, fallback?: { start: string; end: string }): DateRange {
  const today = todayInLA()
  const yesterday = addDays(today, -1)
  switch (preset) {
    case 'yesterday': {
      return { preset, start: fmtDate(yesterday), end: fmtDate(yesterday) }
    }
    case 'today':
      return { preset, start: fmtDate(today), end: fmtDate(today) }
    case 'last7':
      return { preset, start: fmtDate(addDays(yesterday, -6)), end: fmtDate(yesterday) }
    case 'last14':
      return { preset, start: fmtDate(addDays(yesterday, -13)), end: fmtDate(yesterday) }
    case 'last30':
      return { preset, start: fmtDate(addDays(yesterday, -29)), end: fmtDate(yesterday) }
    case 'custom': {
      if (fallback?.start && fallback?.end) {
        return { preset, start: fallback.start, end: fallback.end }
      }
      return { preset, start: fmtDate(addDays(yesterday, -6)), end: fmtDate(yesterday) }
    }
  }
}

/** 区间是否仅 1 天 */
export function isSingleDay(range: DateRange): boolean {
  return range.start === range.end
}

/**
 * 给 KPI 卡标题用的「期次」描述：
 *   单日 → 显示具体日期文案
 *   多日 → 区间总称（「近 N 天」/「自定义」）— 提示 KPI 数字为区间累计
 */
export function periodLabel(range: DateRange): string {
  if (range.preset === 'today') return '今日'
  if (range.preset === 'yesterday') return '昨日'
  if (isSingleDay(range)) return range.end
  if (range.preset === 'last7') return '近 7 天'
  if (range.preset === 'last14') return '近 14 天'
  if (range.preset === 'last30') return '近 30 天'
  return '区间'
}

/** 给页面上"当前区间"展示用的字符串："YYYY-MM-DD ~ YYYY-MM-DD"（单日时只显示一个日期） */
export function rangeDisplay(range: DateRange): string {
  if (isSingleDay(range)) return range.start
  return `${range.start} ~ ${range.end}`
}

/** 序列化为 queryKey 用的稳定字符串（避免对象引用不同导致缓存失效） */
export function rangeKey(range: DateRange): string {
  return `${range.start}~${range.end}`
}

/** 计算区间天数（含两端） */
export function rangeDays(range: DateRange): number {
  const s = new Date(range.start)
  const e = new Date(range.end)
  s.setHours(0, 0, 0, 0)
  e.setHours(0, 0, 0, 0)
  const ms = e.getTime() - s.getTime()
  return Math.max(1, Math.round(ms / 86400000) + 1)
}
