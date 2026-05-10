/**
 * 运营数据面板 — 通用格式化与工具函数
 */

/**
 * USD → 人民币常用展示格式
 *   0 ~ 999       → "$0", "$99", "$999"
 *   1k ~ 999.9k   → "$1.2k", "$99.9k"
 *   1M+           → "$1.23M"
 *
 * 后端字段已是 USD 浮点（已从美分换算）
 */
export function fmtUsd(usd: number): string {
  const v = usd || 0
  if (Math.abs(v) >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
  if (Math.abs(v) >= 1_000) return `$${(v / 1_000).toFixed(1)}k`
  return `$${v.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}`
}

/** 整数 + 「人」（人数格式） */
export function fmtPersons(n: number): string {
  return `${Math.round(n || 0).toLocaleString()}人`
}

/** YYYY-MM-DD → MM/DD（X 轴展示） */
export function fmtDateMMDD(date: string): string {
  if (!date || date.length < 10) return date
  return `${date.slice(5, 7)}/${date.slice(8, 10)}`
}

/**
 * 计算一组日期数组中"最多 N 个"等距刻度，含首尾。
 * 用于 Recharts <XAxis ticks={...}>，避免 X 轴拥挤。
 */
export function pickTicks(dates: string[], maxTicks = 7): string[] {
  if (dates.length === 0) return []
  if (dates.length <= maxTicks) return dates
  const step = (dates.length - 1) / (maxTicks - 1)
  const out: string[] = []
  for (let i = 0; i < maxTicks; i++) {
    out.push(dates[Math.round(i * step)])
  }
  // 去重防呆
  return Array.from(new Set(out))
}

/** 较昨日涨跌 %（last vs prev）。任一缺失返回 null。prev=0 且 last>0 → +∞ 用 null 避免炸 UI */
export function calcDelta(last: number | undefined, prev: number | undefined): number | null {
  if (last == null || prev == null) return null
  if (!isFinite(last) || !isFinite(prev)) return null
  if (prev === 0) return null
  return ((last - prev) / prev) * 100
}
