import type { DailyOpsRow, DateRange } from '@/types/ops'

/**
 * 运营数据 — service 层
 *
 * 现阶段返回 mock 数据，后续替换为真实接口即可（hook 层不变）
 *
 * TODO: 替换为真实接口 GET /api/v1/ops/daily-stats?days={days}
 *   - 金额字段单位「分」（cents），整数
 *   - 字段顺序按日期升序（旧 → 今天）
 *   - 后端用 BIZ 业务库聚合 fact_drama_daily / fact_optimizer_daily 等
 */
export async function fetchOpsStats(days: DateRange): Promise<DailyOpsRow[]> {
  // 模拟 200~400ms 网络延迟，方便看到 loading 态
  await new Promise(resolve => setTimeout(resolve, 200 + Math.random() * 200))

  const today = new Date()
  const rows: DailyOpsRow[] = []

  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(today)
    d.setDate(today.getDate() - i)
    const dateStr = d.toISOString().slice(0, 10) // 'YYYY-MM-DD'

    const ios_sub_revenue = randInt(180_000, 320_000)
    const ios_onetime_revenue = randInt(80_000, 150_000)
    const android_sub_revenue = randInt(100_000, 200_000)
    const android_onetime_revenue = randInt(50_000, 110_000)

    rows.push({
      date: dateStr,
      ios_new_users: randInt(1000, 2000),
      android_new_users: randInt(800, 1500),
      ios_sub_revenue,
      ios_onetime_revenue,
      android_sub_revenue,
      android_onetime_revenue,
      // payers ≈ revenue / 150（与文档保持一致；150 对应 ARPPU 假设）
      ios_sub_payers:         Math.floor(ios_sub_revenue / 150),
      ios_onetime_payers:     Math.floor(ios_onetime_revenue / 150),
      android_sub_payers:     Math.floor(android_sub_revenue / 150),
      android_onetime_payers: Math.floor(android_onetime_revenue / 150),
    })
  }

  return rows
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}
