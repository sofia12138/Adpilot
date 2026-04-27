/**
 * 广告回传分析页面 — Campaign → Adset → Ad 层级视图
 *
 * 架构说明：
 *   - 单次 GET /api/analysis/returned-conversion/hierarchy 请求，
 *     后端按 (campaign_id, adset_id, ad_id) 三维 GROUP BY，返回完整层级行。
 *   - 前端 buildTree() 从同一批数据自下而上聚合，确保父子数据守恒。
 *   - 不再使用三次独立聚合请求拼接树（老做法产生 "-" 空节点和花费不守恒问题）。
 *
 * 跳过规则（对齐需求）：
 *   - campaign_id 和 campaign_name 均为空 → 跳过整条 leaf
 *   - adset_id 和 adset_name 均为空 → 跳过 adset 层，该 leaf 指标仍计入 campaign 合计
 *   - ad_id 和 ad_name 均为空 → 跳过 ad 层，该 leaf 指标仍计入 adset 合计
 *
 * 开发环境下：buildTree 会在控制台打印父子 spend 不一致 warning（预期为跳过的层级行导致）。
 */
import { useState, useCallback, useMemo, Fragment } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ChevronRight, ChevronDown, Loader2, AlertCircle,
  DollarSign, Users, ShoppingCart, TrendingUp, BarChart3, Info,
} from 'lucide-react'
import { PageHeader } from '@/components/common/PageHeader'
import { DateRangeFilter, getDefaultDateRange, type DateRange } from '@/components/common/DateRangeFilter'
import {
  fetchHierarchyRows,
  type BaseReturnedFilter, type ReturnedAvailability,
  type ReturnedFieldAvailability, type HierarchyLeaf,
} from '@/services/returned-conversion'
import { GlobalSyncBar } from '@/components/common/GlobalSyncBar'

// ── 格式化工具 ────────────────────────────────────────────────

const fmtUsd = (n: number) =>
  `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
const fmtInt = (n: number) => n.toLocaleString()
const fmtRoi = (n: number): React.ReactNode => {
  if (n <= 0) return <span className="text-gray-300">-</span>
  return (
    <span className={n >= 1 ? 'text-emerald-600 font-semibold' : 'text-amber-600 font-semibold'}>
      {n.toFixed(4)}
    </span>
  )
}

// ── 树节点类型 ────────────────────────────────────────────────

interface MetricRow {
  spend: number
  impressions: number
  clicks: number
  installs: number
  registrations_returned: number
  purchase_value_returned: number
  purchase_count_returned: number
  subscribe_value_returned: number
  subscribe_count_returned: number
  total_value_returned: number
  cumulative_roi_returned: number
  d0_roi_returned: number
  d1_value_returned: number
  d1_roi_returned: number
  d0_registrations_returned: number
  d0_purchase_value_returned: number
  d0_subscribe_value_returned: number
}

interface AdNode extends MetricRow { id: string; name: string }
interface AdsetNode extends MetricRow { id: string; name: string; ads: AdNode[] }
interface CampaignNode extends MetricRow { id: string; name: string; adsets: AdsetNode[] }

// ── 树构建逻辑 ────────────────────────────────────────────────

const ZERO = (): MetricRow => ({
  spend: 0, impressions: 0, clicks: 0, installs: 0,
  registrations_returned: 0, purchase_value_returned: 0, purchase_count_returned: 0,
  subscribe_value_returned: 0, subscribe_count_returned: 0,
  total_value_returned: 0,
  cumulative_roi_returned: 0, d0_roi_returned: 0,
  d1_value_returned: 0, d1_roi_returned: 0,
  d0_registrations_returned: 0, d0_purchase_value_returned: 0,
  d0_subscribe_value_returned: 0,
})

/** 将 leaf 的可加指标累加到目标节点（ROI 类派生字段不参与累加） */
function accum(target: MetricRow, leaf: HierarchyLeaf) {
  target.spend                    += leaf.spend
  target.impressions              += leaf.impressions
  target.clicks                   += leaf.clicks
  target.installs                 += leaf.installs
  target.registrations_returned   += leaf.registrations_returned
  target.purchase_value_returned  += leaf.purchase_value_returned
  target.purchase_count_returned  += leaf.purchase_count_returned
  target.subscribe_value_returned += leaf.subscribe_value_returned
  target.subscribe_count_returned += leaf.subscribe_count_returned
  target.total_value_returned     += leaf.total_value_returned
  target.d1_value_returned        += leaf.d1_value_returned
  target.d0_registrations_returned   += leaf.d0_registrations_returned
  target.d0_purchase_value_returned  += leaf.d0_purchase_value_returned
  target.d0_subscribe_value_returned += leaf.d0_subscribe_value_returned
}

/** 根据累加后的原始指标重新计算 ROI 派生字段 */
function finalize(m: MetricRow) {
  m.spend = +m.spend.toFixed(4)
  const roi = (v: number) => m.spend > 0 ? +(v / m.spend).toFixed(4) : 0
  m.cumulative_roi_returned = roi(m.total_value_returned)
  m.d0_roi_returned         = roi(m.total_value_returned)
  m.d1_roi_returned         = roi(m.d1_value_returned)
}

/**
 * 从后端层级行列表构建 Campaign → Adset → Ad 树。
 *
 * 跳过规则：
 *   - campaign id+name 均空 → 跳过整行
 *   - adset id+name 均空    → 跳过 adset 层（leaf 指标仍计入 campaign）
 *   - ad id+name 均空       → 跳过 ad 层（leaf 指标仍计入 adset）
 */
function buildTree(leaves: HierarchyLeaf[]): CampaignNode[] {
  const campaignMap = new Map<string, CampaignNode>()
  const adsetMap    = new Map<string, AdsetNode>()

  for (const leaf of leaves) {
    const { campaign_id: cid, campaign_name: cname,
            adset_id:    sid, adset_name:    sname,
            ad_id:       did, ad_name:       dname } = leaf

    // ① 跳过无效 campaign
    if (!cid && !cname) continue

    // ② 确保 Campaign 节点存在
    if (!campaignMap.has(cid)) {
      campaignMap.set(cid, {
        id: cid,
        name: cname || `未命名广告系列 (${cid})`,
        adsets: [],
        ...ZERO(),
      })
    }
    const campaign = campaignMap.get(cid)!
    accum(campaign, leaf)

    // ③ 跳过无效 adset 层（leaf 指标已计入 campaign）
    if (!sid && !sname) continue

    // ④ 确保 Adset 节点存在
    const adsetKey = `${cid}::${sid}`
    if (!adsetMap.has(adsetKey)) {
      const node: AdsetNode = {
        id: sid,
        name: sname || `未命名 Adset (${sid})`,
        ads: [],
        ...ZERO(),
      }
      adsetMap.set(adsetKey, node)
      campaign.adsets.push(node)
    }
    const adset = adsetMap.get(adsetKey)!
    accum(adset, leaf)

    // ⑤ 跳过无效 ad 层（leaf 指标已计入 adset）
    if (!did && !dname) continue

    adset.ads.push({
      id:   did,
      name: dname || `未命名 Ad (${did})`,
      spend:                       leaf.spend,
      impressions:                 leaf.impressions,
      clicks:                      leaf.clicks,
      installs:                    leaf.installs,
      registrations_returned:      leaf.registrations_returned,
      purchase_value_returned:     leaf.purchase_value_returned,
      purchase_count_returned:     leaf.purchase_count_returned,
      subscribe_value_returned:    leaf.subscribe_value_returned,
      subscribe_count_returned:    leaf.subscribe_count_returned,
      total_value_returned:        leaf.total_value_returned,
      cumulative_roi_returned:     leaf.spend > 0 ? +(leaf.total_value_returned / leaf.spend).toFixed(4) : 0,
      d0_roi_returned:             leaf.spend > 0 ? +(leaf.total_value_returned / leaf.spend).toFixed(4) : 0,
      d1_value_returned:           leaf.d1_value_returned,
      d1_roi_returned:             leaf.spend > 0 ? +(leaf.d1_value_returned / leaf.spend).toFixed(4) : 0,
      d0_registrations_returned:   leaf.d0_registrations_returned,
      d0_purchase_value_returned:  leaf.d0_purchase_value_returned,
      d0_subscribe_value_returned: leaf.d0_subscribe_value_returned,
    })
  }

  // ⑥ 自下而上 finalize ROI，并按 spend 降序排列
  const result = [...campaignMap.values()].sort((a, b) => b.spend - a.spend)
  for (const c of result) {
    finalize(c)
    c.adsets.sort((a, b) => b.spend - a.spend)
    for (const a of c.adsets) {
      finalize(a)
      a.ads.sort((x, y) => y.spend - x.spend)
    }
  }

  // ⑦ 开发环境：打印父子 spend 守恒校验
  if (import.meta.env.DEV) {
    for (const c of result) {
      if (c.adsets.length > 0) {
        const adsetSum = c.adsets.reduce((s, a) => s + a.spend, 0)
        if (Math.abs(adsetSum - c.spend) > 0.01) {
          console.warn(
            `[树结构校验] Campaign "${c.name}" spend=${c.spend.toFixed(4)}, ` +
            `adset合计=${adsetSum.toFixed(4)} — 差值 ${(c.spend - adsetSum).toFixed(4)} ` +
            `（可能存在 adset_id/name 均为空的 leaf 被跳过）`
          )
        }
        for (const a of c.adsets) {
          if (a.ads.length > 0) {
            const adSum = a.ads.reduce((s, d) => s + d.spend, 0)
            if (Math.abs(adSum - a.spend) > 0.01) {
              console.warn(
                `[树结构校验] Adset "${a.name}" spend=${a.spend.toFixed(4)}, ` +
                `ad合计=${adSum.toFixed(4)} — 差值 ${(a.spend - adSum).toFixed(4)}`
              )
            }
          }
        }
      }
    }
  }

  return result
}

// ── 小组件 ────────────────────────────────────────────────────

function AvailCell({
  field, value, fmt,
}: {
  field: ReturnedFieldAvailability
  value: number
  fmt: (n: number) => React.ReactNode
}) {
  if (!field.supported) return <span className="text-gray-300">-</span>
  return <>{fmt(value)}</>
}

function Badge({ label, color = 'amber' }: { label: string; color?: 'amber' | 'red' }) {
  const cls = color === 'red'
    ? 'bg-red-50 text-red-600 border border-red-200'
    : 'bg-amber-50 text-amber-700 border border-amber-200'
  return <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${cls}`}>{label}</span>
}

function StatCard({
  label, value, icon: Icon, sub,
}: {
  label: string
  value: React.ReactNode
  icon: React.ElementType
  sub?: string
}) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 p-4 flex flex-col gap-1 shadow-sm">
      <div className="flex items-center gap-2 text-gray-400">
        <Icon className="w-4 h-4" />
        <span className="text-xs">{label}</span>
      </div>
      <div className="text-xl font-bold text-gray-800 truncate">{value}</div>
      {sub && <div className="text-[11px] text-gray-400">{sub}</div>}
    </div>
  )
}

// ── 表格行指标单元格（三层共用） ──────────────────────────────

function MetricCells({ row, avail }: { row: MetricRow; avail: ReturnedAvailability }) {
  return (
    <>
      <td className="text-right px-3 py-2.5 text-sm tabular-nums whitespace-nowrap">
        {fmtUsd(row.spend)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-gray-500 tabular-nums whitespace-nowrap">
        {fmtInt(row.impressions)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-gray-500 tabular-nums whitespace-nowrap">
        {fmtInt(row.clicks)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-gray-500 tabular-nums whitespace-nowrap">
        {fmtInt(row.installs)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-blue-700 font-medium tabular-nums whitespace-nowrap">
        <AvailCell field={avail.registrations_returned} value={row.registrations_returned} fmt={fmtInt} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-emerald-700 tabular-nums whitespace-nowrap">
        <AvailCell field={avail.purchase_value_returned} value={row.purchase_value_returned} fmt={fmtUsd} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-emerald-700 tabular-nums whitespace-nowrap">
        <AvailCell field={avail.purchase_count_returned} value={row.purchase_count_returned} fmt={fmtInt} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-teal-700 tabular-nums whitespace-nowrap">
        <AvailCell field={avail.subscribe_value_returned} value={row.subscribe_value_returned} fmt={fmtUsd} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm text-teal-700 tabular-nums whitespace-nowrap">
        <AvailCell field={avail.subscribe_count_returned} value={row.subscribe_count_returned} fmt={fmtInt} />
      </td>
      <td className="text-right px-3 py-2.5 text-sm font-semibold text-emerald-800 tabular-nums whitespace-nowrap">
        {fmtUsd(row.total_value_returned)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm tabular-nums whitespace-nowrap">
        {fmtRoi(row.cumulative_roi_returned)}
      </td>
      <td className="text-right px-3 py-2.5 text-sm tabular-nums whitespace-nowrap">
        {fmtRoi(row.d0_roi_returned)}
      </td>
    </>
  )
}

// SyncBar 已迁移至全局组件 GlobalSyncBar（@/components/common/GlobalSyncBar）

// ── 常量 ─────────────────────────────────────────────────────

const MEDIA_OPTIONS = [
  { value: '', label: '全部媒体' },
  { value: 'meta', label: 'Meta' },
  { value: 'tiktok', label: 'TikTok' },
  { value: 'google', label: 'Google' },
]

const PLATFORM_OPTIONS = [
  { value: '', label: '全部平台' },
  { value: 'ios', label: 'iOS' },
  { value: 'android', label: 'Android' },
  { value: 'mixed', label: 'Mixed' },
]

const TABLE_HEADERS = [
  'Campaign', '花费', '展示', '点击', '安装',
  '回传注册数',
  '回传充值价值', '回传内购数',
  '回传订阅价值', '回传订阅数',
  '回传总价值', '累计回传ROI', 'D0 ROI（回传）',
]

const COL_COUNT = 13

const _unsupported: ReturnedFieldAvailability = { supported: false, has_nonzero_data: false }
const DEFAULT_AVAIL: ReturnedAvailability = {
  registrations_returned:      _unsupported,
  purchase_value_returned:     _unsupported,
  purchase_count_returned:     _unsupported,
  subscribe_value_returned:    _unsupported,
  subscribe_count_returned:    _unsupported,
  d1_value_returned:           _unsupported,
  d0_registrations_returned:   _unsupported,
  d0_purchase_value_returned:  _unsupported,
  d0_subscribe_value_returned: _unsupported,
}

// ── 主页面组件 ────────────────────────────────────────────────

export default function ReturnedConversionPage() {
  const [dateRange, setDateRange]     = useState<DateRange>(() => getDefaultDateRange('7d'))
  const [mediaSource, setMediaSource] = useState('')
  const [platform, setPlatform]       = useState('')
  const [country, setCountry]         = useState('')
  const [searchKeyword, setSearchKeyword] = useState('')

  const [expandedCampaigns, setExpandedCampaigns] = useState(new Set<string>())
  const [expandedAdsets,    setExpandedAdsets]    = useState(new Set<string>())

  const baseFilter = useMemo<BaseReturnedFilter>(() => ({
    start_date:     dateRange.startDate,
    end_date:       dateRange.endDate,
    media_source:   mediaSource || undefined,
    platform:       platform || undefined,
    country:        country || undefined,
    search_keyword: searchKeyword || undefined,
  }), [dateRange, mediaSource, platform, country, searchKeyword])

  // 单次请求，后端三维 GROUP BY，前端构树
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['returned-conversion', 'hierarchy', baseFilter],
    queryFn: () => fetchHierarchyRows(baseFilter),
    staleTime: 2 * 60_000,
  })

  const summary  = data?.summary
  const avail    = data?.availability ?? DEFAULT_AVAIL
  // 从同一批数据构建树，保证父子守恒
  const campaigns = useMemo(
    () => buildTree(data?.rows ?? []),
    [data?.rows]
  )

  const toggleCampaign = useCallback((id: string) => {
    setExpandedCampaigns(prev => {
      const next = new Set(prev); next.has(id) ? next.delete(id) : next.add(id); return next
    })
  }, [])

  const toggleAdset = useCallback((key: string) => {
    setExpandedAdsets(prev => {
      const next = new Set(prev); next.has(key) ? next.delete(key) : next.add(key); return next
    })
  }, [])

  // ── 渲染 ─────────────────────────────────────────────────

  return (
    <div className="max-w-[1440px] mx-auto">

      {/* 页头 */}
      <PageHeader
        title={
          <span className="flex items-center gap-2">
            广告回传分析
            <Badge label="回传口径" color="amber" />
            <Badge label="非订单真值" color="red" />
          </span>
        }
        description="广告平台归因回传指标 · Campaign → Adset → Ad 层级视图"
      />

      {/* 同步状态栏 */}
      <GlobalSyncBar />

      {/* 免责声明 */}
      <div className="flex items-start gap-2 bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 mb-4 text-sm text-amber-800">
        <Info className="w-4 h-4 mt-0.5 shrink-0 text-amber-500" />
        <span>
          基于广告平台归因回传，仅用于投放优化分析，不等同于后端订单真值。
          所有金额为回传口径（returned），不反映实际财务收入。
        </span>
      </div>

      {/* 筛选区 */}
      <div className="bg-white rounded-xl border border-gray-100 p-4 mb-4 shadow-sm">
        <div className="mb-3">
          <DateRangeFilter value={dateRange} onChange={setDateRange} />
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div>
            <label className="text-xs text-gray-500 block mb-1">媒体来源</label>
            <select
              value={mediaSource}
              onChange={e => setMediaSource(e.target.value)}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            >
              {MEDIA_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">平台</label>
            <select
              value={platform}
              onChange={e => setPlatform(e.target.value)}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            >
              {PLATFORM_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">国家</label>
            <input
              value={country}
              onChange={e => setCountry(e.target.value)}
              placeholder="如 US / CN"
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            />
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">名称搜索</label>
            <input
              value={searchKeyword}
              onChange={e => setSearchKeyword(e.target.value)}
              placeholder="Campaign / Adset / Ad 名称"
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-blue-300"
            />
          </div>
        </div>
      </div>

      {/* 加载状态 */}
      {isLoading && (
        <div className="flex items-center justify-center py-32 text-gray-400">
          <Loader2 className="w-6 h-6 animate-spin mr-2" />
          <span className="text-sm">加载中…</span>
        </div>
      )}

      {/* 错误状态 */}
      {isError && (
        <div className="flex flex-col items-center justify-center py-24 text-red-400 gap-2">
          <AlertCircle className="w-8 h-8" />
          <p className="text-sm font-medium">数据加载失败</p>
          <p className="text-xs text-gray-400">{String(error)}</p>
        </div>
      )}

      {!isLoading && !isError && (
        <>
          {/* 指标卡 — 投放 + 回传两组 */}
          <div className="mb-4 space-y-3">
            <div>
              <div className="text-xs text-gray-400 mb-2 font-medium tracking-wide uppercase">投放指标</div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <StatCard label="花费"  value={summary ? fmtUsd(summary.spend) : '-'}          icon={DollarSign} />
                <StatCard label="展示"  value={summary ? fmtInt(summary.impressions) : '-'}    icon={BarChart3} />
                <StatCard label="点击"  value={summary ? fmtInt(summary.clicks) : '-'}         icon={BarChart3} />
                <StatCard label="安装"  value={summary ? fmtInt(summary.installs) : '-'}       icon={BarChart3} />
              </div>
            </div>
            <div>
              <div className="text-xs text-gray-400 mb-2 font-medium tracking-wide uppercase">回传指标</div>
              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-4 gap-3">
                <StatCard
                  label="回传注册数"
                  value={!summary ? '-' : avail.registrations_returned.supported ? fmtInt(summary.registrations_returned) : '暂未提供'}
                  icon={Users}
                  sub={avail.registrations_returned.supported ? '广告平台归因' : '当前平台不支持'}
                />
                <StatCard
                  label="回传充值价值"
                  value={!summary ? '-' : avail.purchase_value_returned.supported ? fmtUsd(summary.purchase_value_returned) : '暂未提供'}
                  icon={ShoppingCart}
                  sub={avail.purchase_value_returned.supported ? '广告平台归因' : '当前平台不支持'}
                />
                <StatCard
                  label="回传内购数"
                  value={!summary ? '-' : avail.purchase_count_returned.supported ? fmtInt(summary.purchase_count_returned) : '暂未提供'}
                  icon={ShoppingCart}
                  sub={avail.purchase_count_returned.supported ? '购买/充值次数' : '当前平台不支持'}
                />
                <StatCard
                  label="回传订阅价值"
                  value={!summary ? '-' : avail.subscribe_value_returned.supported ? fmtUsd(summary.subscribe_value_returned) : '暂未提供'}
                  icon={TrendingUp}
                  sub={avail.subscribe_value_returned.supported ? '广告平台归因' : '当前平台不支持'}
                />
                <StatCard
                  label="回传订阅数"
                  value={!summary ? '-' : avail.subscribe_count_returned.supported ? fmtInt(summary.subscribe_count_returned) : '暂未提供'}
                  icon={TrendingUp}
                  sub={avail.subscribe_count_returned.supported ? '订阅次数' : '当前平台不支持'}
                />
                <StatCard label="回传总价值"  value={summary ? fmtUsd(summary.total_value_returned) : '-'} icon={TrendingUp} sub="充值 + 订阅" />
                <StatCard
                  label="累计回传ROI"
                  value={summary && summary.cumulative_roi_returned > 0 ? summary.cumulative_roi_returned.toFixed(4) : '-'}
                  icon={BarChart3}
                  sub="回传总价值 / 花费"
                />
                <StatCard
                  label="D0 ROI（回传口径）"
                  value={summary && summary.d0_roi_returned > 0 ? summary.d0_roi_returned.toFixed(4) : '-'}
                  icon={BarChart3}
                  sub={summary?.d0_roi_is_fallback ? '暂无 D0 拆分，等同累计 ROI' : 'D0 价值 / 花费'}
                />
              </div>
            </div>
          </div>

          {/* 层级展开表格 */}
          <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden mb-6">
            <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
              <span className="text-sm font-medium text-gray-700">
                Campaign 层级
                <span className="ml-2 text-xs font-normal text-gray-400">({campaigns.length} 个)</span>
              </span>
              <span className="text-xs text-gray-400 flex items-center gap-1">
                <ChevronRight className="w-3 h-3" />
                点击行展开查看 Adset → Ad
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-gray-100 bg-gray-50/80">
                    {TABLE_HEADERS.map((h, i) => (
                      <th
                        key={h}
                        className={`px-3 py-2.5 text-xs font-medium text-gray-500 whitespace-nowrap ${
                          i === 0 ? 'text-left min-w-[320px]' : 'text-right'
                        }`}
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>

                <tbody>
                  {campaigns.length === 0 ? (
                    <tr>
                      <td colSpan={COL_COUNT} className="py-16 text-center">
                        <BarChart3 className="w-10 h-10 text-gray-200 mx-auto mb-3" />
                        <p className="text-sm text-gray-400">暂无回传数据</p>
                        <p className="text-xs text-gray-300 mt-1">
                          请先通过数据同步任务将广告回传数据写入 ad_returned_conversion_daily 表
                        </p>
                      </td>
                    </tr>
                  ) : (
                    campaigns.map(campaign => {
                      const cExpanded = expandedCampaigns.has(campaign.id)
                      return (
                        <Fragment key={campaign.id}>
                          {/* ── Campaign 行 ── */}
                          <tr
                            className={`border-t border-gray-100 cursor-pointer transition-colors select-none ${
                              cExpanded ? 'bg-blue-50/50' : 'hover:bg-gray-50'
                            }`}
                            onClick={() => toggleCampaign(campaign.id)}
                          >
                            <td className="py-3 pl-3 pr-3">
                              <div className="flex items-center gap-2">
                                <span className="text-gray-400 shrink-0">
                                  {cExpanded
                                    ? <ChevronDown className="w-4 h-4" />
                                    : <ChevronRight className="w-4 h-4" />
                                  }
                                </span>
                                <span
                                  className="text-sm font-semibold text-gray-800 line-clamp-2 break-words leading-tight"
                                  title={campaign.name}
                                >
                                  {campaign.name}
                                </span>
                              </div>
                            </td>
                            <MetricCells row={campaign} avail={avail} />
                          </tr>

                          {/* ── Adset 行（仅 Campaign 展开时渲染） ── */}
                          {cExpanded && campaign.adsets.map(adset => {
                            const adsetKey = `${campaign.id}::${adset.id}`
                            const aExpanded = expandedAdsets.has(adsetKey)
                            return (
                              <Fragment key={adsetKey}>
                                <tr
                                  className="border-t border-gray-100 bg-white hover:bg-blue-50/30 cursor-pointer transition-colors"
                                  onClick={() => toggleAdset(adsetKey)}
                                >
                                  <td className="py-2.5 pl-9 pr-3">
                                    <div className="flex items-center gap-2">
                                      <span className="w-px h-4 bg-gray-200 shrink-0" />
                                      <span className="text-gray-400 shrink-0">
                                        {aExpanded
                                          ? <ChevronDown className="w-3.5 h-3.5" />
                                          : <ChevronRight className="w-3.5 h-3.5" />
                                        }
                                      </span>
                                      <span
                                        className="text-xs text-gray-700 line-clamp-2 break-words leading-tight"
                                        title={adset.name}
                                      >
                                        {adset.name}
                                      </span>
                                    </div>
                                  </td>
                                  <MetricCells row={adset} avail={avail} />
                                </tr>

                                {/* ── Ad 行（仅 Adset 展开时渲染） ── */}
                                {aExpanded && (
                                  adset.ads.length === 0 ? (
                                    <tr key={`${adsetKey}__empty`}>
                                      <td colSpan={COL_COUNT} className="py-2 pl-24 text-xs text-gray-300 italic">
                                        暂无 Ad 数据
                                      </td>
                                    </tr>
                                  ) : (
                                    adset.ads.map(ad => (
                                      <tr
                                        key={`${adsetKey}::${ad.id}`}
                                        className="border-t border-gray-50 hover:bg-gray-50/50 transition-colors"
                                      >
                                        <td className="py-2 pl-20 pr-3">
                                          <div className="flex items-center gap-2">
                                            <span className="w-1.5 h-1.5 rounded-full bg-gray-300 shrink-0" />
                                            <span
                                              className="text-xs text-gray-500 line-clamp-2 break-words leading-tight"
                                              title={ad.name}
                                            >
                                              {ad.name}
                                            </span>
                                          </div>
                                        </td>
                                        <MetricCells row={ad} avail={avail} />
                                      </tr>
                                    ))
                                  )
                                )}
                              </Fragment>
                            )
                          })}

                          {/* 当 campaign 展开但没有 adsets 时的提示 */}
                          {cExpanded && campaign.adsets.length === 0 && (
                            <tr>
                              <td colSpan={COL_COUNT} className="py-2 pl-10 text-xs text-gray-300 italic">
                                暂无 Adset 数据
                              </td>
                            </tr>
                          )}
                        </Fragment>
                      )
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
