interface Props {
  source?: string | null
  /** 显示在 badge 旁边的小说明文字（可选） */
  hint?: string
  className?: string
}

/**
 * 数据源角标 — 用于标识当前视图的数据是来自哪个口径
 *
 * 后端 endpoint 在 ?source=attribution|legacy 切换数据源时，会在响应里返回
 * `data._source`（也可能是 `_source` 顶层字段）。前端读出来传给本组件即可。
 *
 * 仅在 source = 'attribution' 时显示蓝色角标，避免干扰默认 legacy 视图。
 *
 * 用法：
 *   const _source = res?.data?._source || res?._source
 *   <DataSourceBadge source={_source} />
 */
export function DataSourceBadge({ source, hint, className = '' }: Props) {
  if (!source || source === 'legacy') return null

  const isAttribution = source === 'attribution'
  const label = isAttribution ? '数据口径：数仓归因' : `数据源：${source}`
  const tooltip = isAttribution
    ? '当前视图来自 biz_attribution_ad_daily / intraday（数仓真实充值口径）。'
      + '与平台 API 回传 conversion_value 的 legacy 口径相比，spend 基本一致而 revenue 通常更可信。'
    : hint || ''

  return (
    <span
      title={tooltip}
      className={
        'inline-flex items-center gap-1 px-2 py-0.5 text-[11px] font-medium '
        + 'rounded-full border border-blue-200 bg-blue-50 text-blue-700 '
        + className
      }
    >
      <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor">
        <path d="M8 1.5a6.5 6.5 0 100 13 6.5 6.5 0 000-13zm.75 9.5a.75.75 0 11-1.5 0v-3.5a.75.75 0 011.5 0V11zm-.75-6a1 1 0 110 2 1 1 0 010-2z" />
      </svg>
      {label}
    </span>
  )
}
