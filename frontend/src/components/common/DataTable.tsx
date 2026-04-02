import { cn } from '@/utils/cn'

export interface Column<T> {
  key: string
  title: string
  align?: 'left' | 'right' | 'center'
  render?: (row: T, index: number) => React.ReactNode
  width?: string
}

interface Props<T> {
  columns: Column<T>[]
  data: T[]
  rowKey: (row: T, index: number) => string
  emptyText?: string
  className?: string
}

export function DataTable<T>({ columns, data, rowKey, emptyText = '暂无数据', className }: Props<T>) {
  return (
    <div className={cn('overflow-x-auto', className)}>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-100">
            {columns.map(col => (
              <th
                key={col.key}
                className={cn(
                  'px-4 py-3 text-xs font-medium text-gray-400 whitespace-nowrap',
                  col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left',
                )}
                style={col.width ? { width: col.width } : undefined}
              >
                {col.title}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-4 py-12 text-center text-gray-300 text-sm">
                {emptyText}
              </td>
            </tr>
          ) : (
            data.map((row, idx) => (
              <tr
                key={rowKey(row, idx)}
                className="border-b border-gray-50 last:border-0 hover:bg-blue-50/30 transition-colors"
              >
                {columns.map(col => (
                  <td
                    key={col.key}
                    className={cn(
                      'px-4 py-2.5 text-sm text-gray-600',
                      col.align === 'right' ? 'text-right tabular-nums' : col.align === 'center' ? 'text-center' : 'text-left',
                    )}
                  >
                    {col.render
                      ? col.render(row, idx)
                      : String((row as Record<string, unknown>)[col.key] ?? '-')}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}
