import { useState, useRef, useEffect, useMemo } from 'react'
import { X, ChevronDown, Search } from 'lucide-react'
import { ALL_COUNTRIES, POPULAR_CODES, type CountryOption } from '@/constants/countries'

interface Props {
  value: string[]
  onChange: (codes: string[]) => void
  className?: string
}

export function CountryMultiSelect({ value, onChange, className = '' }: Props) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const containerRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const selectedSet = useMemo(() => new Set(value), [value])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    const list = q
      ? ALL_COUNTRIES.filter(c =>
          c.code.toLowerCase().includes(q)
          || c.name_en.toLowerCase().includes(q)
          || c.name_zh.includes(q)
        )
      : [
          ...ALL_COUNTRIES.filter(c => POPULAR_CODES.has(c.code)),
          ...ALL_COUNTRIES.filter(c => !POPULAR_CODES.has(c.code)),
        ]
    return list
  }, [query])

  function toggle(code: string) {
    if (selectedSet.has(code)) {
      onChange(value.filter(c => c !== code))
    } else {
      onChange([...value, code])
    }
  }

  function remove(code: string) {
    onChange(value.filter(c => c !== code))
  }

  const selectedCountries = value.map(code => ALL_COUNTRIES.find(c => c.code === code)).filter(Boolean) as CountryOption[]

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      {/* trigger */}
      <div
        className="w-full min-h-[42px] px-3 py-2 border border-gray-200 rounded-xl text-sm cursor-pointer flex items-center gap-1 flex-wrap focus-within:ring-2 focus-within:ring-blue-500/20 focus-within:border-blue-400 transition bg-white"
        onClick={() => { setOpen(true); setTimeout(() => inputRef.current?.focus(), 0) }}
      >
        {selectedCountries.length === 0 && !open && (
          <span className="text-gray-400 text-sm">请选择投放国家</span>
        )}
        {selectedCountries.map(c => (
          <span key={c.code}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md bg-blue-50 text-blue-600 text-xs font-medium">
            {c.name_zh} ({c.code})
            <button type="button" onClick={e => { e.stopPropagation(); remove(c.code) }}
              className="hover:text-blue-800"><X className="w-3 h-3" /></button>
          </span>
        ))}
        {open && (
          <input ref={inputRef} type="text" value={query} onChange={e => setQuery(e.target.value)}
            placeholder="搜索国家（中文/英文/代码）"
            className="flex-1 min-w-[120px] outline-none text-sm bg-transparent"
            onKeyDown={e => { if (e.key === 'Escape') setOpen(false) }}
          />
        )}
        <ChevronDown className={`w-4 h-4 text-gray-400 ml-auto shrink-0 transition ${open ? 'rotate-180' : ''}`} />
      </div>

      {/* dropdown */}
      {open && (
        <div className="absolute z-50 mt-1 w-full bg-white border border-gray-200 rounded-xl shadow-lg max-h-64 overflow-y-auto">
          {!query && (
            <div className="px-3 py-1.5 text-[11px] text-gray-400 border-b border-gray-100 flex items-center gap-1">
              <Search className="w-3 h-3" /> 输入中文/英文/国家代码搜索 · 共 {ALL_COUNTRIES.length} 个国家
            </div>
          )}
          {filtered.length === 0 && (
            <div className="px-3 py-4 text-sm text-gray-400 text-center">未找到匹配国家</div>
          )}
          {filtered.map((c, i) => {
            const isPopular = POPULAR_CODES.has(c.code)
            const checked = selectedSet.has(c.code)
            const showDivider = !query && i > 0 && isPopular !== POPULAR_CODES.has(filtered[i - 1].code)
            return (
              <div key={c.code}>
                {showDivider && <div className="border-t border-gray-100 mx-3 my-1" />}
                <button
                  type="button"
                  onClick={() => toggle(c.code)}
                  className={`w-full flex items-center gap-3 px-3 py-2 text-left hover:bg-gray-50 transition text-sm ${checked ? 'bg-blue-50/60' : ''}`}
                >
                  <input type="checkbox" checked={checked} readOnly
                    className="w-4 h-4 rounded border-gray-300 text-blue-500 pointer-events-none" />
                  <span className="flex-1 min-w-0">
                    <span className="font-medium text-gray-700">{c.name_zh}</span>
                    <span className="text-gray-400 ml-1.5">{c.name_en}</span>
                  </span>
                  <span className="text-xs text-gray-400 font-mono shrink-0">{c.code}</span>
                </button>
              </div>
            )
          })}
        </div>
      )}

      {/* selected count + clear */}
      {value.length > 0 && (
        <div className="flex items-center justify-between mt-1.5">
          <span className="text-xs text-gray-400">已选 {value.length} 个国家</span>
          <button type="button" onClick={() => onChange([])} className="text-xs text-red-400 hover:text-red-500">清空全部</button>
        </div>
      )}
    </div>
  )
}
