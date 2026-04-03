import { useState } from 'react'
import { useAppStore } from '@/store'
import type { ProductMeta } from '@/types'

interface Props {
  product: ProductMeta
}

export default function ProductCard({ product }: Props) {
  const { pendingRun, upsertProduct, removeProduct } = useAppStore()

  const existing = pendingRun.products.find((p) => p.product === product.id)
  const enabled  = Boolean(existing)

  // Per-band default stats
  const defaultBands = product.bands.map((b) => b.name)
  const defaultStats = [...new Set(product.bands.flatMap((b) => b.default_stats))]

  const [selectedBands, setSelectedBands] = useState<string[]>(existing?.bands ?? defaultBands)
  const [selectedStats, setSelectedStats] = useState<string[]>(existing?.stats ?? defaultStats)
  const [dateStart,     setDateStart]     = useState<string>(existing?.date_start ?? product.date_min)
  const [dateEnd,       setDateEnd]       = useState<string>(existing?.date_end   ?? product.date_max)
  const [expanded,      setExpanded]      = useState(false)

  function syncStore(bands: string[], stats: string[], start: string, end: string) {
    upsertProduct({ product: product.id, bands, stats, date_start: start, date_end: end })
  }

  function toggle() {
    if (enabled) {
      removeProduct(product.id)
    } else {
      setExpanded(true)
      syncStore(selectedBands, selectedStats, dateStart, dateEnd)
    }
  }

  function toggleBand(name: string) {
    const next = selectedBands.includes(name)
      ? selectedBands.filter((b) => b !== name)
      : [...selectedBands, name]
    setSelectedBands(next)
    if (enabled) syncStore(next, selectedStats, dateStart, dateEnd)
  }

  function toggleStat(stat: string) {
    const next = selectedStats.includes(stat)
      ? selectedStats.filter((s) => s !== stat)
      : [...selectedStats, stat]
    setSelectedStats(next)
    if (enabled) syncStore(selectedBands, next, dateStart, dateEnd)
  }

  function onDateChange(start: string, end: string) {
    setDateStart(start)
    setDateEnd(end)
    if (enabled) syncStore(selectedBands, selectedStats, start, end)
  }

  const cadenceLabel: Record<string, string> = {
    daily:    'Daily → monthly chunks',
    composite:'Composite → quarterly chunks',
    seasonal: 'Seasonal → one value per quarter',
    annual:   'Annual chunks',
  }

  const QUARTERS = [
    { label: 'Q1 (Jan–Mar)', value: 1 },
    { label: 'Q2 (Apr–Jun)', value: 4 },
    { label: 'Q3 (Jul–Sep)', value: 7 },
    { label: 'Q4 (Oct–Dec)', value: 10 },
  ]
  const minYear = parseInt(product.date_min.slice(0, 4))
  const maxYear = parseInt(product.date_max.slice(0, 4))
  const years   = Array.from({ length: maxYear - minYear + 1 }, (_, i) => minYear + i)

  function dateToSeason(d: string) {
    const [y, m] = d.split('-').map(Number)
    return { year: y, q: QUARTERS[Math.floor((m - 1) / 3)].value }
  }
  function seasonStart(year: number, q: number) {
    return `${year}-${String(q).padStart(2, '0')}-01`
  }
  function seasonEnd(year: number, q: number) {
    const ends: Record<number, string> = { 1: '03-31', 4: '06-30', 7: '09-30', 10: '12-31' }
    return `${year}-${ends[q]}`
  }

  return (
    <div className={`card transition-colors ${enabled ? 'border-brand-300' : ''}`}>
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2">
        <input
          type="checkbox"
          id={`chk-${product.id}`}
          checked={enabled}
          onChange={toggle}
          className="accent-brand-600 h-4 w-4 flex-shrink-0"
        />
        <label
          htmlFor={`chk-${product.id}`}
          className="text-xs font-medium text-gray-800 flex-1 cursor-pointer"
        >
          {product.label}
        </label>
        {enabled && (
          <button
            className="text-xs text-gray-400 hover:text-gray-600"
            onClick={() => setExpanded((v) => !v)}
          >
            {expanded ? '▲' : '▼'}
          </button>
        )}
      </div>

      {/* Expanded config */}
      {enabled && expanded && (
        <div className="px-3 pb-3 border-t border-gray-100 pt-2 space-y-3">

          {/* Date range */}
          {product.cadence === 'seasonal' ? (
            <div className="flex gap-2">
              {([
                { label: 'Start season', date: dateStart, onPick: (y: number, q: number) => onDateChange(seasonStart(y, q), dateEnd) },
                { label: 'End season',   date: dateEnd,   onPick: (y: number, q: number) => onDateChange(dateStart, seasonEnd(y, q)) },
              ] as const).map(({ label, date, onPick }) => {
                const { year, q } = dateToSeason(date)
                return (
                  <div key={label} className="flex-1">
                    <p className="text-xs text-gray-500 mb-1">{label}</p>
                    <div className="flex gap-1">
                      <select
                        className="input text-xs py-1 flex-1 min-w-0"
                        value={year}
                        onChange={(e) => onPick(Number(e.target.value), q)}
                      >
                        {years.map((y) => <option key={y} value={y}>{y}</option>)}
                      </select>
                      <select
                        className="input text-xs py-1"
                        value={q}
                        onChange={(e) => onPick(year, Number(e.target.value))}
                      >
                        {QUARTERS.map((qo) => <option key={qo.value} value={qo.value}>{qo.label}</option>)}
                      </select>
                    </div>
                  </div>
                )
              })}
            </div>
          ) : (
            <div className="flex gap-2">
              <div className="flex-1">
                <p className="text-xs text-gray-500 mb-1">Start date</p>
                <input
                  type="date"
                  className="input text-xs py-1"
                  value={dateStart}
                  min={product.date_min}
                  max={product.date_max}
                  onChange={(e) => onDateChange(e.target.value, dateEnd)}
                />
              </div>
              <div className="flex-1">
                <p className="text-xs text-gray-500 mb-1">End date</p>
                <input
                  type="date"
                  className="input text-xs py-1"
                  value={dateEnd}
                  min={product.date_min}
                  max={product.date_max}
                  onChange={(e) => onDateChange(dateStart, e.target.value)}
                />
              </div>
            </div>
          )}

          {/* Bands */}
          <div>
            <p className="text-xs text-gray-500 mb-1">Bands</p>
            <div className="flex flex-wrap gap-1">
              {product.bands.map((b) => (
                <button
                  key={b.name}
                  title={b.description}
                  onClick={() => toggleBand(b.name)}
                  className={[
                    'text-xs px-2 py-0.5 rounded-full border transition-colors',
                    selectedBands.includes(b.name)
                      ? 'bg-brand-600 text-white border-brand-600'
                      : 'bg-white text-gray-600 border-gray-300 hover:border-brand-400',
                  ].join(' ')}
                >
                  {b.name}
                </button>
              ))}
            </div>
          </div>

          {/* Stats */}
          <div>
            <p className="text-xs text-gray-500 mb-1">Statistics</p>
            <div className="flex flex-wrap gap-1">
              {product.supported_stats.map((stat) => (
                <button
                  key={stat}
                  onClick={() => toggleStat(stat)}
                  className={[
                    'text-xs px-2 py-0.5 rounded-full border transition-colors',
                    selectedStats.includes(stat)
                      ? 'bg-brand-600 text-white border-brand-600'
                      : 'bg-white text-gray-600 border-gray-300 hover:border-brand-400',
                  ].join(' ')}
                >
                  {stat}
                </button>
              ))}
            </div>
          </div>

          {/* Meta */}
          <p className="text-xs text-gray-400">
            {product.resolution_m}m · {cadenceLabel[product.cadence] ?? product.cadence}
          </p>
        </div>
      )}
    </div>
  )
}
