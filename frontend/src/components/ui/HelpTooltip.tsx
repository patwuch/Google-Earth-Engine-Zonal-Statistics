interface Props {
  text: string
  direction?: 'above' | 'below' | 'right'
}

export default function HelpTooltip({ text, direction = 'above' }: Props) {
  const positionClass =
    direction === 'above' ? 'bottom-full left-1/2 -translate-x-1/2 mb-2' :
    direction === 'below' ? 'top-full left-1/2 -translate-x-1/2 mt-2' :
    'left-full top-1/2 -translate-y-1/2 ml-2'

  const arrow =
    direction === 'above' ? <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-800" /> :
    direction === 'below' ? <span className="absolute bottom-full left-1/2 -translate-x-1/2 border-4 border-transparent border-b-gray-800" /> :
    <span className="absolute right-full top-1/2 -translate-y-1/2 border-4 border-transparent border-r-gray-800" />

  return (
    <span className="relative inline-flex group/tooltip">
      <span className="w-3.5 h-3.5 rounded-full bg-gray-200 text-gray-500 text-[9px] font-bold leading-none flex items-center justify-center cursor-help select-none hover:bg-gray-300 transition-colors">
        ?
      </span>
      <span className={`pointer-events-none absolute w-52 rounded-md bg-gray-800 px-2.5 py-2 text-[11px] text-white leading-relaxed whitespace-normal normal-case tracking-normal font-normal opacity-0 group-hover/tooltip:opacity-100 transition-opacity z-50 shadow-lg ${positionClass}`}>
        {text}
        {arrow}
      </span>
    </span>
  )
}
