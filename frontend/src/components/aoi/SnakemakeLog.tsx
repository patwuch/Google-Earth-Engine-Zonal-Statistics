import { useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getRunLog } from '@/api'
import { useAppStore } from '@/store'
import HelpTooltip from '@/components/ui/HelpTooltip'

interface Props {
  runId: string
}

export default function SnakemakeLog({ runId }: Props) {
  const { data: lines = [], isLoading } = useQuery({
    queryKey: ['run-log', runId],
    queryFn:  () => getRunLog(runId, 200),
    refetchInterval: (query) => {
      // Poll while the run is active — parent will unmount us otherwise
      return 3_000
    },
  })

  const bottomRef = useRef<HTMLDivElement>(null)
  const { activeRunId } = useAppStore()
  const isActive = activeRunId === runId

  // Scroll to bottom when new lines arrive during an active run
  useEffect(() => {
    if (isActive) bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines, isActive])

  return (
    <div className="flex flex-col h-full">
      <p className="section-title shrink-0 flex items-center gap-1.5">Snakemake Log <HelpTooltip text="Raw Snakemake workflow output for this run. Lines are colour-coded: red for errors, yellow for warnings, and green for completions." direction="right" /></p>
      <div className="flex-1 rounded-lg border border-gray-200 bg-gray-950 overflow-y-auto p-3 font-mono text-xs min-h-0">
        {isLoading && (
          <span className="text-gray-500">Loading…</span>
        )}
        {!isLoading && lines.length === 0 && (
          <span className="text-gray-500">No log output yet.</span>
        )}
        {lines.map((line, i) => {
          const isError   = /error|failed|exception/i.test(line)
          const isWarning = /warning/i.test(line)
          const isSuccess = /finished|completed|100%/i.test(line)
          const colour    = isError   ? 'text-red-400'
                          : isWarning ? 'text-yellow-400'
                          : isSuccess ? 'text-green-400'
                          : 'text-gray-300'
          return (
            <div key={i} className={`leading-5 whitespace-pre-wrap break-all ${colour}`}>
              {line || '\u00A0'}
            </div>
          )
        })}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}
