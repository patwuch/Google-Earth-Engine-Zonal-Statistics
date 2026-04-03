import { useQuery } from '@tanstack/react-query'
import { getRun } from '@/api'
import HelpTooltip from '@/components/ui/HelpTooltip'

interface Props {
  runId: string
}

export default function RunDatasetView({ runId }: Props) {
  const { data: run } = useQuery({
    queryKey: ['run', runId],
    queryFn: () => getRun(runId),
    staleTime: 30_000,
  })

  const products = (run?.config as any)?.products as Record<string, any> | undefined

  if (!products || Object.keys(products).length === 0) {
    return (
      <div>
        <p className="section-title flex items-center gap-1.5">Datasets <HelpTooltip text="The satellite data products configured for this run, showing bands, statistics, and date ranges." direction="right" /></p>
        <p className="text-xs text-gray-400">No dataset config available.</p>
      </div>
    )
  }

  return (
    <div>
      <p className="section-title flex items-center gap-1.5">Datasets <HelpTooltip text="The satellite data products configured for this run, showing bands, statistics, and date ranges." direction="right" /></p>
      <div className="flex flex-col gap-2">
        {Object.entries(products).map(([id, cfg]) => (
          <div key={id} className="card border-brand-300 transition-colors">
            <div className="px-3 py-2">
              <p className="text-xs font-medium text-gray-800">{id}</p>
              <p className="text-xs text-gray-400 mt-0.5">
                {cfg.start_date} → {cfg.end_date}
              </p>
            </div>
            <div className="px-3 pb-3 border-t border-gray-100 pt-2 space-y-2">
              <div>
                <p className="text-xs text-gray-500 mb-1">Bands</p>
                <div className="flex flex-wrap gap-1">
                  {(cfg.bands as string[]).map((b: string) => (
                    <span key={b} className="text-xs px-2 py-0.5 rounded-full bg-brand-600 text-white border border-brand-600">
                      {b}
                    </span>
                  ))}
                </div>
              </div>
              <div>
                <p className="text-xs text-gray-500 mb-1">Statistics</p>
                <div className="flex flex-wrap gap-1">
                  {(cfg.statistics as string[]).map((s: string) => (
                    <span key={s} className="text-xs px-2 py-0.5 rounded-full bg-brand-600 text-white border border-brand-600">
                      {s}
                    </span>
                  ))}
                </div>
              </div>
              <p className="text-xs text-gray-400">
                {cfg.resolution_m ? `${cfg.resolution_m}m · ` : ''}{cfg.cadence}
                {cfg.time_chunks ? ` · ${(cfg.time_chunks as string[]).length} chunks` : ''}
              </p>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
