import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState, useEffect } from 'react'
import { getRun, stopRun, pauseRun, resumeRun, triggerPartialCheckout, submitRun, retryRun, resetRun } from '@/api'
import { parquetDownloadUrl, csvDownloadUrl, downloadPartialCheckout, downloadPartialCheckoutCsv } from '@/api'
import { useAppStore } from '@/store'
import RunStatusBadge from '@/components/runs/RunStatusBadge'
import HelpTooltip from '@/components/ui/HelpTooltip'

const DEFAULT_GEE_CONCURRENCY = 2

function GeeConcurrencyInput({ value, onChange }: { value: number; onChange: (v: number) => void }) {
  return (
    <div className="flex items-center gap-2">
      <label className="text-xs text-gray-500 whitespace-nowrap flex items-center gap-1">
        GEE workers
        <HelpTooltip text="Number of concurrent Google Earth Engine requests. Higher values finish faster but risk hitting GEE quota limits." />
      </label>
      <input
        type="number"
        min={1}
        max={10}
        value={value}
        onChange={(e) => onChange(Math.max(1, Math.min(10, Number(e.target.value))))}
        className="w-16 text-xs border border-gray-200 rounded px-2 py-1 text-center"
      />
    </div>
  )
}

interface Props {
  runId: string | null
}

export default function ResultsPanel({ runId }: Props) {
  const queryClient = useQueryClient()
  const { pendingRun, setIsRunning, setActiveRunId } = useAppStore()
  const [retrying, setRetrying] = useState(false)
  const [partialDownloading, setPartialDownloading] = useState<string | null>(null)
  const [partialError, setPartialError] = useState<string | null>(null)
  const [geeConcurrency, setGeeConcurrency] = useState(DEFAULT_GEE_CONCURRENCY)

  const handlePartialDownload = async (product: string, format: 'parquet' | 'csv') => {
    const key = `${product}:${format}`
    setPartialDownloading(key)
    setPartialError(null)
    try {
      if (format === 'csv') {
        await downloadPartialCheckoutCsv(runId!, product)
      } else {
        await downloadPartialCheckout(runId!, product)
      }
    } catch (e) {
      setPartialError((e as Error).message)
    } finally {
      setPartialDownloading(null)
    }
  }

  const { data: run, isLoading } = useQuery({
    queryKey: ['run', runId],
    queryFn:  () => getRun(runId!),
    enabled:  Boolean(runId),
    refetchInterval: (query) => {
      const status = query.state.data?.status
      if (status === 'running' || status === 'paused' || retrying) return 2_000
      return false
    },
  })

  // Sync concurrency input when an existing run is loaded
  useEffect(() => {
    if (run?.gee_concurrency) setGeeConcurrency(run.gee_concurrency)
  }, [run?.gee_concurrency])

  const submitMutation = useMutation({
    mutationFn: () =>
      submitRun({ run_id: pendingRun.run_id, products: pendingRun.products, gee_concurrency: geeConcurrency }),
    onSuccess: (detail) => {
      setIsRunning(true)
      setActiveRunId(detail.run_id)
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['run', detail.run_id] })
    },
  })

  const stopMutation = useMutation({
    mutationFn: () => stopRun(runId!),
    onSuccess: () => {
      setIsRunning(false)
      queryClient.invalidateQueries({ queryKey: ['run', runId] })
    },
  })

  const pauseMutation = useMutation({
    mutationFn: () => pauseRun(runId!),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['run', runId] }),
  })

  const resumeMutation = useMutation({
    mutationFn: () => resumeRun(runId!, geeConcurrency !== run?.gee_concurrency ? geeConcurrency : undefined),
    onSuccess: (detail) => {
      queryClient.setQueryData(['run', runId], detail)
      queryClient.invalidateQueries({ queryKey: ['runs'] })
    },
  })

  const partialMutation = useMutation({
    mutationFn: () => triggerPartialCheckout(runId!),
  })

  const retryMutation = useMutation({
    mutationFn: () => retryRun(runId!, geeConcurrency),
    onMutate: () => {
      setRetrying(true)
      queryClient.setQueryData(['run', runId], (old: any) =>
        old ? { ...old, status: 'running' } : old
      )
    },
    onSuccess: (detail) => {
      setIsRunning(true)
      setActiveRunId(detail.run_id)
      queryClient.setQueryData(['run', detail.run_id], detail)
      queryClient.invalidateQueries({ queryKey: ['runs'] })
    },
    onError: () => {
      queryClient.invalidateQueries({ queryKey: ['run', runId] })
    },
    onSettled: () => setRetrying(false),
  })

  const nukeMutation = useMutation({
    mutationFn: () => resetRun(runId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['run', runId] })
      queryClient.invalidateQueries({ queryKey: ['runs'] })
    },
  })

  // Can submit if we have products configured and this is a fresh (unsubmitted) run
  const canSubmit =
    pendingRun.products.length > 0 &&
    pendingRun.products.every((p) => p.bands.length > 0 && p.stats.length > 0 && p.date_start && p.date_end) &&
    !submitMutation.isPending

  if (!runId && !pendingRun.run_id) {
    return (
      <div className="text-xs text-gray-400">
        Select a saved run or create a new one to get started.
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Submit new run (only when no active run is selected) */}
      {!runId && pendingRun.products.length > 0 && (
        <div className="card p-3">
          <p className="section-title flex items-center gap-1.5">New Run <HelpTooltip text="Submit your configured datasets and area of interest to start downloading satellite data from Google Earth Engine." /></p>
          <p className="text-xs text-gray-500 mb-2 font-mono">{pendingRun.run_id}</p>
          <GeeConcurrencyInput value={geeConcurrency} onChange={setGeeConcurrency} />
          <button
            className="btn-primary w-full mt-2"
            onClick={() => submitMutation.mutate()}
            disabled={!canSubmit}
          >
            {submitMutation.isPending ? 'Submitting…' : 'Run Analysis'}
          </button>
          {submitMutation.isError && (
            <p className="text-xs text-red-600 mt-2">
              {(submitMutation.error as Error)?.message ?? 'Submission failed'}
            </p>
          )}
        </div>
      )}

      {/* Existing run details */}
      {runId && (
        <>
          {isLoading ? (
            <p className="text-xs text-gray-400">Loading run…</p>
          ) : run ? (
            <>
              {/* Header card */}
              <div className="card p-3">
                <div className="flex items-center justify-between mb-1">
                  <span className="font-mono text-xs font-medium text-gray-700 truncate">
                    {run.run_id}
                  </span>
                  <RunStatusBadge status={run.status} />
                </div>
                {(run.config as any)?.aoi_name && (
                  <p className="text-xs text-gray-400 truncate">AOI: {(run.config as any).aoi_name}</p>
                )}

                {/* Failure banner */}
                {(run.status === 'failed' || run.status === 'stopped') && (() => {
                  const lastMsg = [...run.events].reverse().find(
                    (e) => e.level === 'status_change' || e.level === 'job_error'
                  )?.msg
                  return (
                    <div className="mt-2 px-2 py-1.5 rounded bg-red-50 border border-red-200">
                      <p className="text-xs font-medium text-red-700">
                        {run.status === 'stopped' ? 'Run was stopped' : 'Run failed'}
                      </p>
                      {lastMsg && (
                        <p className="text-xs text-red-500 mt-0.5">{lastMsg}</p>
                      )}
                    </div>
                  )
                })()}

                {/* Progress bar */}
                {run.job_counts.total > 0 && (
                  <div className="mt-2">
                    <div className="flex justify-between text-xs text-gray-500 mb-1">
                      <span>Chunks</span>
                      <span>
                        {run.job_counts.done}/{run.job_counts.total}
                        {run.job_counts.failed > 0 && (
                          <span className="text-red-500 ml-1">({run.job_counts.failed} failed)</span>
                        )}
                        {run.job_counts.shelved > 0 && (
                          <span className="text-amber-500 ml-1">({run.job_counts.shelved} shelved)</span>
                        )}
                      </span>
                    </div>
                    <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-brand-500 rounded-full transition-all"
                        style={{ width: `${(run.job_counts.done / run.job_counts.total) * 100}%` }}
                      />
                    </div>
                    {run.job_counts.pending > 0 && (
                      <p className="text-xs text-gray-400 mt-1">
                        {run.job_counts.pending} pending · {run.job_counts.running} running
                      </p>
                    )}
                  </div>
                )}
              </div>

              {/* Scheduled datasets */}
              {(() => {
                const products = (run.config as any)?.products as Record<string, any> | undefined
                if (!products || Object.keys(products).length === 0) return null
                return (
                  <div>
                    <p className="section-title flex items-center gap-1.5">Scheduled Datasets <HelpTooltip text="The satellite data products queued for this run, with their configured date ranges, bands, and processing statistics." /></p>
                    <div className="flex flex-col gap-2">
                      {Object.entries(products).map(([id, cfg]) => (
                        <div key={id} className="card p-3 space-y-1.5">
                          <p className="text-xs font-medium text-gray-800">{id}</p>
                          <p className="text-xs text-gray-500">
                            {cfg.start_date} → {cfg.end_date}
                            <span className="ml-2 text-gray-400">({cfg.cadence})</span>
                          </p>
                          <div className="flex flex-wrap gap-1">
                            {(cfg.bands as string[]).map((b: string) => (
                              <span key={b} className="text-xs px-2 py-0.5 rounded-full bg-brand-50 text-brand-700 border border-brand-200">{b}</span>
                            ))}
                          </div>
                          <div className="flex flex-wrap gap-1">
                            {(cfg.statistics as string[]).map((s: string) => (
                              <span key={s} className="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-600">{s}</span>
                            ))}
                          </div>
                          {cfg.time_chunks && (
                            <p className="text-xs text-gray-400">{(cfg.time_chunks as string[]).length} time chunks</p>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )
              })()}

              {/* Actions */}
              <div className="flex flex-col gap-2">
                {run.status === 'running' && (
                  <button
                    className="btn-secondary w-full"
                    onClick={() => pauseMutation.mutate()}
                    disabled={pauseMutation.isPending}
                  >
                    {pauseMutation.isPending ? 'Pausing…' : 'Pause Run'}
                  </button>
                )}

                {run.status === 'paused' && (
                  <>
                    <div className="card p-3 bg-amber-50 border border-amber-200 flex flex-col gap-2">
                      <p className="text-xs font-medium text-amber-700">Run is paused</p>
                      <GeeConcurrencyInput value={geeConcurrency} onChange={setGeeConcurrency} />
                      {geeConcurrency !== run.gee_concurrency && (
                        <p className="text-xs text-amber-600">
                          Changing workers will restart in-flight jobs
                        </p>
                      )}
                    </div>
                    <button
                      className="btn-primary w-full"
                      onClick={() => resumeMutation.mutate()}
                      disabled={resumeMutation.isPending}
                    >
                      {resumeMutation.isPending ? 'Resuming…' : 'Resume Run'}
                    </button>
                    {resumeMutation.isError && (
                      <p className="text-xs text-red-600 mt-1">
                        {(resumeMutation.error as Error)?.message ?? 'Resume failed'}
                      </p>
                    )}
                    <button
                      className="btn-danger w-full"
                      onClick={() => stopMutation.mutate()}
                      disabled={stopMutation.isPending}
                    >
                      {stopMutation.isPending ? 'Stopping…' : 'Stop Run'}
                    </button>
                  </>
                )}

                {run.status !== 'completed' && run.status !== 'paused' && (() => {
                  const finishedSet = new Set(run.finished_products ?? [])
                  const hasInProgress = run.products.some((p) => !finishedSet.has(p))
                  if (!hasInProgress) return null
                  return (
                    <button
                      className="btn-secondary w-full"
                      onClick={() => partialMutation.mutate()}
                      disabled={partialMutation.isPending}
                    >
                      {partialMutation.isPending ? 'Building…' : 'Build Partial Checkout'}
                    </button>
                  )
                })()}

                {(run.status === 'failed' || run.status === 'stopped') && (
                  <>
                    <GeeConcurrencyInput value={geeConcurrency} onChange={setGeeConcurrency} />
                    <button
                      className="btn-primary w-full"
                      onClick={() => retryMutation.mutate()}
                      disabled={retryMutation.isPending}
                    >
                      {retryMutation.isPending ? 'Retrying…' : 'Retry Run'}
                    </button>
                    {retryMutation.isError && (
                      <p className="text-xs text-red-600 mt-1">
                        {(retryMutation.error as Error)?.message ?? 'Retry failed'}
                      </p>
                    )}
                    <button
                      className="btn-danger w-full"
                      onClick={() => {
                        if (confirm('Delete all generated files for this run? This cannot be undone. (run.yaml and uploaded AOI will be preserved)')) {
                          nukeMutation.mutate()
                        }
                      }}
                      disabled={nukeMutation.isPending}
                    >
                      {nukeMutation.isPending ? 'Resetting…' : 'Reset Run Directory'}
                    </button>
                    {nukeMutation.isError && (
                      <p className="text-xs text-red-600 mt-1">
                        {(nukeMutation.error as Error)?.message ?? 'Reset failed'}
                      </p>
                    )}
                  </>
                )}
              </div>

              {/* Final downloads — completed run or individual finished products mid-run */}
              {(() => {
                const finishedSet = new Set(run.finished_products ?? [])
                const productsToShow = run.status === 'completed'
                  ? run.products
                  : run.products.filter((p) => finishedSet.has(p))
                if (productsToShow.length === 0) return null
                return (
                  <div>
                    <p className="section-title flex items-center gap-1.5">Download Results <HelpTooltip text="Download the fully completed analysis output. GeoParquet preserves geometry; CSV is a flat table without spatial data." /></p>
                    <div className="flex flex-col gap-2">
                      {productsToShow.map((product) => (
                        <div key={product} className="card p-3">
                          <p className="text-xs font-medium text-gray-700 mb-2">{product}</p>
                          <div className="flex gap-2">
                            <a
                              href={parquetDownloadUrl(run.run_id, product)}
                              download
                              className="btn-primary flex-1 text-center text-xs"
                            >
                              GeoParquet
                            </a>
                            <a
                              href={csvDownloadUrl(run.run_id, product)}
                              download
                              className="btn-secondary flex-1 text-center text-xs"
                            >
                              CSV
                            </a>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )
              })()}

              {/* Partial checkout downloads — only in-progress products (not yet merged) */}
              {(() => {
                if (run.status === 'completed') return null
                const finishedSet = new Set(run.finished_products ?? [])
                const inProgressProducts = run.products.filter((p) => !finishedSet.has(p))
                if (inProgressProducts.length === 0) return null
                return (
                  <div>
                    <p className="section-title flex items-center gap-1.5">Partial Checkout <HelpTooltip text="Download data for chunks that have already finished, even while the run is still in progress or has partially failed." /></p>
                    <div className="flex flex-col gap-2">
                      {inProgressProducts.map((product) => (
                        <div key={product} className="card p-3">
                          <p className="text-xs font-medium text-gray-700 mb-2">{product}</p>
                          <div className="flex gap-2">
                            <button
                              onClick={() => handlePartialDownload(product, 'parquet')}
                              disabled={partialDownloading === `${product}:parquet`}
                              className="btn-primary flex-1 text-center text-xs"
                            >
                              {partialDownloading === `${product}:parquet` ? 'Downloading…' : 'GeoParquet'}
                            </button>
                            <button
                              onClick={() => handlePartialDownload(product, 'csv')}
                              disabled={partialDownloading === `${product}:csv`}
                              className="btn-secondary flex-1 text-center text-xs"
                            >
                              {partialDownloading === `${product}:csv` ? 'Downloading…' : 'CSV'}
                            </button>
                          </div>
                        </div>
                      ))}
                      {partialError && (
                        <p className="text-xs text-red-600">{partialError}</p>
                      )}
                    </div>
                  </div>
                )
              })()}

              {/* Run log */}
              {run.events.length > 0 && (
                <div>
                  <p className="section-title flex items-center gap-1.5">Run Log <HelpTooltip text="Detailed event log for this run: job starts, completions, and any errors encountered during processing." direction="right" /></p>
                  <div className="card overflow-hidden">
                    <ul className="divide-y divide-gray-100 max-h-64 overflow-y-auto">
                      {run.events.slice(-50).map((ev, i) => {
                        const colour =
                          ev.level === 'job_error' || ev.level === 'error'
                            ? 'text-red-600'
                            : ev.level === 'job_shelved'
                            ? 'text-amber-600'
                            : ev.level === 'job_done'
                            ? 'text-green-600'
                            : ev.level === 'job_start'
                            ? 'text-brand-600'
                            : 'text-gray-700'
                        return (
                          <li key={i} className="px-3 py-1.5 text-xs">
                            <span className="text-gray-400 mr-2">
                              {new Date(ev.ts).toLocaleTimeString()}
                            </span>
                            <span className={colour}>{ev.msg}</span>
                          </li>
                        )
                      })}
                    </ul>
                  </div>
                </div>
              )}
            </>
          ) : null}
        </>
      )}
    </div>
  )
}
