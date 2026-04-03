import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { listRuns } from '@/api'
import { useAppStore } from '@/store'
import RunStatusBadge from './RunStatusBadge'
import HelpTooltip from '@/components/ui/HelpTooltip'

export default function RunSelector() {
  const { data: runs = [] } = useQuery({
    queryKey: ['runs'],
    queryFn: listRuns,
    refetchInterval: 5_000,
  })

  const { activeRunId, pendingRun, setActiveRunId, setPendingRunId, resetPending } = useAppStore()
  const [newRunId, setNewRunId]         = useState('')
  const [autoId, setAutoId]             = useState('')
  const [showNew, setShowNew]           = useState(false)
  const [showPrevious, setShowPrevious] = useState(false)

  const hasActiveRun  = runs.some((r) => r.status === 'running')
  const activeRunLabel = activeRunId ?? pendingRun.run_id

  function openNew() {
    setAutoId(crypto.randomUUID())
    setNewRunId('')
    setShowNew(true)
  }

  function confirmNewRun() {
    const id = newRunId.trim() || autoId
    resetPending()
    setPendingRunId(id)
    setActiveRunId(null)
    setShowNew(false)
    setNewRunId('')
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-1.5">
          <p className="section-title mb-0">Run Session</p>
          <HelpTooltip text="A run session groups your dataset requests. Create a new one or select a previous run to view its results." />
        </div>
        <button
          className="text-xs text-brand-600 hover:text-brand-800 font-medium disabled:opacity-40 disabled:cursor-not-allowed"
          onClick={showNew ? () => setShowNew(false) : openNew}
          disabled={!showNew && hasActiveRun}
          title={!showNew && hasActiveRun ? 'A run is already in progress' : undefined}
        >
          {showNew ? 'Cancel' : '+ New'}
        </button>
      </div>

      {activeRunLabel && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-md bg-brand-50 border border-brand-200 mb-2">
          <span className="text-brand-500 text-xs font-medium shrink-0">Active run</span>
          <span className="font-mono text-xs text-brand-800 truncate">{activeRunLabel}</span>
        </div>
      )}

      {showNew && (
        <div className="mb-3 flex gap-2">
          <input
            className="input flex-1 font-mono"
            placeholder={autoId}
            value={newRunId}
            onChange={(e) => setNewRunId(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && confirmNewRun()}
          />
          <button className="btn-primary" onClick={confirmNewRun}>
            Confirm
          </button>
        </div>
      )}

      {runs.length > 0 && (
        <div className="mt-1">
          <button
            className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 transition-colors"
            onClick={() => setShowPrevious((v) => !v)}
          >
            <svg
              className={`w-3 h-3 transition-transform ${showPrevious ? 'rotate-90' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            Previous runs ({runs.length})
          </button>

          {showPrevious && (
            <ul className="space-y-1 mt-2 max-h-48 overflow-y-auto">
              {runs.map((run) => (
                <li key={run.run_id}>
                  <button
                    className={[
                      'w-full text-left px-3 py-2 rounded-md text-xs transition-colors',
                      activeRunId === run.run_id
                        ? 'bg-brand-50 border border-brand-200 text-brand-800'
                        : 'hover:bg-gray-50 text-gray-700',
                    ].join(' ')}
                    onClick={() => {
                      setActiveRunId(run.run_id)
                      setShowPrevious(false)
                    }}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-mono font-medium truncate">{run.run_id}</span>
                      <RunStatusBadge status={run.status} />
                    </div>
                    {run.products.length > 0 && (
                      <div className="text-gray-400 mt-0.5 truncate">
                        {run.products.join(', ')}
                      </div>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  )
}
