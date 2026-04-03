import AOIUpload from '@/components/aoi/AOIUpload'
import ResultsPanel from '@/components/results/ResultsPanel'
import { useAppStore } from '@/store'
import { useQuery } from '@tanstack/react-query'
import { getRun } from '@/api'

interface MainContentProps {
  keyValid: boolean
}

export default function MainContent({ keyValid }: MainContentProps) {
  const { activeRunId, pendingRun } = useAppStore()

  const displayRunId = activeRunId

  // Subscribe to the active run so existingAoiName stays reactive
  const { data: activeRun } = useQuery({
    queryKey: ['run', activeRunId],
    queryFn:  () => getRun(activeRunId!),
    enabled:  Boolean(activeRunId),
    staleTime: 30_000,
  })
  const existingAoiName = (activeRun?.config as any)?.aoi_name as string | undefined

  if (!keyValid) {
    return (
      <main className="flex-1 flex items-center justify-center bg-gray-50">
        <div className="text-center max-w-sm px-6">
          <div className="text-5xl mb-4">🛰️</div>
          <h2 className="text-lg font-semibold text-gray-800 mb-2">
            Welcome to GEE Web App
          </h2>
          <p className="text-sm text-gray-500">
            Upload your Google Earth Engine service account key in the sidebar
            to get started.
          </p>
        </div>
      </main>
    )
  }

  return (
    <main className="flex-1 flex overflow-hidden">
      {/* Left column – AOI upload + map */}
      <section className="flex-1 flex flex-col border-r border-gray-200 overflow-y-auto p-4 gap-4">
        {/* Show AOI uploader for the pending run or the active run */}
        <AOIUpload
          runId={displayRunId ?? (pendingRun.run_id || null)}
          existingAoiName={existingAoiName}
        />
      </section>

      {/* Right column – Results / submit */}
      <section className="w-96 flex-shrink-0 flex flex-col overflow-y-auto p-4 gap-4">
        <ResultsPanel runId={displayRunId} />
      </section>
    </main>
  )
}
