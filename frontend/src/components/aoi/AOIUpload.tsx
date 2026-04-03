import { useCallback, useState } from 'react'
import { useDropzone } from 'react-dropzone'
import { useMutation } from '@tanstack/react-query'
import { uploadAOI } from '@/api'
import type { AOIInfo } from '@/types'
import MapPreview from './MapPreview'
import SnakemakeLog from './SnakemakeLog'
import HelpTooltip from '@/components/ui/HelpTooltip'

interface Props {
  runId: string | null
  existingAoiName?: string
}

export default function AOIUpload({ runId, existingAoiName }: Props) {
  const [aoi, setAoi] = useState<AOIInfo | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [uploadPct, setUploadPct] = useState<number | null>(null)

  const mutation = useMutation({
    mutationFn: ({ file }: { file: File }) => {
      if (!runId) throw new Error('Select or create a run first')
      setUploadPct(0)
      return uploadAOI(runId, file, setUploadPct)
    },
    onSuccess: (info) => {
      setAoi(info)
      setError(null)
      setUploadPct(null)
    },
    onError: (err: Error) => {
      setError(err.message)
      setUploadPct(null)
    },
  })

  const onDrop = useCallback(
    (accepted: File[]) => {
      if (accepted[0]) mutation.mutate({ file: accepted[0] })
    },
    [mutation],
  )

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/zip': ['.zip'],
      'application/geo+json': ['.geojson'],
      'application/octet-stream': ['.parquet'],
    },
    maxFiles: 1,
    disabled: !runId || mutation.isPending,
  })

  return (
    <div className="flex flex-col gap-3 h-full">
      <div>
        <p className="section-title flex items-center gap-1.5">Area of Interest <HelpTooltip text="Upload the geographic region to download satellite data for. Accepts shapefiles (.zip), GeoJSON, or GeoParquet files." direction="right" /></p>

        {!runId && (
          <p className="text-xs text-gray-400">
            Select or create a run session first.
          </p>
        )}

        {runId && existingAoiName && (
          <div className="flex items-center gap-2 px-3 py-2 rounded-md bg-gray-50 border border-gray-200">
            <span className="text-gray-400 text-xs">📁</span>
            <span className="text-xs text-gray-600 font-mono truncate">{existingAoiName}</span>
            <span className="text-xs text-gray-400 ml-auto shrink-0">locked</span>
          </div>
        )}

        {runId && !existingAoiName && (
          <div
            {...getRootProps()}
            className={[
              'border-2 border-dashed rounded-lg p-5 text-center cursor-pointer transition-colors',
              isDragActive
                ? 'border-brand-400 bg-brand-50'
                : 'border-gray-300 hover:border-brand-400 hover:bg-gray-50',
              mutation.isPending ? 'opacity-60 cursor-wait' : '',
            ].join(' ')}
          >
            <input {...getInputProps()} />
            <p className="text-sm text-gray-500">
              {mutation.isPending
                ? uploadPct !== null && uploadPct < 100
                  ? `Uploading… ${uploadPct}%`
                  : 'Processing…'
                : isDragActive
                ? 'Drop file here'
                : 'Drop shapefile (.zip), GeoJSON, or GeoParquet'}
            </p>
            {mutation.isPending && (
              <div className="mt-3 w-full bg-gray-200 rounded-full h-1.5 overflow-hidden">
                {uploadPct !== null && uploadPct < 100 ? (
                  <div
                    className="h-full bg-brand-400 rounded-full transition-all duration-200"
                    style={{ width: `${uploadPct}%` }}
                  />
                ) : (
                  <div className="h-full bg-brand-400 rounded-full animate-[indeterminate_1.4s_ease-in-out_infinite] w-1/2" />
                )}
              </div>
            )}
            {!mutation.isPending && <p className="text-xs text-gray-400 mt-1">or click to browse</p>}
          </div>
        )}

        {error && <p className="mt-2 text-xs text-red-600">{error}</p>}

        {aoi && !existingAoiName && (
          <div className="mt-2 text-xs text-gray-600 space-y-0.5">
            <p>
              <span className="font-medium">{aoi.feature_count}</span> features ·{' '}
              <span className="font-medium">{aoi.crs}</span>
            </p>
            <p className="text-gray-400">
              Bounds: [{aoi.bounds.map((v) => v.toFixed(4)).join(', ')}]
            </p>
          </div>
        )}
      </div>

      {/* Map or Snakemake log depending on whether geometry is locked */}
      <div className="flex-1 min-h-0">
        {existingAoiName && runId
          ? <SnakemakeLog runId={runId} />
          : (
            <div className="rounded-lg overflow-hidden border border-gray-200 h-full min-h-64">
              <MapPreview geojson={aoi?.geojson_preview ?? null} bounds={aoi?.bounds ?? null} />
            </div>
          )
        }
      </div>
    </div>
  )
}
