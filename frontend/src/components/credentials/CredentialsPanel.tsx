import { useCallback, useState } from 'react'
import { useDropzone } from 'react-dropzone'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { uploadGeeKey } from '@/api'
import type { GeeKeyStatus } from '@/types'
import HelpTooltip from '@/components/ui/HelpTooltip'

interface Props {
  keyStatus: GeeKeyStatus | null
}

export default function CredentialsPanel({ keyStatus }: Props) {
  const queryClient = useQueryClient()
  const [error, setError]         = useState<string | null>(null)
  const [replacing, setReplacing] = useState(false)

  const mutation = useMutation({
    mutationFn: uploadGeeKey,
    onSuccess: (status) => {
      if (status.valid) {
        setError(null)
        setReplacing(false)
        queryClient.invalidateQueries({ queryKey: ['gee-key-status'] })
      } else {
        setError(status.error ?? 'Invalid key file')
      }
    },
    onError: () => setError('Upload failed. Is the backend running?'),
  })

  const onDrop = useCallback(
    (accepted: File[]) => {
      if (accepted[0]) mutation.mutate(accepted[0])
    },
    [mutation],
  )

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'application/json': ['.json'] },
    maxFiles: 1,
    disabled: mutation.isPending,
  })

  if (keyStatus?.valid && !replacing) {
    return (
      <div>
        <p className="section-title flex items-center gap-1.5">GEE Credentials <HelpTooltip text="Upload your Google Earth Engine service account JSON key to authenticate API access." /></p>
        <div className="card p-3 flex items-center gap-2">
          <span className="text-green-500 text-base">●</span>
          <div className="flex-1 min-w-0">
            <p className="text-xs font-medium text-gray-700 truncate">
              {keyStatus.email}
            </p>
            <p className="text-xs text-gray-400">Authenticated</p>
          </div>
          <button
            className="text-xs text-gray-400 hover:text-gray-600"
            onClick={() => setReplacing(true)}
            title="Replace key"
          >
            Replace
          </button>
        </div>
      </div>
    )
  }

  return (
    <div>
      <p className="section-title">GEE Credentials</p>
      <div
        {...getRootProps()}
        className={[
          'border-2 border-dashed rounded-lg p-4 text-center cursor-pointer transition-colors',
          isDragActive
            ? 'border-brand-400 bg-brand-50'
            : 'border-gray-300 hover:border-brand-400 hover:bg-gray-50',
          mutation.isPending ? 'opacity-60 cursor-wait' : '',
        ].join(' ')}
      >
        <input {...getInputProps()} />
        <p className="text-sm text-gray-500">
          {mutation.isPending
            ? 'Uploading…'
            : isDragActive
            ? 'Drop the JSON key here'
            : 'Drop GEE service account JSON'}
        </p>
        <p className="text-xs text-gray-400 mt-1">or click to browse</p>
      </div>

      {error && (
        <p className="mt-2 text-xs text-red-600">{error}</p>
      )}
    </div>
  )
}
