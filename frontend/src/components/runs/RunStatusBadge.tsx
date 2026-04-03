import type { RunStatus } from '@/types'

const CLASSES: Record<RunStatus, string> = {
  running:   'badge-running',
  paused:    'badge bg-amber-100 text-amber-700',
  completed: 'badge-completed',
  failed:    'badge-failed',
  stopped:   'badge-stopped',
  unknown:   'badge bg-gray-100 text-gray-500',
}

export default function RunStatusBadge({ status }: { status: RunStatus }) {
  return <span className={CLASSES[status]}>{status}</span>
}
