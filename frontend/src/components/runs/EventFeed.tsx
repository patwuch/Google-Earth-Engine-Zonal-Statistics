import { useQuery } from '@tanstack/react-query'
import { listEvents } from '@/api'
import HelpTooltip from '@/components/ui/HelpTooltip'

export default function EventFeed() {
  const { data: events = [] } = useQuery({
    queryKey: ['events'],
    queryFn: () => listEvents(50),
    refetchInterval: 5_000,
  })

  return (
    <div>
      <p className="section-title flex items-center gap-1.5">Event Log <HelpTooltip text="Live feed of events across all runs, showing job starts, completions, and errors in real time." /></p>
      {events.length === 0 ? (
        <p className="text-xs text-gray-400">No events yet.</p>
      ) : (
        <div className="card overflow-hidden">
          <ul className="divide-y divide-gray-100 max-h-48 overflow-y-auto flex flex-col-reverse">
            {events.map((ev, i) => (
              <li key={i} className="px-3 py-1.5 text-xs">
                <div className="flex items-center gap-1.5 text-gray-400 mb-0.5">
                  <span className="font-mono text-brand-600 truncate max-w-[7rem]">{ev.run_id}</span>
                  <span>·</span>
                  <span>{new Date(ev.ts).toLocaleTimeString()}</span>
                </div>
                <span className={ev.level === 'error' || ev.level === 'job_error' ? 'text-red-600' : 'text-gray-700'}>
                  {ev.msg || ev.level}
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
