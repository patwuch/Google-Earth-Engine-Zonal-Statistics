import { useQuery } from '@tanstack/react-query'
import { getGeeKeyStatus } from './api'
import Sidebar from './components/layout/Sidebar'
import MainContent from './components/layout/MainContent'

export default function App() {
  const { data: keyStatus, isLoading } = useQuery({
    queryKey: ['gee-key-status'],
    queryFn: getGeeKeyStatus,
    retry: false,
  })

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center text-gray-500 text-sm">
        Connecting to backend…
      </div>
    )
  }

  return (
    <div className="flex h-full overflow-hidden">
      <Sidebar keyStatus={keyStatus ?? null} />
      <MainContent keyValid={keyStatus?.valid ?? false} />
    </div>
  )
}
