import axios from 'axios'
import type {
  GeeKeyStatus,
  RunSummary,
  RunDetail,
  RunEvent,
  AOIInfo,
  ProductMeta,
  SubmitRunRequest,
} from './types'

const http = axios.create({
  baseURL: '/api',
  timeout: 120_000,
})

// ─── GEE credentials ─────────────────────────────────────────────────────────

export async function uploadGeeKey(file: File): Promise<GeeKeyStatus> {
  const form = new FormData()
  form.append('file', file)
  const { data } = await http.post<GeeKeyStatus>('/gee-key', form)
  return data
}

export async function getGeeKeyStatus(): Promise<GeeKeyStatus> {
  const { data } = await http.get<GeeKeyStatus>('/gee-key')
  return data
}

// ─── Product registry ─────────────────────────────────────────────────────────

export async function getProducts(): Promise<ProductMeta[]> {
  const { data } = await http.get<ProductMeta[]>('/products')
  return data
}

// ─── Runs ─────────────────────────────────────────────────────────────────────

export async function listRuns(): Promise<RunSummary[]> {
  const { data } = await http.get<RunSummary[]>('/runs')
  return data
}

export async function getRun(runId: string): Promise<RunDetail> {
  const { data } = await http.get<RunDetail>(`/runs/${runId}`)
  return data
}

export async function submitRun(body: SubmitRunRequest): Promise<RunDetail> {
  const { data } = await http.post<RunDetail>('/runs', body)
  return data
}

export async function stopRun(runId: string): Promise<void> {
  await http.delete(`/runs/${runId}`)
}

export async function triggerPartialCheckout(runId: string): Promise<void> {
  await http.post(`/runs/${runId}/partial`)
}

export async function retryRun(runId: string, geeConcurrency?: number): Promise<RunDetail> {
  const { data } = await http.post<RunDetail>(`/runs/${runId}/retry`,
    geeConcurrency !== undefined ? { gee_concurrency: geeConcurrency } : {}
  )
  return data
}

export async function pauseRun(runId: string): Promise<void> {
  await http.post(`/runs/${runId}/pause`)
}

export async function resumeRun(runId: string, geeConcurrency?: number): Promise<RunDetail> {
  const { data } = await http.post<RunDetail>(`/runs/${runId}/resume`,
    geeConcurrency !== undefined ? { gee_concurrency: geeConcurrency } : {}
  )
  return data
}

export async function resetRun(runId: string): Promise<void> {
  await http.post(`/runs/${runId}/reset`)
}

export async function getRunLog(runId: string, lines = 100): Promise<string[]> {
  const { data } = await http.get<{ lines: string[] }>(`/runs/${runId}/log`, { params: { lines } })
  return data.lines
}

// ─── Events ──────────────────────────────────────────────────────────────────

export interface GlobalEvent extends RunEvent {
  run_id: string
}

export async function listEvents(limit = 50): Promise<GlobalEvent[]> {
  const { data } = await http.get<GlobalEvent[]>('/events', { params: { limit } })
  return data
}

// ─── AOI ─────────────────────────────────────────────────────────────────────

export async function uploadAOI(
  runId: string,
  file: File,
  onUploadProgress?: (pct: number) => void,
): Promise<AOIInfo> {
  const form = new FormData()
  form.append('file', file)
  const { data } = await http.post<AOIInfo>(`/runs/${runId}/aoi`, form, {
    timeout: 120_000,
    onUploadProgress: onUploadProgress
      ? (e) => onUploadProgress(e.total ? Math.round((e.loaded / e.total) * 100) : 0)
      : undefined,
  })
  return data
}

// ─── Downloads ───────────────────────────────────────────────────────────────

export function parquetDownloadUrl(runId: string, product: string): string {
  return `/api/runs/${runId}/download/${product}`
}

export function csvDownloadUrl(runId: string, product: string): string {
  return `/api/runs/${runId}/download/${product}/csv`
}

export function partialDownloadUrl(runId: string, product: string): string {
  return `/api/runs/${runId}/download/${product}/partial`
}

export async function downloadPartialCheckoutCsv(runId: string, product: string): Promise<void> {
  const url = `/api/runs/${runId}/download/${product}/partial-csv`
  const res = await fetch(url)
  if (!res.ok) {
    if (res.status === 404) {
      throw new Error('No partial data yet — click "Build Partial Checkout" first, then wait a moment')
    }
    throw new Error(`Download failed (${res.status})`)
  }
  const blob = await res.blob()
  const disposition = res.headers.get('content-disposition') ?? ''
  const match = disposition.match(/filename="([^"]+)"/)
  const filename = match ? match[1] : `${product}_partial.csv`
  const objUrl = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = objUrl
  a.download = filename
  a.click()
  URL.revokeObjectURL(objUrl)
}

export async function downloadPartialCheckout(runId: string, product: string): Promise<void> {
  const url = partialDownloadUrl(runId, product)
  const res = await fetch(url)
  if (!res.ok) {
    if (res.status === 404) {
      throw new Error('No partial data yet — click "Build Partial Checkout" first, then wait a moment')
    }
    throw new Error(`Download failed (${res.status})`)
  }
  const blob = await res.blob()
  const disposition = res.headers.get('content-disposition') ?? ''
  const match = disposition.match(/filename="([^"]+)"/)
  const filename = match ? match[1] : `${product}_partial.parquet`
  const objUrl = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = objUrl
  a.download = filename
  a.click()
  URL.revokeObjectURL(objUrl)
}
