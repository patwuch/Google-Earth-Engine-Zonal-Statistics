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

async function _triggerFileDownload(url: string, fallbackFilename: string): Promise<void> {
  // FastAPI GET routes don't handle HEAD, so use a blob GET instead.
  // Strip /api prefix because http already has baseURL '/api'.
  const path = url.startsWith('/api/') ? url.slice('/api'.length) : url
  let blob: Blob
  try {
    const { data } = await http.get<Blob>(path, { responseType: 'blob' })
    blob = data
  } catch (err: any) {
    if (err.response?.status === 404) {
      throw new Error('No partial data yet — click "Build Partial Checkout" first, then wait a moment')
    }
    throw new Error(`Download failed (${err.response?.status ?? 'network error'})`)
  }
  const objectUrl = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = objectUrl
  a.download = fallbackFilename
  a.click()
  URL.revokeObjectURL(objectUrl)
}

export function downloadPartialCheckoutCsv(runId: string, product: string): Promise<void> {
  return _triggerFileDownload(
    `/api/runs/${runId}/download/${product}/partial-csv`,
    `${product}_partial.csv`,
  )
}

export function downloadPartialCheckout(runId: string, product: string): Promise<void> {
  return _triggerFileDownload(
    partialDownloadUrl(runId, product),
    `${product}_partial.parquet`,
  )
}
