// ─── Run registry ────────────────────────────────────────────────────────────

export type RunStatus =
  | 'running'
  | 'paused'
  | 'completed'
  | 'failed'
  | 'stopped'
  | 'unknown'

export interface RunSummary {
  run_id: string
  status: RunStatus
  created_at: string
  updated_at: string
  products: string[]
  aoi_name: string
}

export interface RunDetail extends RunSummary {
  pid: number | null
  run_dir: string
  config: Record<string, unknown>
  job_counts: JobCounts
  events: RunEvent[]
  gee_concurrency: number
  finished_products: string[]
}

export interface JobCounts {
  total: number
  done: number
  failed: number
  running: number
  pending: number
  shelved: number
}

export interface RunEvent {
  ts: string
  level: string
  msg: string
}

// ─── Datasets / Product registry ─────────────────────────────────────────────

export interface BandMeta {
  name: string
  description: string
  default_stats: string[]
  available_stats: string[]
}

export interface ProductMeta {
  id: string
  label: string
  description: string
  date_min: string        // YYYY-MM-DD
  date_max: string        // YYYY-MM-DD
  resolution_m: number
  cadence: string         // 'daily' | 'composite' | 'seasonal' | 'annual'
  categorical: boolean
  bands: BandMeta[]
  supported_stats: string[]
}

// ─── AOI ─────────────────────────────────────────────────────────────────────

export interface AOIInfo {
  feature_count: number
  crs: string
  bounds: [number, number, number, number]  // [minx, miny, maxx, maxy]
  geojson_preview: GeoJSON.FeatureCollection
}

// ─── GEE credentials ─────────────────────────────────────────────────────────

export interface GeeKeyStatus {
  valid: boolean
  email: string | null
  error: string | null
}

// ─── Run config (sent to POST /api/runs) ─────────────────────────────────────

export interface ProductConfig {
  product: string
  bands: string[]
  stats: string[]
  date_start: string   // YYYY-MM-DD
  date_end: string     // YYYY-MM-DD
}

export interface SubmitRunRequest {
  run_id: string
  products: ProductConfig[]
  gee_concurrency?: number
}
