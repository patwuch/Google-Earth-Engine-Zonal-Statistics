import { create } from 'zustand'
import type { ProductConfig } from './types'

interface PendingRun {
  run_id: string
  products: ProductConfig[]
}

interface AppState {
  // Active (already submitted / being monitored) run
  activeRunId: string | null
  setActiveRunId: (id: string | null) => void

  // Config being assembled before submission
  pendingRun: PendingRun
  setPendingRunId: (id: string) => void
  upsertProduct: (p: ProductConfig) => void
  removeProduct: (productId: string) => void
  resetPending: () => void

  isRunning: boolean
  setIsRunning: (v: boolean) => void
}

const defaultPending = (): PendingRun => ({ run_id: '', products: [] })

export const useAppStore = create<AppState>((set) => ({
  activeRunId: null,
  setActiveRunId: (id) => set({ activeRunId: id }),

  pendingRun: defaultPending(),

  setPendingRunId: (id) =>
    set((s) => ({ pendingRun: { ...s.pendingRun, run_id: id } })),

  upsertProduct: (p) =>
    set((s) => {
      const products = s.pendingRun.products.filter((x) => x.product !== p.product)
      return { pendingRun: { ...s.pendingRun, products: [...products, p] } }
    }),

  removeProduct: (productId) =>
    set((s) => ({
      pendingRun: {
        ...s.pendingRun,
        products: s.pendingRun.products.filter((x) => x.product !== productId),
      },
    })),

  resetPending: () => set({ pendingRun: defaultPending() }),

  isRunning: false,
  setIsRunning: (v) => set({ isRunning: v }),
}))
