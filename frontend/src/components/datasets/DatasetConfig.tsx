import { useQuery } from '@tanstack/react-query'
import { getProducts } from '@/api'
import ProductCard from './ProductCard'
import HelpTooltip from '@/components/ui/HelpTooltip'

export default function DatasetConfig() {
  const { data: products = [], isLoading } = useQuery({
    queryKey: ['products'],
    queryFn: getProducts,
  })

  if (isLoading) {
    return <p className="text-xs text-gray-400">Loading datasets…</p>
  }

  return (
    <div>
      <p className="section-title flex items-center gap-1.5">Datasets <HelpTooltip text="Select and configure the satellite data products to download, including bands, statistics, and date range." direction="right" /></p>
      <div className="flex flex-col gap-2">
        {products.map((p) => (
          <ProductCard key={p.id} product={p} />
        ))}
      </div>
    </div>
  )
}
