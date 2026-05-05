import { useQuery } from '@tanstack/react-query'
import { getProducts } from '@/api'
import ProductCard from './ProductCard'
import HelpTooltip from '@/components/ui/HelpTooltip'

export default function DatasetConfig() {
  const { data: products = [], isLoading } = useQuery({
    queryKey: ['products'],
    queryFn: getProducts,
    staleTime: Infinity,
  })

  if (isLoading) {
    return <p className="text-xs text-gray-400">Loading datasets…</p>
  }

  return (
    <div>
      <p className="section-title flex items-center gap-1.5">
        Datasets
        <HelpTooltip
          direction="right"
          text={
            <>
              Select and configure the satellite data products to download, including bands, statistics, and date range.
              <br /><br />
              <strong>Cadence</strong> — how frequently the dataset captures observations (e.g. daily, 8-day composite). This is a fixed property of the source data.
              <br /><br />
              <strong>Chunk</strong> — how your date range is split into separate GEE processing jobs. Each chunk runs independently to stay within GEE computation limits. Chunk size is set automatically based on the dataset cadence.
            </>
          }
        />
      </p>
      <div className="flex flex-col gap-2">
        {products.map((p) => (
          <ProductCard key={p.id} product={p} />
        ))}
      </div>
    </div>
  )
}
