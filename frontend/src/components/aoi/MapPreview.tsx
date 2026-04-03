import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet'
import { useEffect } from 'react'

// bounds: [minx, miny, maxx, maxy] in EPSG:4326
interface FitBoundsProps {
  bounds: [number, number, number, number]
}

function FitBounds({ bounds }: FitBoundsProps) {
  const map = useMap()
  useEffect(() => {
    const [minx, miny, maxx, maxy] = bounds
    map.fitBounds([[miny, minx], [maxy, maxx]], { padding: [20, 20] })
  }, [bounds, map])
  return null
}

interface Props {
  geojson: GeoJSON.FeatureCollection | null
  bounds: [number, number, number, number] | null
}

export default function MapPreview({ geojson, bounds }: Props) {
  return (
    <MapContainer
      center={[20, 0]}
      zoom={2}
      style={{ height: '100%', width: '100%' }}
      scrollWheelZoom
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {geojson && bounds && (
        <>
          <GeoJSON
            key={`${bounds.join(',')}`}
            data={geojson}
            style={{ color: '#16a34a', weight: 2, fillOpacity: 0.15 }}
          />
          <FitBounds bounds={bounds} />
        </>
      )}
    </MapContainer>
  )
}
