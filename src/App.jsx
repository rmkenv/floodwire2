import { useState, useEffect, useCallback } from 'react'
import Map from './components/Map'
import Sidebar from './components/Sidebar'
import Header from './components/Header'
import StatsBar from './components/StatsBar'
import './App.css'

export default function App() {
  const [floods, setFloods] = useState([])
  const [gauges, setGauges] = useState([])
  const [loading, setLoading] = useState(true)
  const [selectedFeature, setSelectedFeature] = useState(null)
  const [filters, setFilters] = useState({
    floodType: 'all',
    dateRange: 30,
    showGauges: true,
    showFloods: true,
    gaugeFilter: 'all',
  })
  const [overlays, setOverlays] = useState({
    radar: false,
    nwsAlerts: false,
  })
  const [mapCenter, setMapCenter] = useState([38.5, -96])
  const [stats, setStats] = useState({})

  useEffect(() => {
    Promise.all([
      fetch('/floods.geojson').then(r => r.json()),
      fetch('/gauges_with_qpf.geojson').then(r => r.json()),
    ]).then(([floodsData, gaugesData]) => {
      setFloods(floodsData.features || [])
      setGauges(gaugesData.features || [])
      setLoading(false)
    }).catch(err => {
      console.error(err)
      setLoading(false)
    })
  }, [])

  useEffect(() => {
    const now = new Date()
    const cutoff = new Date(now - filters.dateRange * 86400000)

    const filtered = floods.filter(f => {
      const pub = new Date(f.properties.published_at)
      if (isNaN(pub)) return false
      if (pub < cutoff) return false
      if (filters.floodType !== 'all' && f.properties.flood_type !== filters.floodType) return false
      return true
    })

    const typeCounts = {}
    filtered.forEach(f => {
      const t = f.properties.flood_type || 'unknown'
      typeCounts[t] = (typeCounts[t] || 0) + 1
    })

    const alertGauges = gauges.filter(g => g.properties.tier !== 'normal')
    const elevatedRisk = gauges.filter(g => g.properties.flood_risk_elevated)

    setStats({
      totalEvents: filtered.length,
      typeCounts,
      activeGauges: gauges.length,
      alertGauges: alertGauges.length,
      elevatedRisk: elevatedRisk.length,
      dateRange: filters.dateRange,
    })
  }, [floods, gauges, filters])

  const filteredFloods = useCallback(() => {
    const now = new Date()
    const cutoff = new Date(now - filters.dateRange * 86400000)
    return floods.filter(f => {
      const pub = new Date(f.properties.published_at)
      if (isNaN(pub)) return false
      if (pub < cutoff) return false
      if (filters.floodType !== 'all' && f.properties.flood_type !== filters.floodType) return false
      return true
    })
  }, [floods, filters])

  const filteredGauges = useCallback(() => {
    if (filters.gaugeFilter === 'elevated') return gauges.filter(g => g.properties.flood_risk_elevated)
    if (filters.gaugeFilter === 'alert') return gauges.filter(g => g.properties.tier !== 'normal')
    return gauges
  }, [gauges, filters])

  if (loading) return (
    <div className="loading-screen">
      <div className="loading-inner">
        <div className="loading-pulse" />
        <p>Loading flood intelligence…</p>
      </div>
    </div>
  )

  return (
    <div className="app">
      <Header stats={stats} />
      <StatsBar stats={stats} />
      <div className="main-layout">
        <Sidebar
          filters={filters}
          setFilters={setFilters}
          overlays={overlays}
          setOverlays={setOverlays}
          selectedFeature={selectedFeature}
          floods={filteredFloods()}
          gauges={filteredGauges()}
          onSelectFeature={(f) => {
            setSelectedFeature(f)
            if (f?.geometry?.coordinates) {
              const [lon, lat] = f.geometry.coordinates
              setMapCenter([parseFloat(lat), parseFloat(lon)])
            }
          }}
        />
        <div className="map-wrapper">
          <Map
            floods={filters.showFloods ? filteredFloods() : []}
            gauges={filters.showGauges ? filteredGauges() : []}
            center={mapCenter}
            selectedFeature={selectedFeature}
            onSelectFeature={setSelectedFeature}
            overlays={overlays}
          />
        </div>
      </div>
    </div>
  )
}
