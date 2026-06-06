import { useEffect, useRef } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

const FLOOD_COLORS = {
  flash_flood: '#ff4d00',
  riverine: '#7c4dff',
  unknown: '#4a9eff',
  sunny_day: '#ffd166',
}

const GAUGE_COLORS = {
  normal: '#2d6a4f',
  action: '#f4a261',
  flood: '#e76f51',
  major: '#d62828',
}

function floodIcon(type) {
  const color = FLOOD_COLORS[type] || FLOOD_COLORS.unknown
  const svg = `<svg width="12" height="12" viewBox="0 0 12 12" xmlns="http://www.w3.org/2000/svg">
    <circle cx="6" cy="6" r="5" fill="${color}" fill-opacity="0.85" stroke="${color}" stroke-width="1"/>
  </svg>`
  return L.divIcon({
    html: svg,
    className: '',
    iconSize: [12, 12],
    iconAnchor: [6, 6],
  })
}

function gaugeIcon(tier, elevated) {
  const color = GAUGE_COLORS[tier] || GAUGE_COLORS.normal
  const size = elevated ? 10 : 8
  const svg = `<svg width="${size}" height="${size}" viewBox="0 0 10 10" xmlns="http://www.w3.org/2000/svg">
    <polygon points="5,0 10,10 0,10" fill="${color}" fill-opacity="${elevated ? 1 : 0.7}" stroke="${elevated ? '#ffd166' : color}" stroke-width="${elevated ? 1.5 : 0.5}"/>
  </svg>`
  return L.divIcon({
    html: svg,
    className: '',
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  })
}

function floodPopup(p) {
  const date = p.published_at ? new Date(p.published_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '—'
  const color = FLOOD_COLORS[p.flood_type] || FLOOD_COLORS.unknown
  return `
    <div style="max-width:240px; font-family:'DM Sans',sans-serif;">
      <div style="display:inline-block;padding:2px 8px;border-radius:3px;font-size:9px;font-family:'Space Mono',monospace;text-transform:uppercase;letter-spacing:0.08em;background:${color}22;color:${color};margin-bottom:6px;">${(p.flood_type || 'unknown').replace('_',' ')}</div>
      <div style="font-size:12px;font-weight:600;color:#fff;line-height:1.4;margin-bottom:6px;">${p.title || '—'}</div>
      <div style="font-size:10px;color:#5a6a82;margin-bottom:2px;">${date}</div>
      <div style="font-size:10px;color:#5a6a82;">${p.source || ''}</div>
      ${p.url ? `<a href="${p.url}" target="_blank" rel="noopener noreferrer" style="display:inline-block;margin-top:6px;font-size:10px;color:#2a8cff;text-decoration:none;">Read article ↗</a>` : ''}
    </div>`
}

function gaugePopup(p) {
  const color = GAUGE_COLORS[p.tier] || GAUGE_COLORS.normal
  const pct = p.stage_ft && p.flood_stage ? Math.min(100, (p.stage_ft / p.flood_stage * 100)).toFixed(0) : 0
  return `
    <div style="max-width:220px; font-family:'DM Sans',sans-serif;">
      <div style="display:inline-block;padding:2px 8px;border-radius:3px;font-size:9px;font-family:'Space Mono',monospace;text-transform:uppercase;letter-spacing:0.08em;background:#00d4ff22;color:#00d4ff;margin-bottom:6px;">GAUGE — ${(p.source || 'USGS')}</div>
      <div style="font-size:12px;font-weight:600;color:#fff;line-height:1.4;margin-bottom:8px;">${p.site_name || '—'}</div>
      <div style="font-size:11px;color:${color};margin-bottom:4px;font-family:'Space Mono',monospace;">${p.stage_ft} ft <span style="color:#5a6a82;font-size:9px;">(${p.tier})</span></div>
      <div style="height:4px;background:#1e2838;border-radius:2px;margin-bottom:6px;overflow:hidden;">
        <div style="height:100%;width:${pct}%;background:${color};border-radius:2px;"></div>
      </div>
      <div style="font-size:10px;color:#5a6a82;">QPF D1: ${p.qpf_day1_inches || p.qpf_day1_in || '—'}" &nbsp;|&nbsp; D2: ${p.qpf_day2_inches || p.qpf_day2_in || '—'}"</div>
      ${p.flood_risk_elevated ? '<div style="font-size:10px;color:#ffd166;margin-top:4px;">⚠ Elevated QPF risk</div>' : ''}
      ${p.flood_event_count ? `<div style="font-size:10px;color:#5a6a82;margin-top:4px;">${p.flood_event_count} nearby flood events</div>` : ''}
      ${p.url ? `<a href="${p.url}" target="_blank" rel="noopener noreferrer" style="display:inline-block;margin-top:6px;font-size:10px;color:#2a8cff;text-decoration:none;">NOAA station ↗</a>` : ''}
    </div>`
}

// NWS alert severity → color
const NWS_COLORS = {
  Extreme:  '#ff2d2d',
  Severe:   '#ff6600',
  Moderate: '#ffd166',
  Minor:    '#4a9eff',
  Unknown:  '#888',
}

function nwsAlertPopup(props) {
  const color = NWS_COLORS[props.severity] || NWS_COLORS.Unknown
  const onset  = props.onset  ? new Date(props.onset).toLocaleString()  : '—'
  const expires = props.expires ? new Date(props.expires).toLocaleString() : '—'
  return `
    <div style="max-width:260px;font-family:'DM Sans',sans-serif;">
      <div style="display:inline-block;padding:2px 8px;border-radius:3px;font-size:9px;font-family:'Space Mono',monospace;text-transform:uppercase;letter-spacing:0.08em;background:${color}22;color:${color};margin-bottom:6px;">${props.severity || 'NWS'} — ${props.certainty || ''}</div>
      <div style="font-size:12px;font-weight:600;color:#fff;line-height:1.4;margin-bottom:6px;">${props.event || '—'}</div>
      <div style="font-size:10px;color:#5a6a82;margin-bottom:2px;">📍 ${props.areaDesc || '—'}</div>
      <div style="font-size:10px;color:#5a6a82;margin-bottom:2px;">Onset: ${onset}</div>
      <div style="font-size:10px;color:#5a6a82;">Expires: ${expires}</div>
      ${props.headline ? `<div style="font-size:10px;color:#aaa;margin-top:6px;border-top:1px solid #2a3040;padding-top:6px;">${props.headline}</div>` : ''}
    </div>`
}

export default function Map({ floods, gauges, center, selectedFeature, onSelectFeature, overlays }) {
  const mapRef = useRef(null)
  const mapInstanceRef = useRef(null)
  const floodLayerRef = useRef(null)
  const gaugeLayerRef = useRef(null)
  const markerMapRef = useRef({})
  const radarLayerRef = useRef(null)
  const nwsLayerRef = useRef(null)
  const radarTimerRef = useRef(null)

  // Init map
  useEffect(() => {
    if (mapInstanceRef.current) return
    const map = L.map(mapRef.current, {
      center: [38.5, -96],
      zoom: 4,
      zoomControl: false,
    })
    L.control.zoom({ position: 'bottomright' }).addTo(map)

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '© OpenStreetMap contributors © CARTO',
      maxZoom: 19,
    }).addTo(map)

    floodLayerRef.current = L.layerGroup().addTo(map)
    gaugeLayerRef.current = L.layerGroup().addTo(map)
    radarLayerRef.current = L.layerGroup()   // not added yet — toggled by overlay state
    nwsLayerRef.current   = L.layerGroup()
    mapInstanceRef.current = map
  }, [])

  // NEXRAD radar tile overlay — auto-refreshes every 5 min
  useEffect(() => {
    if (!mapInstanceRef.current || !radarLayerRef.current) return
    const map = mapInstanceRef.current
    const layer = radarLayerRef.current

    if (overlays?.radar) {
      layer.clearLayers()
      const tile = L.tileLayer(
        'https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-900913/{z}/{x}/{y}.png',
        { opacity: 0.55, attribution: 'NOAA/NWS via IEM', zIndex: 5 }
      )
      layer.addLayer(tile)
      map.addLayer(layer)

      // refresh every 5 minutes so radar stays current
      radarTimerRef.current = setInterval(() => {
        layer.clearLayers()
        layer.addLayer(L.tileLayer(
          `https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-900913/{z}/{x}/{y}.png?_=${Date.now()}`,
          { opacity: 0.55, attribution: 'NOAA/NWS via IEM', zIndex: 5 }
        ))
      }, 5 * 60 * 1000)
    } else {
      clearInterval(radarTimerRef.current)
      map.removeLayer(layer)
    }
    return () => clearInterval(radarTimerRef.current)
  }, [overlays?.radar])

  // NWS active flood alerts — fetched from api.weather.gov
  useEffect(() => {
    if (!mapInstanceRef.current || !nwsLayerRef.current) return
    const map = mapInstanceRef.current
    const layer = nwsLayerRef.current

    if (overlays?.nwsAlerts) {
      layer.clearLayers()
      fetch('https://api.weather.gov/alerts/active?event=Flood%20Watch,Flash%20Flood%20Watch,Flash%20Flood%20Warning,Flood%20Warning,Areal%20Flood%20Advisory&status=actual&message_type=alert&region_type=land')
        .then(r => r.json())
        .then(data => {
          if (!data.features) return
          data.features.forEach(f => {
            if (!f.geometry) return   // some alerts have no polygon — skip
            const sev = f.properties.severity || 'Unknown'
            const color = NWS_COLORS[sev] || NWS_COLORS.Unknown
            L.geoJSON(f, {
              style: {
                color,
                weight: 1.5,
                fillColor: color,
                fillOpacity: 0.15,
                dashArray: sev === 'Extreme' || sev === 'Severe' ? null : '4 4',
              },
            })
              .bindPopup(nwsAlertPopup(f.properties), { className: 'custom-popup', maxWidth: 280 })
              .addTo(layer)
          })
          map.addLayer(layer)
        })
        .catch(err => console.warn('[NWS alerts]', err))
    } else {
      map.removeLayer(layer)
    }
  }, [overlays?.nwsAlerts])

  // Update flood markers
  useEffect(() => {
    if (!floodLayerRef.current) return
    floodLayerRef.current.clearLayers()
    markerMapRef.current = {}

    floods.forEach(f => {
      if (!f.geometry?.coordinates) return
      const [lon, lat] = f.geometry.coordinates
      if (!lat || !lon) return
      const p = f.properties
      const marker = L.marker([parseFloat(lat), parseFloat(lon)], { icon: floodIcon(p.flood_type) })
        .bindPopup(floodPopup(p), { className: 'custom-popup', maxWidth: 260 })
        .on('click', () => onSelectFeature(f))
      marker.addTo(floodLayerRef.current)
      if (p.article_id) markerMapRef.current[p.article_id] = marker
    })
  }, [floods])

  // Update gauge markers
  useEffect(() => {
    if (!gaugeLayerRef.current) return
    gaugeLayerRef.current.clearLayers()

    gauges.forEach(f => {
      if (!f.geometry?.coordinates) return
      const [lon, lat] = f.geometry.coordinates
      if (!lat || !lon) return
      const p = f.properties
      L.marker([parseFloat(lat), parseFloat(lon)], { icon: gaugeIcon(p.tier, p.flood_risk_elevated) })
        .bindPopup(gaugePopup(p), { className: 'custom-popup', maxWidth: 240 })
        .on('click', () => onSelectFeature(f))
        .addTo(gaugeLayerRef.current)
    })
  }, [gauges])

  // Pan to selected
  useEffect(() => {
    if (!selectedFeature || !mapInstanceRef.current) return
    const coords = selectedFeature.geometry?.coordinates
    if (!coords) return
    const [lon, lat] = coords
    mapInstanceRef.current.setView([parseFloat(lat), parseFloat(lon)], Math.max(mapInstanceRef.current.getZoom(), 8), { animate: true })

    // Open popup for flood markers
    const id = selectedFeature.properties?.article_id
    if (id && markerMapRef.current[id]) {
      markerMapRef.current[id].openPopup()
    }
  }, [selectedFeature])

  // Pan to center change
  useEffect(() => {
    if (!mapInstanceRef.current || !center) return
    // handled by selectedFeature above
  }, [center])

  return <div ref={mapRef} style={{ width: '100%', height: '100%' }} />
}
