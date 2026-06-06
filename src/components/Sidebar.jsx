import DetailPanel from './DetailPanel'

const FLOOD_TYPES = [
  { value: 'all', label: 'All' },
  { value: 'flash_flood', label: 'Flash', className: 'flash' },
  { value: 'riverine', label: 'Riverine', className: 'riverine' },
  { value: 'unknown', label: 'Unknown' },
]

const DATE_RANGES = [7, 14, 30, 90, 180, 365]

export default function Sidebar({ filters, setFilters, overlays, setOverlays, selectedFeature, floods, gauges, onSelectFeature }) {
  const setFilter  = (key, val) => setFilters(f => ({ ...f, [key]: val }))
  const setOverlay = (key, val) => setOverlays(o => ({ ...o, [key]: val }))

  const sortedFloods = [...floods].sort((a, b) =>
    new Date(b.properties.published_at) - new Date(a.properties.published_at)
  )

  return (
    <div className="sidebar">
      {/* FILTERS */}
      <div className="sidebar-section">
        <div className="sidebar-section-title">Filters</div>
        <div className="filter-row">
          <div>
            <div className="filter-label">Flood Type</div>
            <div className="filter-chips">
              {FLOOD_TYPES.map(t => (
                <button
                  key={t.value}
                  className={`chip ${t.className || ''} ${filters.floodType === t.value ? 'active' : ''}`}
                  onClick={() => setFilter('floodType', t.value)}
                >
                  {t.label}
                </button>
              ))}
            </div>
          </div>
          <div>
            <div className="filter-label">Date Range: last {filters.dateRange} days</div>
            <input
              type="range"
              className="range-slider"
              min={7}
              max={365}
              step={7}
              value={filters.dateRange}
              onChange={e => setFilter('dateRange', Number(e.target.value))}
            />
          </div>
          <div>
            <div className="filter-label">Layers</div>
            <div className="toggle-row">
              <button
                className={`toggle-btn ${filters.showFloods ? 'active' : ''}`}
                onClick={() => setFilter('showFloods', !filters.showFloods)}
              >
                ● Flood Events
              </button>
              <button
                className={`toggle-btn ${filters.showGauges ? 'active' : ''}`}
                onClick={() => setFilter('showGauges', !filters.showGauges)}
              >
                ▲ Gauges
              </button>
            </div>
          </div>
          <div>
            <div className="filter-label">Weather Overlays</div>
            <div className="toggle-row">
              <button
                className={`toggle-btn ${overlays.radar ? 'active' : ''}`}
                onClick={() => setOverlay('radar', !overlays.radar)}
                title="NEXRAD base reflectivity radar — refreshes every 5 min"
              >
                🌧 Radar
              </button>
              <button
                className={`toggle-btn ${overlays.nwsAlerts ? 'active' : ''}`}
                onClick={() => setOverlay('nwsAlerts', !overlays.nwsAlerts)}
                title="Active NWS Flood Watches & Warnings"
              >
                ⚠ NWS Alerts
              </button>
            </div>
          </div>
          <div>
            <div className="filter-label">Gauge Filter</div>
            <div className="filter-chips">
              {[
                { value: 'all', label: 'All' },
                { value: 'elevated', label: 'Elevated QPF' },
                { value: 'alert', label: 'Alert' },
              ].map(t => (
                <button
                  key={t.value}
                  className={`chip ${filters.gaugeFilter === t.value ? 'active' : ''}`}
                  onClick={() => setFilter('gaugeFilter', t.value)}
                >
                  {t.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* DETAIL */}
      {selectedFeature && (
        <DetailPanel feature={selectedFeature} onClose={() => onSelectFeature(null)} />
      )}

      {/* EVENT LIST */}
      <div className="event-list-header">
        <span className="sidebar-section-title" style={{ marginBottom: 0 }}>Recent Events</span>
        <span className="event-list-count">{sortedFloods.length} events</span>
      </div>
      <div className="event-list">
        {sortedFloods.map((f, i) => {
          const p = f.properties
          const isSelected = selectedFeature?.properties?.article_id === p.article_id
          const date = p.published_at ? new Date(p.published_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '—'
          return (
            <div
              key={p.article_id || i}
              className={`event-item ${isSelected ? 'selected' : ''}`}
              onClick={() => onSelectFeature(f)}
            >
              <div className="event-item-title">{p.title}</div>
              <div className="event-item-meta">
                <div className={`event-type-dot ${p.flood_type}`} />
                <span className="event-item-date">{date}</span>
                <span className="event-item-loc">{p.osm_display || p.mention_text || ''}</span>
              </div>
            </div>
          )
        })}
        {sortedFloods.length === 0 && (
          <div style={{ padding: '24px 16px', color: 'var(--text-muted)', fontSize: 12, textAlign: 'center' }}>
            No events match current filters
          </div>
        )}
      </div>
    </div>
  )
}
