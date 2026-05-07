export default function StatsBar({ stats }) {
  const { totalEvents, typeCounts = {}, activeGauges, alertGauges, elevatedRisk, dateRange } = stats

  return (
    <div className="stats-bar">
      <div className="stat-item">
        <div className="stat-value">{totalEvents ?? '—'}</div>
        <div className="stat-label">Events ({dateRange}d)</div>
      </div>
      <div className="stat-item">
        <div className="stat-value flash">{typeCounts.flash_flood ?? 0}</div>
        <div className="stat-label">Flash Flood</div>
      </div>
      <div className="stat-item">
        <div className="stat-value riverine">{typeCounts.riverine ?? 0}</div>
        <div className="stat-label">Riverine</div>
      </div>
      <div className="stat-item">
        <div className="stat-value">{typeCounts.unknown ?? 0}</div>
        <div className="stat-label">Unknown</div>
      </div>
      <div className="stat-item">
        <div className="stat-value">{activeGauges ?? '—'}</div>
        <div className="stat-label">Gauges Active</div>
      </div>
      <div className="stat-item">
        <div className="stat-value elevated">{elevatedRisk ?? 0}</div>
        <div className="stat-label">Elevated QPF Risk</div>
      </div>
      <div className="stat-item">
        <div className="stat-value alert">{alertGauges ?? 0}</div>
        <div className="stat-label">Gauge Alerts</div>
      </div>
    </div>
  )
}
