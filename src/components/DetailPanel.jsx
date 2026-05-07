export default function DetailPanel({ feature, onClose }) {
  if (!feature) return null
  const p = feature.properties
  const isGauge = !!p.site_id
  const isFlood = !!p.article_id

  const fmtDate = (d) => {
    if (!d) return '—'
    return new Date(d).toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' })
  }

  const getGaugeColor = (tier) => {
    const map = { normal: '#2d6a4f', action: '#f4a261', flood: '#e76f51', major: '#d62828' }
    return map[tier] || '#888'
  }

  const gaugePercent = (stage, floodStage) => {
    if (!stage || !floodStage) return 0
    return Math.min(100, (stage / floodStage) * 100)
  }

  return (
    <div className="detail-panel">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 6 }}>
        {isGauge && <span className="detail-type-badge gauge">GAUGE</span>}
        {isFlood && <span className={`detail-type-badge ${p.flood_type}`}>{(p.flood_type || 'event').replace('_', ' ')}</span>}
        <button
          onClick={onClose}
          style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: 16, lineHeight: 1, padding: '0 0 0 8px' }}
        >×</button>
      </div>

      {isFlood && (
        <div>
          <div className="detail-title">{p.title}</div>
          <div className="detail-meta">
            <div className="detail-row">
              <span className="detail-key">Source</span>
              <span className="detail-val">{p.source || '—'}</span>
            </div>
            <div className="detail-row">
              <span className="detail-key">Published</span>
              <span className="detail-val">{fmtDate(p.published_at)}</span>
            </div>
            <div className="detail-row">
              <span className="detail-key">Location</span>
              <span className="detail-val" style={{ textAlign: 'right', maxWidth: 180, fontSize: 10 }}>{p.osm_display || p.mention_text || '—'}</span>
            </div>
            <div className="detail-row">
              <span className="detail-key">Confidence</span>
              <span className="detail-val">{p.confidence ? `${(p.confidence * 100).toFixed(0)}%` : '—'}</span>
            </div>
            {p.url && (
              <a href={p.url} target="_blank" rel="noopener noreferrer" className="detail-link">
                → Read Article ↗
              </a>
            )}
          </div>
        </div>
      )}

      {isGauge && (
        <div>
          <div className="detail-title">{p.site_name}</div>
          <div className="detail-meta">
            <div className="detail-row">
              <span className="detail-key">Stage</span>
              <span className="detail-val" style={{ color: getGaugeColor(p.tier) }}>
                {p.stage_ft} ft ({p.tier})
              </span>
            </div>
            <div className="gauge-bar-wrap">
              <div className="gauge-bar-bg">
                <div
                  className="gauge-bar-fill"
                  style={{
                    width: `${gaugePercent(p.stage_ft, p.flood_stage)}%`,
                    background: getGaugeColor(p.tier),
                  }}
                />
              </div>
              <div className="gauge-bar-labels">
                <span>0 ft</span>
                <span>Action {p.action_stage}ft</span>
                <span>Flood {p.flood_stage}ft</span>
              </div>
            </div>
            <div className="detail-row">
              <span className="detail-key">QPF Day1</span>
              <span className="detail-val">{p.qpf_day1_inches || p.qpf_day1_in || '—'}"</span>
            </div>
            <div className="detail-row">
              <span className="detail-key">QPF Day2</span>
              <span className="detail-val">{p.qpf_day2_inches || p.qpf_day2_in || '—'}"</span>
            </div>
            <div className="detail-row">
              <span className="detail-key">Elevated Risk</span>
              <span className="detail-val" style={{ color: p.flood_risk_elevated ? 'var(--elevated)' : 'var(--text-muted)' }}>
                {p.flood_risk_elevated ? 'YES' : 'No'}
              </span>
            </div>
            <div className="detail-row">
              <span className="detail-key">Nearby Events</span>
              <span className="detail-val">{p.flood_event_count ?? 0}</span>
            </div>
            {p.nearest_flood_title && (
              <div style={{ marginTop: 6 }}>
                <div className="detail-key" style={{ marginBottom: 4 }}>Nearest Event</div>
                <div style={{ fontSize: 11, color: 'var(--text)', lineHeight: 1.4 }}>{p.nearest_flood_title}</div>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>{p.nearest_flood_miles?.toFixed(1)} mi away</div>
              </div>
            )}
            {p.url && (
              <a href={p.url} target="_blank" rel="noopener noreferrer" className="detail-link">
                → NOAA Station ↗
              </a>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
