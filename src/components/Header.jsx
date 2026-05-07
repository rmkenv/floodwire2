export default function Header({ stats }) {
  const now = new Date().toUTCString().replace('GMT', 'UTC')

  return (
    <header className="header">
      <div className="header-brand">
        <svg className="header-logo" viewBox="0 0 28 28" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M4 20 Q7 14 10 20 Q13 26 16 20 Q19 14 22 20 Q25 26 28 20" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
          <path d="M2 24 Q5 18 8 24 Q11 30 14 24 Q17 18 20 24 Q23 30 26 24" stroke="currentColor" strokeWidth="1" fill="none" strokeLinecap="round" opacity="0.5"/>
          <circle cx="14" cy="10" r="3" fill="currentColor" opacity="0.8"/>
          <path d="M14 13 L14 17" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
        </svg>
        <div>
          <div className="header-title">FLOOD<span>WIRE</span></div>
          <div className="header-meta">US FLOOD INTELLIGENCE DASHBOARD</div>
        </div>
      </div>
      <div className="header-live">
        <div className="live-dot" />
        LIVE — {now}
      </div>
    </header>
  )
}
