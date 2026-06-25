import { useState, useEffect, useRef, useCallback } from 'react'
import './App.css'

const WS_URL         = 'ws://localhost:8001/ws'
const WATCHTOWER_URL = 'http://localhost:9000/status'
const MAX_EVENTS     = 200

function ago(iso) {
  const ms = Date.now() - new Date(iso).getTime()
  if (ms < 1000)      return 'now'
  if (ms < 60_000)    return `${Math.floor(ms / 1000)}s ago`
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ago`
  return `${Math.floor(ms / 3_600_000)}h ago`
}

// ─────────────────────────────────────────────────────────────────
// Status tab
// ─────────────────────────────────────────────────────────────────

function StatusPage() {
  const [result,      setResult]      = useState(null)
  const [loading,     setLoading]     = useState(false)
  const [checkedAt,   setCheckedAt]   = useState(null)
  const [fetchError,  setFetchError]  = useState(false)

  const check = useCallback(async () => {
    setLoading(true)
    setFetchError(false)
    try {
      const r = await fetch(WATCHTOWER_URL)
      setResult(await r.json())
      setCheckedAt(Date.now())
    } catch {
      setFetchError(true)
    } finally {
      setLoading(false)
    }
  }, [])

  // Check once on mount
  useEffect(() => { check() }, [check])

  return (
    <div className="status-page">

      <div className="status-toolbar">
        <button className="check-btn" onClick={check} disabled={loading}>
          {loading ? 'checking…' : 'check now'}
        </button>
        <span className="checked-at">
          {fetchError
            ? 'watchtower unreachable — is it running on :9000?'
            : checkedAt
              ? `last checked ${ago(checkedAt)}`
              : ''}
        </span>
      </div>

      {result && (
        <>
          <div className="overall-row">
            <span className="overall-label">overall</span>
            <span className={`overall-val ${result.overall}`}>
              {result.overall?.toUpperCase() ?? '—'}
            </span>
          </div>

          <div className="svc-table">
            <div className="svc-head">
              <span>service</span>
              <span>url</span>
              <span>status</span>
            </div>
            {(result.services ?? []).map(s => (
              <div className="svc-row" key={s.service}>
                <span className="svc-name">{s.service}</span>
                <span className="svc-url">{s.url}</span>
                <span className={`svc-badge ${s.status}`}>{s.status}</span>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────
// Feed tab
// ─────────────────────────────────────────────────────────────────

function FeedPage({ setLive }) {
  const [events,  setEvents]  = useState([])
  const [agents,  setAgents]  = useState({})
  const [total,   setTotal]   = useState(0)
  const [eps,     setEps]     = useState(0)

  const wsRef   = useRef(null)
  const tickRef = useRef(0)

  useEffect(() => {
    const id = setInterval(() => { setEps(tickRef.current); tickRef.current = 0 }, 1000)
    return () => clearInterval(id)
  }, [])

  // Keep ago() timestamps fresh
  const [, bump] = useState(0)
  useEffect(() => {
    const id = setInterval(() => bump(n => n + 1), 5_000)
    return () => clearInterval(id)
  }, [])

  const connect = useCallback(() => {
    const ws = new WebSocket(WS_URL)
    wsRef.current = ws
    ws.onopen  = () => setLive(true)
    ws.onclose = () => { setLive(false); setTimeout(connect, 3_000) }
    ws.onerror = () => ws.close()
    ws.onmessage = ({ data }) => {
      try {
        const ev = JSON.parse(data)
        tickRef.current++
        setTotal(n => n + 1)
        setEvents(prev => [ev, ...prev].slice(0, MAX_EVENTS))
        setAgents(prev => ({
          ...prev,
          [ev.agent_id]: {
            status:   ev.status,
            lastSeen: ev.timestamp,
            count:    (prev[ev.agent_id]?.count ?? 0) + 1,
          },
        }))
      } catch { /* skip */ }
    }
  }, [setLive])

  useEffect(() => {
    connect()
    return () => { wsRef.current?.close() }
  }, [connect])

  const agentList = Object.entries(agents).sort(
    ([, a], [, b]) => new Date(b.lastSeen) - new Date(a.lastSeen)
  )

  return (
    <>
      <div className="stats">
        <div className="stat">
          <div className="stat-val">{total.toLocaleString()}</div>
          <div className="stat-lbl">events</div>
        </div>
        <div className="stat">
          <div className="stat-val">{agentList.length}</div>
          <div className="stat-lbl">agents</div>
        </div>
        <div className="stat">
          <div className="stat-val">{eps}</div>
          <div className="stat-lbl">/ sec</div>
        </div>
        <div className="stat">
          <div className="stat-val">{events[0] ? ago(events[0].timestamp) : '—'}</div>
          <div className="stat-lbl">last event</div>
        </div>
      </div>

      <div className="grid">
        <div className="panel">
          <div className="panel-hd">agents ({agentList.length})</div>
          <div className="panel-body">
            {agentList.length === 0
              ? <div className="empty">
                  no agents yet
                  <span className="empty-cmd">python scripts/simulate.py</span>
                </div>
              : agentList.map(([id, ag]) => (
                <div className="agent-row" key={id}>
                  <span className="agent-id" title={id}>{id}</span>
                  <span className="agent-status">{ag.status}</span>
                  <span className="agent-count">{ag.count}</span>
                </div>
              ))
            }
          </div>
        </div>

        <div className="panel">
          <div className="panel-hd">live feed</div>
          <div className="panel-body">
            {events.length === 0
              ? <div className="empty">no events — start the simulator or post to /ingest/state</div>
              : events.map((ev, i) => (
                <div className="event-row" key={i}>
                  <span className="ev-agent" title={ev.agent_id}>{ev.agent_id}</span>
                  <span className="ev-status">{ev.status}</span>
                  <span className="ev-time">{ago(ev.timestamp)}</span>
                </div>
              ))
            }
          </div>
        </div>
      </div>
    </>
  )
}

// ─────────────────────────────────────────────────────────────────
// Root
// ─────────────────────────────────────────────────────────────────

export default function App() {
  const [tab,  setTab]  = useState('status')   // status page first
  const [live, setLive] = useState(false)

  return (
    <div className="app">
      <header className="header">
        <span className="title">Beacon</span>
        <nav className="tabs">
          <button className={`tab ${tab === 'feed'   ? 'active' : ''}`} onClick={() => setTab('feed')}>feed</button>
          <button className={`tab ${tab === 'status' ? 'active' : ''}`} onClick={() => setTab('status')}>status</button>
        </nav>
        <span className={`ws-dot ${live ? 'live' : ''}`} title={live ? 'WebSocket live' : 'WebSocket disconnected'}>
          {live ? '● live' : '○ connecting'}
        </span>
      </header>

      {tab === 'feed'   && <FeedPage setLive={setLive} />}
      {tab === 'status' && <StatusPage />}
    </div>
  )
}
