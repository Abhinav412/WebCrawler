import { useState, useEffect, useRef, useCallback } from "react";

const API = "http://localhost:8000";

// ── Palette & constants ──────────────────────────────────────
const NODES = [
  { id: "intent_parser",    label: "Intent Parser",     icon: "◎", phase: "crawl" },
  { id: "url_discovery",    label: "URL Discovery",     icon: "⊕", phase: "crawl" },
  { id: "web_crawler",      label: "Web Crawler",       icon: "⊞", phase: "crawl" },
  { id: "source_verifier",  label: "Source Verifier",   icon: "⊛", phase: "crawl" },
  { id: "mongo_logger",     label: "Mongo Logger",      icon: "⊟", phase: "crawl" },
  { id: "entity_extractor", label: "Entity Extractor",  icon: "⊡", phase: "graph" },
  { id: "neo4j_ingester",   label: "Neo4j Ingester",    icon: "⊠", phase: "graph" },
  { id: "graph_structurer", label: "Graph Structurer",  icon: "⋈", phase: "graph" },
  { id: "metrics_evaluator","label": "Metrics Eval",    icon: "⊕", phase: "graph" },
];

const AGENT_PHASES = [
  { id: "structuring_agent", label: "Structuring Agent", role: "Builds table from ChromaDB" },
  { id: "validator",         label: "Validator",          role: "Checks for missing metrics" },
  { id: "crawler_agent",     label: "Crawler Agent",      role: "Targeted recrawl for gaps" },
  { id: "ranking_agent",     label: "Ranking Agent",      role: "LLM scores + ranks entities" },
];

// ── Styles ────────────────────────────────────────────────────
const css = `
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@300;400;500&display=swap');

  :root {
    --bg:       #060810;
    --surface:  #0d1117;
    --border:   #1e2535;
    --accent:   #00e5ff;
    --accent2:  #7c3aed;
    --accent3:  #f59e0b;
    --text:     #e2e8f0;
    --muted:    #4a5568;
    --crawl:    #06b6d4;
    --graph:    #8b5cf6;
    --agents:   #10b981;
    --rank:     #f59e0b;
    --error:    #ef4444;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    min-height: 100vh;
    overflow-x: hidden;
  }

  .grain {
    position: fixed; inset: 0; pointer-events: none; z-index: 100;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.04'/%3E%3C/svg%3E");
    opacity: 0.6;
  }

  .app {
    max-width: 1400px;
    margin: 0 auto;
    padding: 2rem;
    position: relative;
    z-index: 1;
  }

  /* ── Header ── */
  .header {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    margin-bottom: 3rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid var(--border);
  }

  .logo-mark {
    width: 48px; height: 48px;
    border: 2px solid var(--accent);
    display: flex; align-items: center; justify-content: center;
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.1rem;
    color: var(--accent);
    letter-spacing: -0.05em;
    position: relative;
  }
  .logo-mark::after {
    content: '';
    position: absolute;
    inset: 3px;
    border: 1px solid var(--accent);
    opacity: 0.3;
  }

  .header-title {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.5rem;
    letter-spacing: -0.03em;
    color: var(--text);
  }
  .header-sub {
    font-size: 0.7rem;
    color: var(--muted);
    letter-spacing: 0.15em;
    text-transform: uppercase;
    margin-top: 0.2rem;
  }

  /* ── Query input ── */
  .query-section {
    margin-bottom: 2.5rem;
  }

  .query-label {
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 0.75rem;
  }

  .query-row {
    display: flex;
    gap: 0.75rem;
  }

  .query-input {
    flex: 1;
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.9rem;
    padding: 0.85rem 1.25rem;
    outline: none;
    transition: border-color 0.2s;
  }
  .query-input:focus {
    border-color: var(--accent);
    box-shadow: 0 0 0 1px var(--accent), inset 0 0 20px rgba(0,229,255,0.03);
  }
  .query-input::placeholder { color: var(--muted); }

  .run-btn {
    background: var(--accent);
    color: #000;
    font-family: 'Syne', sans-serif;
    font-weight: 700;
    font-size: 0.8rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    border: none;
    padding: 0 2rem;
    cursor: pointer;
    transition: all 0.15s;
    white-space: nowrap;
  }
  .run-btn:hover { background: #fff; }
  .run-btn:disabled { background: var(--muted); cursor: not-allowed; color: #333; }

  /* ── Layout ── */
  .main-grid {
    display: grid;
    grid-template-columns: 320px 1fr;
    gap: 1.5rem;
    margin-bottom: 2rem;
  }

  @media (max-width: 900px) {
    .main-grid { grid-template-columns: 1fr; }
  }

  /* ── Pipeline sidebar ── */
  .pipeline-panel {
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .panel-title {
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--muted);
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border);
  }

  .phase-group {
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 1rem;
  }

  .phase-label {
    font-size: 0.6rem;
    letter-spacing: 0.25em;
    text-transform: uppercase;
    margin-bottom: 0.75rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border);
  }
  .phase-label.crawl { color: var(--crawl); }
  .phase-label.graph { color: var(--graph); }
  .phase-label.agents-label { color: var(--agents); }

  .node-item {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.4rem 0;
    font-size: 0.75rem;
    color: var(--muted);
    transition: color 0.2s;
    border-left: 2px solid transparent;
    padding-left: 0.5rem;
  }
  .node-item.active {
    color: var(--text);
    border-left-color: var(--accent);
    animation: nodePulse 1.2s ease-in-out infinite;
  }
  .node-item.done { color: var(--agents); border-left-color: var(--agents); }
  .node-item.error { color: var(--error); border-left-color: var(--error); }

  @keyframes nodePulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
  }

  .node-icon {
    font-size: 0.9rem;
    width: 1.2rem;
    text-align: center;
  }

  .node-status {
    margin-left: auto;
    font-size: 0.6rem;
  }

  /* ── Right panel: logs + agent comms ── */
  .right-panel {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    min-height: 0;
  }

  .log-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    flex: 1;
    min-height: 300px;
    max-height: 400px;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
  }

  .log-header {
    padding: 0.75rem 1rem;
    font-size: 0.6rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    background: var(--surface);
    z-index: 2;
    flex-shrink: 0;
  }

  .log-body {
    padding: 0.75rem;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    flex: 1;
  }

  .log-entry {
    font-size: 0.7rem;
    line-height: 1.5;
    padding: 0.25rem 0.5rem;
    border-left: 2px solid var(--border);
    animation: fadeSlide 0.3s ease;
  }
  @keyframes fadeSlide {
    from { opacity: 0; transform: translateY(4px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .log-entry.phase_start { border-color: var(--accent2); color: var(--accent2); }
  .log-entry.phase_complete { border-color: var(--agents); color: var(--agents); }
  .log-entry.node_start { border-color: var(--crawl); color: #7dd3fc; }
  .log-entry.node_complete { border-color: var(--agents); color: #6ee7b7; }
  .log-entry.agent_message { border-color: var(--rank); color: #fcd34d; }
  .log-entry.warning { border-color: var(--accent3); color: var(--accent3); }
  .log-entry.error { border-color: var(--error); color: var(--error); }
  .log-entry.done { border-color: var(--accent); color: var(--accent); }

  .log-time {
    color: var(--muted);
    margin-right: 0.5rem;
  }

  /* ── Agent comm panel ── */
  .agent-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    max-height: 280px;
    overflow-y: auto;
  }

  .agent-header {
    padding: 0.75rem 1rem;
    font-size: 0.6rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    background: var(--surface);
  }

  .agent-msg {
    padding: 0.6rem 1rem;
    border-bottom: 1px solid var(--border);
    font-size: 0.7rem;
    animation: fadeSlide 0.3s ease;
  }

  .agent-route {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin-bottom: 0.25rem;
    color: var(--muted);
    font-size: 0.62rem;
    letter-spacing: 0.05em;
  }

  .agent-name {
    padding: 0.1rem 0.4rem;
    font-size: 0.6rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.1em;
  }
  .agent-name.orchestrator { background: rgba(0,229,255,0.1); color: var(--accent); }
  .agent-name.structuring_agent { background: rgba(124,58,237,0.15); color: #a78bfa; }
  .agent-name.validator { background: rgba(16,185,129,0.1); color: var(--agents); }
  .agent-name.crawler_agent { background: rgba(6,182,212,0.1); color: var(--crawl); }
  .agent-name.ranking_agent { background: rgba(245,158,11,0.1); color: var(--rank); }
  .agent-name.metrics_evaluator { background: rgba(139,92,246,0.1); color: var(--graph); }
  .agent-name.intent_parser { background: rgba(99,102,241,0.1); color: #818cf8; }

  .agent-content { color: var(--text); line-height: 1.4; }

  .chip-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
    margin-top: 0.3rem;
  }

  .chip {
    padding: 0.1rem 0.4rem;
    background: rgba(255,255,255,0.05);
    border: 1px solid var(--border);
    font-size: 0.6rem;
    color: var(--muted);
  }

  /* ── Ranking table ── */
  .ranking-section {
    margin-top: 2rem;
  }

  .ranking-header {
    display: flex;
    align-items: baseline;
    gap: 1rem;
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid var(--border);
  }

  .ranking-title {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.4rem;
    letter-spacing: -0.03em;
  }

  .ranking-rationale {
    font-size: 0.72rem;
    color: var(--muted);
    max-width: 600px;
    line-height: 1.5;
  }

  .criteria-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 1.5rem;
  }

  .criterion-chip {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.35rem 0.75rem;
    background: var(--surface);
    border: 1px solid var(--border);
    font-size: 0.68rem;
  }

  .criterion-weight {
    color: var(--rank);
    font-weight: 600;
  }

  .table-wrap {
    overflow-x: auto;
    border: 1px solid var(--border);
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.75rem;
  }

  thead { background: #0a0e18; }

  th {
    padding: 0.75rem 1rem;
    text-align: left;
    font-size: 0.6rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  th.rank-col { color: var(--rank); }

  tr { transition: background 0.15s; }
  tr:hover td { background: rgba(255,255,255,0.02); }

  td {
    padding: 0.75rem 1rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
    color: var(--text);
  }

  .rank-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 28px; height: 28px;
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 0.85rem;
  }
  .rank-badge.r1 { background: var(--rank); color: #000; }
  .rank-badge.r2 { background: #94a3b8; color: #000; }
  .rank-badge.r3 { background: #92400e; color: #fff; }
  .rank-badge.rn { background: var(--border); color: var(--muted); }

  .score-bar {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .score-track {
    flex: 1;
    height: 3px;
    background: var(--border);
    max-width: 80px;
  }
  .score-fill {
    height: 100%;
    background: var(--accent);
    transition: width 0.8s ease;
  }
  .score-val { font-size: 0.68rem; color: var(--accent); min-width: 2.5rem; }

  .entity-name {
    font-family: 'Syne', sans-serif;
    font-weight: 700;
    font-size: 0.85rem;
    color: var(--text);
  }
  .entity-url {
    font-size: 0.6rem;
    color: var(--muted);
    text-decoration: none;
    display: block;
    margin-top: 0.15rem;
  }
  .entity-url:hover { color: var(--accent); }

  .metric-val { color: var(--text); }
  .metric-missing { color: var(--muted); font-style: italic; font-size: 0.65rem; }

  .source-link {
    color: var(--crawl);
    text-decoration: none;
    font-size: 0.65rem;
  }
  .source-link:hover { text-decoration: underline; }

  /* ── Empty / loading states ── */
  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 4rem 2rem;
    text-align: center;
    gap: 1rem;
    border: 1px dashed var(--border);
  }

  .empty-glyph {
    font-size: 3rem;
    opacity: 0.15;
    font-family: 'Syne', sans-serif;
    font-weight: 800;
  }

  .empty-text {
    font-size: 0.75rem;
    color: var(--muted);
    max-width: 360px;
    line-height: 1.6;
  }

  .spinner {
    display: inline-block;
    width: 12px; height: 12px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  .status-bar {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.6rem 1rem;
    background: var(--surface);
    border: 1px solid var(--border);
    margin-bottom: 1.5rem;
    font-size: 0.7rem;
  }

  .status-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    background: var(--muted);
  }
  .status-dot.running { background: var(--accent); animation: blink 1s ease infinite; }
  .status-dot.completed { background: var(--agents); }
  .status-dot.failed { background: var(--error); }

  @keyframes blink {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
  }
`;

// ── Helpers ───────────────────────────────────────────────────
function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  return d.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function logLabel(ev) {
  switch (ev.type) {
    case "phase_start":    return `▶ ${ev.label}`;
    case "phase_complete": return `✓ ${ev.label}`;
    case "node_start":     return `↻ ${ev.node} — ${ev.label || ""}`;
    case "node_complete":  return `✓ ${ev.node}${ev.count != null ? ` (${ev.count})` : ""} — ${ev.label || ""}`;
    case "agent_message":  return `⟶ ${ev.from} → ${ev.to}: ${ev.content}`;
    case "warning":        return `⚠ ${ev.message}`;
    case "error":          return `✗ ${ev.message}`;
    case "done":           return `✦ Pipeline complete`;
    default:               return JSON.stringify(ev);
  }
}

// ── Component ─────────────────────────────────────────────────
export default function App() {
  const [query, setQuery] = useState("");
  const [jobId, setJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState("idle"); // idle | running | completed | failed
  const [events, setEvents] = useState([]);
  const [agentMsgs, setAgentMsgs] = useState([]);
  const [nodeStates, setNodeStates] = useState({});
  const [rankedTable, setRankedTable] = useState(null);
  const [error, setError] = useState(null);

  const logRef = useRef(null);
  const agentRef = useRef(null);
  const esRef = useRef(null);

  // Auto-scroll logs
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [events]);
  useEffect(() => {
    if (agentRef.current) agentRef.current.scrollTop = agentRef.current.scrollHeight;
  }, [agentMsgs]);

  const handleEvent = useCallback((ev) => {
    setEvents(prev => [...prev, ev]);

    if (ev.type === "node_start") {
      setNodeStates(prev => ({ ...prev, [ev.node]: "active" }));
    }
    if (ev.type === "node_complete") {
      setNodeStates(prev => ({ ...prev, [ev.node]: "done" }));
    }
    if (ev.type === "agent_message") {
      setAgentMsgs(prev => [...prev, ev]);
      // mark agent nodes as done when relevant
      if (ev.from) setNodeStates(prev => ({ ...prev, [ev.from]: "done" }));
    }
    if (ev.type === "phase_start" && ev.phase === "agents") {
      AGENT_PHASES.forEach(a => {
        setNodeStates(prev => ({ ...prev, [a.id]: "pending" }));
      });
    }
    if (ev.type === "done") {
      setJobStatus("completed");
    }
    if (ev.type === "error") {
      setJobStatus("failed");
      setError(ev.message);
    }
  }, []);

  const startPipeline = async () => {
    if (!query.trim() || jobStatus === "running") return;
    setJobStatus("running");
    setEvents([]);
    setAgentMsgs([]);
    setNodeStates({});
    setRankedTable(null);
    setError(null);
    setJobId(null);

    try {
      const res = await fetch(`${API}/crawl/rank`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });
      const data = await res.json();
      const id = data.job_id;
      setJobId(id);

      // SSE stream
      if (esRef.current) esRef.current.close();
      const es = new EventSource(`${API}/crawl/rank/${id}/stream`);
      esRef.current = es;

      const allEventTypes = [
        "phase_start","phase_complete","node_start","node_complete",
        "agent_message","warning","error","done","status"
      ];
      allEventTypes.forEach(type => {
        es.addEventListener(type, (e) => {
          const ev = JSON.parse(e.data);
          handleEvent({ ...ev, type });
        });
      });

      es.addEventListener("status", (e) => {
        const ev = JSON.parse(e.data);
        if (ev.status === "completed") {
          // Fetch final result
          fetch(`${API}/crawl/rank/${id}`)
            .then(r => r.json())
            .then(result => {
              setRankedTable(result.ranked_table);
              setJobStatus("completed");
            });
          es.close();
        } else if (ev.status === "failed") {
          setJobStatus("failed");
          es.close();
        }
      });

      es.onerror = () => {
        // On stream close, poll for result
        fetch(`${API}/crawl/rank/${id}`)
          .then(r => r.json())
          .then(result => {
            setRankedTable(result.ranked_table);
            setJobStatus(result.status);
          });
        es.close();
      };

    } catch (err) {
      setError(err.message);
      setJobStatus("failed");
    }
  };

  // Group nodes by phase
  const crawlNodes = NODES.filter(n => n.phase === "crawl");
  const graphNodes = NODES.filter(n => n.phase === "graph");

  const getNodeState = (id) => nodeStates[id] || "pending";

  const nodeIcon = (state) => {
    if (state === "active") return <span className="spinner" />;
    if (state === "done")   return "✓";
    return "·";
  };

  return (
    <>
      <style>{css}</style>
      <div className="grain" />
      <div className="app">

        {/* ── Header ── */}
        <header className="header">
          <div className="logo-mark">WC</div>
          <div>
            <div className="header-title">WebCrawler Intelligence</div>
            <div className="header-sub">Multi-agent ranking pipeline · Neo4j · LangGraph</div>
          </div>
        </header>

        {/* ── Query ── */}
        <section className="query-section">
          <div className="query-label">Research Question</div>
          <div className="query-row">
            <input
              className="query-input"
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={e => e.key === "Enter" && startPipeline()}
              placeholder="e.g. Best startup incubators in India ranked by funding amount and network"
            />
            <button
              className="run-btn"
              onClick={startPipeline}
              disabled={jobStatus === "running" || !query.trim()}
            >
              {jobStatus === "running" ? "Running…" : "▶ Run"}
            </button>
          </div>
        </section>

        {/* ── Status bar ── */}
        {jobStatus !== "idle" && (
          <div className="status-bar">
            <span className={`status-dot ${jobStatus}`} />
            <span style={{ color: "var(--muted)", fontSize: "0.65rem", letterSpacing: "0.1em", textTransform: "uppercase" }}>
              {jobStatus === "running" ? "Pipeline executing" : jobStatus === "completed" ? "Complete" : "Failed"}
            </span>
            {jobId && (
              <span style={{ marginLeft: "auto", color: "var(--muted)", fontSize: "0.6rem" }}>
                job #{jobId}
              </span>
            )}
            {error && <span style={{ color: "var(--error)", fontSize: "0.68rem" }}>⚠ {error}</span>}
          </div>
        )}

        {/* ── Main grid: pipeline + logs ── */}
        {jobStatus !== "idle" && (
          <div className="main-grid">

            {/* ── Left: pipeline nodes ── */}
            <div className="pipeline-panel">
              <div className="panel-title">Pipeline Nodes</div>

              <div className="phase-group">
                <div className="phase-label crawl">01 · Crawl Layer</div>
                {crawlNodes.map(n => {
                  const state = getNodeState(n.id);
                  return (
                    <div key={n.id} className={`node-item ${state}`}>
                      <span className="node-icon">{state === "active" ? <span className="spinner" /> : n.icon}</span>
                      <span>{n.label}</span>
                      <span className="node-status">
                        {state === "done" ? "✓" : state === "active" ? "…" : "·"}
                      </span>
                    </div>
                  );
                })}
              </div>

              <div className="phase-group">
                <div className="phase-label graph">02 · Graph Layer</div>
                {graphNodes.map(n => {
                  const state = getNodeState(n.id);
                  return (
                    <div key={n.id} className={`node-item ${state}`}>
                      <span className="node-icon">{n.icon}</span>
                      <span>{n.label}</span>
                      <span className="node-status">
                        {state === "done" ? "✓" : state === "active" ? "…" : "·"}
                      </span>
                    </div>
                  );
                })}
              </div>

              <div className="phase-group">
                <div className="phase-label agents-label">03 · Agent Loop</div>
                {AGENT_PHASES.map(a => {
                  const state = getNodeState(a.id);
                  return (
                    <div key={a.id} className={`node-item ${state}`}>
                      <span className="node-icon">⟳</span>
                      <div>
                        <div style={{ fontSize: "0.72rem" }}>{a.label}</div>
                        <div style={{ fontSize: "0.58rem", color: "var(--muted)" }}>{a.role}</div>
                      </div>
                      <span className="node-status">
                        {state === "done" ? "✓" : state === "active" ? "…" : "·"}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* ── Right: logs + agent comms ── */}
            <div className="right-panel">

              {/* Log panel */}
              <div className="log-panel">
                <div className="log-header">Pipeline Log ({events.length} events)</div>
                <div className="log-body" ref={logRef}>
                  {events.map((ev, i) => (
                    <div key={i} className={`log-entry ${ev.type}`}>
                      <span className="log-time">{fmtTime(ev.timestamp)}</span>
                      {logLabel(ev)}
                    </div>
                  ))}
                  {jobStatus === "running" && (
                    <div className="log-entry" style={{ color: "var(--muted)" }}>
                      <span className="spinner" style={{ marginRight: "0.5rem" }} />
                      waiting for events…
                    </div>
                  )}
                </div>
              </div>

              {/* Agent comms panel */}
              <div className="agent-panel">
                <div className="agent-header">Agent Communication Log ({agentMsgs.length})</div>
                <div ref={agentRef}>
                  {agentMsgs.length === 0 && (
                    <div style={{ padding: "1.5rem 1rem", fontSize: "0.68rem", color: "var(--muted)" }}>
                      Agent messages will appear here as agents communicate…
                    </div>
                  )}
                  {agentMsgs.map((msg, i) => (
                    <div key={i} className="agent-msg">
                      <div className="agent-route">
                        <span className={`agent-name ${msg.from}`}>{msg.from}</span>
                        <span style={{ color: "var(--muted)" }}>→</span>
                        <span className={`agent-name ${msg.to}`}>{msg.to}</span>
                        <span style={{ marginLeft: "auto", color: "var(--muted)", fontSize: "0.58rem" }}>
                          {fmtTime(msg.timestamp)}
                        </span>
                      </div>
                      <div className="agent-content">{msg.content}</div>
                      {msg.missing_columns && msg.missing_columns.length > 0 && (
                        <div className="chip-row">
                          {msg.missing_columns.map(c => <span key={c} className="chip">missing: {c}</span>)}
                        </div>
                      )}
                      {msg.columns && msg.columns.length > 0 && (
                        <div className="chip-row">
                          {msg.columns.map(c => <span key={c} className="chip">{c}</span>)}
                        </div>
                      )}
                      {msg.criteria && msg.criteria.length > 0 && (
                        <div className="chip-row">
                          {msg.criteria.map(c => (
                            <span key={c.column} className="chip">
                              {c.column} {(c.weight * 100).toFixed(0)}%
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>

            </div>
          </div>
        )}

        {/* ── Idle empty state ── */}
        {jobStatus === "idle" && (
          <div className="empty-state">
            <div className="empty-glyph">WC</div>
            <div className="empty-text">
              Ask a ranking question above. The pipeline will crawl the web, build a Neo4j knowledge graph, run agent loops to fill missing data, and return a ranked comparison table.
            </div>
          </div>
        )}

        {/* ── Ranking Table ── */}
        {rankedTable && rankedTable.rows && rankedTable.rows.length > 0 && (
          <section className="ranking-section">
            <div className="ranking-header">
              <div className="ranking-title">Ranking Results</div>
              <div style={{ flex: 1 }} />
              <div style={{ fontSize: "0.65rem", color: "var(--muted)" }}>
                {rankedTable.rows.length} entities · {rankedTable.session_id && `session #${rankedTable.session_id?.slice(0,8)}`}
              </div>
            </div>

            {rankedTable.ranking_rationale && (
              <div className="ranking-rationale" style={{ marginBottom: "1.25rem" }}>
                ⟐ {rankedTable.ranking_rationale}
              </div>
            )}

            {rankedTable.criteria && rankedTable.criteria.length > 0 && (
              <div className="criteria-row">
                {rankedTable.criteria.map(c => (
                  <div key={c.column} className="criterion-chip">
                    <span style={{ color: "var(--text)" }}>{c.column}</span>
                    <span className="criterion-weight">{(c.weight * 100).toFixed(0)}%</span>
                    <span style={{ color: "var(--muted)", fontSize: "0.58rem" }}>
                      {c.higher_is_better ? "↑ higher=better" : "↓ lower=better"}
                    </span>
                  </div>
                ))}
              </div>
            )}

            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th className="rank-col">#</th>
                    <th>Entity</th>
                    <th>Score</th>
                    {rankedTable.rows[0]?.fields && Object.keys(rankedTable.rows[0].fields).slice(0, 7).map(k => (
                      <th key={k}>{k}</th>
                    ))}
                    <th>Source</th>
                  </tr>
                </thead>
                <tbody>
                  {rankedTable.rows.map((row, i) => {
                    const rankClass = i === 0 ? "r1" : i === 1 ? "r2" : i === 2 ? "r3" : "rn";
                    const fields = row.fields || {};
                    const fieldKeys = Object.keys(fields).slice(0, 7);

                    return (
                      <tr key={row.entity_name}>
                        <td>
                          <span className={`rank-badge ${rankClass}`}>{row.rank}</span>
                        </td>
                        <td>
                          <div className="entity-name">{row.entity_name}</div>
                          {row.missing_keys?.length > 0 && (
                            <div style={{ fontSize: "0.58rem", color: "var(--muted)", marginTop: "0.2rem" }}>
                              missing: {row.missing_keys.join(", ")}
                            </div>
                          )}
                        </td>
                        <td>
                          <div className="score-bar">
                            <div className="score-track">
                              <div
                                className="score-fill"
                                style={{ width: `${(row.composite_score * 100).toFixed(1)}%` }}
                              />
                            </div>
                            <span className="score-val">{row.composite_score.toFixed(3)}</span>
                          </div>
                        </td>
                        {fieldKeys.map(k => {
                          const v = fields[k];
                          const missing = !v || v === "null" || v === "N/A";
                          return (
                            <td key={k}>
                              {missing
                                ? <span className="metric-missing">—</span>
                                : <span className="metric-val">{String(v)}</span>
                              }
                            </td>
                          );
                        })}
                        <td>
                          {row.source_url ? (
                            <a
                              className="source-link"
                              href={row.source_url.split(",")[0].trim()}
                              target="_blank"
                              rel="noreferrer"
                            >
                              ↗ source
                            </a>
                          ) : <span className="metric-missing">—</span>}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* Completed but no table */}
        {jobStatus === "completed" && (!rankedTable || !rankedTable.rows?.length) && (
          <div className="empty-state" style={{ marginTop: "2rem" }}>
            <div className="empty-glyph">∅</div>
            <div className="empty-text">
              Pipeline completed but no ranking data was produced. This may happen if the crawl found no relevant entities for the query. Try a more specific query.
            </div>
          </div>
        )}

      </div>
    </>
  );
}
