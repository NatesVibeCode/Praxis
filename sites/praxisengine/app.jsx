// app.jsx — top-level page orchestrator.
const { useState, useEffect, useRef, useCallback } = React;

function Logomark({ size = 96 }) {
  return (
    <svg viewBox="108 93 292 327" width={size} height={size * (327 / 292)} aria-label="Praxis Engine">
      <g fill="none" stroke="currentColor" strokeLinejoin="miter" strokeLinecap="square">
        <path d="M 130 100 L 370 100 L 370 188 M 370 252 L 370 340 L 130 340 L 130 252 M 130 188 L 130 100" strokeWidth="10" />
        <line x1="130" y1="220" x2="370" y2="220" strokeWidth="7" />
      </g>
      <g fill="currentColor">
        <circle cx="130" cy="220" r="16" />
        <circle cx="306" cy="220" r="11" />
        <circle cx="370" cy="220" r="23" />
      </g>
      <g transform="translate(228 220)">
        <circle cx="0" cy="0" r="35" fill="#080808" stroke="currentColor" strokeWidth="6" />
        <circle cx="0" cy="0" r="20" fill="#080808" stroke="currentColor" strokeWidth="6" />
        <circle cx="0" cy="0" r="9" fill="currentColor" />
      </g>
    </svg>
  );
}

function TopBar() {
  return (
    <header className="topbar">
      <div className="shell topbar-inner">
        <a href="#top" className="brand" aria-label="Praxis Engine — home">
          <Logomark size={108} />
        </a>
        <nav className="nav" aria-label="Primary">
          <a href="#contract">Contract</a>
          <a href="#practice">In Practice</a>
          <a href="#loop">Loop</a>
        </nav>
        <span className="status-pill" title="Private beta launching 2026">
          <span className="dot" /> Private beta · 2026
        </span>
      </div>
    </header>
  );
}

function Hero() {
  const [status, setStatus] = useState({ text: '\u00a0', cls: '' });
  const [submitting, setSubmitting] = useState(false);

  const submit = async (e) => {
    e.preventDefault();
    const email = e.target.email.value.trim();
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      setStatus({ text: '! invalid email', cls: 'err' });
      return;
    }
    setSubmitting(true);
    setStatus({ text: '> submitting', cls: '' });
    try {
      const response = await fetch('/api/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, source: 'landing', ts: Date.now() }),
      });
      if (!response.ok) throw new Error('subscribe_failed');
      setStatus({ text: '✓ you\'re on the list', cls: 'ok' });
      e.target.reset();
    } catch {
      setStatus({ text: '! could not submit', cls: 'err' });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="hero" id="top">
      <div className="shell">
        <div className="section-eyebrow">
          praxis engine · v1
        </div>
        <h1>
          Confidence <em>as</em><br />
          infrastructure.
        </h1>
        <p className="lede">
          A workspace where humans and AI collaborate in plain language —
          and the <strong>environment</strong>, not the prompt, enforces what
          matters. Every task ships with its own read scope, write scope,
          locked fields, approval gates, verifier, and receipts. Trust
          materialized, before the work begins.
        </p>
        <form className="hero-form" onSubmit={submit} noValidate>
          <input
            type="email"
            name="email"
            placeholder="you@domain.com"
            autoComplete="email"
            aria-label="Email address"
            required
          />
          <button type="submit" disabled={submitting}>
            Notify me
          </button>
        </form>
        <div className={'hero-status ' + status.cls} aria-live="polite">
          {status.text}
        </div>
      </div>
    </section>
  );
}

function ContractSection({ onAttackLocked }) {
  return (
    <section className="block" id="contract" data-screen-label="01 Contract">
      <div className="shell">
        <div className="section-eyebrow">01 · the contract</div>
        <h2>Every task gets a working world,
          not a prompt.</h2>
        <p className="section-lede">
          Before the agent runs, Praxis materializes a{' '}
          <span className="mono" style={{color:'var(--fg1)'}}>task_environment_contract</span>{' '}
          — the rows it can read, the fields it can write, the ones that
          stay locked, the tools it may call, the verifier that has to pass
          before the task closes. One authority. Every layer consumes it.
          This is what trust, materialized, looks like.
        </p>
        <ContractCard onAttackLocked={onAttackLocked} />
        <p className="muted" style={{
          marginTop: 18, fontFamily: 'var(--font-mono)',
          fontSize: 'var(--fs-xs)', letterSpacing: '0.04em',
        }}>
          · click the <span style={{color:'var(--warning)'}}>locked</span> row to see what happens when an agent reaches past its scope
        </p>
      </div>
    </section>
  );
}

function PracticeSection({ runKey, onReplay }) {
  return (
    <section className="block" id="practice" data-screen-label="02 In Practice">
      <div className="shell">
        <div className="section-eyebrow">02 · in practice</div>
        <h2>A refund batch, on rails.</h2>
        <p className="section-lede">
          Forty-seven refund requests came in overnight. Praxis dispatches
          an agent into a contract scoped to the support queue and the
          payments ledger. Most refunds clear policy and process. One
          exceeds the per-customer threshold; the environment refuses the
          write and routes it for human approval. The verifier proves the
          dollars written equal the dollars approved. Every action,
          allowed or refused, leaves a sealed receipt.
        </p>
        <Canary runKey={runKey} />
        <div style={{ marginTop: 16, display: 'flex', justifyContent: 'flex-end' }}>
          <button
            type="button"
            onClick={onReplay}
            style={{
              background: 'transparent',
              border: '1px solid var(--border-soft)',
              borderRadius: 'var(--radius-sm)',
              color: 'var(--fg2)',
              padding: '8px 14px',
              fontFamily: 'var(--font-mono)',
              fontSize: 11,
              letterSpacing: '0.08em',
              textTransform: 'uppercase',
              cursor: 'pointer',
            }}
          >
            ↻ replay run
          </button>
        </div>
      </div>
    </section>
  );
}

function LoopSection() {
  return (
    <section className="block" id="loop" data-screen-label="03 Loop">
      <div className="shell">
        <div className="section-eyebrow">03 · the loop</div>
        <h2>Anti-patterns are part of the patterns.</h2>
        <p className="section-lede">
          When an agent fails — a refused write, a verifier red, a missing
          tool — the failure becomes residue too. Praxis dispatches a fresh
          agent with that record loaded: the contract that was wrong, the
          field that was locked, the assumption that broke. The new agent
          completes the work correctly. The toolbox of what-not-to-do gets
          richer; the next dispatch is informed by it.
        </p>
        <Loop />
      </div>
    </section>
  );
}

function Foot() {
  const year = new Date().getFullYear();
  return (
    <footer className="foot">
      <div className="shell">
        <div className="row">
          <span>STATUS · PRIVATE BETA</span>
          <span>LAUNCH · 2026</span>
          <span>BUILT IN · BELLINGHAM, WA</span>
          <span><a href="mailto:hello@praxisengine.io">HELLO@PRAXISENGINE.IO</a></span>
        </div>
        <div className="copyright">© {year} Praxis Labs, LLC</div>
      </div>
    </footer>
  );
}

function App() {
  const [canaryRun, setCanaryRun] = useState(0);
  const canarySectionRef = useRef(null);

  const triggerCanary = useCallback(() => {
    setCanaryRun(k => k + 1);
    // smooth-scroll demo into view (avoid scrollIntoView per house rules)
    const el = document.getElementById('practice');
    if (el) {
      const top = el.getBoundingClientRect().top + window.scrollY - 24;
      window.scrollTo({ top, behavior: 'smooth' });
    }
  }, []);

  return (
    <>
      <TopBar />
      <Hero />
      <ContractSection onAttackLocked={triggerCanary} />
      <PracticeSection runKey={canaryRun} onReplay={() => setCanaryRun(k => k + 1)} />
      <LoopSection />
      <Foot />
    </>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
