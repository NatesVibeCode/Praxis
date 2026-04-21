import { useEffect, useState } from 'react';

interface AtlasStatus {
  ok: boolean;
  missing: boolean;
  detail: string | null;
}

async function probeAtlas(): Promise<AtlasStatus> {
  try {
    const response = await fetch('/api/atlas.html');
    if (response.ok) return { ok: true, missing: false, detail: null };
    if (response.status === 503) {
      const body = await response.json().catch(() => null);
      return {
        ok: false,
        missing: true,
        detail:
          body?.detail
          || 'Atlas artifact not generated. Run `python3 scripts/praxis_atlas.py`.',
      };
    }
    return { ok: false, missing: false, detail: `HTTP ${response.status}` };
  } catch (error) {
    return {
      ok: false,
      missing: false,
      detail: error instanceof Error ? error.message : 'Network error',
    };
  }
}

export function AtlasPage() {
  const [status, setStatus] = useState<AtlasStatus | null>(null);
  const [frameReady, setFrameReady] = useState(false);
  const [frameIssue, setFrameIssue] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    probeAtlas().then((s) => {
      if (!cancelled) setStatus(s);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!status?.ok) {
      setFrameReady(false);
      setFrameIssue(null);
      return;
    }

    setFrameReady(false);
    setFrameIssue(null);

    let reported = false;
    const onMessage = (event: MessageEvent) => {
      if (event.origin !== window.location.origin) return;
      const data = event.data;
      if (!data || data.type !== 'praxis-atlas-status') return;
      if (data.ok) {
        reported = true;
        setFrameReady(true);
        setFrameIssue(null);
        return;
      }
      reported = true;
      setFrameReady(false);
      setFrameIssue(typeof data.detail === 'string' ? data.detail : 'Atlas runtime reported a render failure.');
    };

    window.addEventListener('message', onMessage);
    const timer = window.setTimeout(() => {
      if (!reported) {
        setFrameIssue((current) => current ?? 'Atlas loaded, but the graph runtime has not reported ready yet.');
      }
    }, 8000);

    return () => {
      window.removeEventListener('message', onMessage);
      window.clearTimeout(timer);
    };
  }, [status?.ok]);

  if (status === null) {
    return (
      <div className="app-shell__fallback">
        <div className="app-shell__fallback-kicker">Loading</div>
        <div className="app-shell__fallback-title">Opening Atlas…</div>
      </div>
    );
  }

  if (!status.ok) {
    return (
      <div className="app-shell__fallback app-shell__fallback--error">
        <div className="app-shell__fallback-kicker">
          {status.missing ? 'Atlas artifact missing' : 'Atlas unavailable'}
        </div>
        <div className="app-shell__fallback-title">
          {status.missing ? 'Generate the atlas first.' : 'Could not load atlas.'}
        </div>
        <p className="app-shell__fallback-copy">{status.detail}</p>
      </div>
    );
  }

  return (
    <div
      style={{
        position: 'absolute',
        inset: 0,
        minHeight: 0,
        background: '#0b0d12',
      }}
    >
      <iframe
        title="Praxis knowledge atlas"
        src="/api/atlas.html"
        onError={() => {
          setFrameReady(false);
          setFrameIssue('Atlas iframe failed to load.');
        }}
        style={{
          display: 'block',
          width: '100%',
          height: '100%',
          border: 0,
          background: '#0b0d12',
        }}
      />
      {(!frameReady || frameIssue) && (
        <div
          aria-live="polite"
          style={{
            position: 'absolute',
            right: 16,
            bottom: 16,
            maxWidth: 360,
            padding: '12px 14px',
            border: '1px solid rgba(255, 255, 255, 0.12)',
            borderRadius: 8,
            background: 'rgba(9, 10, 13, 0.88)',
            color: '#f3efe6',
            boxShadow: '0 18px 42px rgba(0, 0, 0, 0.34)',
            pointerEvents: 'none',
          }}
        >
          <div
            style={{
              fontSize: 10,
              fontWeight: 700,
              letterSpacing: '0.16em',
              textTransform: 'uppercase',
              color: frameIssue ? '#fbbf24' : '#9299ab',
              marginBottom: 4,
            }}
          >
            {frameIssue ? 'Atlas render check' : 'Rendering Atlas'}
          </div>
          <div style={{ fontSize: 12, lineHeight: 1.45, color: '#d8d2c5' }}>
            {frameIssue ?? 'Waiting for the graph canvas to report ready.'}
          </div>
        </div>
      )}
    </div>
  );
}
