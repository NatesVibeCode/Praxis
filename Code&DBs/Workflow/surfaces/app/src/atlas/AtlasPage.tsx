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

  useEffect(() => {
    let cancelled = false;
    probeAtlas().then((s) => {
      if (!cancelled) setStatus(s);
    });
    return () => {
      cancelled = true;
    };
  }, []);

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
    <iframe
      title="Praxis knowledge atlas"
      src="/api/atlas.html"
      style={{
        width: '100%',
        height: '100%',
        border: 0,
        background: '#0b0d12',
      }}
    />
  );
}
