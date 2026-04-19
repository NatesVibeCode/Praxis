import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { APP_CONFIG } from '../config';
import { navigateToShell } from '../shell/routes';
import praxisLockup from '../assets/praxis-lockup-inverse.svg';
import './launcher.css';

interface LauncherDoctor {
  services_ready?: boolean;
  database_reachable?: boolean;
  schema_bootstrapped?: boolean;
  workflow_operational?: boolean;
  api_server_ready?: boolean;
  workflow_api_ready?: boolean;
  mcp_bridge_ready?: boolean;
  ui_ready?: boolean;
  dependency_truth?: {
    ok?: boolean;
    missing_count?: number;
  };
}

interface LauncherService {
  label: string;
  name: string;
  state: string;
  port?: number | null;
  running?: boolean;
}

interface LauncherStatusPayload {
  ready: boolean;
  platform_state: 'ready' | 'degraded';
  launch_url: string;
  praxis_url?: string;
  dashboard_url: string;
  api_docs_url: string;
  doctor: LauncherDoctor;
  services: LauncherService[];
  service_summary?: Record<string, number>;
}

interface LauncherRecoverPayload {
  ok: boolean;
  action: string;
  failure_reason?: string | null;
}

function readinessLabel(ok: boolean | undefined, positive: string = 'Ready'): string {
  return ok ? positive : 'Needs recovery';
}

function serviceTone(ok: boolean | undefined): string {
  return ok === false ? 'launcher-card launcher-card--degraded' : 'launcher-card launcher-card--healthy';
}

export function LauncherFrontdoor() {
  const [status, setStatus] = useState<LauncherStatusPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [recovering, setRecovering] = useState(false);
  const [recoveryMessage, setRecoveryMessage] = useState<string | null>(null);
  const autoRecoveryTriggeredRef = useRef(false);

  const loadStatus = useCallback(async () => {
    try {
      const response = await fetch('/api/launcher/status');
      const payload = await response.json().catch(() => null);
      if (!response.ok || !payload) {
        throw new Error(payload?.detail || 'Failed to load launcher status');
      }
      setStatus(payload as LauncherStatusPayload);
      setError(null);
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : 'Failed to load launcher status');
    } finally {
      setLoading(false);
    }
  }, []);

  const runRecovery = useCallback(async (action: 'launch' | 'restart_all' | 'restart_service' | 'repair_sync') => {
    setRecovering(true);
    setRecoveryMessage(action === 'launch' ? 'Running launcher recovery...' : 'Applying recovery action...');
    try {
      const response = await fetch('/api/launcher/recover', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      });
      const payload = await response.json().catch(() => null) as LauncherRecoverPayload | null;
      if (!response.ok || !payload) {
        throw new Error(payload && 'detail' in payload ? String((payload as Record<string, unknown>).detail) : 'Recovery failed');
      }
      setRecoveryMessage(payload.ok ? 'Recovery completed.' : 'Recovery ran, but the platform still needs attention.');
      await loadStatus();
    } catch (recoverError) {
      setRecoveryMessage(recoverError instanceof Error ? recoverError.message : 'Recovery failed');
    } finally {
      setRecovering(false);
    }
  }, [loadStatus]);

  useEffect(() => {
    void loadStatus();
  }, [loadStatus]);

  useEffect(() => {
    if (!status || status.ready || autoRecoveryTriggeredRef.current) return;
    autoRecoveryTriggeredRef.current = true;
    void runRecovery('launch');
  }, [runRecovery, status]);

  useEffect(() => {
    if (!status || (status.ready && !recovering)) return;
    const timer = window.setInterval(() => {
      void loadStatus();
    }, 5000);
    return () => window.clearInterval(timer);
  }, [loadStatus, recovering, status]);

  const readinessCards = useMemo(() => {
    const doctor = status?.doctor ?? {};
    const databaseReady = Boolean(doctor.database_reachable)
      && Boolean(doctor.workflow_operational ?? doctor.schema_bootstrapped);
    return [
      {
        title: APP_CONFIG.databaseName,
        detail: 'Database truth and schema authority',
        ok: databaseReady,
        meta: readinessLabel(databaseReady, 'Bound'),
      },
      {
        title: 'Praxis API',
        detail: 'Suite API and launcher origin',
        ok: Boolean(doctor.api_server_ready),
        meta: readinessLabel(doctor.api_server_ready),
      },
      {
        title: APP_CONFIG.engineName,
        detail: 'Workflow execution lane and orient surface',
        ok: Boolean(doctor.workflow_api_ready),
        meta: readinessLabel(doctor.workflow_api_ready),
      },
      {
        title: 'MCP Bridge',
        detail: 'Always-on /mcp JSON-RPC surface',
        ok: Boolean(doctor.mcp_bridge_ready),
        meta: readinessLabel(doctor.mcp_bridge_ready, 'Bounded'),
      },
      {
        title: 'Praxis UI',
        detail: 'Suite shell mounted at /app',
        ok: Boolean(doctor.ui_ready),
        meta: readinessLabel(doctor.ui_ready),
      },
      {
        title: 'Dependency Truth',
        detail: 'Runtime manifest completeness',
        ok: Boolean(doctor.dependency_truth?.ok),
        meta: doctor.dependency_truth?.ok
          ? 'Manifest clean'
          : `${doctor.dependency_truth?.missing_count ?? 0} missing`,
      },
    ];
  }, [status]);

  const serviceList = status?.services ?? [];

  // Auto-navigate to the app when healthy — launcher becomes a pass-through
  useEffect(() => {
    if (status && !loading && !error && status.ready && !recovering) {
      navigateToShell();
    }
  }, [status, loading, error, recovering]);

  return (
    <div className="launcher-shell">
      <div className="launcher-backdrop" />
      <div className="launcher-page">
        <section className="launcher-hero">
          <div className="launcher-hero-grid">
            <div className="launcher-logo-panel">
              <img className="launcher-logo" src={praxisLockup} alt="Praxis logo" />
            </div>

            <div className="launcher-copy-panel">
              <div className="launcher-kicker">{APP_CONFIG.name}</div>
              <div className="launcher-heading-row">
                <div>
                  <h1>{APP_CONFIG.suiteName}</h1>
                  <p>
                    Suite shell for {APP_CONFIG.engineName} execution and {APP_CONFIG.databaseName} state truth.
                  </p>
                </div>
                {status?.ready && (
                  <div className="launcher-pill launcher-pill--healthy">Ready</div>
                )}
              </div>

              <div className="launcher-authority-strip">
                <div className="launcher-authority-item">
                  <span>Suite</span>
                  <strong>{APP_CONFIG.suiteName}</strong>
                </div>
                <div className="launcher-authority-item">
                  <span>Engine</span>
                  <strong>{APP_CONFIG.engineName}</strong>
                </div>
                <div className="launcher-authority-item">
                  <span>Data</span>
                  <strong>{APP_CONFIG.databaseName}</strong>
                </div>
              </div>

              <div className="launcher-actions">
                <button
                  type="button"
                  className="launcher-primary"
                  onClick={() => navigateToShell()}
                >
                  Open Praxis
                </button>
                <a className="launcher-secondary" href={status?.dashboard_url ?? '/app'}>
                  Overview
                </a>
                <a className="launcher-secondary" href={status?.api_docs_url ?? '/docs'}>
                  API Docs
                </a>
                <button
                  type="button"
                  className="launcher-secondary"
                  onClick={() => void runRecovery('launch')}
                  disabled={recovering}
                >
                  Retry Recovery
                </button>
              </div>

              <div className="launcher-statusline">
                {loading ? 'Checking suite status...' : error ? error : recoveryMessage || 'Praxis authority connected.'}
              </div>
            </div>
          </div>
        </section>

        <section className="launcher-grid">
          {readinessCards.map((card) => (
            <article key={card.title} className={serviceTone(card.ok)}>
              <div className="launcher-card__title">{card.title}</div>
              <div className="launcher-card__detail">{card.detail}</div>
              <div className="launcher-card__meta">{card.meta}</div>
            </article>
          ))}
        </section>

        <section className="launcher-service-panel">
          <div className="launcher-section-header">
            <div>
              <h2>Runtime services</h2>
              <p>Launchd state plus semantic readiness, without pretending ports are proof.</p>
            </div>
            <div className="launcher-summary">
              {status?.service_summary?.total ?? serviceList.length} tracked
            </div>
          </div>

          <div className="launcher-service-list">
            {serviceList.map((service) => (
              <div key={service.label} className="launcher-service-row">
                <div>
                  <div className="launcher-service-name">{service.name}</div>
                  <div className="launcher-service-label">{service.label}</div>
                </div>
                <div className={`launcher-service-state launcher-service-state--${service.state}`}>
                  {service.port ? `:${service.port}` : 'internal'} · {service.state}
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}
