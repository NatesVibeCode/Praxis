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
  runtime_target?: {
    runtime_target_ref?: string;
    substrate_kind?: string;
  };
  sandbox_contract?: {
    empty_thin_sandbox_default?: boolean;
    blockers?: string[];
  };
  empty_thin_sandbox_default?: boolean;
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

interface Satellite {
  id: string;
  label: string;
  detail: string;
  ok: boolean | undefined;
  meta: string;
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

  const satellites = useMemo<Satellite[]>(() => {
    if (!status) {
      return [
        { id: 'db', label: APP_CONFIG.databaseName, detail: 'authority', ok: undefined, meta: 'checking' },
        { id: 'api', label: 'API', detail: 'suite origin', ok: undefined, meta: 'checking' },
        { id: 'engine', label: APP_CONFIG.engineName, detail: 'workflow lane', ok: undefined, meta: 'checking' },
        { id: 'mcp', label: 'MCP', detail: 'tool bridge', ok: undefined, meta: 'checking' },
        { id: 'ui', label: 'UI', detail: 'suite shell', ok: undefined, meta: 'checking' },
        { id: 'target', label: 'Target', detail: 'runtime authority', ok: undefined, meta: 'checking' },
        { id: 'sandbox', label: 'Sandbox', detail: 'thin default', ok: undefined, meta: 'checking' },
      ];
    }
    const doctor = status.doctor ?? {};
    const databaseReady = Boolean(doctor.database_reachable)
      && Boolean(doctor.workflow_operational ?? doctor.schema_bootstrapped);
    const sandboxReady = Boolean(
      doctor.empty_thin_sandbox_default ?? doctor.sandbox_contract?.empty_thin_sandbox_default,
    );
    const targetRef = doctor.runtime_target?.runtime_target_ref ?? 'target';
    const targetMeta = doctor.runtime_target?.substrate_kind ?? 'unbound';
    return [
      { id: 'db', label: APP_CONFIG.databaseName, detail: 'authority', ok: databaseReady, meta: databaseReady ? 'bound' : 'needs recovery' },
      { id: 'api', label: 'API', detail: 'suite origin', ok: Boolean(doctor.api_server_ready), meta: doctor.api_server_ready ? 'ready' : 'down' },
      { id: 'engine', label: APP_CONFIG.engineName, detail: 'workflow lane', ok: Boolean(doctor.workflow_api_ready), meta: doctor.workflow_api_ready ? 'ready' : 'down' },
      { id: 'mcp', label: 'MCP', detail: 'tool bridge', ok: Boolean(doctor.mcp_bridge_ready), meta: doctor.mcp_bridge_ready ? 'bounded' : 'down' },
      { id: 'target', label: 'Target', detail: targetRef, ok: Boolean(doctor.runtime_target), meta: targetMeta },
      {
        id: 'sandbox',
        label: 'Sandbox',
        detail: 'thin default',
        ok: sandboxReady,
        meta: sandboxReady ? 'empty 500m' : `${doctor.sandbox_contract?.blockers?.length ?? 1} blocker`,
      },
    ];
  }, [status]);

  const serviceList = status?.services ?? [];
  const readyCount = satellites.filter((s) => s.ok === true).length;
  const failing = satellites.filter((s) => s.ok === false);
  const checkingCount = satellites.filter((s) => s.ok === undefined).length;
  const nucleusState: 'breathing' | 'checking' | 'degraded' | 'recovering' = error
    ? 'degraded'
    : recovering
      ? 'recovering'
      : loading || checkingCount > 0
        ? 'checking'
        : status?.ready
          ? 'breathing'
          : 'degraded';
  const statusLine = loading
    ? `Checking ${APP_CONFIG.name}…`
    : error
      ? error
      : recoveryMessage || (failing.length > 0
        ? `${failing.length} subsystem${failing.length === 1 ? '' : 's'} need attention`
        : 'Authority connected — opening suite.');

  // Auto-navigate to the app when healthy — launcher becomes a pass-through.
  useEffect(() => {
    if (status && !loading && !error && status.ready && !recovering) {
      navigateToShell();
    }
  }, [status, loading, error, recovering]);

  // Satellite ring geometry: even spacing around the nucleus, lower-half
  // reserved so the label doesn't fight the statusline.
  const satelliteCount = satellites.length;
  const radius = 172;
  const center = 220;
  const satellitePositions = satellites.map((s, i) => {
    // Spread across the upper 220° arc from 160° to 380° (i.e. 200° → 20°).
    const spread = 220;
    const start = -110; // degrees from 12 o'clock
    const deg = satelliteCount === 1 ? start + spread / 2 : start + (spread * i) / (satelliteCount - 1);
    const rad = (deg * Math.PI) / 180;
    return {
      ...s,
      x: center + radius * Math.sin(rad),
      y: center - radius * Math.cos(rad),
    };
  });

  return (
    <div className={`launcher-shell launcher-shell--${nucleusState}`}>
      <div className="launcher-backdrop" />
      <div className="launcher-nucleus-frame">
        <div className="launcher-nucleus-wrap" role="group" aria-label="Praxis readiness">
          {/* Concentric rings — aspirational geometry. The innermost is the
              nucleus (auth), the outer ring pulses when the system is healthy.
              Ring lines are muted; color stays monochrome. */}
          <div className={`launcher-nucleus launcher-nucleus--${nucleusState}`} aria-hidden="true">
            <span className="launcher-nucleus__halo" />
            <span className="launcher-nucleus__ring" />
            <span className="launcher-nucleus__core" />
          </div>

          {/* Lockup sits on top of the nucleus — the brand IS the core until
              the suite opens. */}
          <div className="launcher-nucleus__brand">
            <img className="launcher-nucleus__logo" src={praxisLockup} alt={`${APP_CONFIG.suiteName} lockup`} />
            <div className="launcher-nucleus__caption">
              <span>Authority</span>
              <strong>{APP_CONFIG.databaseName}</strong>
            </div>
          </div>

          {/* Satellites — six subsystems as small hollow rings. Failing
              satellites earn coral + subtle pull toward the center. */}
          <div className="launcher-satellites" role="list" aria-label="Subsystem readiness">
            {satellitePositions.map((sat) => {
              const toneClass = sat.ok === true
                ? 'launcher-satellite--ok'
                : sat.ok === false
                  ? 'launcher-satellite--failed'
                  : 'launcher-satellite--checking';
              return (
                <div
                  key={sat.id}
                  className={`launcher-satellite ${toneClass}`}
                  style={{ left: sat.x, top: sat.y }}
                  role="listitem"
                  aria-label={`${sat.label}: ${sat.meta}`}
                >
                  <span className="launcher-satellite__dot" />
                  <span className="launcher-satellite__label">{sat.label}</span>
                  <span className="launcher-satellite__meta">{sat.meta}</span>
                </div>
              );
            })}
          </div>
        </div>

        <div className="launcher-nucleus-footer">
          <div className={`launcher-nucleus-statusline launcher-nucleus-statusline--${nucleusState}`} aria-live="polite">
            {statusLine}
          </div>
          <div className="launcher-nucleus-actions">
            <button
              type="button"
              className="launcher-nucleus-primary"
              onClick={() => navigateToShell()}
              disabled={!status?.ready && !error}
            >
              Open {APP_CONFIG.suiteName}
            </button>
            {(failing.length > 0 || error) && (
              <button
                type="button"
                className="launcher-nucleus-recover"
                onClick={() => void runRecovery('launch')}
                disabled={recovering}
              >
                {recovering ? 'Recovering…' : 'Recover'}
              </button>
            )}
            <a className="launcher-nucleus-secondary" href={status?.api_docs_url ?? '/docs'}>
              API
            </a>
            <span className="launcher-nucleus-readiness" aria-label="Readiness summary">
              {readyCount}/{satellites.length} bound
            </span>
          </div>
        </div>

        {/* Legacy service inventory — tucked below the fold, collapsed unless
            things are broken. Keeps the info available without cluttering
            the first impression. */}
        {(failing.length > 0 || serviceList.length > 0) && (
          <details className="launcher-service-details">
            <summary>Runtime service inventory ({status?.service_summary?.total ?? serviceList.length})</summary>
            <div className="launcher-service-list">
              {serviceList.map((service) => (
                <div key={service.label} className="launcher-service-row">
                  <span className={`launcher-service-dot launcher-service-dot--${service.state}`} />
                  <div className="launcher-service-copy">
                    <div className="launcher-service-name">{service.name}</div>
                    <div className="launcher-service-label">{service.label}</div>
                  </div>
                  <div className={`launcher-service-state launcher-service-state--${service.state}`}>
                    {service.port ? `:${service.port}` : 'internal'} · {service.state}
                  </div>
                </div>
              ))}
            </div>
          </details>
        )}
      </div>
    </div>
  );
}
