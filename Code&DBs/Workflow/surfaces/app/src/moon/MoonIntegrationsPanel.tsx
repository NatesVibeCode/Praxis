import React, { useCallback, useEffect, useMemo, useState } from 'react';

import { MoonGlyph } from './MoonGlyph';
import { MoonPickerInput } from './MoonPickers';

interface IntegrationRow {
  id: string;
  name: string;
  description: string;
  provider: string;
  auth_status: string;
  manifest_source: string;
  catalog_dispatch?: boolean;
  capabilities: Array<{ action: string; method?: string; path?: string }>;
  auth_kind?: string;
}

const PLATFORM_IDS = new Set(['notifications', 'praxis-dispatch', 'workflow']);

function isExternalIntegration(row: IntegrationRow): boolean {
  if (row.catalog_dispatch) return false;
  if (PLATFORM_IDS.has(row.id)) return false;
  if (row.provider === 'praxis') return false;
  return true;
}

interface CapabilityDraft {
  action: string;
  method: string;
  path: string;
}

type AuthKind = 'none' | 'env_var' | 'api_key' | 'oauth2';

interface AddDraft {
  id: string;
  name: string;
  description: string;
  provider: string;
  capabilities: CapabilityDraft[];
  authKind: AuthKind;
  envVar: string;
}

interface SecretDraft {
  integrationId: string;
  value: string;
}

interface TestResult {
  integrationId: string;
  status: string;
  detail: string;
}

const EMPTY_DRAFT: AddDraft = {
  id: '',
  name: '',
  description: '',
  provider: 'http',
  capabilities: [{ action: '', method: 'GET', path: '' }],
  authKind: 'none',
  envVar: '',
};

async function _json(resp: Response): Promise<any> {
  let body: any = null;
  try {
    body = await resp.json();
  } catch {
    body = null;
  }
  if (!resp.ok) throw new Error(body?.error || body?.detail || `HTTP ${resp.status}`);
  return body;
}

async function fetchIntegrations(): Promise<IntegrationRow[]> {
  const body = await _json(await fetch('/api/integrations'));
  return Array.isArray(body?.integrations) ? body.integrations : [];
}

async function postIntegration(draft: AddDraft): Promise<void> {
  const capabilities = draft.capabilities
    .filter((c) => c.action.trim() && c.path.trim())
    .map((c) => ({
      action: c.action.trim(),
      method: c.method.trim().toUpperCase() || 'GET',
      path: c.path.trim(),
    }));
  const auth =
    draft.authKind === 'none'
      ? { kind: 'none' }
      : { kind: draft.authKind, env_var: draft.envVar.trim() };
  await _json(await fetch('/api/integrations', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: draft.id.trim(),
      name: draft.name.trim(),
      description: draft.description.trim(),
      provider: draft.provider.trim() || 'http',
      capabilities,
      auth,
      manifest_source: 'ui',
    }),
  }));
}

async function putSecret(integrationId: string, value: string): Promise<void> {
  await _json(await fetch(`/api/integrations/${encodeURIComponent(integrationId)}/secret`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ value }),
  }));
}

async function postTest(integrationId: string): Promise<TestResult> {
  const body = await _json(await fetch(`/api/integrations/${encodeURIComponent(integrationId)}/test`, {
    method: 'POST',
  }));
  return {
    integrationId,
    status: String(body?.credential_status ?? 'unknown'),
    detail: String(body?.detail ?? ''),
  };
}

async function postReload(): Promise<number> {
  const body = await _json(await fetch('/api/integrations/reload', { method: 'POST' }));
  return Number(body?.synced ?? 0);
}

export function MoonIntegrationsPanel() {
  const [open, setOpen] = useState(false);
  const [rows, setRows] = useState<IntegrationRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const [addOpen, setAddOpen] = useState(false);
  const [draft, setDraft] = useState<AddDraft>(EMPTY_DRAFT);

  const [secretDraft, setSecretDraft] = useState<SecretDraft | null>(null);
  const [testResults, setTestResults] = useState<Record<string, TestResult>>({});
  const [showPlatform, setShowPlatform] = useState(false);

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setRows(await fetchIntegrations());
    } catch (e: any) {
      setError(e?.message || 'Failed to load integrations');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    reload();
  }, [open, reload]);

  const externalRows = useMemo(() => rows.filter(isExternalIntegration), [rows]);
  const platformRows = useMemo(() => rows.filter((r) => !isExternalIntegration(r)), [rows]);
  const visibleRows = showPlatform ? rows : externalRows;

  const summary = useMemo(() => {
    const connected = externalRows.filter((r) => r.auth_status === 'connected').length;
    const hiddenNote = platformRows.length > 0 ? ` · ${platformRows.length} platform hidden` : '';
    return `${externalRows.length} external · ${connected} connected${hiddenNote}`;
  }, [externalRows, platformRows]);

  const handleAddCapability = useCallback(() => {
    setDraft((d) => ({ ...d, capabilities: [...d.capabilities, { action: '', method: 'GET', path: '' }] }));
  }, []);

  const handleCreate = useCallback(async () => {
    setLoading(true);
    setError(null);
    setSuccess(null);
    try {
      await postIntegration(draft);
      setSuccess(`Registered ${draft.id.trim()}.`);
      setDraft(EMPTY_DRAFT);
      setAddOpen(false);
      await reload();
    } catch (e: any) {
      setError(e?.message || 'Create failed');
    } finally {
      setLoading(false);
    }
  }, [draft, reload]);

  const handleSaveSecret = useCallback(async () => {
    if (!secretDraft) return;
    if (!secretDraft.value.trim()) {
      setError('Secret value cannot be empty.');
      return;
    }
    setLoading(true);
    setError(null);
    setSuccess(null);
    try {
      await putSecret(secretDraft.integrationId, secretDraft.value);
      setSuccess(`Stored secret for ${secretDraft.integrationId} in Keychain.`);
      setSecretDraft(null);
      await reload();
    } catch (e: any) {
      setError(e?.message || 'Set secret failed');
    } finally {
      setLoading(false);
    }
  }, [secretDraft, reload]);

  const handleTest = useCallback(async (integrationId: string) => {
    setError(null);
    try {
      const result = await postTest(integrationId);
      setTestResults((prev) => ({ ...prev, [integrationId]: result }));
    } catch (e: any) {
      setError(`${integrationId}: ${e?.message || 'Test failed'}`);
    }
  }, []);

  const handleReload = useCallback(async () => {
    setLoading(true);
    setError(null);
    setSuccess(null);
    try {
      const n = await postReload();
      setSuccess(`Registry sync refreshed (${n} rows).`);
      await reload();
    } catch (e: any) {
      setError(e?.message || 'Reload failed');
    } finally {
      setLoading(false);
    }
  }, [reload]);

  return (
    <div className="moon-surface-review">
      <button
        type="button"
        className={`moon-surface-review__toggle${open ? ' moon-surface-review__toggle--open' : ''}`}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="moon-surface-review__toggle-copy">
          <span className="moon-dock__section-label">Integrations</span>
          <span className="moon-action__surface-note">
            Register third-party APIs, store secrets in Keychain, and verify credentials without leaving the canvas.
          </span>
        </span>
        <span className="moon-surface-review__toggle-meta">{summary}</span>
      </button>

      {open && (
        <div className="moon-surface-review__body">
          <div className="moon-dock-form__row">
            <button
              type="button"
              className="moon-dock-form__btn"
              onClick={() => setAddOpen((v) => !v)}
            >
              {addOpen ? 'Cancel add' : 'Add integration'}
            </button>
            <button
              type="button"
              className="moon-dock-form__btn"
              onClick={() => setShowPlatform((v) => !v)}
            >
              {showPlatform ? `Hide platform (${platformRows.length})` : `Show platform (${platformRows.length})`}
            </button>
            <button
              type="button"
              className="moon-dock-form__btn"
              disabled={loading}
              onClick={handleReload}
            >
              Refresh registry
            </button>
          </div>

          {addOpen && (
            <div className="moon-surface-review__editor">
              <label className="moon-dock-form__label" htmlFor="integrations-id">Integration id</label>
              <input
                id="integrations-id"
                className="moon-dock-form__input"
                placeholder="stripe"
                value={draft.id}
                onChange={(e) => setDraft((d) => ({ ...d, id: e.target.value }))}
              />

              <label className="moon-dock-form__label" htmlFor="integrations-name">Display name</label>
              <input
                id="integrations-name"
                className="moon-dock-form__input"
                placeholder="Stripe"
                value={draft.name}
                onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value }))}
              />

              <label className="moon-dock-form__label" htmlFor="integrations-desc">Description</label>
              <textarea
                id="integrations-desc"
                className="moon-dock-form__input moon-surface-review__textarea"
                rows={2}
                value={draft.description}
                onChange={(e) => setDraft((d) => ({ ...d, description: e.target.value }))}
              />

              <label className="moon-dock-form__label" htmlFor="integrations-provider">Provider</label>
              <MoonPickerInput
                id="integrations-provider"
                value={draft.provider}
                onChange={(next) => setDraft((d) => ({ ...d, provider: next }))}
                placeholder="Choose a provider (http, stripe, slack, …)"
                suggestionsUrl="/api/moon/pickers/integration-providers"
                suggestionsKey="providers"
                hint="Picking a known provider lets Moon apply the right auth shape and request defaults."
                ariaLabel="Integration provider"
              />

              <div className="moon-dock__section-label" style={{ marginTop: 16 }}>Capabilities</div>
              {draft.capabilities.map((cap, index) => (
                <div key={index} className="moon-surface-review__columns">
                  <div className="moon-surface-review__column">
                    <label className="moon-dock-form__label" htmlFor={`integrations-cap-action-${index}`}>Action</label>
                    <input
                      id={`integrations-cap-action-${index}`}
                      className="moon-dock-form__input"
                      placeholder="get_ip"
                      value={cap.action}
                      onChange={(e) => setDraft((d) => ({
                        ...d,
                        capabilities: d.capabilities.map((c, i) => i === index ? { ...c, action: e.target.value } : c),
                      }))}
                    />
                    <label className="moon-dock-form__label" htmlFor={`integrations-cap-method-${index}`}>Method</label>
                    <select
                      id={`integrations-cap-method-${index}`}
                      className="moon-dock-form__select"
                      value={cap.method}
                      onChange={(e) => setDraft((d) => ({
                        ...d,
                        capabilities: d.capabilities.map((c, i) => i === index ? { ...c, method: e.target.value } : c),
                      }))}
                    >
                      {['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].map((m) => (
                        <option key={m} value={m}>{m}</option>
                      ))}
                    </select>
                  </div>
                  <div className="moon-surface-review__column">
                    <label className="moon-dock-form__label" htmlFor={`integrations-cap-path-${index}`}>Full URL</label>
                    <input
                      id={`integrations-cap-path-${index}`}
                      className="moon-dock-form__input"
                      placeholder="https://api.ipify.org?format=json"
                      value={cap.path}
                      onChange={(e) => setDraft((d) => ({
                        ...d,
                        capabilities: d.capabilities.map((c, i) => i === index ? { ...c, path: e.target.value } : c),
                      }))}
                    />
                  </div>
                </div>
              ))}
              <div className="moon-dock-form__row">
                <button type="button" className="moon-dock-form__btn" onClick={handleAddCapability}>
                  Add capability
                </button>
              </div>

              <label className="moon-dock-form__label" htmlFor="integrations-auth-kind">Auth kind</label>
              <select
                id="integrations-auth-kind"
                className="moon-dock-form__select"
                value={draft.authKind}
                onChange={(e) => setDraft((d) => ({ ...d, authKind: e.target.value as AuthKind }))}
              >
                <option value="none">None</option>
                <option value="env_var">Env var</option>
                <option value="api_key">API key</option>
                <option value="oauth2">OAuth 2</option>
              </select>

              {draft.authKind !== 'none' && (
                <>
                  <label className="moon-dock-form__label" htmlFor="integrations-env-var">Env var name</label>
                  <input
                    id="integrations-env-var"
                    className="moon-dock-form__input"
                    placeholder="STRIPE_API_KEY"
                    value={draft.envVar}
                    onChange={(e) => setDraft((d) => ({ ...d, envVar: e.target.value }))}
                  />
                </>
              )}

              <div className="moon-dock-form__row">
                <button
                  type="button"
                  className="moon-dock-form__btn"
                  disabled={loading}
                  onClick={handleCreate}
                >
                  {loading ? 'Saving…' : 'Register integration'}
                </button>
              </div>
            </div>
          )}

          <div className="moon-surface-review__target-list">
            {visibleRows.length === 0 ? (
              <div className="moon-dock__empty">
                {loading ? 'Loading integrations…' : 'No external integrations yet. Use Add integration to register a third-party API.'}
              </div>
            ) : visibleRows.map((row) => {
              const test = testResults[row.id];
              const isSecretTarget = secretDraft?.integrationId === row.id;
              const connected = row.auth_status === 'connected';
              const tone = connected ? 'runtime' : 'partial';
              return (
                <div
                  key={row.id}
                  className={`moon-dock__catalog-item moon-dock__catalog-item--${tone}`}
                  style={{ display: 'block' }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <MoonGlyph type="tool" size={14} />
                    <span className="moon-catalog-item__stack" style={{ flex: 1 }}>
                      <span className="moon-catalog-item__label">{row.name || row.id}</span>
                      <span className="moon-catalog-item__detail">
                        {row.id} · {row.manifest_source || 'unknown'} · {row.capabilities.length} action{row.capabilities.length === 1 ? '' : 's'}
                      </span>
                    </span>
                    <span className="moon-catalog-item__meta-row">
                      <span className={`moon-truth-badge moon-truth-badge--${tone}`}>
                        {row.auth_status}
                      </span>
                    </span>
                  </div>
                  {row.description && (
                    <div className="moon-catalog-item__detail" style={{ marginTop: 4 }}>
                      {row.description}
                    </div>
                  )}
                  <div className="moon-dock-form__row" style={{ marginTop: 8 }}>
                    <button
                      type="button"
                      className="moon-dock-form__btn"
                      onClick={() => handleTest(row.id)}
                    >
                      Test credentials
                    </button>
                    {row.auth_kind && row.auth_kind !== 'none' && (
                      <button
                        type="button"
                        className="moon-dock-form__btn"
                        onClick={() => setSecretDraft(isSecretTarget ? null : { integrationId: row.id, value: '' })}
                      >
                        {isSecretTarget ? 'Close secret' : 'Set secret'}
                      </button>
                    )}
                  </div>
                  {test && (
                    <div
                      className={`moon-action__surface-note`}
                      style={{ marginTop: 6 }}
                    >
                      <strong>{test.status}</strong> — {test.detail || '(no detail)'}
                    </div>
                  )}
                  {isSecretTarget && (
                    <div style={{ marginTop: 8 }}>
                      <label className="moon-dock-form__label" htmlFor={`integrations-secret-${row.id}`}>
                        Secret value (stored in macOS Keychain under service=praxis)
                      </label>
                      <input
                        id={`integrations-secret-${row.id}`}
                        className="moon-dock-form__input"
                        type="password"
                        autoComplete="off"
                        value={secretDraft.value}
                        onChange={(e) => setSecretDraft({ integrationId: row.id, value: e.target.value })}
                      />
                      <div className="moon-dock-form__row" style={{ marginTop: 6 }}>
                        <button
                          type="button"
                          className="moon-dock-form__btn"
                          disabled={loading}
                          onClick={handleSaveSecret}
                        >
                          {loading ? 'Saving…' : 'Save to Keychain'}
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          {error && <div className="moon-dock-form__error">{error}</div>}
          {success && <div className="moon-action__success">{success}</div>}
        </div>
      )}
    </div>
  );
}
