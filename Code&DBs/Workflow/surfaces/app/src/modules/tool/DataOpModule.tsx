import React, { useMemo, useState } from 'react';
import { QuadrantProps } from '../types';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

interface DataOpConfig {
  action?: string;
  label?: string;
  description?: string;
  glyph?: string;
  defaultInputPath?: string;
  defaultArgs?: Record<string, unknown>;
}

const ACTION_GLYPHS: Record<string, string> = {
  profile: '◫',
  validate: '◇',
  dedupe: '◐',
  reconcile: '⇄',
  repair_loop: '↻',
  redact: '⌘',
};

interface FieldSpec {
  name: string;
  label: string;
  type: 'text' | 'list';
  hint?: string;
  required?: boolean;
}

const ACTION_FIELDS: Record<string, FieldSpec[]> = {
  profile: [
    { name: 'input_path', label: 'Input path', type: 'text', required: true, hint: 'artifacts/data/users.csv' },
  ],
  validate: [
    { name: 'input_path', label: 'Input path', type: 'text', required: true },
    { name: 'schema_json', label: 'Schema (JSON)', type: 'text', hint: '{"email":{"required":true,"regex":".+@.+"}}' },
  ],
  dedupe: [
    { name: 'input_path', label: 'Input path', type: 'text', required: true },
    { name: 'keys', label: 'Keys (comma-separated)', type: 'list', required: true, hint: 'email,phone' },
    { name: 'strategy', label: 'Strategy', type: 'text', hint: 'first | last | most_complete' },
  ],
  reconcile: [
    { name: 'input_path', label: 'Source path', type: 'text', required: true },
    { name: 'secondary_input_path', label: 'Target path', type: 'text', required: true },
    { name: 'keys', label: 'Keys (comma-separated)', type: 'list', required: true },
  ],
  repair_loop: [
    { name: 'input_path', label: 'Input path', type: 'text', required: true },
    { name: 'repairs_json', label: 'Repairs (JSON)', type: 'text', hint: '{"status":{"value":"active"}}' },
    { name: 'schema_json', label: 'Schema (JSON)', type: 'text', hint: '{"email":{"required":true}}' },
  ],
  redact: [
    { name: 'input_path', label: 'Input path', type: 'text', required: true },
    { name: 'redactions_json', label: 'Redactions (JSON)', type: 'text', hint: '{"email":"mask_email","ssn":"remove"}' },
  ],
};

function parseJsonField(raw: string): unknown {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  try {
    return JSON.parse(trimmed);
  } catch {
    return undefined;
  }
}

function buildIntegrationArgs(
  action: string,
  values: Record<string, string>,
  defaults: Record<string, unknown>,
): { args: Record<string, unknown>; error?: string } {
  const fields = ACTION_FIELDS[action] ?? [];
  const args: Record<string, unknown> = { ...defaults, action, operation: action };
  for (const field of fields) {
    const raw = (values[field.name] ?? '').trim();
    if (!raw) {
      if (field.required) return { args, error: `${field.label} is required.` };
      continue;
    }
    if (field.type === 'list') {
      args[field.name] = raw.split(',').map((s) => s.trim()).filter(Boolean);
    } else if (field.name.endsWith('_json')) {
      const key = field.name.replace(/_json$/, '');
      const parsed = parseJsonField(raw);
      if (parsed === undefined) return { args, error: `${field.label} must be valid JSON.` };
      args[key] = parsed;
    } else {
      args[field.name] = raw;
    }
  }
  if (!args.job_name) {
    args.job_name = `${action}-${Date.now()}`;
  }
  return { args };
}

function DataOpModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as DataOpConfig;
  const action = (cfg.action ?? 'profile').trim() || 'profile';
  const fields = ACTION_FIELDS[action] ?? ACTION_FIELDS.profile;
  const initialValues: Record<string, string> = useMemo(() => {
    const out: Record<string, string> = {};
    for (const f of fields) {
      if (f.name === 'input_path' && cfg.defaultInputPath) {
        out[f.name] = cfg.defaultInputPath;
      } else {
        out[f.name] = '';
      }
    }
    return out;
  }, [fields, cfg.defaultInputPath]);

  const [values, setValues] = useState<Record<string, string>>(initialValues);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [resultPreview, setResultPreview] = useState<string | null>(null);

  const handleField = (name: string, value: string) => {
    setValues((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async () => {
    const { args, error: argErr } = buildIntegrationArgs(action, values, cfg.defaultArgs ?? {});
    if (argErr) {
      setError(argErr);
      return;
    }
    setError(null);
    setSubmitting(true);
    setRunId(null);
    setResultPreview(null);
    try {
      const jobLabel = `${action}-job`;
      const payload = {
        name: cfg.label ?? `praxis_data ${action}`,
        workflow_id: `data-${action}-${Date.now()}`,
        phase: 'execute',
        outcome_goal: `Run deterministic data operation ${action}`,
        anti_requirements: [
          'Do not modify unrelated files',
          'Do not read or write outside the declared workspace root',
        ],
        jobs: [
          {
            label: jobLabel,
            agent: `integration/praxis_data/${action}`,
            integration_id: 'praxis_data',
            integration_action: action,
            integration_args: args,
          },
        ],
      };
      const res = await fetch('/api/workflow-runs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = (await res.json().catch(() => null)) as
        | { run_id?: string; error?: string; jobs?: Array<{ stdout?: string; status?: string }> }
        | null;
      if (!res.ok) {
        throw new Error(data?.error ?? `${res.status} ${res.statusText}`);
      }
      setRunId(data?.run_id ?? null);
      const stdout = data?.jobs?.[0]?.stdout;
      if (typeof stdout === 'string' && stdout.trim()) {
        setResultPreview(stdout.length > 1200 ? stdout.slice(0, 1200) + '...' : stdout);
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 'var(--space-sm, 8px)',
        padding: 'var(--space-md, 16px)',
        width: '100%',
        height: '100%',
        boxSizing: 'border-box',
        backgroundColor: 'var(--bg-card, #161b22)',
        borderRadius: 'var(--radius, 8px)',
        border: '1px solid var(--border, #30363d)',
        overflow: 'auto',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'baseline',
          justifyContent: 'space-between',
          gap: 8,
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'baseline',
            gap: 8,
            color: 'var(--text, #c9d1d9)',
            fontSize: 14,
            fontWeight: 600,
          }}
        >
          {(cfg.glyph ?? ACTION_GLYPHS[action]) && (
            <span
              aria-hidden="true"
              style={{
                fontSize: 16,
                lineHeight: 1,
                color: 'var(--text-muted, #8b949e)',
                fontFamily: 'var(--font-mono, monospace)',
              }}
            >
              {cfg.glyph ?? ACTION_GLYPHS[action]}
            </span>
          )}
          <span>{cfg.label ?? `praxis_data · ${action}`}</span>
        </div>
        <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: 11, fontFamily: 'monospace' }}>
          integration/praxis_data/{action}
        </div>
      </div>
      {cfg.description && (
        <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: 12 }}>{cfg.description}</div>
      )}
      {fields.map((field) => (
        <label key={field.name} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <span style={{ color: 'var(--text-muted, #8b949e)', fontSize: 11 }}>
            {field.label}
            {field.required ? ' *' : ''}
          </span>
          <input
            type="text"
            value={values[field.name] ?? ''}
            placeholder={field.hint ?? ''}
            onChange={(e) => handleField(field.name, e.target.value)}
            style={{
              backgroundColor: 'var(--bg, #0d1117)',
              color: 'var(--text, #c9d1d9)',
              border: '1px solid var(--border, #30363d)',
              borderRadius: 'var(--radius, 6px)',
              padding: '6px 8px',
              fontSize: 13,
              fontFamily: 'monospace',
            }}
          />
        </label>
      ))}
      <button
        onClick={handleSubmit}
        disabled={submitting}
        style={{
          marginTop: 4,
          backgroundColor: 'transparent',
          color: 'var(--text, #c9d1d9)',
          border: '1px solid var(--border, #30363d)',
          borderRadius: 'var(--radius, 6px)',
          padding: '6px 14px',
          fontSize: 12,
          fontWeight: 500,
          cursor: submitting ? 'not-allowed' : 'pointer',
          opacity: submitting ? 0.6 : 1,
          alignSelf: 'flex-end',
        }}
      >
        {submitting ? 'Running…' : `Run ${action}`}
      </button>
      {submitting && <LoadingSkeleton lines={2} height={12} widths={['80%', '60%']} />}
      {error && (
        <div
          style={{
            color: 'var(--danger, #f85149)',
            fontSize: 12,
            border: '1px solid var(--danger, #f85149)',
            borderRadius: 'var(--radius, 6px)',
            padding: '6px 8px',
          }}
        >
          {error}
        </div>
      )}
      {runId && (
        <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: 11, fontFamily: 'monospace' }}>
          run_id: {runId}
        </div>
      )}
      {resultPreview && (
        <pre
          style={{
            backgroundColor: 'var(--bg, #0d1117)',
            border: '1px solid var(--border, #30363d)',
            borderRadius: 'var(--radius, 6px)',
            padding: 8,
            fontSize: 11,
            fontFamily: 'monospace',
            margin: 0,
            whiteSpace: 'pre-wrap',
            maxHeight: 240,
            overflow: 'auto',
          }}
        >
          {resultPreview}
        </pre>
      )}
    </div>
  );
}

export default DataOpModule;
