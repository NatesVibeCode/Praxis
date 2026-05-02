import React, { useCallback, useEffect, useMemo, useState } from 'react';

interface ObjectRow {
  object_kind: string;
  label: string;
  category: string;
  summary: string;
  entries_by_source: Record<string, number>;
}

interface FieldRow {
  object_kind: string;
  field_path: string;
  effective_source: string;
  field_kind: string;
  label: string;
  description: string;
  required: boolean;
  default_value: any;
  valid_values: any[];
  examples: any[];
  deprecation_notes: string;
  display_order: number;
}

interface RelationshipRow {
  entity_id: string;
  entity_type: string;
  name: string;
  summary: string;
  table: string;
  relation: string;
  direction: string;
  source_id: string;
  target_id: string;
  metadata: Record<string, any>;
}

interface DescribePayload {
  object: { object_kind: string; label: string; category: string; summary: string };
  fields: FieldRow[];
  entries_by_source: Record<string, number>;
  relationships?: {
    depends_on: RelationshipRow[];
    referenced_by: RelationshipRow[];
  };
  relationship_counts?: Record<string, number>;
}

const CATEGORIES = [
  'all',
  'table',
  'object_type',
  'integration',
  'dataset',
  'ingest',
  'decision',
  'receipt',
  'tool',
  'object',
] as const;

type Category = typeof CATEGORIES[number];

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

async function fetchObjects(category: Category): Promise<ObjectRow[]> {
  const q = category === 'all' ? '' : `?category=${encodeURIComponent(category)}`;
  const body = await _json(await fetch(`/api/data-dictionary${q}`));
  return Array.isArray(body?.objects) ? body.objects : [];
}

async function fetchDescribe(objectKind: string): Promise<DescribePayload> {
  return _json(await fetch(`/api/data-dictionary/${encodeURIComponent(objectKind)}`));
}

async function putOverride(
  objectKind: string,
  fieldPath: string,
  patch: Record<string, any>,
): Promise<void> {
  await _json(
    await fetch(
      `/api/data-dictionary/${encodeURIComponent(objectKind)}/${encodeURIComponent(fieldPath)}`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      },
    ),
  );
}

async function deleteOverride(objectKind: string, fieldPath: string): Promise<void> {
  await _json(
    await fetch(
      `/api/data-dictionary/${encodeURIComponent(objectKind)}/${encodeURIComponent(fieldPath)}`,
      { method: 'DELETE' },
    ),
  );
}

async function postReproject(): Promise<void> {
  await _json(await fetch('/api/data-dictionary/reproject', { method: 'POST' }));
}

interface EditDraft {
  objectKind: string;
  fieldPath: string;
  label: string;
  description: string;
  fieldKind: string;
}

export function CanvasDataDictionaryPanel() {
  const [open, setOpen] = useState(false);
  const [category, setCategory] = useState<Category>('all');
  const [filter, setFilter] = useState('');
  const [objects, setObjects] = useState<ObjectRow[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<DescribePayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [edit, setEdit] = useState<EditDraft | null>(null);

  const reloadObjects = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setObjects(await fetchObjects(category));
    } catch (e: any) {
      setError(e?.message || 'Failed to load dictionary');
    } finally {
      setLoading(false);
    }
  }, [category]);

  const reloadDetail = useCallback(async (objectKind: string) => {
    setLoading(true);
    setError(null);
    try {
      setDetail(await fetchDescribe(objectKind));
    } catch (e: any) {
      setError(e?.message || 'Failed to load object detail');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    reloadObjects();
  }, [open, reloadObjects]);

  useEffect(() => {
    if (!open || !selected) return;
    reloadDetail(selected);
  }, [open, selected, reloadDetail]);

  const filteredObjects = useMemo(() => {
    const normalized = filter.trim().toLowerCase();
    if (!normalized) return objects;
    return objects.filter(
      (o) =>
        o.object_kind.toLowerCase().includes(normalized) ||
        (o.label || '').toLowerCase().includes(normalized),
    );
  }, [objects, filter]);

  const summary = useMemo(() => {
    const total = objects.length;
    const totalFields = objects.reduce(
      (sum, o) => sum + Object.values(o.entries_by_source || {}).reduce((a, b) => a + Number(b || 0), 0),
      0,
    );
    return `${total} kinds · ${totalFields} fields`;
  }, [objects]);

  const handleReproject = useCallback(async () => {
    setLoading(true);
    setError(null);
    setSuccess(null);
    try {
      await postReproject();
      setSuccess('Refreshed dictionary authority.');
      await reloadObjects();
      if (selected) await reloadDetail(selected);
    } catch (e: any) {
      setError(e?.message || 'Reproject failed');
    } finally {
      setLoading(false);
    }
  }, [reloadObjects, reloadDetail, selected]);

  const handleStartEdit = useCallback((field: FieldRow) => {
    setEdit({
      objectKind: field.object_kind,
      fieldPath: field.field_path,
      label: field.label || '',
      description: field.description || '',
      fieldKind: field.field_kind || 'text',
    });
  }, []);

  const handleSaveEdit = useCallback(async () => {
    if (!edit) return;
    setLoading(true);
    setError(null);
    setSuccess(null);
    try {
      await putOverride(edit.objectKind, edit.fieldPath, {
        label: edit.label,
        description: edit.description,
        field_kind: edit.fieldKind,
      });
      setSuccess(`Override saved for ${edit.fieldPath}.`);
      setEdit(null);
      if (selected) await reloadDetail(selected);
      await reloadObjects();
    } catch (e: any) {
      setError(e?.message || 'Save failed');
    } finally {
      setLoading(false);
    }
  }, [edit, selected, reloadDetail, reloadObjects]);

  const handleClearOverride = useCallback(
    async (field: FieldRow) => {
      setLoading(true);
      setError(null);
      setSuccess(null);
      try {
        await deleteOverride(field.object_kind, field.field_path);
        setSuccess(`Cleared override for ${field.field_path}.`);
        if (selected) await reloadDetail(selected);
        await reloadObjects();
      } catch (e: any) {
        setError(e?.message || 'Clear failed');
      } finally {
        setLoading(false);
      }
    },
    [selected, reloadDetail, reloadObjects],
  );

  const linkedEdges = useMemo(() => {
    if (!detail?.relationships) return [];
    return [
      ...(detail.relationships.depends_on || []).map((row) => ({
        ...row,
        bucket: 'depends_on',
      })),
      ...(detail.relationships.referenced_by || []).map((row) => ({
        ...row,
        bucket: 'referenced_by',
      })),
    ];
  }, [detail]);

  return (
    <div className="canvas-surface-review">
      <button
        type="button"
        className={`canvas-surface-review__toggle${open ? ' canvas-surface-review__toggle--open' : ''}`}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="canvas-surface-review__toggle-copy">
          <span className="canvas-dock__section-label">Data dictionary</span>
          <span className="canvas-action__surface-note">
            Browse auto-projected field descriptors for every injected object. Override any row — operator edits win.
          </span>
        </span>
        <span className="canvas-surface-review__toggle-meta">{summary}</span>
      </button>

      {open && (
        <div className="canvas-surface-review__body">
          <div className="canvas-dock-form__row">
            <select
              className="canvas-dock-form__input"
              value={category}
              onChange={(e) => {
                setSelected(null);
                setDetail(null);
                setCategory(e.target.value as Category);
              }}
            >
              {CATEGORIES.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
            <input
              className="canvas-dock-form__input"
              placeholder="filter"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
            />
            <button
              type="button"
              className="canvas-dock-form__btn"
              disabled={loading}
              onClick={handleReproject}
            >
              Reproject
            </button>
          </div>

          {error && <div className="canvas-dock-form__error">{error}</div>}
          {success && <div className="canvas-action__success">{success}</div>}

          <div className="canvas-dock__section-label" style={{ marginTop: 12 }}>
            Object kinds
          </div>
          <ul className="canvas-catalog__list">
            {filteredObjects.map((o) => {
              const operatorCount = Number(o.entries_by_source?.operator || 0);
              const selectedNow = selected === o.object_kind;
              return (
                <li
                  key={o.object_kind}
                  className={`canvas-catalog__item${selectedNow ? ' canvas-catalog__item--selected' : ''}`}
                  onClick={() => setSelected(o.object_kind)}
                  style={{ cursor: 'pointer' }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span>{o.object_kind}</span>
                    <span className="canvas-action__surface-note">
                      {o.category}
                      {operatorCount > 0 ? ` · ${operatorCount} override${operatorCount === 1 ? '' : 's'}` : ''}
                    </span>
                  </div>
                  {o.summary && <div className="canvas-action__surface-note">{o.summary}</div>}
                </li>
              );
            })}
          </ul>

          {detail && (
            <div className="canvas-surface-review__editor" style={{ marginTop: 16 }}>
              <div className="canvas-dock__section-label">{detail.object.object_kind}</div>
              <div className="canvas-action__surface-note">
                {detail.object.category} · {detail.fields.length} field
                {detail.fields.length === 1 ? '' : 's'}
              </div>

              <table className="canvas-catalog__table" style={{ marginTop: 12, width: '100%' }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left' }}>field_path</th>
                    <th style={{ textAlign: 'left' }}>kind</th>
                    <th style={{ textAlign: 'left' }}>source</th>
                    <th style={{ textAlign: 'left' }}>label / description</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {detail.fields.map((f) => (
                    <tr key={`${f.object_kind}:${f.field_path}`}>
                      <td>{f.field_path}</td>
                      <td>{f.field_kind}</td>
                      <td>{f.effective_source}</td>
                      <td>
                        {f.label && <div>{f.label}</div>}
                        {f.description && (
                          <div className="canvas-action__surface-note">{f.description}</div>
                        )}
                      </td>
                      <td style={{ whiteSpace: 'nowrap' }}>
                        <button
                          type="button"
                          className="canvas-dock-form__btn"
                          onClick={() => handleStartEdit(f)}
                        >
                          Edit
                        </button>
                        {f.effective_source === 'operator' && (
                          <button
                            type="button"
                            className="canvas-dock-form__btn"
                            onClick={() => handleClearOverride(f)}
                            style={{ marginLeft: 6 }}
                          >
                            Clear
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {linkedEdges.length > 0 && (
                <div style={{ marginTop: 16 }}>
                  <div className="canvas-dock__section-label">Linked edges</div>
                  <table className="canvas-catalog__table" style={{ marginTop: 12, width: '100%' }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: 'left' }}>direction</th>
                        <th style={{ textAlign: 'left' }}>relation</th>
                        <th style={{ textAlign: 'left' }}>object</th>
                        <th style={{ textAlign: 'left' }}>metadata</th>
                      </tr>
                    </thead>
                    <tbody>
                      {linkedEdges.map((row) => (
                        <tr key={`${row.bucket}:${row.source_id}:${row.target_id}:${row.relation}`}>
                          <td>{row.direction}</td>
                          <td>{row.relation}</td>
                          <td>
                            <div>{row.name || row.table || row.entity_id}</div>
                            <div className="canvas-action__surface-note">
                              {row.entity_type}
                              {row.summary ? ` · ${row.summary}` : ''}
                            </div>
                          </td>
                          <td className="canvas-action__surface-note">
                            {Object.keys(row.metadata || {}).length > 0
                              ? JSON.stringify(row.metadata)
                              : ''}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {edit && (
            <div className="canvas-surface-review__editor" style={{ marginTop: 16 }}>
              <div className="canvas-dock__section-label">
                Override: {edit.objectKind} · {edit.fieldPath}
              </div>

              <label className="canvas-dock-form__label">Label</label>
              <input
                className="canvas-dock-form__input"
                value={edit.label}
                onChange={(e) => setEdit((d) => (d ? { ...d, label: e.target.value } : d))}
              />

              <label className="canvas-dock-form__label">Description</label>
              <textarea
                className="canvas-dock-form__input canvas-surface-review__textarea"
                rows={3}
                value={edit.description}
                onChange={(e) =>
                  setEdit((d) => (d ? { ...d, description: e.target.value } : d))
                }
              />

              <label className="canvas-dock-form__label">Field kind</label>
              <select
                className="canvas-dock-form__input"
                value={edit.fieldKind}
                onChange={(e) => setEdit((d) => (d ? { ...d, fieldKind: e.target.value } : d))}
              >
                {[
                  'text',
                  'number',
                  'boolean',
                  'enum',
                  'json',
                  'date',
                  'datetime',
                  'reference',
                  'array',
                  'object',
                ].map((k) => (
                  <option key={k} value={k}>
                    {k}
                  </option>
                ))}
              </select>

              <div className="canvas-dock-form__row" style={{ marginTop: 12 }}>
                <button
                  type="button"
                  className="canvas-dock-form__btn"
                  disabled={loading}
                  onClick={handleSaveEdit}
                >
                  Save override
                </button>
                <button
                  type="button"
                  className="canvas-dock-form__btn"
                  onClick={() => setEdit(null)}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
