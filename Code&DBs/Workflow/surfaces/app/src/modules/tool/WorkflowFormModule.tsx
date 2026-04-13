import React, { useState, useEffect } from 'react';
import { QuadrantProps } from '../types';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

function WorkflowFormModule({ config }: QuadrantProps) {
  void config;
  const [models, setModels] = useState<string[]>([]);
  const [selectedModel, setSelectedModel] = useState('');
  const [prompt, setPrompt] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [loadingModels, setLoadingModels] = useState(true);

  useEffect(() => {
    setLoadingModels(true);
    fetch('/api/models?task_type=build')
      .then(res => res.json())
      .then((data: any) => {
        // API returns { models: [{name, ...}] } or array
        const list = Array.isArray(data) ? data : (data.models ?? data.active_models ?? []);
        const names = list.map((m: any) => typeof m === 'string' ? m : (m.name ?? `${m.provider_slug}/${m.model_slug}`));
        setModels(names);
        if (names.length > 0) setSelectedModel(names[0]);
      })
      .catch(() => setModels([]))
      .finally(() => setLoadingModels(false));
  }, []);

  const handleSubmit = async () => {
    if (!prompt.trim() || !selectedModel) return;
    setSubmitting(true);
    setResult(null);
    try {
      const res = await fetch('/api/workflow-job', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt, model: selectedModel }),
      });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const data = await res.json();
      setResult(data.jobs?.[0]?.stdout ?? JSON.stringify(data, null, 2));
    } catch (err: unknown) {
      setResult(`Error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 'var(--space-md, 16px)',
      padding: 'var(--space-lg, 24px)', width: '100%', height: '100%',
      boxSizing: 'border-box', backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
    }}>
      {loadingModels ? (
        <LoadingSkeleton lines={2} height={18} widths={['100%', '76%']} />
      ) : (
        <select
          value={selectedModel}
          onChange={e => setSelectedModel(e.target.value)}
          style={{
            backgroundColor: 'var(--bg, #0d1117)', color: 'var(--text, #c9d1d9)',
            border: '1px solid var(--border, #30363d)', borderRadius: 'var(--radius, 8px)',
            padding: '10px 12px', fontSize: '14px', fontFamily: 'var(--font-sans, sans-serif)',
          }}
        >
          {models.length === 0 && <option value="">No models available</option>}
          {models.map(m => <option key={m} value={m}>{m}</option>)}
        </select>
      )}

      <textarea
        placeholder="Enter prompt..."
        value={prompt}
        onChange={e => setPrompt(e.target.value)}
        rows={5}
        style={{
          flex: 1, backgroundColor: 'var(--bg, #0d1117)', color: 'var(--text, #c9d1d9)',
          border: '1px solid var(--border, #30363d)', borderRadius: 'var(--radius, 8px)',
          padding: '12px', fontSize: '14px', fontFamily: 'var(--font-sans, sans-serif)',
          resize: 'vertical',
        }}
      />

      <button
        onClick={handleSubmit}
        disabled={submitting || !prompt.trim()}
        style={{
          backgroundColor: 'var(--accent, #58a6ff)', color: '#ffffff', border: 'none',
          borderRadius: 'var(--radius, 8px)', padding: '10px 24px', fontSize: '14px',
          fontWeight: 'bold', cursor: submitting ? 'not-allowed' : 'pointer',
          opacity: submitting ? 0.7 : 1, alignSelf: 'flex-end',
        }}
      >
        {submitting ? 'Submitting...' : 'Submit'}
      </button>

      {result && (
        <pre style={{
          backgroundColor: 'var(--bg, #0d1117)', padding: '12px',
          borderRadius: 'var(--radius, 8px)', border: '1px solid var(--border, #30363d)',
          color: 'var(--text, #c9d1d9)', fontSize: 12,
          fontFamily: 'monospace', whiteSpace: 'pre-wrap',
          maxHeight: 300, overflowY: 'auto', margin: 0,
        }}>
          {result}
        </pre>
      )}
    </div>
  );
}

export default WorkflowFormModule;
