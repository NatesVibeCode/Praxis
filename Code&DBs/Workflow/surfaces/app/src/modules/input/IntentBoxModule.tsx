import React, { useState } from 'react';
import { emitPraxisOpenTab } from '../../praxis/events';
import { normalizePraxisBundle, resolvePraxisBundleSurface } from '../../praxis/manifest';
import { QuadrantProps } from '../types';
import { AppPreview } from '../../grid/AppPreview';
import { QuadrantManifest } from '../../grid/QuadrantGrid';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

interface Template {
  id: string;
  name: string;
  description: string;
}

interface GenerateResult {
  manifest_id: string;
  explanation: string;
}

export const IntentBoxModule: React.FC<QuadrantProps> = ({ config }) => {
  void config;
  const [text, setText] = useState('');
  const [loading, setLoading] = useState(false);
  const [templates, setTemplates] = useState<Template[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [showGenerate, setShowGenerate] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [generateResult, setGenerateResult] = useState<GenerateResult | null>(null);
  const [generateError, setGenerateError] = useState<string | null>(null);
  const [previewManifest, setPreviewManifest] = useState<QuadrantManifest | null>(null);
  const [previewExplanation, setPreviewExplanation] = useState('');
  const [previewConfidence, setPreviewConfidence] = useState(0);
  const [previewManifestId, setPreviewManifestId] = useState<string | null>(null);

  const resetGenerateState = () => {
    setShowGenerate(false);
    setGenerating(false);
    setGenerateResult(null);
    setGenerateError(null);
    setPreviewManifest(null);
    setPreviewExplanation('');
    setPreviewConfidence(0);
    setPreviewManifestId(null);
  };

  const searchTemplates = async (query?: string) => {
    setLoading(true);
    setTemplates([]);
    setMessage(null);
    resetGenerateState();
    try {
      const data = query
        ? await fetch(`/api/intent/analyze?q=${encodeURIComponent(query)}`).then(async (response) => {
            if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
            return response.json();
          })
        : await fetch('/api/templates').then(async (response) => {
            if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
            return response.json();
          });

      const found = data.templates ?? [];

      if (query) {
        const analysis = data.analysis ?? {};
        const matcherAuthoritative = analysis.source === 'intent_matcher';
        const matches = analysis.matches ?? {};
        const uiCount = Array.isArray(matches.ui_components) ? matches.ui_components.length : 0;
        const calcCount = Array.isArray(matches.calculations) ? matches.calculations.length : 0;
        const wfCount = Array.isArray(matches.workflows) ? matches.workflows.length : 0;
        const totalMatches = typeof matches.total_count === 'number'
          ? matches.total_count
          : uiCount + calcCount + wfCount;

        if (found.length > 0) {
          setTemplates(found);
          setMessage(`Found ${found.length} template${found.length > 1 ? 's' : ''}`);
        } else if (totalMatches > 0) {
          setMessage(
            `No templates match "${query}" but the intent matcher found ${totalMatches} related piece${totalMatches > 1 ? 's' : ''} `
            + `(${uiCount} components, ${calcCount} calculations, ${wfCount} workflows). A custom workspace can be assembled.`
          );
        } else if (!matcherAuthoritative) {
          setMessage(`No matches for "${query}". Intent matcher is unavailable, so generation is paused until analysis recovers.`);
        } else {
          setMessage(`No matches for "${query}".`);
        }
        setShowGenerate(Boolean(data.can_generate ?? false) && matcherAuthoritative && found.length === 0);
      } else {
        setTemplates(found);
        setMessage(`All templates (${found.length})`);
      }
    } catch (err: any) {
      setMessage(`Error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleGenerate = async () => {
    setGenerating(true);
    setGenerateError(null);
    setGenerateResult(null);
    setPreviewManifest(null);
    try {
      const res = await fetch('/api/manifests/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ intent: text }),
      });
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || `Generation failed (${res.status})`);
      }
      const data = await res.json();
      const manifestId = data.manifest_id;
      const explanation = data.explanation || '';
      const confidence = data.confidence ?? 0.7;

      // Fetch full manifest for preview
      const manifestRes = await fetch(`/api/manifests/${manifestId}`);
      if (!manifestRes.ok) throw new Error('Failed to load generated manifest');
      const manifestData = await manifestRes.json();
      const bundle = normalizePraxisBundle(manifestData, {
        id: manifestId,
        title: typeof manifestData?.name === 'string' ? manifestData.name : undefined,
        description: typeof manifestData?.description === 'string' ? manifestData.description : undefined,
      });
      const previewSurface = resolvePraxisBundleSurface(bundle);
      if (!previewSurface || previewSurface.kind !== 'quadrant_manifest') {
        throw new Error('Generated manifest has no preview surface');
      }

      setGenerateResult({ manifest_id: manifestId, explanation });
      setPreviewManifestId(manifestId);
      setPreviewExplanation(explanation);
      setPreviewConfidence(confidence);
      setPreviewManifest(previewSurface.manifest);
    } catch (err: any) {
      setGenerateError(err.message);
    } finally {
      setGenerating(false);
    }
  };

  const handleSubmit = async () => {
    if (!text.trim()) return;
    await searchTemplates(text);
  };

  // Animated dots for loading
  const LoadingDots: React.FC = () => {
    const [dots, setDots] = React.useState('');
    React.useEffect(() => {
      const iv = setInterval(() => setDots(d => d.length >= 3 ? '' : d + '.'), 500);
      return () => clearInterval(iv);
    }, []);
    return <span>Generating your app{dots}</span>;
  };

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 12,
      padding: 'var(--space-lg, 24px)', width: '100%', boxSizing: 'border-box',
      position: 'relative',
    }}>
      <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
        <input
          type="text"
          placeholder="What would you like to build?"
          value={text}
          onChange={e => setText(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && handleSubmit()}
          disabled={generating}
          style={{
            flex: 1, backgroundColor: 'var(--bg, #0d1117)', color: 'var(--text, #c9d1d9)',
            border: '1px solid var(--border, #30363d)', borderRadius: 'var(--radius, 8px)',
            padding: '12px 16px', fontSize: 15,
            opacity: generating ? 0.5 : 1,
          }}
        />
        <button
          onClick={handleSubmit}
          disabled={loading || generating}
          style={{
            backgroundColor: 'var(--accent, #58a6ff)', color: '#fff', border: 'none',
            borderRadius: 'var(--radius, 8px)', padding: '0 20px', fontSize: 15, fontWeight: 600,
            cursor: (loading || generating) ? 'not-allowed' : 'pointer',
            opacity: (loading || generating) ? 0.7 : 1,
            height: 42,
          }}
        >
          {loading ? '...' : 'Go'}
        </button>
      </div>

      <div style={{ display: 'flex', gap: 16, alignItems: 'center', minHeight: 20 }}>
        {message && (
          <span style={{ color: 'var(--text-muted, #8b949e)', fontSize: 13 }}>{message}</span>
        )}
        <button
          onClick={() => searchTemplates()}
          disabled={loading || generating}
          style={{
            background: 'none', border: 'none', color: 'var(--accent, #58a6ff)',
            fontSize: 13, cursor: 'pointer', padding: 0, textDecoration: 'underline',
            opacity: (loading || generating) ? 0.5 : 1,
          }}
        >
          Browse All Templates
        </button>
      </div>

      {/* Generate App button — shown when no templates or components match */}
      {showGenerate && !generating && !generateResult && !generateError && (
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, padding: '16px 0' }}>
          <button
            onClick={handleGenerate}
            style={{
              backgroundColor: 'var(--accent, #58a6ff)', color: '#fff', border: 'none',
              borderRadius: 'var(--radius, 8px)', padding: '12px 32px', fontSize: 16, fontWeight: 700,
              cursor: 'pointer', boxShadow: '0 0 12px rgba(88, 166, 255, 0.3)',
              transition: 'transform 0.1s, box-shadow 0.15s',
            }}
            onMouseEnter={e => {
              e.currentTarget.style.transform = 'scale(1.03)';
              e.currentTarget.style.boxShadow = '0 0 20px rgba(88, 166, 255, 0.5)';
            }}
            onMouseLeave={e => {
              e.currentTarget.style.transform = 'scale(1)';
              e.currentTarget.style.boxShadow = '0 0 12px rgba(88, 166, 255, 0.3)';
            }}
          >
            Generate App
          </button>
          <span style={{ color: 'var(--text-muted, #8b949e)', fontSize: 12 }}>
            AI will design a custom app for "{text}"
          </span>
        </div>
      )}

      {/* Generating — loading state */}
      {generating && (
        <div style={{
          display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 12, padding: '24px 0',
          color: 'var(--accent, #58a6ff)', fontSize: 16, fontWeight: 600,
        }}>
          <div style={{
            width: 32, height: 32, border: '3px solid var(--border, #30363d)',
            borderTop: '3px solid var(--accent, #58a6ff)', borderRadius: '50%',
            animation: 'spin 1s linear infinite',
          }} />
          <LoadingDots />
          <span style={{ color: 'var(--text-muted, #8b949e)', fontSize: 12, fontWeight: 400 }}>
            This may take up to two minutes
          </span>
          <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
        </div>
      )}

      {/* Generation success — preview overlay */}
      {previewManifest && previewManifestId && (
        <AppPreview
          manifest={previewManifest}
          explanation={previewExplanation}
          confidence={previewConfidence}
          onAccept={() => {
            if (previewManifestId) {
              emitPraxisOpenTab({ kind: 'manifest', manifestId: previewManifestId, tabId: 'main' });
            }
          }}
          onRegenerate={() => {
            setPreviewManifest(null);
            setPreviewManifestId(null);
            setGenerateResult(null);
            handleGenerate();
          }}
          onEdit={() => {
            if (previewManifestId) {
              emitPraxisOpenTab({ kind: 'manifest-editor', manifestId: previewManifestId });
            }
          }}
          onClose={() => {
            setPreviewManifest(null);
            setPreviewManifestId(null);
          }}
        />
      )}

      {/* Generation error */}
      {generateError && (
        <div style={{
          display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, padding: '16px 0',
        }}>
          <span style={{ color: '#f85149', fontSize: 14 }}>{generateError}</span>
          <button
            onClick={handleGenerate}
            style={{
              backgroundColor: 'transparent', color: 'var(--accent, #58a6ff)',
              border: '1px solid var(--accent, #58a6ff)', borderRadius: 'var(--radius, 8px)',
              padding: '8px 20px', fontSize: 14, fontWeight: 600, cursor: 'pointer',
            }}
          >
            Try Again
          </button>
        </div>
      )}

      {loading && (
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
          gap: 8,
        }}>
          <LoadingSkeleton lines={4} height={16}  />
          <LoadingSkeleton lines={4} height={16}  />
          <LoadingSkeleton lines={4} height={16}  />
        </div>
      )}

      {!loading && templates.length > 0 && (
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
          gap: 8,
          overflowY: 'auto',
          maxHeight: 400,
          position: 'relative',
          zIndex: 10,
        }}>
          {templates.map(t => (
            <a
              key={t.id}
              href={`/?manifest=${t.id}`}
              style={{
                display: 'block', padding: '12px 16px',
                backgroundColor: 'var(--bg, #0d1117)', border: '1px solid var(--border, #30363d)',
                borderRadius: 'var(--radius, 8px)', textDecoration: 'none', color: 'var(--text, #c9d1d9)',
                cursor: 'pointer', transition: 'border-color 0.15s, background-color 0.15s',
              }}
              onClick={(event) => {
                event.preventDefault();
                emitPraxisOpenTab({ kind: 'manifest', manifestId: t.id, tabId: 'main' });
              }}
              onMouseEnter={e => {
                e.currentTarget.style.borderColor = 'var(--accent, #58a6ff)';
                e.currentTarget.style.backgroundColor = 'rgba(88, 166, 255, 0.06)';
              }}
              onMouseLeave={e => {
                e.currentTarget.style.borderColor = 'var(--border, #30363d)';
                e.currentTarget.style.backgroundColor = 'var(--bg, #0d1117)';
              }}
            >
              <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 4 }}>{t.name}</div>
              {t.description && (
                <div style={{
                  color: 'var(--text-muted, #8b949e)', fontSize: 12,
                  lineHeight: 1.4, overflow: 'hidden',
                  display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical',
                }}>
                  {t.description}
                </div>
              )}
            </a>
          ))}
        </div>
      )}
    </div>
  );
};

export default IntentBoxModule;
