import React, { useState, useEffect, useRef, useCallback } from 'react';
import { normalizePraxisBundle, resolvePraxisBundleSurface, type PraxisSurfaceBundleV4 } from '../praxis/manifest';
import { QuadrantGrid } from './QuadrantGrid';
import './ManifestEditor.css';

interface ManifestEditorProps {
  manifestId: string;
}

export function ManifestEditor({ manifestId }: ManifestEditorProps) {
  const [jsonText, setJsonText] = useState('');
  const [parsedBundle, setParsedBundle] = useState<PraxisSurfaceBundleV4 | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [dividerX, setDividerX] = useState(50);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();
  const containerRef = useRef<HTMLDivElement>(null);
  const draggingRef = useRef(false);

  // Fetch manifest
  useEffect(() => {
    setLoading(true);
    fetch(`/api/manifests/${manifestId}`)
      .then(r => r.json())
      .then(data => {
        const text = JSON.stringify(data, null, 2);
        setJsonText(text);
        setParsedBundle(normalizePraxisBundle(data, {
          id: manifestId,
          title: typeof data?.name === 'string' ? data.name : undefined,
          description: typeof data?.description === 'string' ? data.description : undefined,
        }));
        setParseError(null);
      })
      .catch(err => {
        setParseError(`Failed to load: ${err.message}`);
      })
      .finally(() => setLoading(false));
  }, [manifestId]);

  // Debounced parse on keystroke
  const handleTextChange = useCallback((value: string) => {
    setJsonText(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      try {
        const obj = JSON.parse(value);
        setParsedBundle(normalizePraxisBundle(obj, {
          id: manifestId,
          title: typeof obj?.name === 'string' ? obj.name : undefined,
          description: typeof obj?.description === 'string' ? obj.description : undefined,
        }));
        setParseError(null);
      } catch (e: any) {
        setParseError(e.message);
      }
    }, 300);
  }, []);

  const handleSave = async () => {
    if (!parsedBundle) return;
    setSaving(true);
    try {
      await fetch('/api/manifests/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: manifestId,
          name: parsedBundle.name ?? parsedBundle.title ?? manifestId,
          description: parsedBundle.description ?? '',
          manifest: parsedBundle,
        }),
      });
    } catch (err: any) {
      setParseError(`Save failed: ${err.message}`);
    } finally {
      setSaving(false);
    }
  };

  const handleFormat = () => {
    try {
      const obj = JSON.parse(jsonText);
      const formatted = JSON.stringify(obj, null, 2);
      setJsonText(formatted);
      setParsedBundle(normalizePraxisBundle(obj, {
        id: manifestId,
        title: typeof obj?.name === 'string' ? obj.name : undefined,
        description: typeof obj?.description === 'string' ? obj.description : undefined,
      }));
      setParseError(null);
    } catch {
      // Can't format invalid JSON
    }
  };

  // Resizable divider
  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    draggingRef.current = true;

    const onMouseMove = (ev: MouseEvent) => {
      if (!draggingRef.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const pct = ((ev.clientX - rect.left) / rect.width) * 100;
      setDividerX(Math.min(80, Math.max(20, pct)));
    };

    const onMouseUp = () => {
      draggingRef.current = false;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, []);

  if (loading) {
    return <div className="manifest-editor-loading">Loading manifest...</div>;
  }

  const previewSurface = parsedBundle ? resolvePraxisBundleSurface(parsedBundle) : null;
  const previewQuadrantSurface = previewSurface?.kind === 'quadrant_manifest' ? previewSurface : null;

  return (
    <div className="manifest-editor" ref={containerRef}>
      {/* Left pane: JSON editor */}
      <div className="manifest-editor-left" style={{ width: `${dividerX}%` }}>
        <div className="manifest-editor-toolbar">
          <button
            className="manifest-editor-btn manifest-editor-btn-save"
            onClick={handleSave}
            disabled={!!parseError || saving}
          >
            {saving ? 'Saving...' : 'Save'}
          </button>
          <button
            className="manifest-editor-btn"
            onClick={handleFormat}
            disabled={!!parseError}
          >
            Format
          </button>
        </div>
        <textarea
          className="manifest-editor-textarea"
          value={jsonText}
          onChange={e => handleTextChange(e.target.value)}
          spellCheck={false}
        />
        {parseError && (
          <div className="manifest-editor-error">{parseError}</div>
        )}
      </div>

      {/* Resizable divider */}
      <div className="manifest-editor-divider" onMouseDown={handleDividerMouseDown} />

      {/* Right pane: live preview */}
      <div className="manifest-editor-right" style={{ width: `${100 - dividerX}%` }}>
        <div className="manifest-editor-preview-label">Preview</div>
        <div className="manifest-editor-preview-container">
          <div className="manifest-editor-preview-scale">
            {previewQuadrantSurface ? (
              <QuadrantGrid manifest={previewQuadrantSurface.manifest} />
            ) : (
              <div style={{ padding: 24, color: 'var(--text-muted)' }}>No preview surface available.</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
