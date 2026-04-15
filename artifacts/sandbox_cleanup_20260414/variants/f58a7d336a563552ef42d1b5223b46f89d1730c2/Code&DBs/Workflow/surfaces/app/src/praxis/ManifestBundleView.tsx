import React, { useEffect, useMemo, useState } from 'react';
import { QuadrantGrid } from '../grid/QuadrantGrid';
import { emitPraxisOpenTab } from './events';
import { normalizePraxisBundle, resolvePraxisBundleSurface, resolvePraxisBundleTab, type PraxisSurfaceBundleV4, type SourceOption } from './manifest';
import { SourceOptionPills } from './SourceOptionPills';

interface ManifestBundleViewProps {
  manifestId: string;
  tabId?: string | null;
}

interface SourceOptionPayload {
  source_options?: SourceOption[];
}

export function ManifestBundleView({ manifestId, tabId }: ManifestBundleViewProps) {
  const [bundle, setBundle] = useState<PraxisSurfaceBundleV4 | null>(null);
  const [sourceOptions, setSourceOptions] = useState<SourceOption[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    const load = async () => {
      try {
        const manifestResponse = await fetch(`/api/manifests/${manifestId}`);
        const manifestPayload = await manifestResponse.json().catch(() => null);
        if (!manifestResponse.ok) {
          throw new Error(manifestPayload?.error || `Failed to load manifest ${manifestId}`);
        }
        if (cancelled) return;
        const nextBundle = normalizePraxisBundle(manifestPayload, {
          id: manifestId,
          title: typeof manifestPayload?.name === 'string' ? manifestPayload.name : undefined,
          description: typeof manifestPayload?.description === 'string' ? manifestPayload.description : undefined,
        });
        setBundle(nextBundle);

        const selectedTab = resolvePraxisBundleTab(nextBundle, tabId);
        const params = new URLSearchParams({ manifest_id: manifestId, tab_id: selectedTab.id });
        const sourceResponse = await fetch(`/api/source-options?${params.toString()}`);
        const sourcePayload = await sourceResponse.json().catch(() => null) as SourceOptionPayload | null;
        if (!sourceResponse.ok) {
          throw new Error(sourcePayload && 'error' in sourcePayload ? String((sourcePayload as Record<string, unknown>).error) : 'Failed to load source options');
        }
        if (cancelled) return;
        setSourceOptions(Array.isArray(sourcePayload?.source_options) ? sourcePayload?.source_options ?? [] : []);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [manifestId, tabId]);

  const selectedTab = useMemo(() => (bundle ? resolvePraxisBundleTab(bundle, tabId) : null), [bundle, tabId]);
  const selectedSurface = useMemo(() => (bundle ? resolvePraxisBundleSurface(bundle, tabId) : null), [bundle, tabId]);

  if (loading) {
    return <div style={{ padding: 24, color: 'var(--text-muted)' }}>Loading manifest…</div>;
  }

  if (error || !bundle || !selectedTab || !selectedSurface) {
    return <div style={{ padding: 24, color: 'var(--danger)' }}>{error || 'Manifest unavailable'}</div>;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 12,
        padding: '16px 20px',
        borderBottom: '1px solid var(--border)',
        background: 'var(--bg)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
          <div style={{ fontSize: 18, fontWeight: 700 }}>{bundle.title}</div>
          <button
            type="button"
            onClick={() => emitPraxisOpenTab({ kind: 'manifest-editor', manifestId })}
            style={{
              padding: '6px 10px',
              borderRadius: 8,
              border: '1px solid var(--border)',
              background: 'var(--bg-card)',
              color: 'var(--text)',
              cursor: 'pointer',
              fontSize: 12,
              fontWeight: 600,
            }}
          >
            Edit JSON
          </button>
        </div>
        {bundle.tabs.length > 1 && (
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {bundle.tabs.map((entry) => (
              <button
                key={entry.id}
                type="button"
                onClick={() => emitPraxisOpenTab({ kind: 'manifest', manifestId, tabId: entry.id })}
                style={{
                  padding: '6px 10px',
                  borderRadius: 999,
                  border: entry.id === selectedTab.id ? '1px solid var(--accent)' : '1px solid var(--border)',
                  background: entry.id === selectedTab.id ? 'var(--surface-accent-soft)' : 'var(--bg-card)',
                  color: 'var(--text)',
                  cursor: 'pointer',
                  fontSize: 12,
                  fontWeight: 600,
                }}
              >
                {entry.label}
              </button>
            ))}
          </div>
        )}
        <SourceOptionPills options={sourceOptions} />
      </div>
      <div style={{ flex: 1, minHeight: 0, overflow: 'auto' }}>
        <QuadrantGrid
          manifest={selectedSurface.manifest}
          saveTarget={{
            manifestId,
            name: bundle.name ?? bundle.title,
            description: bundle.description,
            bundle,
            surfaceId: selectedSurface.id,
          }}
        />
      </div>
    </div>
  );
}
