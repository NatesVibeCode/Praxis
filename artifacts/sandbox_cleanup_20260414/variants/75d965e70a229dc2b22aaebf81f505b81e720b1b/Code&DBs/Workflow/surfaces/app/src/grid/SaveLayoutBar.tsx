import React, { useState } from 'react';
import { emitPraxisOpenTab } from '../praxis/events';
import type { PraxisSurfaceBundleV4 } from '../praxis/manifest';
import { world } from '../world';
import { useSlice } from '../hooks/useSlice';
import { QuadrantManifest } from './QuadrantGrid';
import { useToast } from '../primitives/Toast';

interface SaveLayoutTarget {
  manifestId: string;
  name?: string | null;
  description?: string | null;
  bundle?: PraxisSurfaceBundleV4 | null;
  surfaceId?: string | null;
}

export function SaveLayoutBar({
  manifest,
  saveTarget,
}: {
  manifest: QuadrantManifest;
  saveTarget?: SaveLayoutTarget | null;
}) {
  const overrides = useSlice(world, 'ui.layout.quadrants') as Record<string, any> | null;
  const { show } = useToast();
  const [isSaving, setIsSaving] = useState(false);
  const [isSaveAs, setIsSaveAs] = useState(false);
  const [saveAsName, setSaveAsName] = useState('');

  // Only visible when there are unsaved changes (World ui.layout.quadrants has data)
  if (!overrides || Object.keys(overrides).length === 0) {
    return null;
  }

  if (!saveTarget?.manifestId) {
    return null;
  }

  const getMergedPayload = () => {
    function deepMerge(target: any, source: any) {
      if (typeof target !== 'object' || target === null) return source;
      if (typeof source !== 'object' || source === null) return source;
      const output = { ...target };
      Object.keys(source).forEach(key => {
        if (typeof source[key] === 'object' && source[key] !== null) {
          if (!(key in target)) {
            Object.assign(output, { [key]: source[key] });
          } else {
            output[key] = deepMerge(target[key], source[key]);
          }
        } else {
          Object.assign(output, { [key]: source[key] });
        }
      });
      return output;
    }

    const mergedQuadrants = deepMerge(manifest.quadrants, overrides);
    const mergedManifest = { ...manifest, quadrants: mergedQuadrants };

    if (saveTarget.bundle && saveTarget.surfaceId && saveTarget.bundle.surfaces[saveTarget.surfaceId]) {
      return {
        id: saveTarget.manifestId,
        name: saveTarget.name ?? saveTarget.bundle.name ?? saveTarget.bundle.title ?? saveTarget.manifestId,
        description: saveTarget.description ?? saveTarget.bundle.description ?? '',
        manifest: {
          ...saveTarget.bundle,
          surfaces: {
            ...saveTarget.bundle.surfaces,
            [saveTarget.surfaceId]: {
              ...saveTarget.bundle.surfaces[saveTarget.surfaceId],
              manifest: mergedManifest,
            },
          },
        },
      };
    }

    return {
      id: saveTarget.manifestId,
      name: saveTarget.name ?? manifest.title ?? saveTarget.manifestId,
      description: saveTarget.description ?? '',
      manifest: mergedManifest,
    };
  };

  const handleSave = async () => {
    setIsSaving(true);
    try {
      const merged = getMergedPayload();
      const res = await fetch('/api/manifests/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(merged)
      });
      if (!res.ok) throw new Error('Failed to save manifest');

      // Commit proposed changes into world state so UI doesn't revert
      const savedQuadrants = world.get('ui.layout.quadrants');
      world.clearProposed();
      if (savedQuadrants && typeof savedQuadrants === 'object') {
        world.set('ui.layout.quadrants', savedQuadrants);
      }
      show('Layout saved', 'success');
    } catch (err: any) {
      show(err.message || 'Error saving layout', 'error');
    } finally {
      setIsSaving(false);
    }
  };

  const handleSaveAs = async () => {
    if (!saveAsName.trim()) {
      show('Please enter a name', 'error');
      return;
    }
    setIsSaving(true);
    try {
      const merged = getMergedPayload();
      const res = await fetch('/api/manifests/save-as', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: saveAsName,
          description: merged.description ?? '',
          manifest: merged.manifest,
        })
      });
      if (!res.ok) throw new Error('Failed to save manifest as ' + saveAsName);

      const payload = await res.json().catch(() => null);
      const savedQuadrants = world.get('ui.layout.quadrants');
      world.clearProposed();
      if (savedQuadrants && typeof savedQuadrants === 'object') {
        world.set('ui.layout.quadrants', savedQuadrants);
      }
      show('Saved as ' + saveAsName, 'success');
      if (payload?.id) {
        emitPraxisOpenTab({ kind: 'manifest', manifestId: payload.id, tabId: 'main' });
      }
    } catch (err: any) {
      show(err.message || 'Error saving layout', 'error');
    } finally {
      setIsSaving(false);
      setIsSaveAs(false);
      setSaveAsName('');
    }
  };

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: 'var(--space-sm, 8px)',
      padding: 'var(--space-md, 16px)',
      backgroundColor: 'var(--surface-sunken, #f9fafb)',
      border: '1px solid var(--border-color, #e5e7eb)',
      borderRadius: '8px',
      marginBottom: 'var(--space-lg, 24px)'
    }}>
      <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-color, #111827)' }}>
        Unsaved Layout Changes
      </span>
      <div style={{ flex: 1 }} />
      
      {!isSaveAs ? (
        <>
          <button 
            onClick={() => setIsSaveAs(true)}
            disabled={isSaving}
            style={{ 
              padding: '6px 12px', cursor: 'pointer', background: 'transparent', 
              border: '1px solid var(--border-color, #d1d5db)', borderRadius: '4px',
              fontSize: 14
            }}
          >
            Save As
          </button>
          <button 
            onClick={handleSave} 
            disabled={isSaving}
            style={{ 
              padding: '6px 12px', cursor: 'pointer', background: 'var(--primary, #3b82f6)', 
              color: 'white', border: 'none', borderRadius: '4px', fontSize: 14
            }}
          >
            {isSaving ? 'Saving...' : 'Save'}
          </button>
        </>
      ) : (
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <input 
            type="text" 
            placeholder="New manifest name" 
            value={saveAsName}
            onChange={e => setSaveAsName(e.target.value)}
            disabled={isSaving}
            style={{ 
              padding: '6px 8px', borderRadius: '4px', border: '1px solid var(--border-color, #d1d5db)',
              fontSize: 14
            }}
            autoFocus
          />
          <button 
            onClick={handleSaveAs}
            disabled={isSaving}
            style={{ 
              padding: '6px 12px', cursor: 'pointer', background: 'var(--primary, #3b82f6)', 
              color: 'white', border: 'none', borderRadius: '4px', fontSize: 14
            }}
          >
            Confirm
          </button>
          <button 
            onClick={() => setIsSaveAs(false)}
            disabled={isSaving}
            style={{ 
              padding: '6px 12px', cursor: 'pointer', background: 'transparent', 
              border: '1px solid var(--border-color, #d1d5db)', borderRadius: '4px', fontSize: 14
            }}
          >
            Cancel
          </button>
        </div>
      )}
    </div>
  );
}
