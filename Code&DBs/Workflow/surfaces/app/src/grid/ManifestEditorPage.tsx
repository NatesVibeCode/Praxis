import React from 'react';
import { APP_CONFIG } from '../config';
import { emitPraxisOpenTab } from '../praxis/events';
import { ManifestEditor } from './ManifestEditor';

interface ManifestEditorPageProps {
  manifestId: string;
}

export function ManifestEditorPage({ manifestId }: ManifestEditorPageProps) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
      {/* Nav */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 12, padding: '0 20px', height: 42,
        borderBottom: '1px solid var(--border)', background: 'var(--bg)', flexShrink: 0,
      }}>
        <a href={`/?manifest=${manifestId}`} onClick={(e) => {
          e.preventDefault();
          emitPraxisOpenTab({ kind: 'manifest', manifestId, tabId: 'main' });
        }}
          style={{ color: 'var(--text-muted)', textDecoration: 'none', fontWeight: 600, fontSize: 13, padding: '4px 8px', borderRadius: 4 }}>
          {APP_CONFIG.name}
        </a>
        <span style={{ color: 'var(--text)', fontSize: 13, fontWeight: 600, padding: '4px 10px', borderRadius: 6, background: 'rgba(63,185,80,0.1)', border: '1px solid rgba(63,185,80,0.2)' }}>
          Manifest Editor
        </span>
        <span style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--font-mono, monospace)' }}>
          {manifestId}
        </span>
      </div>

      {/* Editor body */}
      <div style={{ flex: 1, overflow: 'hidden', position: 'relative' }}>
        <ManifestEditor manifestId={manifestId} />
      </div>
    </div>
  );
}
