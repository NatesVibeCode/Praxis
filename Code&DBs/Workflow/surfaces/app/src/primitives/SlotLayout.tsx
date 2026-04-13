import React, { Suspense } from 'react';
import { resolveModule } from '../modules/moduleRegistry';

export interface ManifestLayoutSlot {
  module: string;
  span?: string;
  config?: Record<string, unknown>;
}

export interface ManifestLayout {
  grid: ManifestLayoutSlot[];
  gridColumns?: number;
}

interface SlotLayoutProps {
  layout: ManifestLayout;
}

export function SlotLayout({ layout }: SlotLayoutProps) {
  const cols = layout.gridColumns ?? 2;
  const slots = layout.grid ?? [];

  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: `repeat(${cols}, 1fr)`,
      gap: 'var(--space-md, 12px)',
    }}>
      {slots.map((slot, i) => {
        const resolved = resolveModule(slot.module);
        if (!resolved) {
          return (
            <div key={i} style={{
              background: 'var(--bg-card, #161b22)',
              border: '1px solid var(--border, #30363d)',
              borderRadius: 'var(--radius, 6px)',
              padding: 'var(--space-md, 12px)',
              color: 'var(--text-muted, #8b949e)',
              fontSize: 12,
            }}>
              Unknown module: {slot.module}
            </div>
          );
        }
        const Component = resolved.component;
        const spanCols = slot.span ? parseInt(slot.span, 10) || 1 : 1;
        return (
          <div key={i} style={{ gridColumn: spanCols > 1 ? `span ${spanCols}` : undefined }}>
            <Suspense fallback={null}>
              <Component
                quadrantId={`slot-${i}`}
                span={{ cols: spanCols, rows: 1 }}
                config={slot.config}
              />
            </Suspense>
          </div>
        );
      })}
    </div>
  );
}
