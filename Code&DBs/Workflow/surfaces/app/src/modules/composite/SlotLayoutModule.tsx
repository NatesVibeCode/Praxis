import React from 'react';
import { QuadrantProps } from '../types';
import { SlotLayout, ManifestLayout } from '../../primitives/SlotLayout';

function SlotLayoutModule({ config }: QuadrantProps) {
  const layout = (config?.layout ?? { grid: [], gridColumns: 2 }) as ManifestLayout;

  return (
    <div style={{
      padding: 'var(--space-md, 16px)', width: '100%', height: '100%',
      boxSizing: 'border-box', overflowY: 'auto',
    }}>
      <SlotLayout layout={layout} />
    </div>
  );
}

export default SlotLayoutModule;
