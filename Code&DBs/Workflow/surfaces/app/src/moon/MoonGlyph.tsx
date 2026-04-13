import React from 'react';
import type { GlyphType } from './moonBuildPresenter';

interface MoonGlyphProps {
  type: GlyphType;
  size?: number;
  color?: string;
}

// All paths are 24x24 viewBox, stroke-based
const paths: Record<GlyphType, string> = {
  step:     'M8 5v14l11-7z',
  gate:     'M12 2L3 7v9l9 5 9-5V7l-9-5zm0 2.2L18.5 8 12 11.8 5.5 8 12 4.2z',
  state:    'M4 6h16v2H4zm0 4h16v4H4zm0 6h16v2H4z',
  trigger:  'M7 2v11h3v9l7-12h-4l4-8z',
  human:    'M12 4a4 4 0 110 8 4 4 0 010-8zm0 10c-4.42 0-8 1.79-8 4v2h16v-2c0-2.21-3.58-4-8-4z',
  binding:  'M3.9 12c0-1.71 1.39-3.1 3.1-3.1h4V7H7a5 5 0 000 10h4v-1.9H7c-1.71 0-3.1-1.39-3.1-3.1zM8 13h8v-2H8v2zm9-6h-4v1.9h4c1.71 0 3.1 1.39 3.1 3.1s-1.39 3.1-3.1 3.1h-4V17h4a5 5 0 000-10z',
  research: 'M15.5 14h-.79l-.28-.27A6.47 6.47 0 0016 9.5 6.5 6.5 0 109.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 5L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z',
  classify: 'M5.5 7A1.5 1.5 0 017 5.5h10A1.5 1.5 0 0118.5 7v1A1.5 1.5 0 0117 9.5H7A1.5 1.5 0 015.5 8V7zm0 5A1.5 1.5 0 017 10.5h6a1.5 1.5 0 011.5 1.5v1A1.5 1.5 0 0113 14.5H7A1.5 1.5 0 015.5 13v-1zm0 5A1.5 1.5 0 017 15.5h3a1.5 1.5 0 011.5 1.5v1A1.5 1.5 0 0110 19.5H7A1.5 1.5 0 015.5 18v-1z',
  draft:    'M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04a1 1 0 000-1.41l-2.34-2.34a1 1 0 00-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z',
  notify:   'M12 22c1.1 0 2-.9 2-2h-4a2 2 0 002 2zm6-6v-5c0-3.07-1.63-5.64-4.5-6.32V4c0-.83-.67-1.5-1.5-1.5s-1.5.67-1.5 1.5v.68C7.64 5.36 6 7.92 6 11v5l-2 2v1h16v-1l-2-2z',
  review:   'M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z',
  tool:     'M22.7 19l-9.1-9.1c.9-2.3.4-5-1.5-6.9-2-2-5-2.4-7.4-1.3L9 6 6 9 1.6 4.7C.4 7.1.9 10.1 2.9 12.1c1.9 1.9 4.6 2.4 6.9 1.5l9.1 9.1c.4.4 1 .4 1.4 0l2.3-2.3c.5-.4.5-1.1.1-1.4z',
  blocked:  'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.42 0-8-3.58-8-8 0-1.85.63-3.55 1.69-4.9L16.9 18.31A7.9 7.9 0 0112 20zm6.31-3.1L7.1 5.69A7.9 7.9 0 0112 4c4.42 0 8 3.58 8 8 0 1.85-.63 3.55-1.69 4.9z',
};

export function MoonGlyph({ type, size = 24, color = 'currentColor' }: MoonGlyphProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d={paths[type] || paths.step} fill={color} fillOpacity={0.15} />
    </svg>
  );
}
