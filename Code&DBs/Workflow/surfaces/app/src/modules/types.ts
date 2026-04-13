import React from 'react';

export interface QuadrantProps {
  quadrantId: string;
  span: { cols: number; rows: number };
  config?: Record<string, unknown>;
}

export interface ModuleDefinition {
  id: string;
  name: string;
  type: 'display' | 'input' | 'tool' | 'composite';
  defaultSpan: string;
  component: React.ComponentType<QuadrantProps> | React.LazyExoticComponent<React.ComponentType<QuadrantProps>>;
  description: string;
}
