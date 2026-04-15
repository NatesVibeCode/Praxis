import React from 'react';

export interface MenuAction {
  id: string;
  label: string;
  description?: string;
  keywords?: string[];
  icon?: React.ReactNode;
  meta?: string;
  shortcut?: string;
  disabled?: boolean;
  selected?: boolean;
  tone?: 'default' | 'danger';
  keepOpen?: boolean;
  onPointerDown?: (event: React.PointerEvent<HTMLButtonElement>) => void;
  onSelect: () => void;
}

export interface MenuSection {
  id: string;
  title?: string;
  items: MenuAction[];
}
