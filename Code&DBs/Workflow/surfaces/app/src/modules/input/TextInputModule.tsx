import React, { useState, KeyboardEvent } from 'react';
import { QuadrantProps } from '../types';
import { world } from '../../world';

interface TextInputConfig {
  placeholder?: string;
  worldPath?: string;
  onSubmitEndpoint?: string;
}

export const TextInputModule: React.FC<QuadrantProps> = ({ config: rawConfig }) => {
  const config = (rawConfig || {}) as TextInputConfig;
  const [value, setValue] = useState('');
  const { placeholder = 'Enter text...', worldPath, onSubmitEndpoint } = config;

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = e.target.value;
    setValue(newValue);
    if (worldPath) {
      world.set(worldPath, newValue);
    }
  };

  const handleKeyDown = async (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && onSubmitEndpoint) {
      try {
        await fetch(onSubmitEndpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ value })
        });
      } catch (err) {
        console.error('Submit error:', err);
      }
    }
  };

  return (
    <div style={{ padding: 'var(--space-md, 16px)', display: 'flex', flexDirection: 'column', width: '100%', height: '100%', boxSizing: 'border-box' }}>
      <input
        type="text"
        placeholder={placeholder}
        value={value}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        style={{
          width: '100%',
          backgroundColor: 'var(--bg, #0d1117)',
          color: 'var(--text, #c9d1d9)',
          border: '1px solid var(--border, #30363d)',
          borderRadius: 'var(--radius, 8px)',
          padding: '12px',
          fontSize: '14px',
          fontFamily: 'var(--font-sans, sans-serif)',
          boxSizing: 'border-box'
        }}
      />
    </div>
  );
};
