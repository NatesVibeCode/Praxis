import React, { useState, useEffect } from 'react';
import { QuadrantProps } from '../types';
import { world } from '../../world';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

interface Option {
  label: string;
  value: string;
}

interface DropdownConfig {
  options?: Option[];
  endpoint?: string;
  label?: string;
  worldPath?: string;
}

export const DropdownSelectModule: React.FC<QuadrantProps> = ({ config: rawConfig }) => {
  const config = (rawConfig || {}) as DropdownConfig;
  const [options, setOptions] = useState<Option[]>(config.options || []);
  const [loading, setLoading] = useState(false);
  const [selectedValue, setSelectedValue] = useState('');

  const { endpoint, label = 'Select an option', worldPath } = config;

  useEffect(() => {
    if (!endpoint) {
      setOptions(config.options ?? []);
    }
  }, [config.options, endpoint]);

  useEffect(() => {
    if (endpoint) {
      setLoading(true);
      fetch(endpoint)
        .then(res => res.json())
        .then(data => {
          if (Array.isArray(data)) {
            const formatted = data.map(item => typeof item === 'string' ? { label: item, value: item } : item);
            setOptions(formatted);
          }
        })
        .catch(err => console.error('Fetch options error:', err))
        .finally(() => setLoading(false));
    }
  }, [endpoint]);

  const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = e.target.value;
    setSelectedValue(val);
    if (worldPath) {
      world.set(worldPath, val);
    }
  };

  return (
    <div style={{ padding: 'var(--space-md, 16px)', display: 'flex', flexDirection: 'column', width: '100%', height: '100%', boxSizing: 'border-box' }}>
      {label && <label style={{ marginBottom: '8px', color: 'var(--text, #c9d1d9)', fontSize: '14px', fontFamily: 'var(--font-sans, sans-serif)' }}>{label}</label>}
      {loading ? (
        <LoadingSkeleton lines={2} height={18} widths={['100%', '72%']} />
      ) : (
        <select
          value={selectedValue}
          onChange={handleChange}
          style={{
            width: '100%',
            backgroundColor: 'var(--bg, #0d1117)',
            color: 'var(--text, #c9d1d9)',
            border: '1px solid var(--border, #30363d)',
            borderRadius: 'var(--radius, 8px)',
            padding: '12px',
            fontSize: '14px',
            fontFamily: 'var(--font-sans, sans-serif)',
            cursor: 'pointer'
          }}
        >
          <option value="" disabled>Select...</option>
          {options.map((opt, i) => (
            <option key={i} value={opt.value}>{opt.label}</option>
          ))}
        </select>
      )}
    </div>
  );
};
