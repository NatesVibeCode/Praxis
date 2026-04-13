import React, { useState, DragEvent } from 'react';
import { QuadrantProps } from '../types';
import { world } from '../../world';

interface FileDropConfig {
  accept?: string;
  worldPath?: string;
  label?: string;
}

export const FileDropModule: React.FC<QuadrantProps> = ({ config: rawConfig }) => {
  const config = (rawConfig || {}) as FileDropConfig;
  const [isHovered, setIsHovered] = useState(false);
  const [filename, setFilename] = useState<string | null>(null);

  const { accept = '.txt', worldPath, label = 'Drag and drop a file here' } = config;

  const handleDragOver = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsHovered(true);
  };

  const handleDragLeave = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsHovered(false);
  };

  const handleDrop = async (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsHovered(false);

    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      const file = e.dataTransfer.files[0];
      setFilename(file.name);

      try {
        const text = await file.text();
        if (worldPath) {
          world.set(worldPath, text);
        }
      } catch (err) {
        console.error('File read error:', err);
      }
    }
  };

  return (
    <div style={{ padding: 'var(--space-md, 16px)', width: '100%', height: '100%', boxSizing: 'border-box', display: 'flex', flexDirection: 'column' }}>
      <div
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          border: `2px dashed ${isHovered ? 'var(--accent, #58a6ff)' : 'var(--border, #30363d)'}`,
          borderRadius: 'var(--radius, 8px)',
          backgroundColor: isHovered ? 'rgba(88, 166, 255, 0.1)' : 'var(--bg-card, #161b22)',
          color: 'var(--text-muted, #8b949e)',
          cursor: 'pointer',
          transition: 'all 0.2s ease',
          fontFamily: 'var(--font-sans, sans-serif)',
          padding: '24px'
        }}
      >
        {filename ? (
          <span style={{ color: 'var(--success, #3fb950)', fontWeight: 'bold' }}>{filename}</span>
        ) : (
          <span>{label}</span>
        )}
        <span style={{ fontSize: '12px', marginTop: '8px' }}>Accepts {accept}</span>
      </div>
    </div>
  );
};
