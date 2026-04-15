import React from 'react';
import { QuadrantGrid, QuadrantManifest } from './QuadrantGrid';
import './AppPreview.css';

interface AppPreviewProps {
  manifest: QuadrantManifest;
  explanation: string;
  confidence: number;
  onAccept: () => void;
  onRegenerate: () => void;
  onEdit: () => void;
  onClose: () => void;
}

function confidenceLabel(confidence: number): { text: string; level: string } {
  if (confidence >= 0.8) return { text: `${Math.round(confidence * 100)}% confident`, level: 'high' };
  if (confidence >= 0.5) return { text: `${Math.round(confidence * 100)}% confident`, level: 'medium' };
  return { text: `${Math.round(confidence * 100)}% confident`, level: 'low' };
}

export function AppPreview({
  manifest,
  explanation,
  confidence,
  onAccept,
  onRegenerate,
  onEdit,
  onClose,
}: AppPreviewProps) {
  const badge = confidenceLabel(confidence);

  const handleOverlayClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) onClose();
  };

  return (
    <div className="app-preview-overlay" onClick={handleOverlayClick}>
      <div className="app-preview-card">
        <div className="app-preview-header">
          <h2>Preview: Generated App</h2>
          <span className={`app-preview-confidence ${badge.level}`}>{badge.text}</span>
          <button className="app-preview-close" onClick={onClose} title="Close">&times;</button>
        </div>

        <div className="app-preview-explanation">{explanation}</div>

        <div className="app-preview-grid-container">
          <div className="app-preview-grid-scaler">
            <QuadrantGrid manifest={manifest} />
          </div>
        </div>

        <div className="app-preview-actions">
          <button className="app-preview-btn primary" onClick={onAccept}>
            Open Tab
          </button>
          <button className="app-preview-btn default" onClick={onRegenerate}>
            Regenerate
          </button>
          <button className="app-preview-btn subtle" onClick={onEdit}>
            Edit JSON
          </button>
        </div>
      </div>
    </div>
  );
}

export default AppPreview;
