import React, { useEffect, useState } from 'react';
import { listModules } from '../modules/moduleRegistry';
import { resolveModule } from '../modules/moduleRegistry';
import { listPresetsByCategory } from '../modules/presets';
import { useObjectTypes } from '../shared/hooks/useObjectTypes';
import type { GridDragPayload } from './useGridDrag';
import './ModulePalette.css';

interface ModulePaletteProps {
  isOpen: boolean;
  onClose: () => void;
  startDrag: (e: React.PointerEvent, payload: GridDragPayload) => void;
}

export function ModulePalette({ isOpen, onClose, startDrag }: ModulePaletteProps) {
  const [mounted, setMounted] = useState(false);
  const [showAllModules, setShowAllModules] = useState(false);
  const { objectTypes, loading: objLoading } = useObjectTypes();

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!isOpen) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [isOpen, onClose]);

  if (!mounted) return null;

  const presetGroups = listPresetsByCategory();
  const allModules = listModules();

  return (
    <>
      {isOpen && <div className="module-palette-overlay" />}
      <div className={`module-palette ${isOpen ? 'open' : ''}`}>
        <div className="module-palette-header">
          <h2>Add to Grid</h2>
          <button className="module-palette-close" onClick={onClose}>&times;</button>
        </div>
        <div className="module-palette-content">
          {/* Preset categories */}
          {presetGroups.map(group => (
            <div key={group.category} className="module-group">
              <h3>{group.label}</h3>
              {group.presets.map(preset => {
                const span = preset.span ?? resolveModule(preset.moduleId)?.defaultSpan ?? '1x1';
                return (
                  <div
                    key={preset.presetId}
                    className="preset-card"
                    onPointerDown={(e) => {
                      startDrag(e, {
                        kind: 'preset',
                        label: preset.name,
                        data: {
                          module: preset.moduleId,
                          span,
                          config: preset.config,
                          presetId: preset.presetId,
                        },
                      });
                    }}
                  >
                    {preset.icon && <span className="preset-card-icon">{preset.icon}</span>}
                    <div className="preset-card-body">
                      <div className="preset-card-name">{preset.name}</div>
                      <div className="preset-card-desc">{preset.description}</div>
                    </div>
                    <span className="preset-card-span">{span}</span>
                  </div>
                );
              })}
            </div>
          ))}

          {/* DB Object Types */}
          {!objLoading && objectTypes.length > 0 && (
            <div className="module-group">
              <h3>DB Objects</h3>
              {objectTypes.map(obj => (
                <div
                  key={obj.type_id}
                  className="object-type-card"
                  onPointerDown={(e) => {
                    startDrag(e, {
                      kind: 'object',
                      label: obj.name,
                      data: {
                        objectTypeId: obj.type_id,
                        objectName: obj.name,
                        module: 'object-view',
                        span: '1x1',
                      },
                    });
                  }}
                >
                  <span className="object-type-icon">{obj.icon || '📦'}</span>
                  <div className="object-type-info">
                    <div className="object-type-name">{obj.name}</div>
                    {obj.description && (
                      <div className="object-type-desc">{obj.description}</div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Collapsed all-modules section for advanced use */}
          <div className="module-group module-group-collapsed">
            <h3
              className="module-group-toggle"
              onClick={() => setShowAllModules(v => !v)}
            >
              All Modules {showAllModules ? '▾' : '▸'}
            </h3>
            {showAllModules && allModules.map(mod => (
              <div
                key={mod.id}
                className="module-card"
                onPointerDown={(e) => {
                  startDrag(e, {
                    kind: 'palette',
                    label: mod.name,
                    data: { module: mod.id, span: mod.defaultSpan },
                  });
                }}
              >
                <div className="module-card-header">
                  <span className="module-card-name">{mod.name}</span>
                  <span className="module-card-span">{mod.defaultSpan}</span>
                </div>
                <span className={`module-card-badge module-badge-${mod.type}`}>
                  {mod.type}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </>
  );
}
