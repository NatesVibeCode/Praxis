import React, { useEffect, useRef } from 'react';
import './ContextMenu.css';

interface ContextMenuProps {
  x: number;
  y: number;
  moduleId: string;
  moduleType: string;
  quadrantId: string;
  onAction: (instruction: string) => void;
  onClose: () => void;
}

export function ContextMenu({
  x,
  y,
  moduleId,
  moduleType,
  quadrantId,
  onAction,
  onClose,
}: ContextMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        onClose();
      }
    };
    window.addEventListener('mousedown', handleClickOutside);
    return () => window.removeEventListener('mousedown', handleClickOutside);
  }, [onClose]);

  const getMenuItems = () => {
    switch (moduleType) {
      case 'display':
        return [
          { label: 'Change data source', instruction: `Change the data source for the module in quadrant ${quadrantId}` },
          { label: 'Add column', instruction: `Add a new column to the data table in quadrant ${quadrantId}` },
          { label: 'Change chart type', instruction: `Change the chart in quadrant ${quadrantId} to a pie chart` },
          { label: 'Remove module', instruction: `Remove the module from quadrant ${quadrantId}` },
        ];
      case 'input':
        return [
          { label: 'Change placeholder', instruction: `Change the placeholder for the input in quadrant ${quadrantId}` },
          { label: 'Change label', instruction: `Change the label for the input in quadrant ${quadrantId}` },
          { label: 'Remove module', instruction: `Remove the module from quadrant ${quadrantId}` },
        ];
      case 'tool':
        return [
          { label: 'Configure pipeline', instruction: `Configure the pipeline for the tool in quadrant ${quadrantId}` },
          { label: 'Change endpoint', instruction: `Change the endpoint for the tool in quadrant ${quadrantId}` },
          { label: 'Remove module', instruction: `Remove the module from quadrant ${quadrantId}` },
        ];
      case 'composite':
        return [
          { label: 'Change object type', instruction: `Change the object type for the composite module in quadrant ${quadrantId}` },
          { label: 'Toggle selection publishing', instruction: `Toggle selection publishing for the module in quadrant ${quadrantId}` },
          { label: 'Remove module', instruction: `Remove the module from quadrant ${quadrantId}` },
        ];
      default:
        return [
          { label: 'Remove module', instruction: `Remove the module from quadrant ${quadrantId}` },
        ];
    }
  };

  const items = getMenuItems();

  return (
    <div
      ref={menuRef}
      className="context-menu"
      style={{ left: x, top: y }}
    >
      {items.map((item, index) => (
        <button
          key={index}
          className="context-menu__item"
          onClick={(e) => {
            e.stopPropagation();
            onAction(item.instruction);
          }}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}
