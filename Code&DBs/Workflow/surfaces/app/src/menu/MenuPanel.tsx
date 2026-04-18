import React, { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import type { MenuAction, MenuSection } from './menuTypes';
import { filterMenuSections, flattenMenuSections } from './menuUtils';
import './MenuPanel.css';

interface MenuPanelProps {
  open: boolean;
  sections: MenuSection[];
  onClose: () => void;
  title?: string;
  subtitle?: string;
  emptyLabel?: string;
  searchPlaceholder?: string;
  searchable?: boolean;
  variant?: 'popover' | 'dialog';
  width?: number;
  anchorRect?: DOMRect | null;
}

interface PositionState {
  left: number;
  top: number;
}

const VIEWPORT_PAD = 12;

function clamp(value: number, min: number, max: number): number {
  if (Number.isNaN(value)) return min;
  return Math.min(Math.max(value, min), max);
}

export function MenuPanel({
  open,
  sections,
  onClose,
  title,
  subtitle,
  emptyLabel = 'No results',
  searchPlaceholder = 'Search…',
  searchable = true,
  variant = 'popover',
  width,
  anchorRect,
}: MenuPanelProps) {
  const panelRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState('');
  const [activeId, setActiveId] = useState<string | null>(null);
  const [position, setPosition] = useState<PositionState | null>(null);

  const filteredSections = useMemo(
    () => filterMenuSections(sections, query),
    [query, sections],
  );

  const flatItems = useMemo(
    () => flattenMenuSections(filteredSections).filter((item) => !item.disabled),
    [filteredSections],
  );

  useEffect(() => {
    if (!open) {
      setQuery('');
      setActiveId(null);
      setPosition(null);
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const nextActive = flatItems[0]?.id ?? null;
    setActiveId((current) => (current && flatItems.some((item) => item.id === current) ? current : nextActive));
  }, [flatItems, open]);

  useEffect(() => {
    if (!open || !activeId) return;
    const activeButton = panelRef.current?.querySelector(
      `[data-menu-item-id="${CSS.escape(activeId)}"]`,
    ) as HTMLButtonElement | null;
    if (!activeButton) return;

    if (!searchable) {
      activeButton.focus({ preventScroll: true });
    }

    if (typeof activeButton.scrollIntoView === 'function') {
      activeButton.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }
  }, [activeId, open, searchable]);

  useEffect(() => {
    if (!open) return;
    if (searchable) {
      const timer = window.setTimeout(() => searchRef.current?.focus(), 0);
      return () => window.clearTimeout(timer);
    }
    return undefined;
  }, [open, searchable]);

  useEffect(() => {
    if (!open) return;

    const handlePointerDown = (event: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(event.target as Node)) {
        onClose();
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        onClose();
        return;
      }

      if (!flatItems.length) return;

      const currentIndex = flatItems.findIndex((item) => item.id === activeId);
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        const nextIndex = currentIndex < 0 ? 0 : (currentIndex + 1) % flatItems.length;
        setActiveId(flatItems[nextIndex]?.id ?? null);
      } else if (event.key === 'ArrowUp') {
        event.preventDefault();
        const nextIndex = currentIndex < 0 ? flatItems.length - 1 : (currentIndex - 1 + flatItems.length) % flatItems.length;
        setActiveId(flatItems[nextIndex]?.id ?? null);
      } else if (event.key === 'Enter' && activeId) {
        const action = flatItems.find((item) => item.id === activeId);
        if (!action) return;
        event.preventDefault();
        action.onSelect();
        if (!action.keepOpen) onClose();
      }
    };

    document.addEventListener('mousedown', handlePointerDown);
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [activeId, flatItems, onClose, open]);

  useLayoutEffect(() => {
    if (!open || !panelRef.current) return;

    const rect = panelRef.current.getBoundingClientRect();
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;

    if (variant === 'dialog' || !anchorRect) {
      const centeredLeft = Math.round((viewportWidth - rect.width) / 2);
      const centeredTop = Math.round((viewportHeight - rect.height) / 2);
      setPosition({
        left: clamp(centeredLeft, VIEWPORT_PAD, viewportWidth - rect.width - VIEWPORT_PAD),
        top: clamp(centeredTop, VIEWPORT_PAD, viewportHeight - rect.height - VIEWPORT_PAD),
      });
      return;
    }

    const desiredLeft = anchorRect.left + anchorRect.width - rect.width;
    const desiredTop = anchorRect.bottom + 8;
    const nextLeft = clamp(desiredLeft, VIEWPORT_PAD, viewportWidth - rect.width - VIEWPORT_PAD);
    const nextTop = clamp(desiredTop, VIEWPORT_PAD, viewportHeight - rect.height - VIEWPORT_PAD);
    const fallbackAbove = anchorRect.top - rect.height - 8;

    setPosition({
      left: nextLeft,
      top: nextTop < desiredTop && fallbackAbove >= VIEWPORT_PAD ? fallbackAbove : nextTop,
    });
  }, [anchorRect, filteredSections, open, query, variant, width]);

  if (!open) return null;

  const body = (
    <>
      {variant === 'dialog' && <div className="menu-panel-overlay" onClick={onClose} />}
      <div
        ref={panelRef}
        className={`menu-panel menu-panel--${variant}`}
        style={{
          width: width ? Math.min(width, window.innerWidth - VIEWPORT_PAD * 2) : undefined,
          left: position?.left ?? VIEWPORT_PAD,
          top: position?.top ?? VIEWPORT_PAD,
        }}
        role="dialog"
        aria-modal={variant === 'dialog' ? 'true' : undefined}
        aria-label={title || 'Menu'}
      >
        {(title || subtitle) && (
          <div className="menu-panel__header">
            {title && <div className="menu-panel__title">{title}</div>}
            {subtitle && <div className="menu-panel__subtitle">{subtitle}</div>}
          </div>
        )}

        {searchable && (
          <input
            ref={searchRef}
            className="menu-panel__search"
            type="text"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={searchPlaceholder}
          />
        )}

        <div className="menu-panel__body">
          {filteredSections.length === 0 ? (
            <div className="menu-panel__empty">{emptyLabel}</div>
          ) : (
            filteredSections.map((section) => (
              <div key={section.id} className="menu-panel__section">
                {section.title && <div className="menu-panel__section-title">{section.title}</div>}
                <div className="menu-panel__list">
                  {section.items.map((item) => {
                    const isActive = item.id === activeId;
                    return (
                      <button
                        key={item.id}
                        type="button"
                        data-menu-item-id={item.id}
                        aria-selected={isActive}
                        className={[
                          'menu-panel__item',
                          isActive ? 'menu-panel__item--active' : '',
                          item.tone === 'danger' ? 'menu-panel__item--danger' : '',
                        ].filter(Boolean).join(' ')}
                        disabled={item.disabled}
                        onFocus={() => setActiveId(item.id)}
                        onMouseEnter={() => setActiveId(item.id)}
                        onPointerDown={item.onPointerDown}
                        onClick={() => {
                          item.onSelect();
                          if (!item.keepOpen) onClose();
                        }}
                      >
                        {item.icon && <span className="menu-panel__icon">{item.icon}</span>}
                        <span className="menu-panel__content">
                          <span className="menu-panel__label-row">
                            <span className="menu-panel__label">{item.label}</span>
                            {item.selected && <span className="menu-panel__check">Current</span>}
                          </span>
                          {item.description && <span className="menu-panel__description">{item.description}</span>}
                        </span>
                        <span className="menu-panel__meta">{item.shortcut || item.meta}</span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </>
  );

  return createPortal(body, document.body);
}
