import React, { useEffect } from 'react';

export interface Tab {
  /** Stable identifier — passed back via onChange */
  value: string;
  /** Display label */
  label: string;
  /** Single-letter accelerator. Bound globally as Shift+letter when bindKeyboard is true. */
  kbd: string;
}

export interface TabstripProps {
  tabs: Tab[];
  /** Currently active tab value (controlled). If omitted, uncontrolled with defaultValue. */
  value?: string;
  defaultValue?: string;
  onChange?: (value: string, tab: Tab) => void;
  /** Bind Shift+kbd globally (default true). Skipped when focus is in INPUT/TEXTAREA. */
  bindKeyboard?: boolean;
  /** Optional aria-label for the tablist */
  ariaLabel?: string;
}

/**
 * Tabstrip — kbd-prefix tab bar with optional global keyboard accelerators.
 *
 * Renders the prx-tabstrip CSS structure. Each tab's `kbd` letter binds
 * Shift+letter to activate it, unless focus is in a form input. Pass
 * bindKeyboard={false} to disable.
 */
export function Tabstrip({
  tabs,
  value: controlledValue,
  defaultValue,
  onChange,
  bindKeyboard = true,
  ariaLabel,
}: TabstripProps) {
  const isControlled = controlledValue !== undefined;
  const [internal, setInternal] = React.useState<string>(
    defaultValue ?? tabs[0]?.value ?? '',
  );
  const active = isControlled ? (controlledValue as string) : internal;

  function activate(tab: Tab) {
    if (!isControlled) setInternal(tab.value);
    onChange?.(tab.value, tab);
  }

  useEffect(() => {
    if (!bindKeyboard) return;
    function onKey(e: KeyboardEvent) {
      if (!e.shiftKey || e.metaKey || e.ctrlKey || e.altKey) return;
      const target = e.target as HTMLElement | null;
      const tag = target?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || target?.isContentEditable) return;
      const k = e.key?.toLowerCase();
      const match = tabs.find((t) => t.kbd.toLowerCase() === k);
      if (match) {
        e.preventDefault();
        activate(match);
      }
    }
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tabs, bindKeyboard, isControlled]);

  function moveFocus(currentIndex: number, delta: number) {
    if (tabs.length === 0) return;
    const next = (currentIndex + delta + tabs.length) % tabs.length;
    activate(tabs[next]);
    // focus the new active tab
    requestAnimationFrame(() => {
      const el = document.querySelector<HTMLElement>(
        `[data-testid="prx-tab-${tabs[next].value}"]`,
      );
      el?.focus();
    });
  }

  return (
    <div className="prx-tabstrip" role="tablist" aria-label={ariaLabel ?? 'Primary tabs'} data-testid="prx-tabstrip">
      {tabs.map((tab, index) => {
        const isActive = tab.value === active;
        return (
          <span
            key={tab.value}
            role="tab"
            aria-selected={isActive}
            tabIndex={isActive ? 0 : -1}
            className={'tab' + (isActive ? ' active' : '')}
            data-value={tab.value}
            data-kbd={tab.kbd}
            data-testid={`prx-tab-${tab.value}`}
            onClick={() => activate(tab)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                activate(tab);
              } else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
                e.preventDefault();
                moveFocus(index, +1);
              } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
                e.preventDefault();
                moveFocus(index, -1);
              } else if (e.key === 'Home') {
                e.preventDefault();
                moveFocus(-1, +1);
              } else if (e.key === 'End') {
                e.preventDefault();
                moveFocus(tabs.length, -1);
              }
            }}
          >
            <span className="kbd" aria-hidden="true">{tab.kbd.toUpperCase()}</span>
            {tab.label}
          </span>
        );
      })}
    </div>
  );
}
