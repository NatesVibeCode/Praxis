import React, { useEffect, useRef, useState } from 'react';

type ToastType = 'success' | 'error' | 'info';

interface ToastOptions {
  actionLabel?: string;
  durationMs?: number;
  onAction?: () => void;
}

interface ToastItem {
  id: number;
  message: string;
  type: ToastType;
  actionLabel?: string;
  durationMs?: number;
  onAction?: () => void;
}

let _id = 0;
const listeners = new Set<(item: ToastItem) => void>();

function emit(item: ToastItem) {
  listeners.forEach((l) => l(item));
}

export function useToast() {
  return {
    show(message: string, type: ToastType = 'info', options: ToastOptions = {}) {
      emit({ id: ++_id, message, type, ...options });
    },
  };
}

export function Toast() {
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const timeoutById = useRef(new Map<number, ReturnType<typeof setTimeout>>());

  const getToastKey = (item: ToastItem): string => `${item.type}|${item.message}|${item.actionLabel ?? ''}`;

  const removeTimeoutForToast = (id: number) => {
    const timeoutId = timeoutById.current.get(id);
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
      timeoutById.current.delete(id);
    }
  };

  const dismissToast = (id: number) => {
    removeTimeoutForToast(id);
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  useEffect(() => {
    const handler = (item: ToastItem) => {
      const newToastKey = getToastKey(item);
      let replacedId: number | undefined;

      setToasts((prev) => {
        const duplicateIdx = prev.findIndex((toast) => getToastKey(toast) === newToastKey);
        if (duplicateIdx !== -1) {
          replacedId = prev[duplicateIdx].id;
          const next = [...prev];
          next[duplicateIdx] = item;
          return next;
        }
        return [...prev, item];
      });

      if (replacedId !== undefined) {
        removeTimeoutForToast(replacedId);
      }

      const timeoutMs = item.durationMs ?? (item.actionLabel ? 5000 : 3000);
      timeoutById.current.set(
        item.id,
        setTimeout(() => {
          dismissToast(item.id);
        }, timeoutMs),
      );
    };

    listeners.add(handler);
    return () => {
      listeners.delete(handler);
      timeoutById.current.forEach((timeoutId) => {
        clearTimeout(timeoutId);
      });
      timeoutById.current.clear();
    };
  }, []);

  if (toasts.length === 0) return null;

  return (
    <div className="app-toast-stack" aria-live="polite" aria-atomic="true">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`app-toast app-toast--${t.type}`}
          role={t.type === 'error' ? 'alert' : 'status'}
          aria-atomic="true"
        >
          <div className="app-toast__row">
            <span className="app-toast__message">{t.message}</span>
            <div className="app-toast__actions">
              {t.actionLabel && t.onAction ? (
                <button
                  type="button"
                  onClick={() => {
                    t.onAction?.();
                    dismissToast(t.id);
                  }}
                  className="app-toast__action"
                >
                  {t.actionLabel}
                </button>
              ) : null}
              <button
                type="button"
                className="app-toast__dismiss"
                aria-label="Dismiss toast"
                onClick={() => dismissToast(t.id)}
              >
                ×
              </button>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
