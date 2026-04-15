import React, { useEffect, useState } from 'react';

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

  useEffect(() => {
    const handler = (item: ToastItem) => {
      setToasts((prev) => [...prev, item]);
      const timeoutMs = item.durationMs ?? (item.actionLabel ? 5000 : 3000);
      setTimeout(() => {
        setToasts((prev) => prev.filter((t) => t.id !== item.id));
      }, timeoutMs);
    };
    listeners.add(handler);
    return () => {
      listeners.delete(handler);
    };
  }, []);

  if (toasts.length === 0) return null;

  return (
    <div className="app-toast-stack" aria-live="polite" aria-atomic="true">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`app-toast app-toast--${t.type}`}
        >
          <div className="app-toast__row">
            <span className="app-toast__message">{t.message}</span>
            {t.actionLabel && t.onAction ? (
              <button
                type="button"
                onClick={() => {
                  t.onAction?.();
                  setToasts((prev) => prev.filter((item) => item.id !== t.id));
                }}
                className="app-toast__action"
              >
                {t.actionLabel}
              </button>
            ) : null}
          </div>
        </div>
      ))}
    </div>
  );
}
