import React, { useEffect, useState } from 'react';

type ToastType = 'success' | 'error' | 'info';

interface ToastItem {
  id: number;
  message: string;
  type: ToastType;
}

let _id = 0;
const listeners = new Set<(item: ToastItem) => void>();

function emit(item: ToastItem) {
  listeners.forEach((l) => l(item));
}

export function useToast() {
  return {
    show(message: string, type: ToastType = 'info') {
      emit({ id: ++_id, message, type });
    },
  };
}

export function Toast() {
  const [toasts, setToasts] = useState<ToastItem[]>([]);

  useEffect(() => {
    const handler = (item: ToastItem) => {
      setToasts((prev) => [...prev, item]);
      setTimeout(() => {
        setToasts((prev) => prev.filter((t) => t.id !== item.id));
      }, 3000);
    };
    listeners.add(handler);
    return () => {
      listeners.delete(handler);
    };
  }, []);

  if (toasts.length === 0) return null;

  return (
    <div style={{
      position: 'fixed', bottom: 24, right: 24,
      display: 'flex', flexDirection: 'column', gap: 8,
      zIndex: 9999,
    }}>
      {toasts.map((t) => (
        <div key={t.id} style={{
          padding: '10px 16px',
          borderRadius: 6,
          fontSize: 13,
          fontWeight: 500,
          color: '#fff',
          background: t.type === 'success' ? '#238636' : t.type === 'error' ? '#da3633' : '#1f6feb',
          boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
          minWidth: 200,
          maxWidth: 360,
        }}>
          {t.message}
        </div>
      ))}
    </div>
  );
}
