import React, { useMemo } from 'react';
import { QuadrantProps } from '../types';

function parseMarkdown(md: string): string {
  let html = md
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  // Code blocks
  html = html.replace(/```([\s\S]*?)```/g, (_m, code) =>
    `<pre style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:var(--space-md);overflow-x:auto;font-family:var(--font-mono);font-size:13px">${code.trim()}</pre>`
  );

  // Inline code
  html = html.replace(/`([^`]+)`/g,
    '<code style="background:var(--bg);padding:2px 6px;border-radius:4px;font-family:var(--font-mono);font-size:13px">$1</code>'
  );

  // Headers
  html = html.replace(/^### (.+)$/gm, '<h3 style="font-size:16px;margin:var(--space-md) 0 var(--space-sm)">$1</h3>');
  html = html.replace(/^## (.+)$/gm, '<h2 style="font-size:18px;margin:var(--space-md) 0 var(--space-sm)">$1</h2>');
  html = html.replace(/^# (.+)$/gm, '<h1 style="font-size:22px;margin:var(--space-md) 0 var(--space-sm)">$1</h1>');

  // Bold and italic
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

  // Unordered lists
  html = html.replace(/^- (.+)$/gm, '<li style="margin-left:var(--space-lg);list-style:disc">$1</li>');

  // Line breaks
  html = html.replace(/\n\n/g, '<br/><br/>');

  return html;
}

function MarkdownModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as { content?: string };
  const content = cfg.content ?? '';

  const rendered = useMemo(() => parseMarkdown(content), [content]);

  if (!content) {
    return (
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
        color: 'var(--text-muted)', textAlign: 'center',
      }}>
        No content
      </div>
    );
  }

  return (
    <div
      style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius)',
        padding: 'var(--space-lg)',
        lineHeight: 1.6,
      }}
      dangerouslySetInnerHTML={{ __html: rendered }}
    />
  );
}

export default MarkdownModule;
