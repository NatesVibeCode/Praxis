import React from 'react';

/**
 * Simple markdown renderer — no external dependencies.
 * Supports: bold, italic, inline code, code blocks, links, lists, headings.
 */

function escapeHtml(text: string): string {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function renderInline(text: string): string {
  let result = escapeHtml(text);
  // Bold
  result = result.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  // Italic
  result = result.replace(/\*(.+?)\*/g, '<em>$1</em>');
  // Inline code
  result = result.replace(/`([^`]+)`/g, '<code class="ws-md-inline-code">$1</code>');
  // Links
  result = result.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener" class="ws-md-link">$1</a>');
  return result;
}

export function MarkdownRenderer({ content }: { content: string }) {
  if (!content) return null;

  const lines = content.split('\n');
  const html: string[] = [];
  let inCodeBlock = false;
  let codeBlockLang = '';
  let codeBlockLines: string[] = [];

  for (const line of lines) {
    // Code block start/end
    if (line.startsWith('```')) {
      if (inCodeBlock) {
        html.push(`<pre class="ws-md-code-block"><code>${escapeHtml(codeBlockLines.join('\n'))}</code></pre>`);
        codeBlockLines = [];
        inCodeBlock = false;
      } else {
        inCodeBlock = true;
        codeBlockLang = line.slice(3).trim();
      }
      continue;
    }

    if (inCodeBlock) {
      codeBlockLines.push(line);
      continue;
    }

    // Headings
    if (line.startsWith('### ')) {
      html.push(`<h4 class="ws-md-h4">${renderInline(line.slice(4))}</h4>`);
    } else if (line.startsWith('## ')) {
      html.push(`<h3 class="ws-md-h3">${renderInline(line.slice(3))}</h3>`);
    } else if (line.startsWith('# ')) {
      html.push(`<h2 class="ws-md-h2">${renderInline(line.slice(2))}</h2>`);
    }
    // Unordered list
    else if (line.match(/^[-*] /)) {
      html.push(`<li class="ws-md-li">${renderInline(line.slice(2))}</li>`);
    }
    // Ordered list
    else if (line.match(/^\d+\. /)) {
      html.push(`<li class="ws-md-li">${renderInline(line.replace(/^\d+\. /, ''))}</li>`);
    }
    // Horizontal rule
    else if (line.match(/^---+$/)) {
      html.push('<hr class="ws-md-hr" />');
    }
    // Empty line
    else if (line.trim() === '') {
      html.push('<br />');
    }
    // Regular paragraph
    else {
      html.push(`<p class="ws-md-p">${renderInline(line)}</p>`);
    }
  }

  // Close unclosed code block
  if (inCodeBlock && codeBlockLines.length > 0) {
    html.push(`<pre class="ws-md-code-block"><code>${escapeHtml(codeBlockLines.join('\n'))}</code></pre>`);
  }

  return (
    <div
      className="ws-md"
      dangerouslySetInnerHTML={{ __html: html.join('') }}
    />
  );
}
