import React from 'react';

/**
 * Simple markdown renderer — no external dependencies.
 * Supports: bold, italic, inline code, code blocks, links, lists, headings.
 */

type ListType = 'ul' | 'ol';

type HtmlBlock = {
  kind: 'html';
  html: string;
};

type ListBlock = {
  kind: 'list';
  listType: ListType;
  items: ListItem[];
};

type ListItem = {
  content: string;
  children: Block[];
};

type Block = HtmlBlock | ListBlock;

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escapeAttribute(text: string): string {
  return escapeHtml(text);
}

function isExternalLink(href: string): boolean {
  return /^https?:\/\//i.test(href);
}

function renderLink(label: string, href: string): string {
  const attrs = [`href="${escapeAttribute(href)}"`, 'class="ws-md-link"'];
  if (isExternalLink(href)) {
    attrs.push('target="_blank"', 'rel="noopener noreferrer"');
  }
  return `<a ${attrs.join(' ')}>${label}</a>`;
}

function renderInline(text: string): string {
  const tokens: string[] = [];
  const stash = (value: string): string => {
    const token = `@@WS_MD_${tokens.length}@@`;
    tokens.push(value);
    return token;
  };

  let result = escapeHtml(text);

  result = result.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_match, label: string, href: string) =>
    stash(renderLink(label, href)),
  );
  result = result.replace(/`([^`]+)`/g, (_match, code: string) =>
    stash(`<code class="ws-md-inline-code">${code}</code>`),
  );
  result = result.replace(/\*\*(.+?)\*\*/g, (_match, value: string) => stash(`<strong>${value}</strong>`));
  result = result.replace(/\*(.+?)\*/g, (_match, value: string) => stash(`<em>${value}</em>`));

  return result.replace(/@@WS_MD_(\d+)@@/g, (_match, index: string) => tokens[Number(index)] ?? '');
}

function countIndent(prefix: string): number {
  let width = 0;
  for (const char of prefix) {
    width += char === '\t' ? 4 : 1;
  }
  return width;
}

function renderBlocks(blocks: Block[]): string {
  return blocks
    .map((block) => {
      if (block.kind === 'html') {
        return block.html;
      }

      const listItems = block.items
        .map((item) => `<li class="ws-md-li">${item.content}${renderBlocks(item.children)}</li>`)
        .join('');

      return `<${block.listType} class="ws-md-${block.listType}">${listItems}</${block.listType}>`;
    })
    .join('');
}

export function MarkdownRenderer({ content }: { content: string }) {
  if (!content) {
    return <div className="ws-md" />;
  }

  const lines = (content ?? '').split('\n');
  const blocks: Block[] = [];
  const listStack: Array<{ indent: number; list: ListBlock }> = [];
  let inCodeBlock = false;
  let codeBlockLines: string[] = [];

  const closeLists = (targetDepth = 0) => {
    while (listStack.length > targetDepth) {
      listStack.pop();
    }
  };

  const currentBlockContainer = (): Block[] => {
    if (listStack.length === 0) {
      return blocks;
    }

    const parentItem = listStack[listStack.length - 1].list.items[listStack[listStack.length - 1].list.items.length - 1];
    return parentItem ? parentItem.children : blocks;
  };

  const openList = (listType: ListType, indent: number) => {
    const list: ListBlock = { kind: 'list', listType, items: [] };
    currentBlockContainer().push(list);
    listStack.push({ indent, list });
    return list;
  };

  for (const line of lines) {
    if (line.startsWith('```')) {
      closeLists();
      if (inCodeBlock) {
        blocks.push({
          kind: 'html',
          html: `<pre class="ws-md-code-block"><code>${escapeHtml(codeBlockLines.join('\n'))}</code></pre>`,
        });
        codeBlockLines = [];
        inCodeBlock = false;
      } else {
        inCodeBlock = true;
      }
      continue;
    }

    if (inCodeBlock) {
      codeBlockLines.push(line);
      continue;
    }

    const listMatch = line.match(/^([ \t]*)([-*]|\d+\.)\s+(.*)$/);
    if (listMatch) {
      const [, prefix, marker, itemText] = listMatch;
      const indent = countIndent(prefix);
      const listType: ListType = /^\d+\.$/.test(marker) ? 'ol' : 'ul';

      while (listStack.length > 0 && indent < listStack[listStack.length - 1].indent) {
        listStack.pop();
      }

      if (listStack.length === 0) {
        openList(listType, indent);
      } else {
        const current = listStack[listStack.length - 1];
        if (indent > current.indent) {
          openList(listType, indent);
        } else if (current.list.listType !== listType) {
          listStack.pop();
          openList(listType, indent);
        }
      }

      const activeList = listStack[listStack.length - 1]?.list;
      activeList?.items.push({ content: renderInline(itemText), children: [] });
      continue;
    }

    closeLists();

    if (line.startsWith('### ')) {
      blocks.push({ kind: 'html', html: `<h4 class="ws-md-h4">${renderInline(line.slice(4))}</h4>` });
    } else if (line.startsWith('## ')) {
      blocks.push({ kind: 'html', html: `<h3 class="ws-md-h3">${renderInline(line.slice(3))}</h3>` });
    } else if (line.startsWith('# ')) {
      blocks.push({ kind: 'html', html: `<h2 class="ws-md-h2">${renderInline(line.slice(2))}</h2>` });
    } else if (/^---+$/.test(line)) {
      blocks.push({ kind: 'html', html: '<hr class="ws-md-hr" />' });
    } else if (line.trim() === '') {
      blocks.push({ kind: 'html', html: '<br />' });
    } else {
      blocks.push({ kind: 'html', html: `<p class="ws-md-p">${renderInline(line)}</p>` });
    }
  }

  closeLists();

  if (inCodeBlock) {
    blocks.push({
      kind: 'html',
      html: `<pre class="ws-md-code-block"><code>${escapeHtml(codeBlockLines.join('\n'))}</code></pre>`,
    });
  }

  return <div className="ws-md" dangerouslySetInnerHTML={{ __html: renderBlocks(blocks) }} />;
}
