import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { MarkdownRenderer } from './MarkdownRenderer';

describe('MarkdownRenderer', () => {
  test('wraps a nested bullet list inside a numbered list item', () => {
    const { container } = render(
      <MarkdownRenderer
        content={[
          '1. Parent item',
          '   - Nested bullet',
          '2. Second item',
        ].join('\n')}
      />,
    );

    const nestedItem = container.querySelector('ol > li > ul > li');
    expect(nestedItem).toBeInTheDocument();
    expect(nestedItem).toHaveTextContent('Nested bullet');
  });

  test('preserves 4-space indentation inside code blocks', () => {
    render(
      <MarkdownRenderer
        content={[
          '```',
          '    const foo = 1;',
          '\treturn foo;',
          '```',
        ].join('\n')}
      />,
    );

    expect(screen.getByText(/const foo = 1;/).closest('code')).toHaveTextContent('    const foo = 1;\n\treturn foo;');
  });

  test('adds noopener noreferrer and target blank to external links', () => {
    render(<MarkdownRenderer content="[External](http://example.com)" />);

    const link = screen.getByRole('link', { name: 'External' });
    expect(link).toHaveAttribute('href', 'http://example.com');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('does not add rel or target to internal links', () => {
    render(<MarkdownRenderer content="[Internal](/docs/getting-started)" />);

    const link = screen.getByRole('link', { name: 'Internal' });
    expect(link).toHaveAttribute('href', '/docs/getting-started');
    expect(link).not.toHaveAttribute('target');
    expect(link).not.toHaveAttribute('rel');
  });

  test('renders an empty string without error', () => {
    const { container } = render(<MarkdownRenderer content="" />);

    expect(container.querySelector('.ws-md')).toBeInTheDocument();
    expect(container.querySelector('.ws-md')).toBeEmptyDOMElement();
  });

  test('renders plain text content', () => {
    render(<MarkdownRenderer content="Just plain text." />);

    expect(screen.getByText('Just plain text.')).toBeInTheDocument();
  });
});
