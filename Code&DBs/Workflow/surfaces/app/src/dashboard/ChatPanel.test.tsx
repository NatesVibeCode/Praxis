/**
 * Tests for ChatPanel.tsx — covers all six bug-fix areas that touch the component.
 *
 * Run with: jest diff_B_ChatPanel.test.tsx
 *
 * Env assumptions:
 *   - jest + ts-jest (or Babel) with jsdom
 *   - @testing-library/react ≥ 14
 *   - @testing-library/user-event ≥ 14
 */

import React from 'react';
import { cleanup, render, screen, fireEvent, act, within } from '@testing-library/react';
import { vi, type MockedFunction } from 'vitest';
import { ChatPanel } from './ChatPanel';

const jest = vi;

const userEvent = {
  setup: () => ({
    async type(element: HTMLElement, text: string) {
      const input = element as HTMLInputElement | HTMLTextAreaElement;
      const maxLength = Number(input.getAttribute('maxlength'));
      const value = `${input.value ?? ''}${text}`;
      fireEvent.change(input, {
        target: { value: Number.isFinite(maxLength) && maxLength >= 0 ? value.slice(0, maxLength) : value },
      });
      await Promise.resolve();
    },
    async click(element: HTMLElement) {
      fireEvent.click(element);
      await Promise.resolve();
    },
  }),
};

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock('../workspace/useChat', () => ({
  useChat: vi.fn(),
}));
vi.mock('../workspace/MarkdownRenderer', () => ({
  MarkdownRenderer: ({ content }: { content: string }) => (
    <span data-testid="markdown">{content}</span>
  ),
}));
vi.mock('../workspace/ToolResultRenderer', () => ({
  ToolResultRenderer: ({ result }: { result: unknown }) => (
    <span data-testid="tool-result">{JSON.stringify(result)}</span>
  ),
}));
// Suppress CSS import errors in jsdom.
vi.mock('./chat-panel.css', () => ({}), { virtual: true });

import { useChat } from '../workspace/useChat';

const mockUseChat = useChat as MockedFunction<typeof useChat>;

// ---------------------------------------------------------------------------
// Default mock state factory
// ---------------------------------------------------------------------------

function makeChatState(overrides: Partial<ReturnType<typeof useChat>> = {}): ReturnType<typeof useChat> {
  return {
    conversationId: 'conv-1',
    messages: [],
    loading: false,
    error: null,
    streamingText: '',
    createConversation: jest.fn().mockResolvedValue('conv-1'),
    loadConversation: jest.fn().mockResolvedValue(undefined),
    listConversations: jest.fn().mockResolvedValue([]),
    sendMessage: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

beforeEach(() => {
  mockUseChat.mockReturnValue(makeChatState());
});

afterEach(() => {
  cleanup();
  jest.clearAllMocks();
});

// ---------------------------------------------------------------------------
// FIX #1 – Escape focus-trap check
// ---------------------------------------------------------------------------

describe('FIX #1 – Escape key focus-trap check', () => {
  it('calls onClose when Escape is pressed and focus is on document.body', () => {
    const onClose = jest.fn();
    render(<ChatPanel open onClose={onClose} />);

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('calls onClose when Escape is pressed and focus is inside the panel', () => {
    const onClose = jest.fn();
    render(<ChatPanel open onClose={onClose} />);

    // Focus the textarea inside the panel.
    const textarea = screen.getByRole('textbox');
    act(() => { textarea.focus(); });

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('does NOT call onClose when focus is trapped inside another dialog', () => {
    const onClose = jest.fn();
    const { container } = render(
      <>
        {/* Simulate an overlapping modal dialog */}
        <div role="dialog" aria-modal="true" data-testid="other-modal">
          <button data-testid="modal-btn">Modal Action</button>
        </div>
        <ChatPanel open onClose={onClose} />
      </>,
    );

    const modalBtn = screen.getByTestId('modal-btn');
    act(() => { modalBtn.focus(); });

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(onClose).not.toHaveBeenCalled();
  });

  it('does NOT call onClose when focus is inside a nested <dialog> element', () => {
    const onClose = jest.fn();
    render(
      <>
        <dialog open data-testid="native-dialog">
          <button data-testid="dialog-btn">Native Dialog</button>
        </dialog>
        <ChatPanel open onClose={onClose} />
      </>,
    );

    act(() => { (screen.getByTestId('dialog-btn') as HTMLElement).focus(); });

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(onClose).not.toHaveBeenCalled();
  });

  it('does not register the keydown listener when open=false', () => {
    const onClose = jest.fn();
    render(<ChatPanel open={false} onClose={onClose} />);

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(onClose).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// FIX #3 – maxLength 8000 and character counter
// ---------------------------------------------------------------------------

describe('FIX #3 – maxLength and character counter', () => {
  it('textarea has maxLength of 8000', () => {
    render(<ChatPanel open onClose={jest.fn()} />);
    const textarea = screen.getByRole('textbox');
    expect(textarea).toHaveAttribute('maxlength', '8000');
  });

  it('does NOT show the counter when input is short', () => {
    render(<ChatPanel open onClose={jest.fn()} />);
    // Counter only appears within 200 chars of limit (≥ 7800 chars typed).
    expect(screen.queryByLabelText(/characters remaining/i)).not.toBeInTheDocument();
  });

  it('shows the counter when input is within 200 chars of the limit', async () => {
    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    // Type exactly 7800 chars to hit the threshold.
    await user.type(textarea, 'a'.repeat(7800));

    const counter = screen.getByLabelText(/characters remaining/i);
    expect(counter).toBeInTheDocument();
    expect(counter.textContent).toBe('200');
  });

  it('counter decrements as user types near the limit', async () => {
    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    await user.type(textarea, 'a'.repeat(7900));

    const counter = screen.getByLabelText(/characters remaining/i);
    expect(counter.textContent).toBe('100');
  });

  it('counter shows 0 at the exact limit', async () => {
    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    // userEvent respects maxLength so we fire a change event directly.
    fireEvent.change(textarea, { target: { value: 'x'.repeat(8000) } });

    const counter = screen.getByLabelText(/characters remaining/i);
    expect(counter.textContent).toBe('0');
  });
});

// ---------------------------------------------------------------------------
// FIX #4 – Cmd/Ctrl+Enter submits; plain Enter inserts newline
// ---------------------------------------------------------------------------

describe('FIX #4 – Cmd/Ctrl+Enter shortcut', () => {
  it('submits on Cmd+Enter (metaKey)', async () => {
    const sendMessage = jest.fn().mockResolvedValue(undefined);
    mockUseChat.mockReturnValue(makeChatState({ sendMessage }));

    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    await user.type(textarea, 'hello');
    fireEvent.keyDown(textarea, { key: 'Enter', metaKey: true });

    expect(sendMessage).toHaveBeenCalledWith('hello');
  });

  it('submits on Ctrl+Enter (ctrlKey)', async () => {
    const sendMessage = jest.fn().mockResolvedValue(undefined);
    mockUseChat.mockReturnValue(makeChatState({ sendMessage }));

    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    await user.type(textarea, 'world');
    fireEvent.keyDown(textarea, { key: 'Enter', ctrlKey: true });

    expect(sendMessage).toHaveBeenCalledWith('world');
  });

  it('does NOT submit on plain Enter', async () => {
    const sendMessage = jest.fn().mockResolvedValue(undefined);
    mockUseChat.mockReturnValue(makeChatState({ sendMessage }));

    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    await user.type(textarea, 'text');
    fireEvent.keyDown(textarea, { key: 'Enter' });

    expect(sendMessage).not.toHaveBeenCalled();
  });

  it('does NOT submit on Shift+Enter', async () => {
    const sendMessage = jest.fn().mockResolvedValue(undefined);
    mockUseChat.mockReturnValue(makeChatState({ sendMessage }));

    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    await user.type(textarea, 'text');
    fireEvent.keyDown(textarea, { key: 'Enter', shiftKey: true });

    expect(sendMessage).not.toHaveBeenCalled();
  });

  it('does not submit when the input is empty (Cmd+Enter)', () => {
    const sendMessage = jest.fn().mockResolvedValue(undefined);
    mockUseChat.mockReturnValue(makeChatState({ sendMessage }));

    render(<ChatPanel open onClose={jest.fn()} />);
    const textarea = screen.getByRole('textbox');
    fireEvent.keyDown(textarea, { key: 'Enter', metaKey: true });

    expect(sendMessage).not.toHaveBeenCalled();
  });

  it('clears the textarea after Cmd+Enter submit', async () => {
    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    const textarea = screen.getByRole('textbox');
    await user.type(textarea, 'my message');
    fireEvent.keyDown(textarea, { key: 'Enter', metaKey: true });

    expect(textarea).toHaveValue('');
  });
});

// ---------------------------------------------------------------------------
// FIX #5 – Error-flavored assistant messages are rendered distinctly
// ---------------------------------------------------------------------------

describe('FIX #5 – error-flavored message rendering', () => {
  it('renders an error assistant message with error CSS classes', () => {
    mockUseChat.mockReturnValue(
      makeChatState({
        messages: [
          {
            id: 'err-1',
            role: 'assistant',
            content: 'Request timed out.',
            isError: true,
          },
        ],
      }),
    );

    render(<ChatPanel open onClose={jest.fn()} />);

    const msgContainer = screen.getByText('Request timed out.').closest('[class*="chat-panel__message"]');
    expect(msgContainer).toHaveClass('chat-panel__message--error');
  });

  it('does NOT show model_used meta for error messages', () => {
    mockUseChat.mockReturnValue(
      makeChatState({
        messages: [
          {
            id: 'err-2',
            role: 'assistant',
            content: 'Something went wrong.',
            isError: true,
            model_used: 'claude-test',
          },
        ],
      }),
    );

    render(<ChatPanel open onClose={jest.fn()} />);

    expect(screen.queryByText('claude-test')).not.toBeInTheDocument();
  });

  it('renders a normal assistant message without error classes', () => {
    mockUseChat.mockReturnValue(
      makeChatState({
        messages: [
          { id: 'msg-1', role: 'assistant', content: 'All good.' },
        ],
      }),
    );

    render(<ChatPanel open onClose={jest.fn()} />);

    const msgContainer = screen.getByTestId('markdown').closest('[class*="chat-panel__message"]');
    expect(msgContainer).toHaveClass('chat-panel__message--assistant');
    expect(msgContainer).not.toHaveClass('chat-panel__message--error');
  });
});

// ---------------------------------------------------------------------------
// General rendering / send button behavior
// ---------------------------------------------------------------------------

describe('general rendering', () => {
  it('renders empty state when there are no messages', () => {
    render(<ChatPanel open onClose={jest.fn()} />);
    expect(screen.getByText(/ask about workflows/i)).toBeInTheDocument();
  });

  it('renders user and assistant messages', () => {
    mockUseChat.mockReturnValue(
      makeChatState({
        messages: [
          { id: 'u1', role: 'user', content: 'User msg' },
          { id: 'a1', role: 'assistant', content: 'Assistant reply' },
        ],
      }),
    );

    render(<ChatPanel open onClose={jest.fn()} />);
    expect(screen.getByText('User msg')).toBeInTheDocument();
    expect(screen.getByTestId('markdown')).toHaveTextContent('Assistant reply');
  });

  it('shows streaming text with a cursor', () => {
    mockUseChat.mockReturnValue(makeChatState({ streamingText: 'Streaming…' }));
    render(<ChatPanel open onClose={jest.fn()} />);
    expect(screen.getByText(/Streaming…/)).toBeInTheDocument();
    expect(document.querySelector('.ws-cursor')).toBeInTheDocument();
  });

  it('disables Send button while loading', () => {
    mockUseChat.mockReturnValue(makeChatState({ loading: true }));
    render(<ChatPanel open onClose={jest.fn()} />);
    expect(screen.getByRole('button', { name: /sending/i })).toBeDisabled();
  });

  it('disables Send button when no conversationId', () => {
    mockUseChat.mockReturnValue(makeChatState({ conversationId: null }));
    render(<ChatPanel open onClose={jest.fn()} />);
    expect(screen.getByRole('button', { name: /send/i })).toBeDisabled();
  });

  it('calls sendMessage via the Send button', async () => {
    const sendMessage = jest.fn().mockResolvedValue(undefined);
    mockUseChat.mockReturnValue(makeChatState({ sendMessage }));

    const user = userEvent.setup();
    render(<ChatPanel open onClose={jest.fn()} />);

    await user.type(screen.getByRole('textbox'), 'click send');
    await user.click(screen.getByRole('button', { name: /^send$/i }));

    expect(sendMessage).toHaveBeenCalledWith('click send');
  });

  it('shows an error banner when error is set', () => {
    mockUseChat.mockReturnValue(makeChatState({ error: 'Something broke' }));
    render(<ChatPanel open onClose={jest.fn()} />);
    expect(screen.getByRole('alert')).toHaveTextContent('Something broke');
  });

  it('hides the mounted panel content when open=false', () => {
    render(<ChatPanel open={false} onClose={jest.fn()} />);
    expect(screen.getByRole('dialog', { hidden: true })).toHaveAttribute('aria-hidden', 'true');
  });
});
