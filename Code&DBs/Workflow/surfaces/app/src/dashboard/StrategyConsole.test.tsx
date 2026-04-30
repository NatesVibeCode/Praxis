import '@testing-library/jest-dom';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, test, vi } from 'vitest';

import { StrategyConsole } from './StrategyConsole';

const chatMocks = vi.hoisted(() => ({
  sendMessage: vi.fn(),
  createConversation: vi.fn(),
  loadConversation: vi.fn(),
  listConversations: vi.fn(),
}));

vi.mock('../workspace/useChat', () => ({
  useChat: () => ({
    conversationId: null,
    messages: [],
    loading: false,
    error: null,
    streamingText: '',
    sendMessage: chatMocks.sendMessage,
    createConversation: chatMocks.createConversation,
    loadConversation: chatMocks.loadConversation,
    listConversations: chatMocks.listConversations,
  }),
}));

describe('StrategyConsole', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    chatMocks.sendMessage.mockReset();
    chatMocks.createConversation.mockReset();
    chatMocks.loadConversation.mockReset();
    chatMocks.listConversations.mockReset();
    chatMocks.createConversation.mockResolvedValue('thread-1');
    chatMocks.listConversations.mockResolvedValue([]);
    Element.prototype.scrollIntoView = vi.fn();
    vi.spyOn(window, 'requestAnimationFrame').mockImplementation((callback) => {
      callback(0);
      return 0;
    });
    vi.spyOn(window, 'cancelAnimationFrame').mockImplementation(() => undefined);
  });

  test('shows the fixed operator chat engine without loading picker routes', async () => {
    const fetchMock = vi.fn();
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await act(async () => {
      render(<StrategyConsole stage="sidebar" onStageChange={() => undefined} />);
    });

    expect(screen.getByLabelText('Chat model')).toHaveTextContent('DeepSeek V4 Pro');
    expect(screen.getByLabelText('Chat model')).toHaveTextContent('Together');
    expect(screen.queryByRole('combobox', { name: /chat model/i })).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalledWith('/api/models?task_type=chat');
  });

  test('sends chat turns without a client-selected model override', async () => {
    await act(async () => {
      render(<StrategyConsole stage="sidebar" onStageChange={() => undefined} />);
    });

    fireEvent.change(screen.getByPlaceholderText('Ask, inspect, or steer...'), {
      target: { value: 'Inspect the current authority gap.' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => {
      expect(chatMocks.sendMessage).toHaveBeenCalled();
    });
    expect(chatMocks.sendMessage.mock.calls[0]).toEqual([
      'Inspect the current authority gap.',
      undefined,
      'thread-1',
    ]);
  });
});
