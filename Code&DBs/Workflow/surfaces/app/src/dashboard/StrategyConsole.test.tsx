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

  function route(provider: string, model: string, overrides: Record<string, unknown> = {}) {
    return {
      candidate_ref: `dispatch_option.chat.api.${provider}.${model.replace(/[^a-zA-Z0-9_.-]/g, '_')}`,
      candidate_set_hash: 'hash-1',
      provider_slug: provider,
      model_slug: model,
      transport_type: 'API',
      execution_target_kind: 'control_plane_api',
      execution_target_ref: 'execution_target.control_plane_api',
      execution_profile_ref: 'execution_profile.praxis.control_plane_api',
      rank: 1,
      permitted: true,
      route_health_score: 0.9,
      benchmark_score: null,
      route_tier: 'high',
      latency_class: 'interactive',
      ...overrides,
    };
  }

  function routingResponse(candidates = [route('openrouter', 'deepseek/deepseek-v4-pro')]) {
    return {
      ok: true,
      result: {
        ok: true,
        operation: 'execution.dispatch_options.list',
        candidate_set_hash: 'hash-1',
        candidates,
      },
    };
  }

  function jsonResponse(payload: unknown) {
    return {
      ok: true,
      json: async () => payload,
    } as Response;
  }

  test('loads selectable dispatch options from execution authority', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(routingResponse()));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await act(async () => {
      render(<StrategyConsole stage="sidebar" onStageChange={() => undefined} />);
    });

    await waitFor(() => {
      expect(screen.getByLabelText('Chat model')).toHaveTextContent('deepseek/deepseek-v4-pro');
    });
    fireEvent.click(screen.getByLabelText('Chat model'));
    expect(screen.getByRole('option', { name: /deepseek\/deepseek-v4-pro/i })).toBeInTheDocument();
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toMatchObject({
      operation: 'execution.dispatch_options.list',
      input: { task_slug: 'auto/chat', workload_kind: 'chat', include_disabled: true },
      mode: 'query',
    });
  });

  test('commits the default dispatch choice without sending a client model override', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(routingResponse()))
      .mockResolvedValueOnce(jsonResponse({
        ok: true,
        result: {
          ok: true,
          dispatch_choice_ref: 'dispatch_choice.default',
          selected_candidate_ref: 'dispatch_option.chat.api.openrouter.deepseek_deepseek-v4-pro',
        },
      }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await act(async () => {
      render(<StrategyConsole stage="sidebar" onStageChange={() => undefined} />);
    });

    await waitFor(() => {
      expect(screen.getByLabelText('Chat model')).toHaveTextContent('deepseek/deepseek-v4-pro');
    });

    fireEvent.change(screen.getByPlaceholderText('Ask, inspect, or steer...'), {
      target: { value: 'Inspect the current authority gap.' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => {
      expect(chatMocks.sendMessage).toHaveBeenCalled();
    });
    const options = chatMocks.sendMessage.mock.calls[0]?.[3] ?? {};
    expect(options).toMatchObject({
      dispatchChoiceRef: 'dispatch_choice.default',
      candidateSetHash: 'hash-1',
    });
    expect(options).not.toHaveProperty('model');
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toMatchObject({
      operation: 'execution.dispatch_choice.commit',
      input: {
        candidate_set_hash: 'hash-1',
        selection_kind: 'default',
      },
      mode: 'command',
    });
  });

  test('clicking a route pins that provider/model for the dispatch', async () => {
    const candidates = [
      route('openrouter', 'deepseek/deepseek-v4-pro'),
      route('openrouter', 'openai/gpt-5.4', { rank: 2 }),
    ];
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(routingResponse(candidates)))
      .mockResolvedValueOnce(jsonResponse({
        ok: true,
        result: {
          ok: true,
          dispatch_choice_ref: 'dispatch_choice.clicked',
          selected_candidate_ref: candidates[1].candidate_ref,
        },
      }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await act(async () => {
      render(<StrategyConsole stage="sidebar" onStageChange={() => undefined} />);
    });

    await waitFor(() => {
      expect(screen.getByLabelText('Chat model')).toHaveTextContent('deepseek/deepseek-v4-pro');
    });
    fireEvent.click(screen.getByLabelText('Chat model'));
    fireEvent.click(screen.getByRole('option', { name: /openai\/gpt-5.4/i }));

    fireEvent.change(screen.getByPlaceholderText('Ask, inspect, or steer...'), {
      target: { value: 'Use the selected route.' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => {
      expect(chatMocks.sendMessage).toHaveBeenCalled();
    });
    expect(chatMocks.sendMessage.mock.calls[0]?.[3]).toMatchObject({
      model: 'openrouter/openai/gpt-5.4',
      dispatchChoiceRef: 'dispatch_choice.clicked',
      selectedCandidateRef: candidates[1].candidate_ref,
      candidateSetHash: 'hash-1',
    });
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toMatchObject({
      input: {
        selected_candidate_ref: candidates[1].candidate_ref,
        selected_provider_slug: 'openrouter',
        selected_model_slug: 'openai/gpt-5.4',
        selection_kind: 'explicit_click',
      },
    });
  });

  test('disabled route rows cannot become the selected dispatch model', async () => {
    const candidates = [
      route('openrouter', 'deepseek/deepseek-v4-pro'),
      route('openrouter', 'disabled/model', {
        rank: 2,
        permitted: false,
        disabled_reason: 'provider.disabled',
      }),
    ];
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse(routingResponse(candidates)))
      .mockResolvedValueOnce(jsonResponse({
        ok: true,
        result: {
          ok: true,
          dispatch_choice_ref: 'dispatch_choice.default',
          selected_candidate_ref: candidates[0].candidate_ref,
        },
      }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await act(async () => {
      render(<StrategyConsole stage="sidebar" onStageChange={() => undefined} />);
    });

    await waitFor(() => {
      expect(screen.getByLabelText('Chat model')).toHaveTextContent('deepseek/deepseek-v4-pro');
    });
    fireEvent.click(screen.getByLabelText('Chat model'));
    const disabled = screen.getByRole('option', { name: /disabled\/model/i });
    expect(disabled).toHaveAttribute('aria-disabled', 'true');
    fireEvent.click(disabled);

    fireEvent.change(screen.getByPlaceholderText('Ask, inspect, or steer...'), {
      target: { value: 'Do not use disabled.' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    await waitFor(() => {
      expect(chatMocks.sendMessage).toHaveBeenCalled();
    });
    const options = chatMocks.sendMessage.mock.calls[0]?.[3] ?? {};
    expect(options).not.toHaveProperty('model');
  });
});
