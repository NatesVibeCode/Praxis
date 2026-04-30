import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useChat, type Conversation } from '../workspace/useChat';
import {
  clearMoonChatHandoff,
  getMoonChatHandoff,
  moonChatSelectionContext,
  subscribeMoonChatHandoff,
  type MoonChatHandoff,
} from '../moon/moonChatContext';
import { MarkdownRenderer } from '../workspace/MarkdownRenderer';
import { ToolResultRenderer } from '../workspace/ToolResultRenderer';
import './strategy-console.css';

export type StrategyStage = 'icon' | 'sidebar' | 'full';

export interface StrategyConsoleProps {
  stage: StrategyStage;
  onStageChange: (stage: StrategyStage) => void;
}

const INPUT_MAX_LENGTH = 8000;
const INPUT_COUNTER_THRESHOLD = 200;
const QUICK_PROMPTS = [
  'What changed since my last session?',
  'Help me plan the next build step.',
  'Find the relevant context for this screen.',
];
const MAX_CHAT_FILES = 6;
const DEFAULT_CHAT_TASK_SLUG = 'auto/chat';

interface ChatRouteCandidate {
  provider_slug: string;
  model_slug: string;
  transport_type: string | null;
  rank: number | null;
  permitted: boolean | null;
  route_health_score: number | null;
  benchmark_score: number | null;
  route_tier: string | null;
  latency_class: string | null;
}

function chatRouteKey(route: ChatRouteCandidate | null): string {
  if (!route) return '';
  return `${route.provider_slug}|${route.model_slug}|${route.transport_type ?? ''}`;
}

function chatRouteOverride(route: ChatRouteCandidate | null): string | undefined {
  if (!route) return undefined;
  return `${route.provider_slug}/${route.model_slug}`;
}

async function fetchChatRoutingOptions(taskSlug: string): Promise<ChatRouteCandidate[]> {
  const res = await fetch('/api/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operation: 'chat.routing_options.list',
      input: { task_slug: taskSlug },
      mode: 'query',
    }),
  });
  if (!res.ok) {
    throw new Error(`routing options fetch failed (${res.status})`);
  }
  const payload = await res.json();
  const candidates = payload?.result?.candidates ?? payload?.candidates ?? [];
  return Array.isArray(candidates) ? (candidates as ChatRouteCandidate[]) : [];
}
const MAX_FILE_CONTEXT_BYTES = 80_000;
const MAX_TOTAL_FILE_CONTEXT_BYTES = 240_000;

interface PendingChatFile {
  id: string;
  name: string;
  type: string;
  size: number;
  content: string;
  clipped: boolean;
}

function formatFileSize(size: number): string {
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${Math.round(size / 1024)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function chatFileId(file: File): string {
  return `${file.name}-${file.size}-${file.lastModified}-${Math.random().toString(36).slice(2)}`;
}

function formatConversationTime(value?: string): string {
  if (!value) return 'No activity yet';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Recently updated';
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(date);
}

function conversationTitle(conversation: Conversation): string {
  return conversation.title?.trim() || 'Untitled conversation';
}

export function StrategyConsole({ stage, onStageChange }: StrategyConsoleProps) {
  const {
    conversationId,
    messages,
    loading,
    error,
    streamingText,
    sendMessage,
    createConversation,
    loadConversation,
    listConversations,
  } = useChat();
  const [input, setInput] = useState('');
  const [threadsOpen, setThreadsOpen] = useState(false);
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [conversationsLoading, setConversationsLoading] = useState(false);
  const [conversationQuery, setConversationQuery] = useState('');
  const [attachedFiles, setAttachedFiles] = useState<PendingChatFile[]>([]);
  const [dropActive, setDropActive] = useState(false);
  const [moonHandoff, setMoonHandoff] = useState<MoonChatHandoff | null>(null);
  const [routeCandidates, setRouteCandidates] = useState<ChatRouteCandidate[]>([]);
  const [selectedRoute, setSelectedRoute] = useState<ChatRouteCandidate | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [routesError, setRoutesError] = useState<string | null>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const pickerRef = useRef<HTMLDivElement>(null);
  const processedHandoffIdsRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    let cancelled = false;
    fetchChatRoutingOptions(DEFAULT_CHAT_TASK_SLUG)
      .then((candidates) => {
        if (cancelled) return;
        setRouteCandidates(candidates);
        setRoutesError(null);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setRoutesError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!pickerOpen) return;
    function handleOutside(event: MouseEvent) {
      if (!pickerRef.current) return;
      if (event.target instanceof Node && pickerRef.current.contains(event.target)) return;
      setPickerOpen(false);
    }
    document.addEventListener('mousedown', handleOutside);
    return () => document.removeEventListener('mousedown', handleOutside);
  }, [pickerOpen]);

  const refreshConversations = useCallback(async () => {
    setConversationsLoading(true);
    try {
      setConversations(await listConversations());
    } finally {
      setConversationsLoading(false);
    }
  }, [listConversations]);

  useEffect(() => {
    if (stage === 'icon') return;
    void refreshConversations();
  }, [refreshConversations, stage]);

  useEffect(() => {
    if (stage === 'icon') return;
    const applyHandoff = (event: MoonChatHandoff | null) => {
      if (!event) return;
      setThreadsOpen(false);
      setMoonHandoff(event);
    };
    applyHandoff(getMoonChatHandoff());
    return subscribeMoonChatHandoff(applyHandoff);
  }, [stage]);

  useEffect(() => {
    if (stage === 'icon') return;
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [messages, streamingText, stage]);

  useEffect(() => {
    if (stage === 'icon' || threadsOpen) return;
    const frame = window.requestAnimationFrame(() => inputRef.current?.focus());
    return () => window.cancelAnimationFrame(frame);
  }, [stage, threadsOpen]);

  const visibleConversations = useMemo(() => {
    const query = conversationQuery.trim().toLowerCase();
    if (!query) return conversations;
    return conversations.filter((conversation) => {
      const title = conversationTitle(conversation).toLowerCase();
      const updatedAt = (conversation.updated_at || '').toLowerCase();
      return title.includes(query) || updatedAt.includes(query);
    });
  }, [conversationQuery, conversations]);

  const charsRemaining = INPUT_MAX_LENGTH - input.length;
  const showCounter = charsRemaining <= INPUT_COUNTER_THRESHOLD;
  const statusLabel = loading || streamingText ? 'Thinking' : conversationId ? 'Ready' : 'No thread';

  const attachFiles = useCallback(async (fileList: FileList | File[]) => {
    const files = Array.from(fileList).slice(0, MAX_CHAT_FILES);
    if (files.length === 0) return;

    let remainingBytes = Math.max(
      0,
      MAX_TOTAL_FILE_CONTEXT_BYTES - attachedFiles.reduce((total, file) => total + file.content.length, 0),
    );
    const nextFiles: PendingChatFile[] = [];

    for (const file of files) {
      if (remainingBytes <= 0) break;
      const readBytes = Math.min(file.size, MAX_FILE_CONTEXT_BYTES, remainingBytes);
      if (readBytes <= 0) continue;
      const content = await file.slice(0, readBytes).text();
      remainingBytes -= content.length;
      nextFiles.push({
        id: chatFileId(file),
        name: file.name,
        type: file.type || 'text/plain',
        size: file.size,
        content,
        clipped: file.size > readBytes,
      });
    }

    if (nextFiles.length) {
      setAttachedFiles((current) => [...current, ...nextFiles].slice(-MAX_CHAT_FILES));
    }
  }, [attachedFiles]);

  const handleSend = useCallback(async () => {
    const attachmentContext = attachedFiles.map((file) => ({
      type: 'chat_file_context',
      filename: file.name,
      mime_type: file.type,
      size_bytes: file.size,
      clipped: file.clipped,
      content: file.content,
    }));
    const content = input.trim() || (attachmentContext.length ? 'Use the attached files as context.' : '');
    if (!content || loading) return;

    let targetConversationId = conversationId;
    if (!targetConversationId) {
      targetConversationId = await createConversation(content.slice(0, 60));
      if (!targetConversationId) return;
    }

    setInput('');
    setAttachedFiles([]);
    setThreadsOpen(false);
    // Splice the active Moon workflow + selection state into selection_context
    // when the user has the canvas open. Tools default-target it, so the user
    // can ask "what's in this workflow" or "add a Slack node here" without
    // naming the workflow id explicitly.
    const moonCtx = moonChatSelectionContext();
    const mergedSelection = moonCtx.length || attachmentContext.length
      ? [...moonCtx, ...attachmentContext]
      : undefined;
    const modelOverride = chatRouteOverride(selectedRoute);
    void sendMessage(
      content,
      mergedSelection,
      targetConversationId,
      modelOverride ? { model: modelOverride } : undefined,
    );
    void refreshConversations();
  }, [attachedFiles, conversationId, createConversation, input, loading, refreshConversations, selectedRoute, sendMessage]);

  useEffect(() => {
    if (stage === 'icon' || loading) return;
    if (!moonHandoff || moonHandoff.phase !== 'chat_fallback' || !moonHandoff.prompt) return;
    if (processedHandoffIdsRef.current.has(moonHandoff.handoff_id)) return;
    processedHandoffIdsRef.current.add(moonHandoff.handoff_id);

    let cancelled = false;
    const runHandoff = async () => {
      let targetConversationId = conversationId;
      if (!targetConversationId) {
        targetConversationId = await createConversation(
          moonHandoff.workflow_id ? `Materialize recovery ${moonHandoff.workflow_id}` : 'Materialize recovery',
        );
        if (!targetConversationId || cancelled) return;
      }
      const moonCtx = moonChatSelectionContext();
      const selectionContext = [
        ...moonCtx,
        {
          kind: 'moon_materialize_handoff',
          workflow_id: moonHandoff.workflow_id,
          workflow_name: moonHandoff.workflow_name ?? null,
          phase: moonHandoff.phase,
          status_message: moonHandoff.status_message,
          operation_receipt_id: moonHandoff.operation_receipt_id ?? null,
          correlation_id: moonHandoff.correlation_id ?? null,
          graph_summary: moonHandoff.graph_summary ?? null,
        },
      ];
      void sendMessage(
        moonHandoff.prompt || '',
        selectionContext,
        targetConversationId,
        { timeoutMs: 240000 },
      );
      clearMoonChatHandoff();
      setMoonHandoff(moonHandoff);
      void refreshConversations();
    };

    void runHandoff();
    return () => {
      cancelled = true;
    };
  }, [conversationId, createConversation, loading, moonHandoff, refreshConversations, sendMessage, stage]);

  const handleStartNew = useCallback(async () => {
    const id = await createConversation();
    if (!id) return;
    setThreadsOpen(false);
    setConversationQuery('');
    void refreshConversations();
  }, [createConversation, refreshConversations]);

  const handleContinue = useCallback(async (id: string) => {
    await loadConversation(id);
    setThreadsOpen(false);
    setConversationQuery('');
  }, [loadConversation]);

  if (stage === 'icon') {
    return (
      <button
        type="button"
        className="strategy-icon-trigger"
        onClick={() => onStageChange('sidebar')}
        aria-label="Open chat"
      >
        <span className="strategy-icon-trigger__mark" aria-hidden="true" />
        <span className="strategy-icon-trigger__label">Chat</span>
      </button>
    );
  }

  return (
    <aside className={`strategy-console strategy-console--${stage}`} aria-label="Chat">
      <header className="strategy-console__header">
        <div className="strategy-console__identity">
          <span className="strategy-console__face" aria-hidden="true">[._.]</span>
          <div className="strategy-console__title-block">
            <strong>STRATEGY_CONSOLE</strong>
            <span>{statusLabel}</span>
          </div>
        </div>
        <div className="strategy-console__actions">
          <button
            type="button"
            className={`strategy-console__action ${threadsOpen ? 'strategy-console__action--active' : ''}`}
            onClick={() => setThreadsOpen((open) => !open)}
          >
            Threads
          </button>
          <button
            type="button"
            className={`strategy-console__action ${stage === 'sidebar' ? 'strategy-console__action--active' : ''}`}
            onClick={() => onStageChange('sidebar')}
          >
            Sidebar
          </button>
          <button
            type="button"
            className={`strategy-console__action ${stage === 'full' ? 'strategy-console__action--active' : ''}`}
            onClick={() => onStageChange('full')}
          >
            Focus
          </button>
          <button
            type="button"
            className="strategy-console__action strategy-console__action--close"
            onClick={() => onStageChange('icon')}
            aria-label="Minimize chat"
          >
            Minimize
          </button>
        </div>
      </header>

      {threadsOpen && (
        <section className="strategy-console__threads" aria-label="Saved conversations">
          <div className="strategy-console__threads-toolbar">
            <button type="button" className="strategy-console__new-thread" onClick={handleStartNew}>
              New thread
            </button>
            <label className="strategy-console__thread-search">
              <span>Find thread</span>
              <input
                type="search"
                value={conversationQuery}
                onChange={(event) => setConversationQuery(event.target.value)}
                placeholder="Search saved chats"
              />
            </label>
          </div>
          <div className="strategy-console__thread-list">
            {conversationsLoading && <div className="strategy-console__empty">Loading saved chats...</div>}
            {!conversationsLoading && conversations.length === 0 && (
              <div className="strategy-console__empty">No saved chats yet.</div>
            )}
            {!conversationsLoading && conversations.length > 0 && visibleConversations.length === 0 && (
              <div className="strategy-console__empty">No matching chats.</div>
            )}
            {!conversationsLoading && visibleConversations.map((conversation) => (
              <button
                key={conversation.id}
                type="button"
                className={`strategy-console__thread${conversation.id === conversationId ? ' strategy-console__thread--active' : ''}`}
                onClick={() => void handleContinue(conversation.id)}
              >
                <span>{conversationTitle(conversation)}</span>
                <em>
                  {conversation.message_count ?? 0} messages - {formatConversationTime(conversation.updated_at)}
                </em>
              </button>
            ))}
          </div>
        </section>
      )}

      <div className="strategy-console__stream" role="log" aria-live="polite" aria-relevant="additions">
        {moonHandoff && (
          <div className={`strategy-console__handoff strategy-console__handoff--${moonHandoff.phase}`}>
            <span className="strategy-console__handoff-kicker">Materialize handoff</span>
            <strong>{moonHandoff.phase === 'chat_fallback' ? 'Recovery is running' : moonHandoff.phase === 'ready' ? 'Materialize ready' : moonHandoff.phase === 'blocked' ? 'Materialize needs attention' : 'Materialize in progress'}</strong>
            <p>{moonHandoff.status_message}</p>
          </div>
        )}

        {messages.length === 0 && !streamingText && (
          <div className="strategy-console__empty-state">
            <strong>Start from the work in front of you.</strong>
            <div className="strategy-console__quick-prompts">
              {QUICK_PROMPTS.map((prompt) => (
                <button
                  key={prompt}
                  type="button"
                  onClick={() => {
                    setInput(prompt);
                    setThreadsOpen(false);
                    inputRef.current?.focus();
                  }}
                >
                  {prompt}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((message) => {
          if (message.role === 'tool_result' && message.tool_results) {
            return (
              <div key={message.id} className="strategy-message strategy-message--tool">
                <div className="strategy-message__meta">Tool</div>
                <ToolResultRenderer result={message.tool_results} />
              </div>
            );
          }

          const isUser = message.role === 'user';
          const isError = !isUser && message.isError === true;
          return (
            <div
              key={message.id}
              className={`strategy-message ${isUser ? 'strategy-message--user' : isError ? 'strategy-message--error' : 'strategy-message--assistant'}`}
            >
              <div className="strategy-message__meta">{isUser ? 'You' : isError ? 'Error' : 'Praxis'}</div>
              <div className="strategy-message__content">
                {isUser ? message.content : <MarkdownRenderer content={message.content} />}
              </div>
              {!isUser && !isError && message.model_used && (
                <div className="strategy-message__model">{message.model_used}</div>
              )}
            </div>
          );
        })}

        {streamingText && (
          <div className="strategy-message strategy-message--assistant">
            <div className="strategy-message__meta">Praxis</div>
            <div className="strategy-message__content">
              {streamingText}
              <span className="ws-cursor" />
            </div>
          </div>
        )}

        <div ref={chatEndRef} />
      </div>

      {error && (
        <div className="strategy-console__error" role="alert" aria-live="assertive">
          {error}
        </div>
      )}

      <form
        className={`strategy-console__composer${dropActive ? ' strategy-console__composer--drop-active' : ''}`}
        onDragEnter={(event) => {
          event.preventDefault();
          setDropActive(true);
        }}
        onDragOver={(event) => {
          event.preventDefault();
          setDropActive(true);
        }}
        onDragLeave={() => setDropActive(false)}
        onDrop={(event) => {
          event.preventDefault();
          setDropActive(false);
          void attachFiles(event.dataTransfer.files);
        }}
        onSubmit={(event) => {
          event.preventDefault();
          void handleSend();
        }}
      >
        <div className="strategy-console__composer-tools">
          <div className="strategy-console__model-picker" ref={pickerRef}>
            <button
              type="button"
              className="strategy-console__model-trigger"
              aria-haspopup="listbox"
              aria-expanded={pickerOpen}
              aria-label="Chat model"
              onClick={() => setPickerOpen((open) => !open)}
              disabled={routeCandidates.length === 0 && !routesError}
            >
              <span>Engine</span>
              {selectedRoute ? (
                <>
                  <strong>{selectedRoute.model_slug}</strong>
                  <em>{selectedRoute.provider_slug}</em>
                  {selectedRoute.transport_type && (
                    <span className="strategy-console__transport-chip">{selectedRoute.transport_type}</span>
                  )}
                </>
              ) : routeCandidates.length > 0 ? (
                <>
                  <strong>{routeCandidates[0].model_slug}</strong>
                  <em>{routeCandidates[0].provider_slug}</em>
                  {routeCandidates[0].transport_type && (
                    <span className="strategy-console__transport-chip">{routeCandidates[0].transport_type}</span>
                  )}
                </>
              ) : routesError ? (
                <em>routing unavailable</em>
              ) : (
                <em>loading routes…</em>
              )}
            </button>
            {pickerOpen && routeCandidates.length > 0 && (
              <ul className="strategy-console__model-list" role="listbox">
                {routeCandidates.map((route) => {
                  const key = chatRouteKey(route);
                  const isSelected = selectedRoute
                    ? chatRouteKey(selectedRoute) === key
                    : route === routeCandidates[0];
                  return (
                    <li
                      key={key}
                      role="option"
                      aria-selected={isSelected}
                      className={
                        isSelected
                          ? 'strategy-console__model-option strategy-console__model-option--selected'
                          : 'strategy-console__model-option'
                      }
                      onClick={() => {
                        setSelectedRoute(route);
                        setPickerOpen(false);
                      }}
                    >
                      <strong>{route.model_slug}</strong>
                      <em>{route.provider_slug}</em>
                      {route.transport_type && (
                        <span className="strategy-console__transport-chip">{route.transport_type}</span>
                      )}
                      {typeof route.rank === 'number' && (
                        <span className="strategy-console__model-rank">#{route.rank}</span>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
          <button
            type="button"
            className="strategy-console__file-button"
            onClick={() => fileInputRef.current?.click()}
          >
            Files
          </button>
          <input
            ref={fileInputRef}
            className="strategy-console__file-input"
            type="file"
            multiple
            onChange={(event) => {
              if (event.target.files) {
                void attachFiles(event.target.files);
              }
              event.currentTarget.value = '';
            }}
          />
        </div>

        {attachedFiles.length > 0 && (
          <div className="strategy-console__attachments" aria-label="Files attached as chat context">
            {attachedFiles.map((file) => (
              <span key={file.id} className="strategy-console__attachment">
                <span>{file.name}</span>
                <em>{formatFileSize(file.size)}{file.clipped ? ' clipped' : ''}</em>
                <button
                  type="button"
                  aria-label={`Remove ${file.name}`}
                  onClick={() => setAttachedFiles((current) => current.filter((item) => item.id !== file.id))}
                >
                  x
                </button>
              </span>
            ))}
          </div>
        )}

        <div className="strategy-console__input-wrap">
          <textarea
            ref={inputRef}
            maxLength={INPUT_MAX_LENGTH}
            placeholder="Ask, inspect, or steer..."
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) {
                event.preventDefault();
                void handleSend();
              }
            }}
            rows={2}
          />
          {showCounter && (
            <div
              className={`strategy-console__char-counter${charsRemaining <= 0 ? ' strategy-console__char-counter--limit' : ''}`}
              aria-live="polite"
              aria-label={`${charsRemaining} characters remaining`}
            >
              {charsRemaining}
            </div>
          )}
        </div>
        <div className="strategy-console__composer-footer">
          <span>{conversationId ? 'Return sends - saved thread' : 'Return sends - new thread'}</span>
          <button type="submit" disabled={loading || (!input.trim() && attachedFiles.length === 0)}>
            {loading ? 'Sending' : 'Send'}
          </button>
        </div>
      </form>
    </aside>
  );
}
