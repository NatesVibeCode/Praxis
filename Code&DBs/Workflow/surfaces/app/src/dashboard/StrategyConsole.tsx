import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useChat, type Conversation } from '../workspace/useChat';
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
const CHAT_MODEL_STORAGE_KEY = 'praxis-chat-model';
const MAX_CHAT_FILES = 6;
const MAX_FILE_CONTEXT_BYTES = 80_000;
const MAX_TOTAL_FILE_CONTEXT_BYTES = 240_000;

interface ChatModelOption {
  slug: string;
  provider: string;
  model: string;
  route_rank?: number;
  route_tier?: string | null;
  latency_class?: string | null;
}

interface PendingChatFile {
  id: string;
  name: string;
  type: string;
  size: number;
  content: string;
  clipped: boolean;
}

function readStoredChatModel(): string {
  try {
    return window.localStorage.getItem(CHAT_MODEL_STORAGE_KEY) || '';
  } catch {
    return '';
  }
}

function storeChatModel(value: string): void {
  try {
    if (value) {
      window.localStorage.setItem(CHAT_MODEL_STORAGE_KEY, value);
      return;
    }
    window.localStorage.removeItem(CHAT_MODEL_STORAGE_KEY);
  } catch {
    // Local storage can be unavailable in private or test contexts.
  }
}

function formatFileSize(size: number): string {
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${Math.round(size / 1024)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function chatFileId(file: File): string {
  return `${file.name}-${file.size}-${file.lastModified}-${Math.random().toString(36).slice(2)}`;
}

function modelLabel(option: ChatModelOption): string {
  const rank = option.route_rank ? `#${option.route_rank} ` : '';
  return `${rank}${option.provider}/${option.model}`;
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
  const [availableModels, setAvailableModels] = useState<ChatModelOption[]>([]);
  const [modelsLoading, setModelsLoading] = useState(false);
  const [selectedModel, setSelectedModel] = useState(readStoredChatModel);
  const [attachedFiles, setAttachedFiles] = useState<PendingChatFile[]>([]);
  const [dropActive, setDropActive] = useState(false);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

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
    let cancelled = false;
    setModelsLoading(true);
    fetch('/api/models?task_type=chat')
      .then((response) => response.ok ? response.json() : Promise.reject(new Error(`models ${response.status}`)))
      .then((data) => {
        if (cancelled) return;
        const models = Array.isArray(data?.models) ? data.models : [];
        setAvailableModels(models.map((model: any) => ({
          slug: String(model.slug || ''),
          provider: String(model.provider || ''),
          model: String(model.model || ''),
          route_rank: typeof model.route_rank === 'number' ? model.route_rank : undefined,
          route_tier: typeof model.route_tier === 'string' ? model.route_tier : null,
          latency_class: typeof model.latency_class === 'string' ? model.latency_class : null,
        })).filter((model: ChatModelOption) => model.slug && model.provider && model.model));
      })
      .catch(() => {
        if (!cancelled) {
          setAvailableModels([]);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setModelsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
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
  const selectedModelKnown = selectedModel
    ? availableModels.some((model) => model.slug === selectedModel)
    : true;

  const handleModelChange = useCallback((value: string) => {
    setSelectedModel(value);
    storeChatModel(value);
  }, []);

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
    void sendMessage(
      content,
      attachmentContext.length ? attachmentContext : undefined,
      targetConversationId,
      { model: selectedModel || null },
    );
    void refreshConversations();
  }, [attachedFiles, conversationId, createConversation, input, loading, refreshConversations, selectedModel, sendMessage]);

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
          <label className="strategy-console__model-picker">
            <span>Model</span>
            <select
              value={selectedModel}
              onChange={(event) => handleModelChange(event.target.value)}
              aria-label="Chat model"
              disabled={modelsLoading}
            >
              <option value="">{modelsLoading ? 'Loading routes...' : 'Auto route'}</option>
              {selectedModel && !selectedModelKnown && (
                <option value={selectedModel}>{selectedModel}</option>
              )}
              {availableModels.map((model) => (
                <option key={model.slug} value={model.slug}>
                  {modelLabel(model)}
                </option>
              ))}
            </select>
          </label>
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
