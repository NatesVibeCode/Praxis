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
  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

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

  const handleSend = useCallback(async () => {
    const content = input.trim();
    if (!content || loading) return;

    let targetConversationId = conversationId;
    if (!targetConversationId) {
      targetConversationId = await createConversation(content.slice(0, 60));
      if (!targetConversationId) return;
    }

    setInput('');
    setThreadsOpen(false);
    void sendMessage(content, undefined, targetConversationId);
    void refreshConversations();
  }, [conversationId, createConversation, input, loading, refreshConversations, sendMessage]);

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
          <span
            className={`strategy-console__status-dot strategy-console__status-dot--${statusLabel.toLowerCase().replace(' ', '-')}`}
            aria-hidden="true"
          />
          <div className="strategy-console__title-block">
            <strong>Chat</strong>
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
            Dock
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
            x
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
        className="strategy-console__composer"
        onSubmit={(event) => {
          event.preventDefault();
          void handleSend();
        }}
      >
        <div className="strategy-console__input-wrap">
          <textarea
            ref={inputRef}
            maxLength={INPUT_MAX_LENGTH}
            placeholder="Ask, inspect, or steer..."
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
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
          <span>{conversationId ? 'Saved thread' : 'New thread on send'}</span>
          <button type="submit" disabled={loading || !input.trim()}>
            {loading ? 'Sending' : 'Send'}
          </button>
        </div>
      </form>
    </aside>
  );
}
