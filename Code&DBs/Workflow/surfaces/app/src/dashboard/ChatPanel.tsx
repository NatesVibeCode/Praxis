import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useChat } from '../workspace/useChat';
import { MarkdownRenderer } from '../workspace/MarkdownRenderer';
import { ToolResultRenderer } from '../workspace/ToolResultRenderer';
import { canvasChatSelectionContext } from '../canvas/canvasChatContext';
import './chat-panel.css';

export interface ChatPanelProps {
  open: boolean;
  onClose: () => void;
}

const INPUT_MAX_LENGTH = 8000;
const INPUT_COUNTER_THRESHOLD = 200;
const QUICK_START_PROMPTS = [
  'What changed since my last session?',
  'Help me plan the next build step.',
  'Find relevant context from earlier chats.',
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

export function ChatPanel({ open, onClose }: ChatPanelProps) {
  const {
    conversationId,
    messages,
    loading,
    error,
    streamingText,
    createConversation,
    loadConversation,
    listConversations,
    sendMessage,
  } = useChat();
  const [input, setInput] = useState('');
  const [conversationMode, setConversationMode] = useState<'chooser' | 'chat'>('chooser');
  const [conversations, setConversations] = useState<Awaited<ReturnType<typeof listConversations>>>([]);
  const [conversationsLoading, setConversationsLoading] = useState(false);
  const [conversationQuery, setConversationQuery] = useState('');
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  // FIX #1: ref used to check whether focus is inside this panel.
  const panelRef = useRef<HTMLElement>(null);

  const refreshConversations = useCallback(async () => {
    setConversationsLoading(true);
    try {
      setConversations(await listConversations());
    } finally {
      setConversationsLoading(false);
    }
  }, [listConversations]);

  useEffect(() => {
    if (!open) return;
    setConversationMode(conversationId ? 'chat' : 'chooser');
    void refreshConversations();
  }, [open, conversationId, refreshConversations]);

  useEffect(() => {
    if (!open) return;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        // FIX #1: if focus is trapped in another dialog/modal that sits on top of
        // this panel, let that element's own handler take the event first.
        const active = document.activeElement;
        const isInAnotherModal =
          active !== null &&
          active !== document.body &&
          !panelRef.current?.contains(active) &&
          !!active.closest('[role="dialog"], dialog, [aria-modal="true"]');

        if (isInAnotherModal) return;

        event.preventDefault();
        onClose();
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [open, onClose]);

  useEffect(() => {
    if (!open) return;
    const frame = window.requestAnimationFrame(() => {
      inputRef.current?.focus();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const scrollIntoView = messagesEndRef.current?.scrollIntoView;
    if (typeof scrollIntoView === 'function') {
      scrollIntoView.call(messagesEndRef.current, { behavior: 'smooth', block: 'end' });
    }
  }, [open, messages, streamingText, loading]);

  const handleSend = useCallback(() => {
    const content = input.trim();
    if (!content || loading || !conversationId) return;
    setInput('');
    // Forward the active Canvas workflow + selection state so the orchestrator
    // can render it into the prompt and the canvas_* tools can default-target it.
    // Snapshot at send time — we do NOT subscribe in render to avoid
    // re-renders on every selection change.
    const canvasCtx = canvasChatSelectionContext();
    void sendMessage(content, canvasCtx.length ? canvasCtx : undefined);
  }, [input, loading, conversationId, sendMessage]);

  const handleStartNew = useCallback(async () => {
    const id = await createConversation();
    if (!id) return;
    setConversationMode('chat');
    void refreshConversations();
  }, [createConversation, refreshConversations]);

  const handleContinue = useCallback(async (id: string) => {
    await loadConversation(id);
    setConversationMode('chat');
  }, [loadConversation]);

  const charsRemaining = INPUT_MAX_LENGTH - input.length;
  const showCounter = charsRemaining <= INPUT_COUNTER_THRESHOLD;
  const normalizedConversationQuery = conversationQuery.trim().toLowerCase();
  const visibleConversations = normalizedConversationQuery
    ? conversations.filter((conversation) => {
        const title = (conversation.title || '').toLowerCase();
        const updatedAt = (conversation.updated_at || '').toLowerCase();
        return title.includes(normalizedConversationQuery) || updatedAt.includes(normalizedConversationQuery);
      })
    : conversations;
  const totalPersistedMessages = conversations.reduce(
    (total, conversation) => total + (conversation.message_count ?? 0),
    0,
  );

  return (
    <>
      <div
        className={`chat-panel-backdrop${open ? ' chat-panel-backdrop--open' : ''}`}
        onClick={onClose}
        aria-hidden={!open}
      />

      <aside
        ref={panelRef}
        className={`chat-panel${open ? ' chat-panel--open' : ''}`}
        role="dialog"
        aria-modal={open ? 'true' : undefined}
        aria-hidden={!open}
        aria-label="Chat panel"
      >
        {open && (
          <>
        <div className="chat-panel__header">
          <div className="chat-panel__header-main">
            {conversationMode === 'chat' && (
              <button
                className="chat-panel__back"
                type="button"
                onClick={() => {
                  setConversationMode('chooser');
                  void refreshConversations();
                }}
                aria-label="Back to conversations"
              >
                Back
              </button>
            )}
            <div>
              <div className="chat-panel__title">Assistant · build</div>
              <div className="chat-panel__subtitle">
                {conversationMode === 'chat' ? 'Persistent Praxis chat' : 'Start new or continue'}
              </div>
            </div>
          </div>
          <button className="chat-panel__close" type="button" onClick={onClose} aria-label="Close chat">
            &times;
          </button>
        </div>
        {conversationMode === 'chooser' ? (
          <div className="chat-panel__chooser">
            <button className="chat-panel__new-chat" type="button" onClick={handleStartNew}>
              <span>New chat</span>
              <strong>Start with a clean thread</strong>
            </button>

            <div className="chat-panel__memory-card" aria-label="Chat memory status">
              <div>
                <strong>Praxis remembers the useful trail.</strong>
                <span>
                  {conversations.length} saved chats · {totalPersistedMessages} messages available for context.
                </span>
              </div>
              <p>
                New chats stay clean, but relevant prior context can still be packed in when it matches what you ask.
              </p>
            </div>

            <div className="chat-panel__section-row">
              <div className="chat-panel__section-label">Continue</div>
              <button className="chat-panel__refresh" type="button" onClick={() => void refreshConversations()}>
                Refresh
              </button>
            </div>

            <label className="chat-panel__search">
              <span>Find a previous thread</span>
              <input
                type="search"
                value={conversationQuery}
                onChange={(event) => setConversationQuery(event.target.value)}
                placeholder="Search saved chats"
              />
            </label>

            {conversationsLoading && (
              <div className="chat-panel__empty">Loading saved conversations...</div>
            )}

            {!conversationsLoading && conversations.length === 0 && (
              <div className="chat-panel__empty">
                No saved conversations yet. Start one and Praxis will keep the thread in its chat ledger.
              </div>
            )}

            {!conversationsLoading && conversations.length > 0 && visibleConversations.length === 0 && (
              <div className="chat-panel__empty">
                Nothing matched that search. The chats are still persisted; this only narrows the chooser.
              </div>
            )}

            {!conversationsLoading && visibleConversations.map((conversation) => (
              <button
                key={conversation.id}
                className={`chat-panel__conversation${conversation.id === conversationId ? ' chat-panel__conversation--active' : ''}`}
                type="button"
                onClick={() => void handleContinue(conversation.id)}
              >
                <span className="chat-panel__conversation-title">{conversation.title || 'Untitled conversation'}</span>
                <span className="chat-panel__conversation-meta">
                  {conversation.message_count ?? 0} messages · {formatConversationTime(conversation.updated_at)}
                </span>
              </button>
            ))}
          </div>
        ) : (
          <>
          <div className="chat-panel__active-strip">
            <div>
              <strong>Saved thread</strong>
              <span>{messages.length} visible messages · prior context can join when relevant</span>
            </div>
            <button type="button" onClick={handleStartNew}>
              New chat
            </button>
          </div>

        <div className="chat-panel__messages" role="log" aria-live="polite" aria-relevant="additions">
            {messages.length === 0 && !streamingText && (
              <div className="chat-panel__empty">
                <strong>Start with a useful ask.</strong>
                <span>Praxis can answer from the control plane, persisted chats, and workflow history.</span>
                <div className="chat-panel__quick-prompts">
                  {QUICK_START_PROMPTS.map((prompt) => (
                    <button
                      key={prompt}
                      type="button"
                      onClick={() => {
                        setInput(prompt);
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
                  <div key={message.id} className="chat-panel__message chat-panel__message--tool">
                    <ToolResultRenderer result={message.tool_results} />
                  </div>
                );
              }

              const isUser = message.role === 'user';
              // FIX #5: render error-flavored assistant messages with a distinct class.
              const isError = !isUser && message.isError === true;
              return (
                <div
                  key={message.id}
                  className={`chat-panel__message ${
                    isUser
                      ? 'chat-panel__message--user'
                      : isError
                      ? 'chat-panel__message--error'
                      : 'chat-panel__message--assistant'
                  }`}
                >
                  <div
                    className={`chat-panel__bubble ${
                      isUser
                        ? 'chat-panel__bubble--user'
                        : isError
                        ? 'chat-panel__bubble--error'
                        : 'chat-panel__bubble--assistant'
                    }`}
                  >
                    <div className="chat-panel__bubble-content">
                      {isUser ? message.content : <MarkdownRenderer content={message.content} />}
                    </div>
                    {!isUser && !isError && message.model_used && (
                      <div className="chat-panel__bubble-meta">{message.model_used}</div>
                    )}
                  </div>
                </div>
              );
            })}

            {streamingText && (
              <div className="chat-panel__message chat-panel__message--assistant">
                <div className="chat-panel__bubble chat-panel__bubble--assistant">
                  <div className="chat-panel__bubble-content">
                    {streamingText}
                    <span className="ws-cursor" />
                  </div>
                </div>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          {error && (
            <div className="chat-panel__error" role="alert" aria-live="assertive">
              {error}
            </div>
          )}

          <div className="chat-panel__input-bar">
            <div className="chat-panel__input-wrap">
              <textarea
                ref={inputRef}
                className="chat-panel__input"
                // FIX #3: prevent runaway inputs; counter shown near limit.
                maxLength={INPUT_MAX_LENGTH}
                placeholder="Type a message... (⌘↵ to send)"
                value={input}
                onChange={(event) => setInput(event.target.value)}
                onKeyDown={(event) => {
                  // FIX #4: Cmd+Enter (mac) or Ctrl+Enter (other) submits.
                  // Plain Enter inserts a newline (default textarea behavior).
                  if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
                    event.preventDefault();
                    handleSend();
                  }
                }}
                rows={1}
              />
              {/* FIX #3: character counter - only visible when within 200 chars of limit. */}
              {showCounter && (
                <div
                  className={`chat-panel__char-counter${charsRemaining <= 0 ? ' chat-panel__char-counter--limit' : ''}`}
                  aria-live="polite"
                  aria-label={`${charsRemaining} characters remaining`}
                >
                  {charsRemaining}
                </div>
              )}
            </div>
            <button
              className="chat-panel__send"
              type="button"
              onClick={handleSend}
              disabled={loading || !input.trim() || !conversationId}
            >
              {loading ? 'Sending...' : 'Send'}
            </button>
          </div>
          </>
        )}
          </>
        )}
      </aside>
    </>
  );
}
