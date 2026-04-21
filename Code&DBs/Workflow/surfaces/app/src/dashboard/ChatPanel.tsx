import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useChat } from '../workspace/useChat';
import { MarkdownRenderer } from '../workspace/MarkdownRenderer';
import { ToolResultRenderer } from '../workspace/ToolResultRenderer';
import './chat-panel.css';

export interface ChatPanelProps {
  open: boolean;
  onClose: () => void;
}

const INPUT_MAX_LENGTH = 8000;
const INPUT_COUNTER_THRESHOLD = 200;

export function ChatPanel({ open, onClose }: ChatPanelProps) {
  const {
    conversationId,
    messages,
    loading,
    error,
    streamingText,
    createConversation,
    sendMessage,
  } = useChat();
  const [input, setInput] = useState('');
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  // FIX #1: ref used to check whether focus is inside this panel.
  const panelRef = useRef<HTMLElement>(null);

  useEffect(() => {
    if (!open || conversationId) return;
    void createConversation();
  }, [open, conversationId, createConversation]);

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
    void sendMessage(content);
  }, [input, loading, conversationId, sendMessage]);

  const charsRemaining = INPUT_MAX_LENGTH - input.length;
  const showCounter = charsRemaining <= INPUT_COUNTER_THRESHOLD;

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
          <div className="chat-panel__title">Ask Anything</div>
          <button className="chat-panel__close" type="button" onClick={onClose} aria-label="Close chat">
            &times;
          </button>
        </div>

        <div className="chat-panel__messages" role="log" aria-live="polite" aria-relevant="additions">
          {messages.length === 0 && !streamingText && (
            <div className="chat-panel__empty">
              Ask about workflows, recent runs, or anything in the control plane.
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
              placeholder="Type a message… (⌘↵ to send)"
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
            {/* FIX #3: character counter — only visible when within 200 chars of limit. */}
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
      </aside>
    </>
  );
}
