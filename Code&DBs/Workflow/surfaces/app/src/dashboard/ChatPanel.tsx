import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useChat } from '../workspace/useChat';
import { MarkdownRenderer } from '../workspace/MarkdownRenderer';
import { ToolResultRenderer } from '../workspace/ToolResultRenderer';
import './chat-panel.css';

export interface ChatPanelProps {
  open: boolean;
  onClose: () => void;
}

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

  useEffect(() => {
    if (!open || conversationId) return;
    void createConversation();
  }, [open, conversationId, createConversation]);

  useEffect(() => {
    if (!open) return;
    const frame = window.requestAnimationFrame(() => {
      inputRef.current?.focus();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [open]);

  useEffect(() => {
    if (!open) return;
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [open, messages, streamingText, loading]);

  const handleSend = useCallback(() => {
    const content = input.trim();
    if (!content || loading || !conversationId) return;
    setInput('');
    void sendMessage(content);
  }, [input, loading, conversationId, sendMessage]);

  return (
    <>
      <div
        className={`chat-panel-backdrop${open ? ' chat-panel-backdrop--open' : ''}`}
        onClick={onClose}
        aria-hidden={!open}
      />

      <aside
        className={`chat-panel${open ? ' chat-panel--open' : ''}`}
        role="dialog"
        aria-hidden={!open}
        aria-label="Chat panel"
      >
        <div className="chat-panel__header">
          <div className="chat-panel__title">Ask Anything</div>
          <button className="chat-panel__close" type="button" onClick={onClose} aria-label="Close chat">
            &times;
          </button>
        </div>

        <div className="chat-panel__messages">
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
            return (
              <div
                key={message.id}
                className={`chat-panel__message ${isUser ? 'chat-panel__message--user' : 'chat-panel__message--assistant'}`}
              >
                <div className={`chat-panel__bubble ${isUser ? 'chat-panel__bubble--user' : 'chat-panel__bubble--assistant'}`}>
                  <div className="chat-panel__bubble-content">
                    {isUser ? message.content : <MarkdownRenderer content={message.content} />}
                  </div>
                  {!isUser && message.model_used && (
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

        {error && <div className="chat-panel__error">{error}</div>}

        <div className="chat-panel__input-bar">
          <textarea
            ref={inputRef}
            className="chat-panel__input"
            placeholder="Type a message..."
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                handleSend();
              }
            }}
            rows={1}
          />
          <button
            className="chat-panel__send"
            type="button"
            onClick={handleSend}
            disabled={loading || !input.trim() || !conversationId}
          >
            {loading ? 'Sending...' : 'Send'}
          </button>
        </div>
      </aside>
    </>
  );
}
