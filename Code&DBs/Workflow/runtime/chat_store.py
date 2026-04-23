"""Postgres-backed chat persistence for conversation state.

This module owns SQL for the conversations + conversation_messages tables.
"""

from __future__ import annotations

from typing import Any


class ChatStore:
    """Postgres-backed store for chat conversations and messages."""

    def __init__(self, pg_conn: Any) -> None:
        self._pg = pg_conn

    def create_conversation(self, title: str | None = None) -> str:
        import uuid

        conversation_id = str(uuid.uuid4())
        self._pg.execute(
            "INSERT INTO conversations (id, title) VALUES ($1, $2)",
            conversation_id,
            title or "New conversation",
        )
        return conversation_id

    def get_conversation_summary(self, conversation_id: str) -> dict[str, Any] | None:
        rows = self._pg.execute(
            "SELECT id, title, created_at, updated_at FROM conversations WHERE id = $1",
            conversation_id,
        )
        if not rows:
            return None
        return dict(rows[0])

    def list_conversation_messages(self, conversation_id: str) -> list[dict[str, Any]]:
        rows = self._pg.execute(
            "SELECT id, role, content, tool_calls, tool_results, model_used, latency_ms, cost_usd, created_at "
            "FROM conversation_messages WHERE conversation_id = $1 ORDER BY created_at",
            conversation_id,
        )
        return [dict(row) for row in rows]

    def list_conversations(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self._pg.execute(
            "SELECT c.id, c.title, c.created_at, c.updated_at, "
            "(SELECT COUNT(*) FROM conversation_messages WHERE conversation_id = c.id) as message_count "
            "FROM conversations c ORDER BY c.updated_at DESC LIMIT $1",
            limit,
        )
        return [dict(row) for row in rows]

    def list_recent_context_messages(
        self,
        *,
        exclude_conversation_id: str,
        limit: int = 80,
    ) -> list[dict[str, Any]]:
        """Return recent persisted chat messages that can seed context recall."""
        rows = self._pg.execute(
            "SELECT cm.id, cm.conversation_id, c.title, cm.role, cm.content, cm.created_at "
            "FROM conversation_messages cm "
            "JOIN conversations c ON c.id = cm.conversation_id "
            "WHERE cm.conversation_id <> $1 "
            "AND cm.role IN ('user', 'assistant') "
            "AND cm.content IS NOT NULL "
            "AND LENGTH(TRIM(cm.content)) > 0 "
            "ORDER BY cm.created_at DESC "
            "LIMIT $2",
            exclude_conversation_id,
            limit,
        )
        return [dict(row) for row in rows]

    def append_message(
        self,
        conversation_id: str,
        role: str,
        content: str,
        *,
        tool_calls: str | None = None,
        tool_results: str | None = None,
        model_used: str | None = None,
        latency_ms: int | None = None,
        cost_usd: float | None = None,
    ) -> str:
        import uuid

        msg_id = str(uuid.uuid4())
        self._pg.execute(
            "INSERT INTO conversation_messages (id, conversation_id, role, content, tool_calls, tool_results, model_used, latency_ms, cost_usd) "
            "VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9)",
            msg_id,
            conversation_id,
            role,
            content,
            tool_calls,
            tool_results,
            model_used,
            latency_ms,
            cost_usd,
        )
        return msg_id

    def get_conversation_cost(self, conversation_id: str) -> float:
        rows = self._pg.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM conversation_messages WHERE conversation_id = $1",
            conversation_id,
        )
        return float(rows[0]["total"]) if rows else 0.0

    def get_title(self, conversation_id: str) -> str | None:
        rows = self._pg.execute("SELECT title FROM conversations WHERE id = $1", conversation_id)
        if not rows:
            return None
        return rows[0]["title"]

    def update_title(self, conversation_id: str, title: str) -> None:
        self._pg.execute("UPDATE conversations SET title = $1 WHERE id = $2", title, conversation_id)

    def touch_updated_at(self, conversation_id: str) -> None:
        self._pg.execute("UPDATE conversations SET updated_at = NOW() WHERE id = $1", conversation_id)
