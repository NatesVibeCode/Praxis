"""Tests for memory.ingest — the deterministic ingestion pipeline."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from _pg_test_conn import get_test_conn

from memory.engine import MemoryEngine
from memory.ingest import (
    IngestKind,
    IngestPayload,
    IngestResult,
    IngestRouter,
    SecretRedactor,
)
from memory.types import EntityType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    with MemoryEngine(conn=get_test_conn()) as eng:
        yield eng


@pytest.fixture()
def router(engine):
    return IngestRouter(engine)


def _payload(
    kind: IngestKind = IngestKind.DOCUMENT,
    content: str = "hello world",
    source: str = "test",
    metadata: dict | None = None,
    idempotency_key: str | None = None,
) -> IngestPayload:
    return IngestPayload(
        kind=kind,
        content=content,
        source=source,
        metadata=metadata or {},
        timestamp=datetime.now(timezone.utc),
        idempotency_key=idempotency_key,
    )


# ---------------------------------------------------------------------------
# DOCUMENT ingest
# ---------------------------------------------------------------------------

class TestDocumentIngest:
    def test_creates_entity_in_engine(self, router, engine):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        docs_before = len(engine.list(EntityType.document))
        result = router.ingest(_payload(
            kind=IngestKind.DOCUMENT,
            content=f"Design doc for feature {pfx}",
            source=f"document:{pfx}",
            metadata={"title": f"Feature {pfx}"},
        ))
        assert result.accepted is True
        assert result.entities_created == 1
        entities = engine.list(EntityType.document)
        assert len(entities) - docs_before == 1
        assert any(e.name == f"Feature {pfx}" for e in entities)


# ---------------------------------------------------------------------------
# BUILD_EVENT ingest
# ---------------------------------------------------------------------------

class TestBuildEventIngest:
    def test_extracts_task_entity(self, router, engine):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        event = json.dumps({"type": "task", "name": f"deploy-{pfx}", "id": f"t_{pfx}"})
        result = router.ingest(_payload(kind=IngestKind.BUILD_EVENT, content=event))
        assert result.accepted is True
        assert result.entities_created >= 1
        tasks = engine.list(EntityType.task)
        assert any(t.name == f"deploy-{pfx}" for t in tasks)

    def test_extracts_decision_entity(self, router, engine):
        event = json.dumps({"type": "decision", "name": "use-postgres", "id": "d1"})
        result = router.ingest(_payload(kind=IngestKind.BUILD_EVENT, content=event))
        assert result.accepted is True
        decisions = engine.list(EntityType.decision)
        assert any(d.name == "use-postgres" for d in decisions)

    def test_plain_text_fallback(self, router, engine):
        result = router.ingest(_payload(
            kind=IngestKind.BUILD_EVENT,
            content="ran CI pipeline successfully",
        ))
        assert result.accepted is True
        assert result.entities_created == 1


# ---------------------------------------------------------------------------
# EXTRACTION ingest
# ---------------------------------------------------------------------------

class TestExtractionIngest:
    def test_inserts_entities_and_edges(self, router, engine):
        payload_content = json.dumps({
            "entities": [
                {"id": "e1", "entity_type": "fact", "name": "fact-one", "content": "x"},
                {"id": "e2", "entity_type": "topic", "name": "topic-one", "content": "y"},
            ],
            "edges": [
                {"source_id": "e1", "target_id": "e2", "relation_type": "related_to"},
            ],
        })
        result = router.ingest(_payload(kind=IngestKind.EXTRACTION, content=payload_content))
        assert result.accepted is True
        assert result.entities_created == 2
        assert result.edges_created == 1

    def test_invalid_json_returns_error(self, router):
        result = router.ingest(_payload(kind=IngestKind.EXTRACTION, content="NOT JSON"))
        assert result.accepted is False
        assert any("JSON" in e for e in result.errors)


# ---------------------------------------------------------------------------
# IMPORT ingest
# ---------------------------------------------------------------------------

class TestImportIngest:
    def test_bulk_insert(self, router, engine):
        import uuid
        pfx = uuid.uuid4().hex[:8]
        tasks_before = len(engine.list(EntityType.task, limit=10000))
        facts_before = len(engine.list(EntityType.fact, limit=10000))
        items = json.dumps([
            {"id": f"i1_{pfx}", "entity_type": "task", "name": f"task-a-{pfx}", "content": "do a"},
            {"id": f"i2_{pfx}", "entity_type": "task", "name": f"task-b-{pfx}", "content": "do b"},
            {"id": f"i3_{pfx}", "entity_type": "fact", "name": f"fact-c-{pfx}", "content": "c"},
        ])
        result = router.ingest(_payload(kind=IngestKind.IMPORT, content=items))
        assert result.accepted is True
        assert result.entities_created == 3
        assert len(engine.list(EntityType.task, limit=10000)) - tasks_before == 2
        assert len(engine.list(EntityType.fact, limit=10000)) - facts_before == 1


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_same_key_twice_skips_second(self, router):
        p = _payload(idempotency_key="unique-1")
        r1 = router.ingest(p)
        r2 = router.ingest(p)
        assert r1.accepted is True
        assert r2.accepted is False
        assert r2.duplicates_skipped == 1


# ---------------------------------------------------------------------------
# Payload size limit
# ---------------------------------------------------------------------------

class TestPayloadSizeLimit:
    def test_rejects_oversized_content(self, engine):
        router = IngestRouter(engine, max_content_bytes=50)
        big = "x" * 100
        result = router.ingest(_payload(content=big))
        assert result.accepted is False
        assert any("max size" in e for e in result.errors)


# ---------------------------------------------------------------------------
# Secret redaction
# ---------------------------------------------------------------------------

class TestSecretRedaction:
    @pytest.fixture()
    def redactor(self):
        return SecretRedactor()

    def test_api_key_sk(self, redactor):
        assert "[REDACTED_API_KEY]" in redactor.redact("key is sk-abc123def456ghi789jkl012")

    def test_api_key_akia(self, redactor):
        assert "[REDACTED_API_KEY]" in redactor.redact("aws AKIAIOSFODNN7EXAMPLE")

    def test_api_key_ghp(self, redactor):
        assert "[REDACTED_API_KEY]" in redactor.redact("ghp_ABCDEFghijklmnopqrstuv")

    def test_api_key_ghs(self, redactor):
        assert "[REDACTED_API_KEY]" in redactor.redact("ghs_ABCDEFghijklmnopqrstuv")

    def test_bearer_token(self, redactor):
        text = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.test"
        assert "[REDACTED_BEARER]" in redactor.redact(text)

    def test_password_key_value(self, redactor):
        assert "password=[REDACTED]" in redactor.redact("password=hunter2")

    def test_secret_key_value(self, redactor):
        assert "secret=[REDACTED]" in redactor.redact("secret=mysupersecretvalue")

    def test_generic_base64_blob(self, redactor):
        blob = "A" * 50
        assert "[REDACTED_TOKEN]" in redactor.redact(f"data: {blob}")

    def test_ingest_redacts_before_storing(self, router, engine):
        """Secrets in content should be redacted in the stored entity."""
        import uuid
        pfx = uuid.uuid4().hex[:8]
        result = router.ingest(_payload(
            content=f"my key is sk-abcdefghijklmnopqrstuvwxyz {pfx}",
            metadata={"title": f"redact-test-{pfx}"},
        ))
        assert result.accepted is True
        docs = engine.list(EntityType.document)
        test_doc = [d for d in docs if pfx in d.content]
        assert len(test_doc) == 1
        assert "sk-" not in test_doc[0].content
        assert "[REDACTED_API_KEY]" in test_doc[0].content


# ---------------------------------------------------------------------------
# Empty content
# ---------------------------------------------------------------------------

class TestEmptyContent:
    def test_empty_document(self, router, engine):
        result = router.ingest(_payload(content=""))
        assert result.accepted is True
        assert result.entities_created == 1

    def test_empty_build_event(self, router, engine):
        result = router.ingest(_payload(kind=IngestKind.BUILD_EVENT, content=""))
        assert result.accepted is True


# ---------------------------------------------------------------------------
# CONVERSATION ingest
# ---------------------------------------------------------------------------

class TestConversationIngest:
    def test_creates_conversation_entity(self, router, engine):
        content = "Hello @alice\nHi @bob, how are you?"
        result = router.ingest(_payload(kind=IngestKind.CONVERSATION, content=content))
        assert result.accepted is True
        # Should have conversation doc + 2 person entities
        assert result.entities_created >= 3
        persons = engine.list(EntityType.person)
        names = {p.name for p in persons}
        assert "alice" in names
        assert "bob" in names
