from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from pathlib import Path
import sys

_WF = Path(__file__).resolve().parents[2]
if str(_WF) not in sys.path:
    sys.path.insert(0, str(_WF))

from memory.multimodal_ingest import (
    SUPPORTED_MULTIMODAL_SOURCE_TYPES,
    IngestReceipt,
    build_multimodal_extraction_payload,
)
import memory.multimodal_ingest as multimodal_mod
from surfaces.api.handlers import workflow_query_core
from surfaces.mcp.tools import knowledge


def test_supported_multimodal_sources_include_transcripts() -> None:
    assert "meeting_transcript" in SUPPORTED_MULTIMODAL_SOURCE_TYPES


def test_build_multimodal_extraction_payload_turns_transcript_into_graph_entities() -> None:
    payload = build_multimodal_extraction_payload(
        "Alice: TODO review PR\nBob: DECISION ship on Friday",
        source="meeting/2026-04-07",
        source_type="meeting_transcript",
    )

    assert len(payload["entities"]) >= 3
    assert payload["entities"][0]["entity_type"] == "document"
    assert payload["entities"][0]["metadata"]["multimodal"] is True

    entity_types = {entity["entity_type"] for entity in payload["entities"][1:]}
    assert "action" in entity_types
    assert "decision" in entity_types

    assert len(payload["edges"]) == len(payload["entities"]) - 1
    assert all(edge["relation_type"] == "derived_from" for edge in payload["edges"])


def test_praxis_ingest_routes_transcripts_through_multimodal_pipeline(monkeypatch) -> None:
    fake_graph_result = SimpleNamespace(
        accepted=True,
        entities_created=3,
        edges_created=2,
        duplicates_skipped=0,
        errors=(),
    )

    def fake_ingest_multimodal_to_knowledge_graph(*_args, **_kwargs):
        return {
            "source_type": "meeting_transcript",
            "staging_receipt": IngestReceipt(
                payload_source="meeting_transcript",
                entities_classified=2,
                entities_written=2,
                skipped_reason=None,
                timestamp=datetime(2026, 4, 7, tzinfo=timezone.utc),
            ),
            "graph_result": fake_graph_result,
        }

    monkeypatch.setattr(knowledge, "ingest_multimodal_to_knowledge_graph", fake_ingest_multimodal_to_knowledge_graph)
    monkeypatch.setattr(knowledge._subs, "get_knowledge_graph", lambda: object())

    result = knowledge.tool_praxis_ingest(
        {
            "kind": "meeting_transcript",
            "content": "Alice: TODO review PR",
            "source": "meeting/2026-04-07",
        }
    )

    assert result["accepted"] is True
    assert result["entities_created"] == 3
    assert result["multimodal"]["source_type"] == "meeting_transcript"
    assert result["multimodal"]["staging_receipt"]["entities_written"] == 2


def test_api_ingest_routes_transcripts_through_multimodal_pipeline(monkeypatch) -> None:
    fake_graph_result = SimpleNamespace(
        accepted=True,
        entities_created=3,
        edges_created=2,
        duplicates_skipped=0,
        errors=(),
    )

    def fake_ingest_multimodal_to_knowledge_graph(*_args, **_kwargs):
        return {
            "source_type": "meeting_transcript",
            "staging_receipt": IngestReceipt(
                payload_source="meeting_transcript",
                entities_classified=2,
                entities_written=2,
                skipped_reason=None,
                timestamp=datetime(2026, 4, 7, tzinfo=timezone.utc),
            ),
            "graph_result": fake_graph_result,
        }

    class FakeSubs:
        def get_knowledge_graph(self):
            return object()

    monkeypatch.setattr(
        multimodal_mod,
        "ingest_multimodal_to_knowledge_graph",
        fake_ingest_multimodal_to_knowledge_graph,
    )

    result = workflow_query_core.handle_ingest(
        FakeSubs(),
        {
            "kind": "meeting_transcript",
            "content": "Alice: TODO review PR",
            "source": "meeting/2026-04-07",
        },
    )

    assert result["accepted"] is True
    assert result["entities_created"] == 3
    assert result["multimodal"]["source_type"] == "meeting_transcript"
