"""Tests for multimodal_ingest and missing_detector modules (~22 tests)."""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

# Direct-file imports to avoid runtime/__init__.py (which pulls in Python 3.10+ code)
_WF = Path(__file__).resolve().parents[2]  # …/Workflow

def _import_from_file(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod

_ingest_mod = _import_from_file(
    "multimodal_ingest", _WF / "memory" / "multimodal_ingest.py"
)
_missing_mod = _import_from_file(
    "missing_detector", _WF / "runtime" / "missing_detector.py"
)

PostureMode = _ingest_mod.PostureMode
IngestSource = _ingest_mod.IngestSource
MultimodalPayload = _ingest_mod.MultimodalPayload
IngestReceipt = _ingest_mod.IngestReceipt
MultimodalIngester = _ingest_mod.MultimodalIngester

MissingContentType = _missing_mod.MissingContentType
MissingFinding = _missing_mod.MissingFinding
MissingContentDetector = _missing_mod.MissingContentDetector
FindingPrioritizer = _missing_mod.FindingPrioritizer


class _RecordingIngestStore:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def write_entities(self, **kwargs):
        self.calls.append(kwargs)
        return len(kwargs.get("entities") or [])


# ===================================================================
# Module 1: MultimodalIngester
# ===================================================================

class TestPostureAndSourceEnums:
    def test_posture_values(self):
        assert PostureMode.OBSERVE.value == "observe"
        assert PostureMode.OPERATE.value == "operate"
        assert PostureMode.BUILD.value == "build"

    def test_source_values(self):
        assert IngestSource.MEETING_TRANSCRIPT.value == "meeting_transcript"
        assert IngestSource.CRM_EXPORT.value == "crm_export"


class TestMultimodalPayload:
    def test_frozen(self):
        p = MultimodalPayload(source_type=IngestSource.CRM_EXPORT, content="x")
        with pytest.raises(AttributeError):
            p.content = "y"

    def test_defaults(self):
        p = MultimodalPayload(source_type=IngestSource.GENERIC_STRUCTURED, content="z")
        assert p.posture == PostureMode.OBSERVE
        assert p.dry_run is False
        assert p.metadata == {}


class TestClassifyTranscript:
    def test_extracts_action_items(self):
        text = "Alice: TODO review PR\nBob: sounds good\nAlice: ACTION send notes"
        ingester = MultimodalIngester()
        entities = ingester.classify(text, IngestSource.MEETING_TRANSCRIPT)
        actions = [e for e in entities if e["type"] == "action_item"]
        assert len(actions) == 2

    def test_extracts_decisions(self):
        text = "Alice: DECISION we ship on Friday"
        ingester = MultimodalIngester()
        entities = ingester.classify(text, IngestSource.MEETING_TRANSCRIPT)
        assert entities[0]["type"] == "decision"
        assert entities[0]["speaker"] == "Alice"

    def test_empty_transcript(self):
        assert MultimodalIngester().classify("", IngestSource.MEETING_TRANSCRIPT) == []


class TestClassifyCRM:
    def test_parses_csv(self):
        csv_text = "name,organization\nAlice,Acme\nBob,Globex"
        entities = MultimodalIngester().classify(csv_text, IngestSource.CRM_EXPORT)
        persons = [e for e in entities if e["type"] == "person"]
        orgs = [e for e in entities if e["type"] == "organization"]
        assert len(persons) == 2
        assert len(orgs) == 2


class TestClassifyProfile:
    def test_structured_profile(self):
        text = "Name: Jane Doe\nRole: CTO\nOrganization: WidgetCo"
        entities = MultimodalIngester().classify(text, IngestSource.PROFILE_DOCUMENT)
        assert len(entities) == 1
        assert entities[0]["name"] == "Jane Doe"
        assert entities[0]["role"] == "CTO"

    def test_missing_name_returns_empty(self):
        assert MultimodalIngester().classify("Role: CTO", IngestSource.PROFILE_DOCUMENT) == []


class TestClassifyGeneric:
    def test_json_list(self):
        data = '[{"key": "val"}, {"key": "val2"}]'
        entities = MultimodalIngester().classify(data, IngestSource.GENERIC_STRUCTURED)
        assert len(entities) == 2
        assert all(e["type"] == "structured" for e in entities)

    def test_fallback_line_by_line(self):
        text = "line one\nline two\n\nline four"
        entities = MultimodalIngester().classify(text, IngestSource.GENERIC_STRUCTURED)
        assert len(entities) == 3  # blank lines skipped


class TestIngest:
    def test_observe_no_writes(self):
        payload = MultimodalPayload(
            source_type=IngestSource.MEETING_TRANSCRIPT,
            content="Alice: TODO fix bug",
            posture=PostureMode.OBSERVE,
        )
        receipt = MultimodalIngester().ingest(payload)
        assert receipt.entities_classified == 1
        assert receipt.entities_written == 0
        assert receipt.skipped_reason is not None

    def test_dry_run_overrides_posture(self):
        payload = MultimodalPayload(
            source_type=IngestSource.MEETING_TRANSCRIPT,
            content="Alice: TODO fix bug",
            posture=PostureMode.BUILD,
            dry_run=True,
        )
        receipt = MultimodalIngester().ingest(payload)
        assert receipt.entities_written == 0
        assert receipt.skipped_reason == "dry_run"

    def test_operate_writes(self):
        store = _RecordingIngestStore()
        payload = MultimodalPayload(
            source_type=IngestSource.CRM_EXPORT,
            content="name,organization\nAlice,Acme",
            posture=PostureMode.OPERATE,
        )
        receipt = MultimodalIngester(engine=store).ingest(payload)
        assert receipt.entities_written > 0
        assert receipt.skipped_reason is None
        assert len(store.calls) == 1

    def test_build_writes(self):
        store = _RecordingIngestStore()
        payload = MultimodalPayload(
            source_type=IngestSource.GENERIC_STRUCTURED,
            content='{"a": 1}',
            posture=PostureMode.BUILD,
        )
        receipt = MultimodalIngester(engine=store).ingest(payload)
        assert receipt.entities_written == 1
        assert len(store.calls) == 1


# ===================================================================
# Module 2: MissingContentDetector
# ===================================================================

_NOW = datetime.now(timezone.utc)


def _make_topic(tid: str, days_ago: int) -> dict:
    return {
        "id": tid,
        "type": "topic",
        "name": f"topic-{tid}",
        "updated_at": (_NOW - timedelta(days=days_ago)).isoformat(),
    }


class TestDetectStaleTopics:
    def test_flags_stale(self):
        entities = [_make_topic("1", 45)]
        findings = MissingContentDetector(stale_days=30).detect_stale_topics(entities)
        assert len(findings) == 1
        assert findings[0].finding_type == MissingContentType.STALE_TOPIC

    def test_ignores_fresh(self):
        entities = [_make_topic("2", 5)]
        findings = MissingContentDetector(stale_days=30).detect_stale_topics(entities)
        assert len(findings) == 0


class TestDetectWeeklyGaps:
    def test_detects_gap(self):
        entities = [{
            "type": "document",
            "updated_at": (_NOW - timedelta(days=10)).isoformat(),
        }]
        findings = MissingContentDetector().detect_weekly_gaps(entities)
        assert len(findings) == 1
        assert findings[0].finding_type == MissingContentType.WEEKLY_GAP

    def test_no_gap_when_recent(self):
        entities = [{
            "type": "document",
            "updated_at": (_NOW - timedelta(days=2)).isoformat(),
        }]
        findings = MissingContentDetector().detect_weekly_gaps(entities)
        assert len(findings) == 0

    def test_cadence_check_disabled(self):
        entities = [{
            "type": "document",
            "updated_at": (_NOW - timedelta(days=30)).isoformat(),
        }]
        findings = MissingContentDetector(weekly_cadence_check=False).detect_weekly_gaps(entities)
        assert len(findings) == 0


class TestDetectOrphanedActions:
    def test_orphan_found(self):
        entities = [{"id": "a1", "type": "action", "name": "do thing"}]
        edges: list = []
        findings = MissingContentDetector().detect_orphaned_actions(entities, edges)
        assert len(findings) == 1
        assert findings[0].entity_id == "a1"

    def test_connected_not_orphan(self):
        entities = [{"id": "a1", "type": "action", "name": "do thing"}]
        edges = [{"source": "a1", "target": "ws1"}]
        findings = MissingContentDetector().detect_orphaned_actions(entities, edges)
        assert len(findings) == 0


class TestScanAll:
    def test_combined_sorted_by_severity(self):
        entities = [
            _make_topic("old", 90),
            {"id": "orphan", "type": "action", "name": "orphan"},
        ]
        edges: list = []
        findings = MissingContentDetector(stale_days=30).scan_all(entities, edges)
        assert len(findings) >= 2
        assert findings[0].severity == "high"


class TestFindingPrioritizer:
    def test_caps_results(self):
        findings = [
            MissingFinding(MissingContentType.STALE_TOPIC, "a", None, "low", "x"),
            MissingFinding(MissingContentType.STALE_TOPIC, "b", None, "high", "x"),
            MissingFinding(MissingContentType.WEEKLY_GAP, "c", None, "medium", "x"),
            MissingFinding(MissingContentType.ORPHANED_ACTION, "d", "1", "high", "x"),
        ]
        result = FindingPrioritizer().prioritize(findings, max_surfaced=2)
        assert len(result) == 2
        assert all(f.severity == "high" for f in result)

    def test_severity_validation(self):
        with pytest.raises(ValueError):
            MissingFinding(MissingContentType.STALE_TOPIC, "bad", None, "critical", "x")
