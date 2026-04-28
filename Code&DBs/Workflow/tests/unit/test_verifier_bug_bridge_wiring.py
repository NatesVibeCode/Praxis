"""Wiring checks for runtime.verifier_bug_bridge repository delegation."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


_mod_path = Path(__file__).resolve().parents[2] / "runtime" / "verifier_bug_bridge.py"
_spec = importlib.util.spec_from_file_location("verifier_bug_bridge_wiring", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["verifier_bug_bridge_wiring"] = _mod
_spec.loader.exec_module(_mod)


def test_link_bug_evidence_writes_through_bug_evidence_repository(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeRepository:
        def upsert_bug_evidence_link(self, **kwargs):
            captured.update(kwargs)
            return {"bug_evidence_link_id": "bug_evidence_link:test"}

    monkeypatch.setattr(_mod, "_bug_evidence_repository", lambda conn: _FakeRepository())

    _mod._link_bug_evidence(
        bug_id="BUG-12345678",
        evidence_kind="verification_run",
        evidence_ref="verification_run:test",
        evidence_role="validates_fix",
        notes="repository path",
        conn=object(),
    )

    assert captured["bug_id"] == "BUG-12345678"
    assert captured["evidence_kind"] == "verification_run"
    assert captured["evidence_ref"] == "verification_run:test"
    assert captured["evidence_role"] == "validates_fix"
    assert captured["created_by"] == "verifier_authority"
    assert captured["notes"] == "repository path"


def test_file_control_plane_bug_links_discovery_evidence_when_filing(monkeypatch) -> None:
    import runtime.bug_tracker as bug_tracker_mod

    captured_file_bug: dict[str, object] = {}
    captured_link: dict[str, object] = {}
    sentinel_conn = object()

    class _FakeTracker:
        def __init__(self, db) -> None:
            assert db is sentinel_conn

        def file_bug(self, **kwargs):
            captured_file_bug.update(kwargs)
            return SimpleNamespace(bug_id="BUG-CTRL"), []

    monkeypatch.setattr(bug_tracker_mod, "BugTracker", _FakeTracker)
    monkeypatch.setattr(
        _mod,
        "_link_bug_evidence",
        lambda **kwargs: captured_link.update(kwargs),
    )

    bug = _mod._file_control_plane_bug(
        kind="verification",
        primary_ref="verifier.platform.receipt_provenance",
        primary_display_name="Receipt Provenance",
        status="error",
        target_kind="path",
        target_ref="/tmp/example.py",
        fingerprint="fp.verify",
        recent_failures=3,
        outputs={"summary": "receipt provenance failed"},
        discovery_evidence_kind="verification_run",
        discovery_evidence_ref="verification_run:test",
        conn=sentinel_conn,
    )

    assert bug.bug_id == "BUG-CTRL"
    assert captured_file_bug["filed_by"] == "verifier_authority"
    assert captured_link["bug_id"] == "BUG-CTRL"
    assert captured_link["evidence_kind"] == "verification_run"
    assert captured_link["evidence_ref"] == "verification_run:test"
    assert captured_link["evidence_role"] == "discovered_by"
