"""Wiring checks for runtime.verifier_bug_bridge repository delegation."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


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
