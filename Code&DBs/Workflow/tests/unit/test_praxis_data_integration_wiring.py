"""Wiring tests for the praxis_data integration surface in the plan composer.

Covers the four moving pieces that close the "built-not-wired" gap on the
deterministic data plane:
  1. intent_suggestion exposes data-flavored verbs as capability hints.
  2. build_section_sandbox pulls connected integrations from
     integration_registry.
  3. build_shared_prefix surfaces them as AVAILABLE INTEGRATION AGENTS.
  4. _coerce_packet_response preserves integration_id / action / args and
     derives them from the agent slug when the LLM emits the slug but
     drops the sibling fields.
"""

from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime.intent_binding import BoundIntent
from runtime.intent_dependency import SkeletalPacket
from runtime.intent_suggestion import (
    SuggestedAtoms,
    _STAGE_VERB_HINTS,
    _suggest_step_types,
)
from runtime.plan_section_author import (
    SectionSandbox,
    _coerce_packet_response,
    _load_available_integrations,
    build_shared_prefix,
)


# ---------------------------------------------------------------------------
# 1. Verb hints surface deterministic data ops.
# ---------------------------------------------------------------------------


def test_data_verbs_registered_in_stage_hints() -> None:
    for verb in (
        "dedupe",
        "deduplicate",
        "normalize",
        "reconcile",
        "aggregate",
        "profile",
        "redact",
        "backfill",
        "parse",
    ):
        assert verb in _STAGE_VERB_HINTS, f"missing data verb: {verb}"
        _stage, _conf, caps = _STAGE_VERB_HINTS[verb]
        assert "data_op" in caps, f"verb {verb!r} should advertise data_op capability"


def test_data_verb_intent_emits_data_op_capability_hint() -> None:
    suggestions = _suggest_step_types("Dedupe users.csv on email and reconcile against billing.json")
    assert suggestions, "data verbs should produce at least one step suggestion"
    assert any(
        "data_op" in s.capability_hints
        for s in suggestions
    ), "expected a data_op capability hint among suggestions"


# ---------------------------------------------------------------------------
# 2. Sandbox loader pulls connected integrations from integration_registry.
# ---------------------------------------------------------------------------


class _CursorStub:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.last_sql: str | None = None

    def __enter__(self) -> "_CursorStub":
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, sql: str, *_args) -> None:
        self.last_sql = sql

    def fetchall(self) -> list[tuple]:
        return list(self._rows)


class _ConnStub:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def cursor(self) -> _CursorStub:
        return _CursorStub(self._rows)


def test_load_available_integrations_flattens_capabilities() -> None:
    rows = [
        (
            "praxis_data",
            "Praxis Data Plane",
            "Deterministic record-level data operations.",
            [
                {"action": "dedupe", "description": "Deduplicate by key."},
                {"action": "validate", "description": "Validate against schema."},
            ],
        ),
        (
            "webhook",
            "Webhook",
            "Post payloads to HTTP endpoints.",
            [{"action": "post", "description": "POST to URL."}],
        ),
    ]
    conn = _ConnStub(rows)
    integrations = _load_available_integrations(conn)
    by_id = {i["id"]: i for i in integrations}
    assert "praxis_data" in by_id
    assert "webhook" in by_id
    actions = [a["action"] for a in by_id["praxis_data"]["actions"]]
    assert actions == ["dedupe", "validate"]


def test_load_available_integrations_handles_missing_conn() -> None:
    assert _load_available_integrations(None) == []


def test_load_available_integrations_handles_failure() -> None:
    class _BrokenConn:
        def cursor(self) -> "_CursorStub":
            raise RuntimeError("registry unavailable")

    assert _load_available_integrations(_BrokenConn()) == []


# ---------------------------------------------------------------------------
# 3. Shared prefix surfaces integrations + authoring guidance.
# ---------------------------------------------------------------------------


def _empty_atoms(intent: str = "Dedupe users on email") -> SuggestedAtoms:
    return SuggestedAtoms(
        intent=intent,
        pills=BoundIntent(intent=intent),
        suggested_pills=[],
        step_types=[],
        parameters=[],
    )


def test_shared_prefix_surfaces_registered_integration_agents() -> None:
    sandbox = SectionSandbox(
        plan_field_schema=[],
        stage_io={},
        integrations=[
            {
                "id": "praxis_data",
                "name": "Praxis Data Plane",
                "description": "Deterministic record-level data operations.",
                "actions": [
                    {"action": "dedupe", "description": "Deduplicate by key."},
                    {"action": "validate", "description": "Validate against schema."},
                ],
            }
        ],
    )
    prefix = build_shared_prefix(_empty_atoms(), sandbox)
    assert "AVAILABLE INTEGRATION AGENTS" in prefix
    assert "integration/praxis_data/dedupe" in prefix
    assert "integration/praxis_data/validate" in prefix
    assert "Deduplicate by key." in prefix


def test_shared_prefix_handles_zero_integrations() -> None:
    sandbox = SectionSandbox(plan_field_schema=[], stage_io={}, integrations=[])
    prefix = build_shared_prefix(_empty_atoms(), sandbox)
    assert "AVAILABLE INTEGRATION AGENTS" in prefix
    assert "(none registered)" in prefix


def test_shared_prefix_documents_integration_packet_shape() -> None:
    sandbox = SectionSandbox(plan_field_schema=[], stage_io={}, integrations=[])
    prefix = build_shared_prefix(_empty_atoms(), sandbox)
    # The authoring conventions block must teach the LLM the four-field
    # integration shape so a routed packet executes through the
    # integration handler instead of spawning an LLM.
    assert "integration_id" in prefix
    assert "integration_action" in prefix
    assert "integration_args" in prefix


# ---------------------------------------------------------------------------
# 4. Packet coercion preserves and derives integration fields.
# ---------------------------------------------------------------------------


def _skeleton(label: str = "dedupe_users", stage: str = "build") -> SkeletalPacket:
    return SkeletalPacket(
        label=label,
        stage=stage,
        description="dedupe records",
        clause_span=label,
        clause_offset=0,
        consumes_floor=[],
        produces_floor=[],
        capabilities_floor=[],
        gates_scaffold=[],
        depends_on=[],
        pill_writes=[],
        pill_reads=[],
        confidence=1.0,
    )


def test_coerce_preserves_integration_fields_from_llm_output() -> None:
    parsed = {
        "agent": "integration/praxis_data/dedupe",
        "integration_id": "praxis_data",
        "integration_action": "dedupe",
        "integration_args": {"input_path": "artifacts/data/users.csv", "keys": ["email"]},
        "prompt": "Dedupe users by email.",
        "write": ["artifacts/build/dedupe_users/"],
        "parameters": {"input_path": "artifacts/data/users.csv"},
    }
    packet = _coerce_packet_response(
        target=_skeleton(),
        parsed=parsed,
        raw="{}",
        provider_slug="praxis",
        model_slug="data-plane",
    )
    assert packet.agent == "integration/praxis_data/dedupe"
    assert packet.integration_id == "praxis_data"
    assert packet.integration_action == "dedupe"
    assert packet.integration_args["keys"] == ["email"]


def test_coerce_derives_integration_fields_from_agent_slug() -> None:
    # LLM emitted only the agent slug — derive id and action so the runtime
    # can still execute via the integration handler.
    parsed = {
        "agent": "integration/praxis_data/validate",
        "prompt": "Validate users.",
        "write": ["artifacts/build/validate_users/"],
        "parameters": {"input_path": "artifacts/data/users.csv"},
    }
    packet = _coerce_packet_response(
        target=_skeleton(label="validate_users"),
        parsed=parsed,
        raw="{}",
        provider_slug="praxis",
        model_slug="data-plane",
    )
    assert packet.integration_id == "praxis_data"
    assert packet.integration_action == "validate"
    assert packet.integration_args == {}


def test_coerce_leaves_integration_fields_empty_for_llm_agents() -> None:
    parsed = {
        "agent": "auto/build",
        "prompt": "Refactor the module.",
        "write": ["artifacts/build/refactor/"],
        "parameters": {"target": "module.py"},
    }
    packet = _coerce_packet_response(
        target=_skeleton(label="refactor"),
        parsed=parsed,
        raw="{}",
        provider_slug="praxis",
        model_slug="auto-build",
    )
    assert packet.integration_id is None
    assert packet.integration_action is None
    assert packet.integration_args == {}
