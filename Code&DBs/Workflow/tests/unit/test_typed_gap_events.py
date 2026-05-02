"""Tests for typed_gap.created event emission (Phase 1.6 emission wiring).

The helper pairs with the authority_event_contracts row registered in
migration 226. Writes to public.system_events via emit_system_event;
best-effort — failures do not propagate, the caller gets None.
"""
from __future__ import annotations

import json

from runtime.typed_gap_events import emit_typed_gap


class _RecordingConn:
    """Minimal Postgres conn stub: records all INSERT calls.

    ``events`` returns only system_events writes (one per emit) so existing
    count-based assertions stay accurate after the dual-write to
    authority_events landed (BUG-A0983040 follow-up + Phase 2 CQRS migration).
    ``all_writes`` returns every INSERT for tests that care about both paths.
    """

    def __init__(self) -> None:
        self.all_writes: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        self.all_writes.append((sql, args))
        return []

    @property
    def events(self) -> list[tuple[str, tuple]]:
        return [
            (sql, args)
            for sql, args in self.all_writes
            if "INSERT INTO system_events" in sql
        ]

    @property
    def authority_events(self) -> list[tuple[str, tuple]]:
        return [
            (sql, args)
            for sql, args in self.all_writes
            if "INSERT INTO authority_events" in sql
        ]


def _captured_payload(conn: _RecordingConn) -> dict:
    assert conn.events, "expected at least one INSERT"
    sql, args = conn.events[-1]
    assert "INSERT INTO system_events" in sql
    return json.loads(args[3])


def test_emit_typed_gap_success_returns_gap_id():
    conn = _RecordingConn()
    gap_id = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="stage_template.missing",
        legal_repair_actions=["add_stage_template", "use_known_stage"],
        source_ref="packet:p1",
        context={"stage_attempted": "rumble"},
    )
    assert gap_id is not None
    assert gap_id.startswith("typed_gap.")
    assert len(conn.events) == 1
    sql, args = conn.events[0]
    assert args[0] == "typed_gap.created"
    assert args[1] == gap_id  # source_id mirrors gap_id
    assert args[2] == "typed_gap"  # source_type


def test_emit_typed_gap_payload_shape():
    conn = _RecordingConn()
    gap_id = emit_typed_gap(
        conn,
        gap_kind="type_contract_slug",
        missing_type="data_dictionary_object",
        reason_code="data_dictionary.object_kind.missing",
        legal_repair_actions=["add_data_dictionary_objects_row"],
        source_ref="tool:praxis_bugs",
        context={"slug": "praxis.bug.record"},
    )
    payload = _captured_payload(conn)
    assert payload["gap_id"] == gap_id
    assert payload["gap_kind"] == "type_contract_slug"
    assert payload["missing_type"] == "data_dictionary_object"
    assert payload["reason_code"] == "data_dictionary.object_kind.missing"
    assert payload["legal_repair_actions"] == ["add_data_dictionary_objects_row"]
    assert payload["source_ref"] == "tool:praxis_bugs"
    assert payload["context"] == {"slug": "praxis.bug.record"}


def test_emit_typed_gap_coerces_repair_actions_to_strings():
    conn = _RecordingConn()
    emit_typed_gap(
        conn,
        gap_kind="source_ref",
        missing_type="source_authority_resolver",
        reason_code="source_ref.unresolvable",
        legal_repair_actions=[1, 2, "three"],  # type: ignore[list-item]
    )
    payload = _captured_payload(conn)
    assert payload["legal_repair_actions"] == ["1", "2", "three"]


def test_emit_typed_gap_defaults_for_optional_fields():
    conn = _RecordingConn()
    emit_typed_gap(
        conn,
        gap_kind="verifier",
        missing_type="verifier",
        reason_code="verifier.no_admitted_for_extension",
        # no legal_repair_actions, source_ref, or context
    )
    payload = _captured_payload(conn)
    assert payload["legal_repair_actions"] == []
    assert payload["source_ref"] is None
    assert payload["context"] == {}


def test_emit_typed_gap_generates_unique_gap_ids():
    conn = _RecordingConn()
    id1 = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    id2 = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    assert id1 != id2


class _BrokenConn:
    """Conn stub whose execute() always raises — for both-paths-fail tests."""

    def execute(self, *args, **kwargs):
        raise RuntimeError("simulated DB outage")


def test_emit_typed_gap_returns_none_when_both_paths_fail(monkeypatch):
    """When both authority_events and system_events writes fail, helper
    returns None rather than propagating. Single-path failure still
    succeeds via the surviving path (dual-write resilience).
    """
    import runtime.system_events as se_mod

    def broken(*args, **kwargs):
        raise RuntimeError("simulated outage")

    monkeypatch.setattr(se_mod, "emit_system_event", broken)
    # _BrokenConn fails the authority_events INSERT too; both paths now broken.
    result = emit_typed_gap(
        _BrokenConn(),
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    assert result is None


def test_emit_typed_gap_resilient_when_only_sidecar_fails(monkeypatch):
    """When only the system_events sidecar fails, the authority_events
    write still lands the event, so the caller gets the gap_id back."""
    import runtime.system_events as se_mod

    monkeypatch.setattr(
        se_mod, "emit_system_event", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("sidecar down"))
    )
    conn = _RecordingConn()
    result = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    # authority_events write still happened, so the gap_id is returned.
    assert result is not None
    assert conn.authority_events  # canonical stream did receive the event


def test_emit_typed_gap_dual_writes_to_authority_events_and_system_events():
    """Phase 2 CQRS migration: every typed_gap.created emission lands in
    BOTH authority_events (canonical) AND system_events (sidecar) so
    consumers in either stream see the gap. Removed once consumers migrate
    off the sidecar.
    """
    conn = _RecordingConn()
    gap_id = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="stage.template_missing",
        source_ref="compose_plan_from_intent:smoke",
    )
    assert gap_id is not None
    # authority_events row: event_type=typed_gap.created, operation_ref=source_ref
    assert len(conn.authority_events) == 1
    auth_sql, auth_args = conn.authority_events[0]
    # auth args: (authority_domain_ref, aggregate_ref, event_type, payload_json, operation_ref, emitted_by)
    assert auth_args[2] == "typed_gap.created"
    assert auth_args[1] == gap_id  # aggregate_ref mirrors gap_id
    assert auth_args[4] == "compose_plan_from_intent:smoke"
    # system_events sidecar row: event_type='typed_gap.created'
    assert len(conn.events) == 1
    sys_sql, sys_args = conn.events[0]
    assert sys_args[0] == "typed_gap.created"


def test_emit_typed_gap_resilient_when_only_authority_path_fails(monkeypatch):
    """When the authority_events INSERT fails but the sidecar succeeds,
    the caller still gets the gap_id back (the sidecar wrote it)."""
    import runtime.typed_gap_events as tge_mod

    monkeypatch.setattr(
        tge_mod, "_write_typed_gap_to_authority_events", lambda *a, **kw: False
    )
    conn = _RecordingConn()
    result = emit_typed_gap(
        conn,
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    assert result is not None
    assert conn.events  # sidecar stream received the event


def test_emit_typed_gap_returns_none_on_import_failure(monkeypatch):
    """When system_events module can't be imported AND the authority_events
    write fails, helper returns None."""
    import builtins
    import runtime.typed_gap_events as tge_mod

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "runtime.system_events":
            raise ImportError("simulated")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.setattr(
        tge_mod, "_write_typed_gap_to_authority_events", lambda *a, **kw: False
    )
    result = emit_typed_gap(
        _RecordingConn(),
        gap_kind="stage",
        missing_type="stage_template",
        reason_code="r",
    )
    assert result is None


def test_emit_typed_gap_copies_context_defensively():
    """Mutating the source context after emit doesn't affect the emitted
    payload."""
    conn = _RecordingConn()
    source_context = {"key": "value"}
    emit_typed_gap(
        conn,
        gap_kind="x",
        missing_type="x",
        reason_code="x",
        context=source_context,
    )
    source_context["mutated"] = True
    payload = _captured_payload(conn)
    assert payload["context"] == {"key": "value"}


def test_emit_typed_gap_copies_repair_actions_defensively():
    """Mutating the source list after emit doesn't affect the emitted payload."""
    conn = _RecordingConn()
    source_actions = ["action_one"]
    emit_typed_gap(
        conn,
        gap_kind="x",
        missing_type="x",
        reason_code="x",
        legal_repair_actions=source_actions,
    )
    source_actions.append("action_two")
    payload = _captured_payload(conn)
    assert payload["legal_repair_actions"] == ["action_one"]


def test_emit_typed_gaps_for_verification_gaps_returns_count():
    from runtime.typed_gap_events import emit_typed_gaps_for_verification_gaps

    conn = _RecordingConn()
    gaps = [
        {"file": "a.js", "missing_type": "verifier", "reason_code": "verifier.no_admitted_for_extension"},
        {"file": "b.sql", "missing_type": "verifier", "reason_code": "verifier.no_admitted_for_extension"},
    ]
    emitted = emit_typed_gaps_for_verification_gaps(
        conn,
        gaps,
        source_ref="packet:p1",
    )
    assert emitted == 2
    assert len(conn.events) == 2


def test_emit_typed_gaps_for_verification_gaps_empty_returns_zero():
    from runtime.typed_gap_events import emit_typed_gaps_for_verification_gaps

    conn = _RecordingConn()
    assert emit_typed_gaps_for_verification_gaps(conn, []) == 0
    assert emit_typed_gaps_for_verification_gaps(conn, None) == 0
    assert conn.events == []


def test_emit_typed_gaps_for_verification_gaps_payload_carries_file_and_source():
    from runtime.typed_gap_events import emit_typed_gaps_for_verification_gaps

    conn = _RecordingConn()
    emit_typed_gaps_for_verification_gaps(
        conn,
        [
            {
                "file": "view.tsx",
                "missing_type": "verifier",
                "reason_code": "verifier.no_admitted_for_extension",
            }
        ],
        source_ref="workflow_run:abc123",
    )
    payload = _captured_payload(conn)
    assert payload["gap_kind"] == "verifier"
    assert payload["context"]["file"] == "view.tsx"
    assert payload["source_ref"] == "workflow_run:abc123"
    assert payload["legal_repair_actions"] == ["add_verifier_catalog_entry"]


def test_emit_typed_gaps_for_verification_gaps_skips_non_dict_entries():
    from runtime.typed_gap_events import emit_typed_gaps_for_verification_gaps

    conn = _RecordingConn()
    emitted = emit_typed_gaps_for_verification_gaps(
        conn,
        [
            None,  # type: ignore[list-item]
            "not a dict",  # type: ignore[list-item]
            {"file": "ok.sql", "missing_type": "verifier", "reason_code": "r"},
        ],
    )
    assert emitted == 1


def test_emit_typed_gaps_for_compile_errors_unresolved_source_ref():
    import json
    from runtime.spec_materializer import UnresolvedSourceRefError
    from runtime.typed_gap_events import emit_typed_gaps_for_compile_errors

    conn = _RecordingConn()
    err = UnresolvedSourceRefError(["decision.X", "review.Y", "discovery.Z"])
    emitted = emit_typed_gaps_for_compile_errors(conn, err, source_ref="launch_plan")
    assert emitted == 3
    payload_0 = json.loads(conn.events[0][1][3])
    assert payload_0["gap_kind"] == "source_ref"
    assert payload_0["missing_type"] == "source_authority_resolver"
    assert payload_0["source_ref"] == "launch_plan"
    assert payload_0["context"]["ref"] in {"decision.X", "review.Y", "discovery.Z"}


def test_emit_typed_gaps_for_compile_errors_unresolved_stage():
    import json
    from runtime.spec_materializer import UnresolvedStageError
    from runtime.typed_gap_events import emit_typed_gaps_for_compile_errors

    conn = _RecordingConn()
    err = UnresolvedStageError(
        [
            {"index": 0, "label": "p0", "stage": "rumble"},
            {"index": 2, "label": "p2", "stage": "zig"},
        ]
    )
    emitted = emit_typed_gaps_for_compile_errors(conn, err)
    assert emitted == 2
    payload_0 = json.loads(conn.events[0][1][3])
    assert payload_0["gap_kind"] == "stage"
    assert payload_0["missing_type"] == "stage_template"
    assert "add_stage_template" in payload_0["legal_repair_actions"]
    assert payload_0["context"]["packet_index"] == 0
    assert payload_0["context"]["stage"] == "rumble"


def test_emit_typed_gaps_for_compile_errors_unresolved_write_scope():
    import json
    from runtime.spec_materializer import UnresolvedWriteScopeError
    from runtime.typed_gap_events import emit_typed_gaps_for_compile_errors

    conn = _RecordingConn()
    err = UnresolvedWriteScopeError(
        [{"index": 0, "label": "p0", "description_preview": "do thing"}]
    )
    emitted = emit_typed_gaps_for_compile_errors(conn, err)
    assert emitted == 1
    payload = json.loads(conn.events[0][1][3])
    assert payload["gap_kind"] == "write_scope"
    assert payload["missing_type"] == "write_scope"
    assert "supply_write" in payload["legal_repair_actions"]
    assert "add_source_ref" in payload["legal_repair_actions"]
    assert payload["context"]["description_preview"] == "do thing"


def test_emit_typed_gaps_for_compile_errors_unknown_error_type_returns_zero():
    from runtime.typed_gap_events import emit_typed_gaps_for_compile_errors

    conn = _RecordingConn()
    # Plain ValueError — not an Unresolved* type → return 0, no events.
    result = emit_typed_gaps_for_compile_errors(conn, ValueError("something else"))
    assert result == 0
    assert conn.events == []


def test_emit_typed_gaps_for_compile_errors_empty_entries_returns_zero():
    from runtime.spec_materializer import UnresolvedStageError
    from runtime.typed_gap_events import emit_typed_gaps_for_compile_errors

    conn = _RecordingConn()
    err = UnresolvedStageError([])
    assert emit_typed_gaps_for_compile_errors(conn, err) == 0
    assert conn.events == []


def test_emit_typed_gaps_for_type_flow_errors_parses_structured_error():
    import json
    from runtime.typed_gap_events import emit_typed_gaps_for_type_flow_errors

    conn = _RecordingConn()
    errors = [
        "workflow.type_flow.unsatisfied_inputs:node_42:research_findings,evidence_pack",
    ]
    emitted = emit_typed_gaps_for_type_flow_errors(
        conn, errors, source_ref="compose_plan:mine"
    )
    assert emitted == 1
    payload = json.loads(conn.events[0][1][3])
    assert payload["gap_kind"] == "type_flow"
    assert payload["missing_type"] == "type_flow_input"
    assert payload["context"]["node_id"] == "node_42"
    assert payload["context"]["missing_types"] == [
        "research_findings",
        "evidence_pack",
    ]


def test_emit_typed_gaps_for_type_flow_errors_handles_unparseable():
    """Error strings that don't match the expected prefix still emit,
    carrying the raw string in context.error — no drop-on-floor."""
    import json
    from runtime.typed_gap_events import emit_typed_gaps_for_type_flow_errors

    conn = _RecordingConn()
    emitted = emit_typed_gaps_for_type_flow_errors(
        conn, ["some totally unstructured error text"]
    )
    assert emitted == 1
    payload = json.loads(conn.events[0][1][3])
    assert payload["context"]["error"] == "some totally unstructured error text"
    # No node_id/missing_types keys populated.
    assert "node_id" not in payload["context"]


def test_emit_typed_gaps_for_type_flow_errors_empty_returns_zero():
    from runtime.typed_gap_events import emit_typed_gaps_for_type_flow_errors

    conn = _RecordingConn()
    assert emit_typed_gaps_for_type_flow_errors(conn, []) == 0
    assert emit_typed_gaps_for_type_flow_errors(conn, None) == 0
    assert conn.events == []


def test_emit_typed_gaps_for_build_issues_promotes_typed_issue():
    from runtime.typed_gap_events import emit_typed_gaps_for_build_issues

    conn = _RecordingConn()
    emitted = emit_typed_gaps_for_build_issues(
        conn,
        [
            {
                "issue_id": "issue:typed-gap:blocking-input:1",
                "kind": "typed_gap",
                "node_id": "step-001",
                "label": "Resolve typed input gap",
                "summary": "Workflow input gap needs source authority.",
                "typed_gap": {
                    "gap_kind": "workflow_input",
                    "missing_type": "workflow_input",
                    "reason_code": "workflow.blocking_input.missing",
                    "legal_repair_actions": ["add_producer_node"],
                    "context": {
                        "input_label": "Authentication setup",
                        "node_id": "step-001",
                    },
                },
            }
        ],
        source_ref="compile:wf_alpha",
    )

    assert emitted == 1
    payload = _captured_payload(conn)
    assert payload["gap_kind"] == "workflow_input"
    assert payload["missing_type"] == "workflow_input"
    assert payload["reason_code"] == "workflow.blocking_input.missing"
    assert payload["legal_repair_actions"] == ["add_producer_node"]
    assert payload["source_ref"] == "compile:wf_alpha"
    assert payload["context"]["issue_id"] == "issue:typed-gap:blocking-input:1"
    assert payload["context"]["input_label"] == "Authentication setup"


def test_emit_typed_gaps_for_build_issues_ignores_non_typed_issues():
    from runtime.typed_gap_events import emit_typed_gaps_for_build_issues

    conn = _RecordingConn()
    emitted = emit_typed_gaps_for_build_issues(
        conn,
        [
            {
                "issue_id": "issue:missing-route:step-001",
                "kind": "missing_route",
                "gate_rule": {"required_field": "execution_setup.phases.agent_route"},
            }
        ],
    )

    assert emitted == 0
    assert conn.events == []


def test_emit_typed_gap_gap_id_format():
    """gap_id is 'typed_gap.' + 16 hex chars."""
    conn = _RecordingConn()
    gap_id = emit_typed_gap(
        conn,
        gap_kind="x",
        missing_type="x",
        reason_code="x",
    )
    assert gap_id is not None
    prefix, _, rest = gap_id.partition(".")
    assert prefix == "typed_gap"
    assert len(rest) == 16
    assert all(c in "0123456789abcdef" for c in rest)
