from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime import spec_materializer
from runtime.spec_materializer import (
    ApprovalHashMismatchError,
    ApprovedPlan,
    MaterializedSpec,
    MaterializePlanError,
    LaunchReceipt,
    PaidModelApprovalError,
    Plan,
    PlanPacket,
    ProposedPlan,
    _coerce_plan,
    approve_proposed_plan,
    materialize_plan,
    launch_approved,
    launch_plan,
    launch_proposed,
    propose_plan,
)


def _stub_compile_spec(intent_dict, *, conn):
    label = intent_dict.get("label") or intent_dict["description"].split()[0].lower()
    return (
        MaterializedSpec(
            prompt=f"PROMPT({intent_dict['description']})",
            scope_write=list(intent_dict.get("write") or []),
            scope_read=intent_dict.get("read"),
            capabilities=["capability.code.python"],
            tier="mid",
            label=f"{intent_dict['stage']}:{label}",
            task_type=intent_dict["stage"],
            verify_refs=[f"verify.{label}"],
            workspace_ref="workspace.default",
            runtime_profile_ref="runtime.default",
        ),
        [],
    )


class _FakeConn:
    pass


def test_compile_plan_translates_packets_into_multi_job_spec(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    plan = {
        "name": "wave_0_authority",
        "why": "fix bug tracker before burning down dependent bugs",
        "packets": [
            {
                "description": "fix bug evidence authority so FIXED requires verifier linkage",
                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                "stage": "build",
                "label": "bug-authority",
                "bug_ref": "BUG-175EB9F3",
            },
            {
                "description": "require superseding evidence before FIXED transitions",
                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                "stage": "build",
                "label": "fixed-transition-evidence",
                "bug_ref": "BUG-9B812B32",
                "depends_on": ["bug-authority"],
            },
        ],
    }

    spec_dict, warnings = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")

    assert warnings == []
    assert spec_dict["name"] == "wave_0_authority"
    assert spec_dict["why"] == "fix bug tracker before burning down dependent bugs"
    assert spec_dict["workflow_id"].startswith("plan.")
    assert spec_dict["execution_manifest_ref"].startswith(
        f"execution_manifest:{spec_dict['workflow_id']}:definition."
    )
    assert spec_dict["execution_manifest"]["manifest_kind"] == "launch_plan_inline_execution_manifest"
    assert spec_dict["execution_manifest"]["verify_refs"] == [
        "verify.bug-authority",
        "verify.fixed-transition-evidence",
    ]
    assert spec_dict["phase"] == "build"
    assert spec_dict["workdir"] == "/repo"
    assert spec_dict["workspace_ref"] == "workspace.default"
    assert spec_dict["runtime_profile_ref"] == "runtime.default"
    assert len(spec_dict["jobs"]) == 2

    first, second = spec_dict["jobs"]
    assert first["label"] == "bug-authority"
    assert first["agent"] == "auto/build"
    assert first["write_scope"] == ["Code&DBs/Workflow/runtime/bugs.py"]
    assert first["workdir"] == "/repo"
    assert first["task_type"] == "build"
    assert first["verify_refs"] == ["verify.bug-authority"]
    assert first["bug_ref"] == "BUG-175EB9F3"
    assert "depends_on" not in first

    assert second["label"] == "fixed-transition-evidence"
    assert second["depends_on"] == ["bug-authority"]
    assert second["bug_ref"] == "BUG-9B812B32"


def test_launch_plan_routes_through_command_bus(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    captured: dict[str, object] = {}

    def _fake_submit_command(conn, **kwargs):
        captured["conn"] = conn
        captured["kwargs"] = kwargs
        return {
            "run_id": "workflow_abc123",
            "status": "queued",
            "total_jobs": len(kwargs["inline_spec"]["jobs"]),
            "spec_name": kwargs["inline_spec"]["name"],
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit_command)

    plan = Plan(
        name="bug_burn_wave_0",
        packets=[
            PlanPacket(
                description="fix bug authority",
                write=["Code&DBs/Workflow/runtime/bugs.py"],
                stage="build",
                label="bug-authority",
                bug_ref="BUG-175EB9F3",
            ),
        ],
    )

    receipt = launch_plan(plan, conn=_FakeConn(), workdir="/repo")

    assert isinstance(receipt, LaunchReceipt)
    assert receipt.run_id == "workflow_abc123"
    assert receipt.spec_name == "bug_burn_wave_0"
    assert receipt.total_jobs == 1
    # Legacy fields preserved, enriched per Phase 1.1.c (expected-envelope-
    # vs-actual-truth-separation policy): inferred_stage, resolved_agent,
    # capabilities, write_envelope, expected_gates, verification_gaps.
    assert len(receipt.packet_map) == 1
    entry = receipt.packet_map[0]
    assert entry["label"] == "bug-authority"
    assert entry["bug_ref"] == "BUG-175EB9F3"
    assert entry["bug_refs"] is None
    assert entry["agent"] == "auto/build"
    assert entry["stage"] == "build"
    assert entry["inferred_stage"] == "build"
    assert entry["resolved_agent"] == "auto/build"
    assert entry["capabilities"] == ["capability.code.python"]
    assert entry["write_envelope"] == ["Code&DBs/Workflow/runtime/bugs.py"]
    assert entry["expected_gates"] == ["verify.bug-authority"]
    assert entry["verification_gaps"] == []

    command_kwargs = captured["kwargs"]
    # Kind must be in control_commands._LOCAL_AUTO_EXECUTE_REQUESTER_KINDS or
    # the submit command stays in REQUESTED state forever (no run_id, silent
    # 'approval_required' status). The specific caller is preserved via
    # dispatch_reason, not kind.
    assert command_kwargs["requested_by_kind"] == "workflow"
    assert command_kwargs["requested_by_ref"] == "bug_burn_wave_0"
    assert command_kwargs["spec_name"] == "bug_burn_wave_0"
    assert command_kwargs["total_jobs"] == 1
    assert command_kwargs["dispatch_reason"] == "launch_plan:bug_burn_wave_0"
    inline_spec = command_kwargs["inline_spec"]
    assert inline_spec["name"] == "bug_burn_wave_0"
    assert inline_spec["jobs"][0]["prompt"].startswith("PROMPT(")


def test_launch_plan_rejects_empty_packets() -> None:
    with pytest.raises(ValueError, match="at least one packet"):
        materialize_plan({"name": "empty", "packets": []}, conn=_FakeConn())


def test_job_prompt_is_enriched_with_bug_and_verify_context(monkeypatch) -> None:
    """Every job's prompt carries a Context: section when bug_refs + verify_refs exist."""
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    plan = {
        "name": "enriched",
        "packets": [
            {
                "description": "fix bug evidence authority",
                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                "stage": "build",
                "label": "bug-authority",
                "bug_ref": "BUG-175EB9F3",
                "read": ["Code&DBs/Workflow/runtime/audit.py"],
            }
        ],
    }
    spec_dict, _ = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")
    prompt = spec_dict["jobs"][0]["prompt"]

    # Base prompt from stub still in place.
    assert prompt.startswith("PROMPT(")
    # Context block appended.
    assert "\n---\nContext:" in prompt
    assert "Addresses bug(s): BUG-175EB9F3" in prompt
    assert "Reference files (read before writing): Code&DBs/Workflow/runtime/audit.py" in prompt
    assert "Must pass verifier(s): verify." in prompt  # stub produces verify.<label>


def test_job_prompt_has_no_context_block_when_no_extras(monkeypatch) -> None:
    """No bug_refs / verify_refs / read → plain base prompt, no Context: section."""

    def _plain_stub(intent_dict, *, conn):
        return (
            MaterializedSpec(
                prompt=f"PROMPT({intent_dict['description']})",
                scope_write=list(intent_dict.get("write") or []),
                capabilities=["cap"],
                tier="mid",
                label=intent_dict["stage"],
                task_type=intent_dict["stage"],
                verify_refs=None,  # no verifiers
            ),
            [],
        )

    monkeypatch.setattr(spec_materializer, "materialize_spec", _plain_stub)

    plan = {
        "name": "plain",
        "packets": [
            {
                "description": "just do it",
                "write": ["x.py"],
                "stage": "build",
                "label": "plain-1",
                # no bug_ref, no read
            }
        ],
    }
    spec_dict, _ = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")
    prompt = spec_dict["jobs"][0]["prompt"]
    assert "Context:" not in prompt
    assert "\n---\n" not in prompt


def test_job_prompt_enrichment_joins_multiple_bug_refs(monkeypatch) -> None:
    """When a cluster has bug_refs list, the Context lists all of them."""
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    plan = {
        "name": "cluster",
        "packets": [
            {
                "description": "resolve a cluster",
                "write": ["x.py"],
                "stage": "fix",
                "label": "cluster-1",
                "bug_refs": ["BUG-A", "BUG-B", "BUG-C"],
            }
        ],
    }
    spec_dict, _ = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")
    prompt = spec_dict["jobs"][0]["prompt"]
    assert "Addresses bug(s): BUG-A, BUG-B, BUG-C" in prompt


def test_launch_plan_deduplicates_colliding_labels(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    plan = {
        "name": "same_label_twice",
        "packets": [
            {"description": "first pass", "write": ["a.py"], "stage": "build", "label": "do-it"},
            {"description": "second pass", "write": ["b.py"], "stage": "build", "label": "do-it"},
        ],
    }
    spec_dict, _ = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")
    labels = [job["label"] for job in spec_dict["jobs"]]
    assert labels == ["do-it", "do-it__2"]


def _install_empty_binding(monkeypatch) -> None:
    """Stub bind_data_pills to return an empty BoundIntent.

    Most propose_plan tests don't care about binding — they care about the
    translation + preview path. This stub keeps binding-related warnings
    out of those tests.
    """
    import runtime.intent_binding as intent_binding_mod

    def _fake_bind(intent, *, conn, object_kinds=None):
        return intent_binding_mod.BoundIntent(intent=intent)

    monkeypatch.setattr(intent_binding_mod, "bind_data_pills", _fake_bind)


def test_propose_plan_returns_spec_preview_and_declarations_without_submit(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)
    _install_empty_binding(monkeypatch)

    # Preview stub — replicates the preview_workflow_execution payload shape
    # we care about without needing a real Postgres connection.
    def _fake_preview(conn, *, inline_spec, **_kwargs):
        assert inline_spec["workspace_ref"] == "workspace.default"
        assert inline_spec["runtime_profile_ref"] == "runtime.default"
        return {
            "action": "preview",
            "jobs": [
                {
                    "label": job["label"],
                    "resolved_agent": "openai/gpt-5.4-mini",
                    "route_status": "resolved",
                }
                for job in inline_spec["jobs"]
            ],
            "warnings": [],
        }

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(admission_mod, "preview_workflow_execution", _fake_preview)

    # Submit must NOT be called in preview mode. If it is, the test fails.
    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit_workflow_command should not run in preview")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    plan = {
        "name": "preview_wave",
        "packets": [
            {
                "description": "fix bug evidence authority",
                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                "stage": "build",
                "label": "bug-authority",
                "bug_ref": "BUG-175EB9F3",
            }
        ],
    }

    proposed = propose_plan(plan, conn=_FakeConn(), workdir="/repo")

    assert isinstance(proposed, ProposedPlan)
    assert proposed.spec_name == "preview_wave"
    assert proposed.total_jobs == 1
    assert proposed.spec_dict["jobs"][0]["label"] == "bug-authority"
    assert proposed.preview["jobs"][0]["resolved_agent"] == "openai/gpt-5.4-mini"

    # packet_declarations expose what the caller declared so Canvas / CLI
    # can render declared-vs-derived side by side.
    declaration = proposed.packet_declarations[0]
    assert declaration["label"] == "bug-authority"
    assert declaration["declared_description"] == "fix bug evidence authority"
    assert declaration["declared_write"] == ["Code&DBs/Workflow/runtime/bugs.py"]
    assert declaration["declared_stage"] == "build"
    assert declaration["declared_bug_ref"] == "BUG-175EB9F3"

    # Binding runs automatically; with nothing in the description to match,
    # all buckets stay empty and no binding warnings surface.
    assert proposed.binding_summary["totals"] == {"bound": 0, "ambiguous": 0, "unbound": 0}
    assert proposed.binding_summary["unbound_refs"] == []
    assert proposed.binding_summary["ambiguous_refs"] == []
    # This preview stub returns route_status=resolved, so no unresolved.
    assert proposed.unresolved_routes == []


def test_propose_plan_surfaces_unresolved_auto_routes_as_warning(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)
    _install_empty_binding(monkeypatch)

    def _unresolved_preview(conn, *, inline_spec, **_kwargs):
        return {
            "action": "preview",
            "jobs": [
                {
                    "label": inline_spec["jobs"][0]["label"],
                    "requested_agent": "auto/build",
                    "resolved_agent": None,
                    "route_status": "unresolved",
                    "route_reason": "task_type_router could not resolve",
                }
            ],
            "warnings": [],
        }

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(admission_mod, "preview_workflow_execution", _unresolved_preview)

    proposed = propose_plan(
        {
            "name": "unresolved_wave",
            "packets": [
                {"description": "do a thing", "write": ["x.py"], "stage": "build", "label": "pkt-1"}
            ],
        },
        conn=_FakeConn(),
        workdir="/repo",
    )

    assert len(proposed.unresolved_routes) == 1
    entry = proposed.unresolved_routes[0]
    assert entry["label"] == "pkt-1"
    assert entry["route_status"] == "unresolved"
    assert "task_type_router" in (entry["route_reason"] or "")
    assert any("unresolved agent routes" in w for w in proposed.warnings)
    assert any("pkt-1" in w for w in proposed.warnings)


def test_propose_plan_appends_bound_data_fields_to_job_prompt(monkeypatch) -> None:
    """Bound pills from binding surface in the job prompt so agents see typed fields."""
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    import runtime.intent_binding as intent_binding_mod

    def _fake_bind_with_bound(intent, *, conn, object_kinds=None):
        return intent_binding_mod.BoundIntent(
            intent=intent,
            bound=[
                intent_binding_mod.BoundPill(
                    matched_span="users.first_name",
                    object_kind="users",
                    field_path="first_name",
                    field_kind="text",
                    source="auto",
                    display_order=1,
                ),
                intent_binding_mod.BoundPill(
                    matched_span="users.email",
                    object_kind="users",
                    field_path="email",
                    field_kind="text",
                    source="auto",
                    display_order=2,
                ),
            ],
        )

    monkeypatch.setattr(intent_binding_mod, "bind_data_pills", _fake_bind_with_bound)

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(
        admission_mod,
        "preview_workflow_execution",
        lambda conn, *, inline_spec, **_kwargs: {"action": "preview", "jobs": [], "warnings": []},
    )

    proposed = propose_plan(
        {
            "name": "bound_pills_prompt",
            "packets": [
                {
                    "description": "update users.first_name when users.email changes",
                    "write": ["src/profile.py"],
                    "stage": "build",
                    "label": "update-name",
                }
            ],
        },
        conn=_FakeConn(),
        workdir="/repo",
    )

    prompt = proposed.spec_dict["jobs"][0]["prompt"]
    assert "Bound data fields:" in prompt
    assert "users.first_name (text)" in prompt
    assert "users.email (text)" in prompt


def test_propose_plan_omits_bound_data_fields_line_when_no_bound_pills(monkeypatch) -> None:
    """No bound pills → no Bound data fields line in prompt."""
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)
    _install_empty_binding(monkeypatch)

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(
        admission_mod,
        "preview_workflow_execution",
        lambda conn, *, inline_spec, **_kwargs: {"action": "preview", "jobs": [], "warnings": []},
    )

    proposed = propose_plan(
        {
            "name": "no_pills",
            "packets": [
                {
                    "description": "just do it",
                    "write": ["x.py"],
                    "stage": "build",
                    "label": "no-pills",
                }
            ],
        },
        conn=_FakeConn(),
        workdir="/repo",
    )

    assert "Bound data fields:" not in proposed.spec_dict["jobs"][0]["prompt"]


def test_propose_plan_surfaces_unbound_data_pills_as_warnings(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    import runtime.intent_binding as intent_binding_mod

    def _fake_bind(intent, *, conn, object_kinds=None):
        # Simulate one unbound reference per packet description.
        return intent_binding_mod.BoundIntent(
            intent=intent,
            unbound=[
                intent_binding_mod.UnboundCandidate(
                    matched_span="users.first_nm",
                    object_kind="users",
                    field_path="first_nm",
                    reason="field_path_not_in_object",
                )
            ],
        )

    monkeypatch.setattr(intent_binding_mod, "bind_data_pills", _fake_bind)

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(
        admission_mod,
        "preview_workflow_execution",
        lambda conn, *, inline_spec, **_kwargs: {"action": "preview", "jobs": [], "warnings": []},
    )

    plan = {
        "name": "typo_wave",
        "packets": [
            {
                "description": "copy users.first_nm into the profile",
                "write": ["profile.py"],
                "stage": "build",
                "label": "typo-packet",
            }
        ],
    }

    proposed = propose_plan(plan, conn=_FakeConn(), workdir="/repo")

    summary = proposed.binding_summary
    assert summary["totals"]["unbound"] == 1
    assert summary["totals"]["bound"] == 0
    assert summary["unbound_refs"][0]["label"] == "typo-packet"
    assert summary["unbound_refs"][0]["matched_span"] == "users.first_nm"
    assert summary["unbound_refs"][0]["reason"] == "field_path_not_in_object"

    # Warning surfaces the unbound count + affected packet labels so the
    # caller fixes typos before spending compile time.
    assert any("unbound data-pill reference" in w for w in proposed.warnings)
    assert any("typo-packet" in w for w in proposed.warnings)

    # Per-packet data_pills carry the full detail for display.
    pills = proposed.packet_declarations[0]["data_pills"]
    assert pills["unbound"][0]["matched_span"] == "users.first_nm"


class _BugFetchConn:
    """Conn stand-in for _plan_packets_from_bugs SQL lookup.

    ``execute`` returns the bugs that match the IN-clause. Any bug IDs
    not in ``rows_by_id`` simulate missing rows (derive_bug_packets drops
    them silently).
    """

    def __init__(self, rows_by_id: dict[str, dict[str, object]]) -> None:
        self._rows_by_id = rows_by_id

    def execute(self, query, *params):
        # Defensive: only handle the bugs IN-query this test needs.
        assert query.startswith("SELECT bug_id, title"), f"unexpected query: {query!r}"
        return [self._rows_by_id[bug_id] for bug_id in params if bug_id in self._rows_by_id]


class _MultiSourceConn:
    """Conn that dispatches bug / roadmap / idea / friction queries by prefix."""

    def __init__(
        self,
        *,
        bugs: dict[str, dict[str, object]] | None = None,
        roadmap_items: dict[str, dict[str, object]] | None = None,
        ideas: dict[str, dict[str, object]] | None = None,
        friction_events: dict[str, dict[str, object]] | None = None,
    ) -> None:
        self._bugs = bugs or {}
        self._roadmap_items = roadmap_items or {}
        self._ideas = ideas or {}
        self._friction = friction_events or {}

    def execute(self, query, *params):
        if query.startswith("SELECT bug_id, title"):
            return [self._bugs[bid] for bid in params if bid in self._bugs]
        if query.startswith("SELECT roadmap_item_id"):
            return [self._roadmap_items[rid] for rid in params if rid in self._roadmap_items]
        if query.startswith("SELECT idea_id"):
            return [self._ideas[iid] for iid in params if iid in self._ideas]
        if query.startswith("SELECT event_id, friction_type"):
            return [self._friction[eid] for eid in params if eid in self._friction]
        raise AssertionError(f"unexpected query: {query!r}")


def _fake_derive(**kwargs):
    """Stand-in for derive_bug_packets used by from_bugs tests.

    Returns two clusters across two waves so we can assert wave→label
    depends_on wiring. The shape matches the real function's output.
    """
    bugs = kwargs["bugs"]
    program_id = kwargs["program_id"]
    bugs_by_id = {bug["bug_id"]: bug for bug in bugs}
    wave_0_ids = [bug_id for bug_id in ("BUG-AUTH-1", "BUG-AUTH-2") if bug_id in bugs_by_id]
    wave_1_ids = [bug_id for bug_id in ("BUG-RUN-1",) if bug_id in bugs_by_id]
    derived: list[dict[str, object]] = []
    if wave_0_ids:
        derived.append(
            {
                "packet_id": f"{program_id}.w0-authority-repair",
                "packet_slug": "w0-authority-repair",
                "packet_kind": "authority_bug_repair",
                "wave_id": "W0",
                "depends_on_wave": [],
                "lane_id": "authority_bug_system",
                "lane_label": "Authority / bug system",
                "bug_ids": wave_0_ids,
                "bug_titles": [bugs_by_id[b].get("title") for b in wave_0_ids],
                "highest_severity": "P1",
                "authority_owner": "bugs_authority",
                "cluster": {
                    "cluster_key": "authority_repair",
                    "label": "Authority repair",
                    "reason_code": "bug_cluster.authority",
                },
                "verification_surface": "workflow orient + bug stats must return clean",
                "done_criteria": ["Authority path deterministic", "Bugs prove closure"],
                "stop_boundary": "Do not widen beyond authority repair",
                "replay_ready_count": len(wave_0_ids),
                "replay_blocked_bug_ids": [],
                "blocked_reason_codes": [],
                "categories": ["ARCHITECTURE"],
            }
        )
    if wave_1_ids:
        derived.append(
            {
                "packet_id": f"{program_id}.w1-runtime-repair",
                "packet_slug": "w1-runtime-repair",
                "packet_kind": "runtime_bug_repair",
                "wave_id": "W1",
                "depends_on_wave": ["W0"],
                "lane_id": "workflow_runtime",
                "lane_label": "Workflow / runtime",
                "bug_ids": wave_1_ids,
                "bug_titles": [bugs_by_id[b].get("title") for b in wave_1_ids],
                "highest_severity": "P2",
                "authority_owner": "workflow_runtime",
                "cluster": {
                    "cluster_key": "runtime_repair",
                    "label": "Runtime repair",
                    "reason_code": "bug_cluster.runtime",
                },
                "verification_surface": "focused runtime tests + evidence",
                "done_criteria": ["Runtime path stable"],
                "stop_boundary": "Do not touch unrelated UI",
                "replay_ready_count": len(wave_1_ids),
                "replay_blocked_bug_ids": [],
                "blocked_reason_codes": [],
                "categories": ["RUNTIME"],
            }
        )
    return derived


def test_compile_plan_collects_all_packet_failures_before_raising(monkeypatch) -> None:
    """Atomic materialize_plan: caller sees every failing packet, not just the first."""

    def _failing_on_marked_packets(intent_dict, *, conn):
        if intent_dict.get("label", "").startswith("bad-"):
            raise ValueError(f"synthetic failure for {intent_dict['label']}")
        return (
            MaterializedSpec(
                prompt=f"PROMPT({intent_dict['description']})",
                scope_write=list(intent_dict["write"]),
                capabilities=["cap"],
                tier="mid",
                label=intent_dict["label"],
                task_type=intent_dict["stage"],
                verify_refs=["v"],
            ),
            [],
        )

    monkeypatch.setattr(spec_materializer, "materialize_spec", _failing_on_marked_packets)

    plan = {
        "name": "mixed",
        "packets": [
            {"description": "ok 1", "write": ["a.py"], "stage": "build", "label": "good-1"},
            {"description": "bad 1", "write": ["b.py"], "stage": "build", "label": "bad-1"},
            {"description": "ok 2", "write": ["c.py"], "stage": "build", "label": "good-2"},
            {"description": "bad 2", "write": ["d.py"], "stage": "build", "label": "bad-2"},
        ],
    }

    with pytest.raises(MaterializePlanError) as exc_info:
        materialize_plan(plan, conn=_FakeConn(), workdir="/repo")

    failures = exc_info.value.failures
    assert len(failures) == 2
    labels = {entry["label"] for entry in failures}
    assert labels == {"bad-1", "bad-2"}
    # Rendered message lists both packets with their indices.
    message = str(exc_info.value)
    assert "2 packet(s) failed" in message
    assert "packet[1] label='bad-1'" in message
    assert "packet[3] label='bad-2'" in message


def test_compile_plan_deterministic_workflow_id_when_not_supplied(monkeypatch) -> None:
    """Same plan content → same workflow_id across repeated compiles."""
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    plan = {
        "name": "idempotent",
        "packets": [
            {"description": "thing one", "write": ["x.py"], "stage": "build", "label": "t1"},
            {"description": "thing two", "write": ["y.py"], "stage": "build", "label": "t2"},
        ],
    }

    spec_a, _ = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")
    spec_b, _ = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")

    assert spec_a["workflow_id"] == spec_b["workflow_id"]
    assert spec_a["workflow_id"].startswith("plan.")
    # No random uuid noise; the 16-char hash is stable.
    assert len(spec_a["workflow_id"]) == len("plan.") + 16


def test_compile_plan_different_plans_get_different_workflow_ids(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    plan_a = {
        "name": "plan_a",
        "packets": [{"description": "one", "write": ["x.py"], "stage": "build"}],
    }
    plan_b = {
        "name": "plan_b",  # different name
        "packets": [{"description": "one", "write": ["x.py"], "stage": "build"}],
    }

    spec_a, _ = materialize_plan(plan_a, conn=_FakeConn(), workdir="/repo")
    spec_b, _ = materialize_plan(plan_b, conn=_FakeConn(), workdir="/repo")

    assert spec_a["workflow_id"] != spec_b["workflow_id"]


def test_explicit_workflow_id_wins_over_hash(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    plan = {
        "name": "override_id",
        "workflow_id": "caller_chosen_id",
        "packets": [{"description": "x", "write": ["x.py"], "stage": "build"}],
    }
    spec, _ = materialize_plan(plan, conn=_FakeConn(), workdir="/repo")
    assert spec["workflow_id"] == "caller_chosen_id"


def test_coerce_plan_with_from_bugs_materializes_clustered_packets(monkeypatch) -> None:
    import runtime.bug_resolution_program as bug_program_mod

    monkeypatch.setattr(bug_program_mod, "derive_bug_packets", _fake_derive)

    conn = _BugFetchConn(
        {
            "BUG-AUTH-1": {"bug_id": "BUG-AUTH-1", "title": "Authority 1", "replay_ready": True},
            "BUG-AUTH-2": {"bug_id": "BUG-AUTH-2", "title": "Authority 2", "replay_ready": True},
            "BUG-RUN-1": {"bug_id": "BUG-RUN-1", "title": "Runtime 1", "replay_ready": True},
        }
    )

    plan = _coerce_plan(
        {
            "name": "bug_wave_burn",
            "from_bugs": ["BUG-AUTH-1", "BUG-AUTH-2", "BUG-RUN-1"],
        },
        conn=conn,
    )

    assert plan.name == "bug_wave_burn"
    assert plan.from_bugs == ["BUG-AUTH-1", "BUG-AUTH-2", "BUG-RUN-1"]
    assert len(plan.packets) == 2
    auth, runtime = plan.packets

    # Cluster 1: authority wave, no deps, bug_refs carries both IDs.
    assert auth.label == "w0-authority-repair"
    # Bug-derived packets keep fix intent in the prompt/bug refs, but route
    # through the registered coding lane instead of inventing auto/fix.
    assert auth.stage == "build"
    assert auth.write == ["."]
    assert auth.bug_refs == ["BUG-AUTH-1", "BUG-AUTH-2"]
    assert auth.bug_ref == "BUG-AUTH-1"  # primary
    assert auth.depends_on is None
    assert "Authority repair" in auth.description
    assert "BUG-AUTH-1, BUG-AUTH-2" in auth.description
    assert "Done when all of:" in auth.description

    # Cluster 2: runtime wave, depends on the authority cluster's label.
    assert runtime.label == "w1-runtime-repair"
    assert runtime.depends_on == ["w0-authority-repair"]
    assert runtime.bug_refs == ["BUG-RUN-1"]


def test_coerce_plan_from_bugs_uses_repo_paths_from_bug_authority(monkeypatch) -> None:
    import runtime.bug_resolution_program as bug_program_mod

    monkeypatch.setattr(bug_program_mod, "derive_bug_packets", _fake_derive)

    conn = _BugFetchConn(
        {
            "BUG-AUTH-1": {
                "bug_id": "BUG-AUTH-1",
                "title": "Authority 1",
                "description": (
                    "Evidence: Code&DBs/Workflow/runtime/workflow/_shared.py::_circuit_breakers "
                    "and Code&DBs/Workflow/runtime/workflow_validation.py must be updated."
                ),
                "replay_ready": True,
            },
        }
    )

    plan = _coerce_plan(
        {"name": "bug_wave_burn", "from_bugs": ["BUG-AUTH-1"]},
        conn=conn,
    )

    packet = plan.packets[0]
    expected_paths = [
        "Code&DBs/Workflow/runtime/workflow/_shared.py",
        "Code&DBs/Workflow/runtime/workflow_validation.py",
    ]
    assert packet.write == expected_paths
    assert packet.read == expected_paths
    assert "Derived repo scope:" in packet.description
    assert "Code&DBs/Workflow/runtime/workflow/_shared.py" in packet.description


def test_coerce_plan_from_bugs_derives_replay_state_without_bug_columns(monkeypatch) -> None:
    import runtime.bug_resolution_program as bug_program_mod
    import runtime.bug_tracker as bug_tracker_mod

    captured: dict[str, object] = {}

    def _capture_derive(**kwargs):
        captured.update(kwargs)
        return _fake_derive(**kwargs)

    class _ReplayTracker:
        def __init__(self, conn) -> None:
            self.conn = conn

        def replay_hint(self, bug_id, *, receipt_limit=1, allow_backfill=True):
            assert allow_backfill is False
            return {"available": True, "reason_code": "bug.replay_ready"}

    class _NoReplayColumnConn(_BugFetchConn):
        def execute(self, query, *params):
            assert "replay_ready" not in query
            assert "replay_reason_code" not in query
            return super().execute(query, *params)

    monkeypatch.setattr(bug_program_mod, "derive_bug_packets", _capture_derive)
    monkeypatch.setattr(bug_tracker_mod, "BugTracker", _ReplayTracker)

    plan = _coerce_plan(
        {
            "name": "bug_wave_burn",
            "from_bugs": ["BUG-AUTH-1"],
        },
        conn=_NoReplayColumnConn(
            {
                "BUG-AUTH-1": {
                    "bug_id": "BUG-AUTH-1",
                    "title": "Authority 1",
                    "status": "OPEN",
                },
            }
        ),
    )

    assert len(plan.packets) == 1
    assert captured["bugs"][0]["replay_ready"] is True
    assert captured["bugs"][0]["replay_reason_code"] == "bug.replay_ready"


def test_fix_stage_routes_through_build_lane_without_losing_task_type(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)

    spec_dict, _ = materialize_plan(
        {
            "name": "fix_route_alias",
            "packets": [
                {
                    "description": "fix a Python bug",
                    "write": ["Code&DBs/Workflow/runtime/example.py"],
                    "stage": "fix",
                    "label": "fix-python-bug",
                }
            ],
        },
        conn=_FakeConn(),
        workdir="/repo",
    )

    job = spec_dict["jobs"][0]
    assert job["agent"] == "auto/build"
    assert job["task_type"] == "fix"


def test_coerce_plan_rejects_both_packets_and_from_bugs(monkeypatch) -> None:
    with pytest.raises(ValueError, match="either explicit 'packets'"):
        _coerce_plan(
            {
                "name": "ambiguous",
                "packets": [{"description": "x", "write": ["a"], "stage": "build"}],
                "from_bugs": ["BUG-1"],
            },
            conn=_BugFetchConn({}),
        )


def test_coerce_plan_from_bugs_rejects_empty_result(monkeypatch) -> None:
    import runtime.bug_resolution_program as bug_program_mod

    monkeypatch.setattr(bug_program_mod, "derive_bug_packets", lambda **_kwargs: [])

    conn = _BugFetchConn({"BUG-KNOWN": {"bug_id": "BUG-KNOWN", "title": "x", "replay_ready": True}})
    with pytest.raises(ValueError, match="no packets could be materialized"):
        _coerce_plan(
            {"name": "empty", "from_bugs": ["BUG-MISSING"]},
            conn=conn,
        )


def test_coerce_plan_with_from_roadmap_items_materializes_packets() -> None:
    conn = _MultiSourceConn(
        roadmap_items={
            "roadmap_item.ship_ui_polish": {
                "roadmap_item_id": "roadmap_item.ship_ui_polish",
                "title": "Ship UI polish",
                "summary": "Tidy up Canvas dashboard spacing, labels, and hover states.",
                "acceptance_criteria": {
                    "must_have": [
                        "Canvas dashboard hover states consistent",
                        "Spacing scale applied to every panel",
                    ],
                    "outcome_gate": "Operator reports no remaining visual friction in Canvas.",
                },
                "priority": "p2",
                "lifecycle": "planned",
                "source_bug_id": None,
            },
            "roadmap_item.retire_legacy_api": {
                "roadmap_item_id": "roadmap_item.retire_legacy_api",
                "title": "Retire legacy v1 API",
                "summary": "Remove legacy /api/v1 routes after v2 migration is complete.",
                "acceptance_criteria": {"must_have": ["No callers remain on /api/v1"]},
                "priority": "p1",
                "lifecycle": "planned",
                "source_bug_id": "BUG-LEGACY-API",
            },
            "roadmap_item.already_done": {
                "roadmap_item_id": "roadmap_item.already_done",
                "title": "Old stuff",
                "summary": "Completed long ago.",
                "acceptance_criteria": {},
                "priority": "p3",
                "lifecycle": "completed",
                "source_bug_id": None,
            },
        },
    )

    plan = _coerce_plan(
        {
            "name": "roadmap_landing",
            "from_roadmap_items": [
                "roadmap_item.ship_ui_polish",
                "roadmap_item.retire_legacy_api",
                "roadmap_item.already_done",
            ],
        },
        conn=conn,
    )

    # Completed items get dropped — only the two active ones become packets.
    assert len(plan.packets) == 2
    ui, legacy = plan.packets

    assert ui.label == "ship_ui_polish"
    assert ui.stage == "build"  # no source_bug_id
    assert ui.bug_ref is None
    assert "Roadmap item: Ship UI polish" in ui.description
    assert "Priority: p2" in ui.description
    assert "Must have:" in ui.description
    assert "Canvas dashboard hover states consistent" in ui.description
    assert "Outcome gate:" in ui.description

    assert legacy.stage == "fix"  # source_bug_id present
    assert legacy.bug_ref == "BUG-LEGACY-API"
    assert "Retire legacy v1 API" in legacy.description


def test_coerce_plan_combines_from_bugs_and_from_roadmap_items(monkeypatch) -> None:
    import runtime.bug_resolution_program as bug_program_mod

    monkeypatch.setattr(bug_program_mod, "derive_bug_packets", _fake_derive)

    conn = _MultiSourceConn(
        bugs={"BUG-AUTH-1": {"bug_id": "BUG-AUTH-1", "title": "a", "replay_ready": True}},
        roadmap_items={
            "roadmap_item.ship_it": {
                "roadmap_item_id": "roadmap_item.ship_it",
                "title": "Ship it",
                "summary": "Launch the thing.",
                "acceptance_criteria": {"must_have": ["launched"]},
                "priority": "p1",
                "lifecycle": "planned",
                "source_bug_id": None,
            }
        },
    )

    plan = _coerce_plan(
        {
            "name": "combined",
            "from_bugs": ["BUG-AUTH-1"],
            "from_roadmap_items": ["roadmap_item.ship_it"],
        },
        conn=conn,
    )

    # Bug cluster first (extends), then roadmap item.
    assert len(plan.packets) == 2
    assert plan.packets[0].bug_refs == ["BUG-AUTH-1"]  # bug cluster
    assert plan.packets[1].label == "ship_it"  # roadmap item


def test_coerce_plan_with_from_ideas_materializes_open_ideas_only() -> None:
    conn = _MultiSourceConn(
        ideas={
            "operator_idea.ingest_shopify_orders": {
                "idea_id": "operator_idea.ingest_shopify_orders",
                "title": "Ingest Shopify orders",
                "summary": "Pull Shopify orders into the canonical order dataset daily.",
                "status": "open",
                "owner_ref": "nate@praxis",
                "decision_ref": "decision.2026-04-15.data-ingest-scope",
            },
            "operator_idea.canvas_inbox_digest": {
                "idea_id": "operator_idea.canvas_inbox_digest",
                "title": "Canvas inbox digest",
                "summary": "Daily digest of Canvas notifications.",
                "status": "open",
                "owner_ref": None,
                "decision_ref": "decision.2026-04-20.canvas-surfaces",
            },
            "operator_idea.old_promoted": {
                "idea_id": "operator_idea.old_promoted",
                "title": "Already promoted",
                "summary": "Moved to roadmap already.",
                "status": "promoted",
                "owner_ref": None,
                "decision_ref": "decision.2026-04-01.whatever",
            },
        },
    )

    plan = _coerce_plan(
        {
            "name": "idea_intake",
            "from_ideas": [
                "operator_idea.ingest_shopify_orders",
                "operator_idea.canvas_inbox_digest",
                "operator_idea.old_promoted",
            ],
        },
        conn=conn,
    )

    assert len(plan.packets) == 2  # promoted idea dropped
    assert plan.packets[0].stage == "build"
    assert plan.packets[0].label == "ingest_shopify_orders"
    assert "Operator idea: Ingest Shopify orders" in plan.packets[0].description
    assert "Owner: nate@praxis" in plan.packets[0].description
    assert plan.packets[1].label == "canvas_inbox_digest"


def test_coerce_plan_with_from_friction_materializes_fix_packets() -> None:
    conn = _MultiSourceConn(
        friction_events={
            "friction.workflow_submit_001": {
                "event_id": "friction.workflow_submit_001",
                "friction_type": "submission_required_missing",
                "source": "workflow.submit",
                "job_label": "smoke_submit",
                "message": "Sealed submission payload was not present.",
                "timestamp": "2026-04-24T09:00:00+00:00",
            },
            "friction.catalog_hang_017": {
                "event_id": "friction.catalog_hang_017",
                "friction_type": "catalog_timeout",
                "source": "mcp.catalog",
                "job_label": "list_tools",
                "message": "Catalog load exceeded 30s.",
                "timestamp": "2026-04-24T10:15:00+00:00",
            },
        },
    )

    plan = _coerce_plan(
        {
            "name": "friction_burn",
            "from_friction": [
                "friction.workflow_submit_001",
                "friction.catalog_hang_017",
            ],
        },
        conn=conn,
    )

    assert len(plan.packets) == 2
    assert all(packet.stage == "fix" for packet in plan.packets)
    first = plan.packets[0]
    assert first.label.startswith("friction_")
    assert "Friction event: submission_required_missing" in first.description
    assert "Source: workflow.submit" in first.description
    assert "Sealed submission payload" in first.description


def test_coerce_plan_combines_all_four_source_shortcuts(monkeypatch) -> None:
    import runtime.bug_resolution_program as bug_program_mod

    monkeypatch.setattr(bug_program_mod, "derive_bug_packets", _fake_derive)

    conn = _MultiSourceConn(
        bugs={"BUG-AUTH-1": {"bug_id": "BUG-AUTH-1", "title": "a", "replay_ready": True}},
        roadmap_items={
            "roadmap_item.ship": {
                "roadmap_item_id": "roadmap_item.ship",
                "title": "Ship it",
                "summary": "Launch.",
                "acceptance_criteria": {"must_have": ["launched"]},
                "priority": "p1",
                "lifecycle": "planned",
                "source_bug_id": None,
            }
        },
        ideas={
            "operator_idea.something": {
                "idea_id": "operator_idea.something",
                "title": "Something",
                "summary": "An idea.",
                "status": "open",
                "owner_ref": None,
                "decision_ref": "decision.x",
            }
        },
        friction_events={
            "friction.evt_1": {
                "event_id": "friction.evt_1",
                "friction_type": "thing",
                "source": "system",
                "job_label": "job",
                "message": "broke",
                "timestamp": "2026-04-24T00:00:00+00:00",
            }
        },
    )

    plan = _coerce_plan(
        {
            "name": "combined_all",
            "from_bugs": ["BUG-AUTH-1"],
            "from_roadmap_items": ["roadmap_item.ship"],
            "from_ideas": ["operator_idea.something"],
            "from_friction": ["friction.evt_1"],
        },
        conn=conn,
    )

    # Source order: bugs, roadmap, ideas, friction.
    labels = [p.label for p in plan.packets]
    assert len(plan.packets) == 4
    assert labels[0] == "w0-authority-repair"  # bug cluster
    assert labels[1] == "ship"  # roadmap
    assert labels[2] == "something"  # idea
    assert labels[3].startswith("friction_")


def test_propose_plan_from_bugs_warns_on_workspace_root_scope(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)
    _install_empty_binding(monkeypatch)

    import runtime.bug_resolution_program as bug_program_mod

    monkeypatch.setattr(bug_program_mod, "derive_bug_packets", _fake_derive)

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(
        admission_mod,
        "preview_workflow_execution",
        lambda conn, *, inline_spec, **_kwargs: {"action": "preview", "jobs": [], "warnings": []},
    )

    conn = _BugFetchConn(
        {"BUG-AUTH-1": {"bug_id": "BUG-AUTH-1", "title": "Authority 1", "replay_ready": True}}
    )

    proposed = propose_plan(
        {"name": "bug_wave", "from_bugs": ["BUG-AUTH-1"]},
        conn=conn,
        workdir="/repo",
    )

    assert any("workspace root" in warning for warning in proposed.warnings)
    assert proposed.spec_dict["jobs"][0]["bug_refs"] == ["BUG-AUTH-1"]
    assert proposed.packet_declarations[0]["declared_bug_refs"] == ["BUG-AUTH-1"]


def test_launch_proposed_submits_previously_built_spec(monkeypatch) -> None:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)
    _install_empty_binding(monkeypatch)

    def _fake_preview(conn, *, inline_spec, **_kwargs):
        return {"action": "preview", "jobs": [], "warnings": []}

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(admission_mod, "preview_workflow_execution", _fake_preview)

    proposed = propose_plan(
        {
            "name": "two_phase",
            "packets": [
                {"description": "do a thing", "write": ["x.py"], "stage": "build", "label": "thing-1"},
            ],
        },
        conn=_FakeConn(),
        workdir="/repo",
    )

    captured: dict[str, object] = {}

    def _fake_submit_command(conn, **kwargs):
        captured["kwargs"] = kwargs
        return {
            "run_id": "workflow_def456",
            "status": "queued",
            "total_jobs": kwargs["total_jobs"],
            "spec_name": kwargs["spec_name"],
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit_command)

    receipt = launch_proposed(proposed, conn=_FakeConn())

    assert isinstance(receipt, LaunchReceipt)
    assert receipt.run_id == "workflow_def456"
    assert receipt.spec_name == "two_phase"
    assert receipt.total_jobs == 1
    assert captured["kwargs"]["dispatch_reason"] == "launch_proposed:two_phase"
    assert captured["kwargs"]["inline_spec"] is proposed.spec_dict


def _build_proposed_for_approval(monkeypatch) -> ProposedPlan:
    """Build a fresh ProposedPlan to feed approval tests."""
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)
    _install_empty_binding(monkeypatch)

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(
        admission_mod,
        "preview_workflow_execution",
        lambda conn, *, inline_spec, **_kwargs: {"action": "preview", "jobs": [], "warnings": []},
    )

    return propose_plan(
        {
            "name": "approval_target",
            "packets": [
                {"description": "do the thing", "write": ["x.py"], "stage": "build", "label": "thing-1"},
            ],
        },
        conn=_FakeConn(),
        workdir="/repo",
    )


def test_approve_proposed_plan_records_approver_and_hash(monkeypatch) -> None:
    proposed = _build_proposed_for_approval(monkeypatch)

    approved = approve_proposed_plan(
        proposed,
        approved_by="nate@praxis",
        approval_note="Looks good; proceed.",
    )

    assert isinstance(approved, ApprovedPlan)
    assert approved.approved_by == "nate@praxis"
    assert approved.approval_note == "Looks good; proceed."
    assert approved.approved_at.endswith("+00:00")  # ISO-8601 UTC
    assert len(approved.proposal_hash) >= 16
    assert approved.proposed is proposed


def test_approve_proposed_plan_rejects_empty_approver(monkeypatch) -> None:
    proposed = _build_proposed_for_approval(monkeypatch)
    with pytest.raises(ValueError, match="approved_by is required"):
        approve_proposed_plan(proposed, approved_by="   ")


def test_approve_proposed_plan_requires_provider_freshness_gate() -> None:
    proposed = ProposedPlan(
        spec_dict={"name": "proof_gate", "jobs": []},
        preview={},
        warnings=[],
        workflow_id="plan.proof_gate",
        spec_name="proof_gate",
        total_jobs=0,
        packet_declarations=[],
        binding_summary={"totals": {"bound": 0, "ambiguous": 0, "unbound": 0}, "unbound_refs": [], "ambiguous_refs": []},
        unresolved_routes=[],
    )

    with pytest.raises(
        spec_materializer.ProviderFreshnessGateError,
        match="fresh provider route truth or a recent provider availability refresh receipt",
    ):
        approve_proposed_plan(proposed, approved_by="nate@praxis")


def test_approve_proposed_plan_accepts_recent_refresh_receipt_evidence() -> None:
    proposed = ProposedPlan(
        spec_dict={"name": "proof_gate", "jobs": []},
        preview={},
        warnings=[],
        workflow_id="plan.proof_gate",
        spec_name="proof_gate",
        total_jobs=0,
        packet_declarations=[],
        binding_summary={"totals": {"bound": 0, "ambiguous": 0, "unbound": 0}, "unbound_refs": [], "ambiguous_refs": []},
        unresolved_routes=[],
        provider_freshness={
            "refresh_receipt_ref": "receipt:provider_availability_refresh:abc123",
            "refresh_receipt_issued_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    approved = approve_proposed_plan(proposed, approved_by="nate@praxis")

    assert approved.approved_by == "nate@praxis"


def test_approve_proposed_plan_accepts_preview_route_truth_freshness(monkeypatch) -> None:
    proposed = _build_proposed_for_approval(monkeypatch)

    assert proposed.provider_freshness is not None
    assert proposed.provider_freshness["route_truth_ref"].startswith("preview:")
    assert proposed.provider_freshness["route_truth_checked_at"]

    approved = approve_proposed_plan(proposed, approved_by="nate@praxis")

    assert approved.approved_by == "nate@praxis"


def test_launch_approved_submits_on_matching_hash(monkeypatch) -> None:
    proposed = _build_proposed_for_approval(monkeypatch)
    approved = approve_proposed_plan(proposed, approved_by="nate@praxis")

    captured: dict[str, object] = {}

    def _fake_submit(conn, **kwargs):
        captured["kwargs"] = kwargs
        return {
            "run_id": "workflow_approved_001",
            "status": "queued",
            "total_jobs": kwargs["total_jobs"],
            "spec_name": kwargs["spec_name"],
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit)

    receipt = launch_approved(approved, conn=_FakeConn())

    assert receipt.run_id == "workflow_approved_001"
    # See comment in test_launch_plan_routes_through_command_bus: kind must
    # be in the auto-execute allowlist or submit hangs in REQUESTED state.
    assert captured["kwargs"]["requested_by_kind"] == "workflow"
    # requested_by_ref defaults to approved_by for audit trail.
    assert captured["kwargs"]["requested_by_ref"] == "nate@praxis"


def test_launch_approved_rejects_stale_provider_freshness(monkeypatch) -> None:
    proposed = _build_proposed_for_approval(monkeypatch)
    approved = approve_proposed_plan(proposed, approved_by="nate@praxis")

    # BUG-72420B56 evidence: freshness must remain machine-checkable at launch,
    # not only at approval time.
    approved.proposed.provider_freshness["route_truth_checked_at"] = "2020-01-01T00:00:00+00:00"

    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit_workflow_command should not run on stale freshness evidence")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    with pytest.raises(
        spec_materializer.ProviderFreshnessGateError,
        match="provider freshness evidence is stale",
    ):
        launch_approved(approved, conn=_FakeConn())


def test_launch_approved_fails_closed_on_tampered_spec(monkeypatch) -> None:
    proposed = _build_proposed_for_approval(monkeypatch)
    approved = approve_proposed_plan(proposed, approved_by="nate@praxis")

    # Simulate tampering — mutate the spec_dict after approval. Since
    # ProposedPlan is frozen we can still mutate its contained dicts.
    approved.proposed.spec_dict["name"] = "TAMPERED"

    def _forbid_submit(*_args, **_kwargs):
        raise AssertionError("submit_workflow_command should not run on tampered plans")

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _forbid_submit)

    with pytest.raises(ApprovalHashMismatchError, match="hash mismatch"):
        launch_approved(approved, conn=_FakeConn())


class _PaidModelConn:
    def __init__(self) -> None:
        self.bound_run_id: str | None = None
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.execute_calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))
        if "FROM private_model_access_control_matrix" in sql:
            return [
                {
                    "runtime_profile_ref": "runtime.default",
                    "job_type": "build",
                    "transport_type": "API",
                    "adapter_type": "llm_task",
                    "provider_slug": "fireworks",
                    "model_slug": "kimi-k2p6",
                    "cost_structure": "metered_api",
                    "cost_metadata": {"billing_model": "usage_based"},
                    "control_enabled": False,
                    "control_state": "denied",
                    "control_scope": "hard_off",
                    "control_reason_code": "paid_model.default_hard_off",
                    "control_operator_message": "Paid model hard-off by default",
                    "control_decision_ref": "decision.paid-model-hard-off",
                }
            ]
        if "UPDATE private_paid_model_access_leases" in sql:
            self.bound_run_id = args[1]
            return [
                {
                    "lease_id": args[0][0],
                    "runtime_profile_ref": "runtime.default",
                    "job_type": "build",
                    "transport_type": "API",
                    "adapter_type": "llm_task",
                    "provider_slug": "fireworks",
                    "model_slug": "kimi-k2p6",
                    "approval_ref": "approval.fireworks.kimi",
                    "proposal_hash": "proposal-hash",
                    "status": "bound",
                    "bound_run_id": args[1],
                    "expires_at": "2026-05-01T12:30:00+00:00",
                    "cost_posture": {"cost_structure": "metered_api"},
                }
            ]
        return []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        return {
            "lease_id": args[0],
            "runtime_profile_ref": args[1],
            "job_type": args[2],
            "transport_type": args[3],
            "adapter_type": args[4],
            "provider_slug": args[5],
            "model_slug": args[6],
            "approval_ref": args[7],
            "approved_by": args[8],
            "approval_note": args[9],
            "proposal_hash": args[10],
            "status": "active",
            "expires_at": args[11],
            "cost_posture": {"cost_structure": "metered_api"},
        }


def _paid_model_proposed(monkeypatch) -> tuple[ProposedPlan, _PaidModelConn]:
    monkeypatch.setattr(spec_materializer, "materialize_spec", _stub_compile_spec)
    _install_empty_binding(monkeypatch)

    import runtime.workflow._admission as admission_mod

    monkeypatch.setattr(
        admission_mod,
        "preview_workflow_execution",
        lambda conn, *, inline_spec, **_kwargs: {
            "action": "preview",
            "workspace": {"runtime_profile_ref": "runtime.default"},
            "jobs": [
                {
                    "label": "thing-1",
                    "resolved_agent": "fireworks/kimi-k2p6",
                    "route_status": "resolved",
                    "task_type": "build",
                    "adapter_type": "llm_task",
                }
            ],
            "warnings": [],
        },
    )
    conn = _PaidModelConn()
    proposed = propose_plan(
        {
            "name": "paid_approval_target",
            "packets": [
                {"description": "do the thing", "write": ["x.py"], "stage": "build", "label": "thing-1"},
            ],
        },
        conn=conn,
        workdir="/repo",
    )
    return proposed, conn


def test_propose_plan_surfaces_paid_model_requirement(monkeypatch) -> None:
    proposed, _conn = _paid_model_proposed(monkeypatch)

    assert proposed.paid_model_requirements
    requirement = proposed.paid_model_requirements[0]
    assert requirement["provider_slug"] == "fireworks"
    assert requirement["model_slug"] == "kimi-k2p6"
    assert requirement["transport_type"] == "API"
    assert requirement["adapter_type"] == "llm_task"
    assert requirement["lease_scope"] == "one_workflow_run"


def test_approve_proposed_plan_requires_exact_paid_model_ack(monkeypatch) -> None:
    proposed, conn = _paid_model_proposed(monkeypatch)

    with pytest.raises(PaidModelApprovalError, match="acknowledge every exact paid route"):
        approve_proposed_plan(proposed, approved_by="nate@praxis", conn=conn)

    approved = approve_proposed_plan(
        proposed,
        approved_by="nate@praxis",
        conn=conn,
        paid_model_approvals=[
            {
                "runtime_profile_ref": "runtime.default",
                "job_type": "build",
                "transport_type": "API",
                "adapter_type": "llm_task",
                "provider_slug": "fireworks",
                "model_slug": "kimi-k2p6",
                "acknowledged": True,
                "approval_ref": "approval.fireworks.kimi",
            }
        ],
    )

    assert approved.paid_model_leases
    assert approved.paid_model_leases[0]["approval_ref"] == "approval.fireworks.kimi"


def test_launch_approved_binds_paid_model_lease_before_submit(monkeypatch) -> None:
    proposed, conn = _paid_model_proposed(monkeypatch)
    approved = approve_proposed_plan(
        proposed,
        approved_by="nate@praxis",
        conn=conn,
        paid_model_approvals=[
            {
                "runtime_profile_ref": "runtime.default",
                "job_type": "build",
                "transport_type": "API",
                "adapter_type": "llm_task",
                "provider_slug": "fireworks",
                "model_slug": "kimi-k2p6",
                "acknowledged": True,
                "approval_ref": "approval.fireworks.kimi",
            }
        ],
    )

    captured: dict[str, object] = {}

    def _fake_submit(conn_arg, **kwargs):
        captured["conn"] = conn_arg
        captured["kwargs"] = kwargs
        return {
            "run_id": kwargs["run_id"],
            "status": "queued",
            "total_jobs": kwargs["total_jobs"],
            "spec_name": kwargs["spec_name"],
        }

    import runtime.control_commands as control_commands_mod

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit)

    receipt = launch_approved(approved, conn=conn)

    assert receipt.run_id == conn.bound_run_id
    assert captured["kwargs"]["force_fresh_run"] is True
    assert receipt.paid_model_access[0]["lease_id"] == approved.paid_model_leases[0]["lease_id"]
    assert receipt.paid_model_access[0]["approval_ref"] == "approval.fireworks.kimi"
