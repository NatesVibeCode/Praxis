"""Regression tests for _ensure_execution_packet_revisions.

BUG-D384AB69: queue-spec and chain-submitted runs don't carry
``definition_revision`` / ``plan_revision`` in the raw snapshot (those are
graph-runtime fields populated by spec_materializer). Before the fix,
_build_execution_packet warned and returned None for every non-graph run,
silently dropping execution-packet rows. The fix synthesizes deterministic
fallbacks from the spec itself, preserving diagnostic context while still
letting explicit graph-runtime values win when present.
"""

from __future__ import annotations

from types import SimpleNamespace

from runtime.workflow._context_building import _ensure_execution_packet_revisions


def _make_spec(name: str = "demo", task_type: str = "build", jobs=None):
    """Minimal spec stand-in — helper only reads .jobs, .name, .task_type."""
    return SimpleNamespace(
        name=name,
        task_type=task_type,
        jobs=list(jobs or []),
    )


def test_revisions_present_returned_unchanged():
    """Graph-runtime path: explicit revisions win, no synthesis."""
    raw_snapshot = {
        "name": "demo",
        "task_type": "build",
        "definition_revision": "def_explicit_from_graph",
        "plan_revision": "plan_explicit_from_graph",
        "jobs": [{"label": "j1"}],
    }
    def_rev, plan_rev = _ensure_execution_packet_revisions(
        raw_snapshot=raw_snapshot,
        spec=_make_spec(jobs=[{"label": "j1"}]),
        workflow_id="wf-1",
        run_id="run-1",
    )
    assert def_rev == "def_explicit_from_graph"
    assert plan_rev == "plan_explicit_from_graph"
    # Snapshot untouched — caller-supplied values preserved exactly.
    assert raw_snapshot["definition_revision"] == "def_explicit_from_graph"
    assert raw_snapshot["plan_revision"] == "plan_explicit_from_graph"
    assert raw_snapshot["packet_revision_authority"]["provenance_kind"] == "compiled"
    assert raw_snapshot["packet_revision_authority"]["synthetic_fields"] == []


def test_both_missing_synthesizes_deterministic_fallbacks():
    """Chain-submit / queue-spec path: fallbacks synthesized with prefixes."""
    raw_snapshot = {
        "name": "demo",
        "task_type": "build",
        "jobs": [{"label": "j1", "agent": "auto/build"}],
    }
    def_rev, plan_rev = _ensure_execution_packet_revisions(
        raw_snapshot=raw_snapshot,
        spec=_make_spec(jobs=[{"label": "j1", "agent": "auto/build"}]),
        workflow_id="wf-chain",
        run_id="run-chain",
    )
    assert def_rev.startswith("def_")
    assert plan_rev.startswith("plan_")
    # Stable-hash is hex; 16-char prefix after underscore.
    assert len(def_rev) == len("def_") + 16
    assert len(plan_rev) == len("plan_") + 16
    # Side effect: snapshot stamped for downstream readers.
    assert raw_snapshot["definition_revision"] == def_rev
    assert raw_snapshot["plan_revision"] == plan_rev
    assert raw_snapshot["packet_revision_authority"] == {
        "kind": "execution_packet_revision_authority",
        "provenance_kind": "synthetic",
        "definition_revision": def_rev,
        "plan_revision": plan_rev,
        "synthetic_fields": ["definition_revision", "plan_revision"],
        "reason_code": "packet.revision.synthetic_fallback",
        "workflow_id": "wf-chain",
        "run_id": "run-chain",
    }


def test_synthesis_is_deterministic_across_calls():
    """Same spec + same workflow_id → same fallback hashes."""
    spec_jobs = [{"label": "a"}, {"label": "b"}]
    snap1 = {"name": "demo", "task_type": "build", "jobs": list(spec_jobs)}
    snap2 = {"name": "demo", "task_type": "build", "jobs": list(spec_jobs)}
    def1, plan1 = _ensure_execution_packet_revisions(
        raw_snapshot=snap1,
        spec=_make_spec(jobs=spec_jobs),
        workflow_id="wf-det",
        run_id="run-A",
    )
    def2, plan2 = _ensure_execution_packet_revisions(
        raw_snapshot=snap2,
        spec=_make_spec(jobs=spec_jobs),
        workflow_id="wf-det",
        run_id="run-B",  # different run_id must not perturb the hash
    )
    assert def1 == def2
    assert plan1 == plan2


def test_synthesis_differs_for_different_specs():
    """Different job payloads → different definition hashes."""
    snap_a = {"name": "demo", "task_type": "build", "jobs": [{"label": "a"}]}
    snap_b = {"name": "demo", "task_type": "build", "jobs": [{"label": "b"}]}
    def_a, plan_a = _ensure_execution_packet_revisions(
        raw_snapshot=snap_a,
        spec=_make_spec(jobs=snap_a["jobs"]),
        workflow_id="wf",
        run_id="run",
    )
    def_b, plan_b = _ensure_execution_packet_revisions(
        raw_snapshot=snap_b,
        spec=_make_spec(jobs=snap_b["jobs"]),
        workflow_id="wf",
        run_id="run",
    )
    assert def_a != def_b
    assert plan_a != plan_b


def test_falls_back_to_spec_jobs_when_raw_snapshot_missing_jobs_list():
    """If raw_snapshot has no jobs list, we read them off the spec instead."""
    raw_snapshot = {"name": "demo", "task_type": "build"}  # no 'jobs' key
    spec_jobs = [{"label": "from-spec", "agent": "auto/build"}]
    def_rev, plan_rev = _ensure_execution_packet_revisions(
        raw_snapshot=raw_snapshot,
        spec=_make_spec(jobs=spec_jobs),
        workflow_id="wf",
        run_id="run",
    )
    assert def_rev.startswith("def_")
    assert plan_rev.startswith("plan_")

    # Must match what we'd get if the jobs were inlined in the snapshot.
    raw_snapshot_2 = {"name": "demo", "task_type": "build", "jobs": list(spec_jobs)}
    def_rev_2, plan_rev_2 = _ensure_execution_packet_revisions(
        raw_snapshot=raw_snapshot_2,
        spec=_make_spec(jobs=spec_jobs),
        workflow_id="wf",
        run_id="run",
    )
    assert def_rev == def_rev_2
    assert plan_rev == plan_rev_2


def test_partial_presence_preserves_given_synthesizes_missing():
    """If only definition_revision is present, plan_revision is synthesized
    from the explicit definition_revision — not from a fresh hash."""
    raw_snapshot = {
        "name": "demo",
        "task_type": "build",
        "definition_revision": "def_explicit",
        "jobs": [{"label": "j"}],
    }
    def_rev, plan_rev = _ensure_execution_packet_revisions(
        raw_snapshot=raw_snapshot,
        spec=_make_spec(jobs=[{"label": "j"}]),
        workflow_id="wf",
        run_id="run",
    )
    assert def_rev == "def_explicit"
    assert plan_rev.startswith("plan_")
    assert raw_snapshot["definition_revision"] == "def_explicit"
    assert raw_snapshot["plan_revision"] == plan_rev
    assert raw_snapshot["packet_revision_authority"]["provenance_kind"] == "partial_synthetic"
    assert raw_snapshot["packet_revision_authority"]["synthetic_fields"] == ["plan_revision"]


def test_route_plan_stripped_from_canonicalized_jobs():
    """_route_plan is executor scratch state — must not affect the fingerprint.
    Otherwise a no-op resubmit from a router-decorated snapshot would appear
    to change the spec definition, invalidating caching downstream."""
    snap_clean = {
        "name": "demo",
        "task_type": "build",
        "jobs": [{"label": "j"}],
    }
    snap_with_route_plan = {
        "name": "demo",
        "task_type": "build",
        "jobs": [{"label": "j", "_route_plan": {"model": "gpt-5"}}],
    }
    def_clean, plan_clean = _ensure_execution_packet_revisions(
        raw_snapshot=dict(snap_clean),
        spec=_make_spec(jobs=snap_clean["jobs"]),
        workflow_id="wf",
        run_id="run",
    )
    def_routed, plan_routed = _ensure_execution_packet_revisions(
        raw_snapshot=dict(snap_with_route_plan),
        spec=_make_spec(jobs=snap_with_route_plan["jobs"]),
        workflow_id="wf",
        run_id="run",
    )
    assert def_clean == def_routed
    assert plan_clean == plan_routed
