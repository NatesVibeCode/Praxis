"""Regression tests for preview-vs-execution parity.

BUG-D3CD86B8: preview built messages via _execution_model_messages but real
execution prepended a platform_context block and sent the result as
full_prompt. Preview's rendered_prompt therefore overstated parity —
helpers agreed, but the string the backend actually saw diverged.

BUG-31C147A8: preview echoed workspace.repo_root and workspace.workdir from
the spec as if they were authoritative, but sharded execution replaces
these with materialized_repo_root/materialized_workdir resolved from the
active fork/worktree binding. Preview was therefore reporting host paths
as if they were the worker-facing payload.

Fix (shared): extracted build_platform_context + assemble_full_prompt into
runtime/workflow/_context_building.py; both preview and execution now call
the same assembler. Preview exposes rendered_full_prompt (the backend-bound
string) alongside the pre-platform-context rendered_prompt, and flags the
materialized workspace as unresolved_until_execution with a clear note.
"""

from __future__ import annotations

from runtime import _workflow_database
from runtime.workflow._context_building import (
    assemble_full_prompt,
    build_platform_context,
)


# ------------------------------------------------------------- shared helpers


def test_build_platform_context_contains_repo_root_and_workspace_note():
    ctx = build_platform_context("/host/repo")
    assert "--- PLATFORM CONTEXT ---" in ctx
    assert "--- END PLATFORM CONTEXT ---" in ctx
    assert "/host/repo" in ctx
    # The 'Command workspace' caveat is the user-visible authority for
    # telling the worker not to assume host paths — keep it in the assertion
    # so a future refactor cannot silently drop it.
    assert "Command workspace" in ctx
    assert "/workspace" in ctx


def test_build_platform_context_redacts_database_credentials(monkeypatch):
    monkeypatch.setattr(
        _workflow_database,
        "resolve_runtime_database_url",
        lambda *, required=False: "postgresql://praxis:secret@192.168.86.249:5432/praxis",
    )

    ctx = build_platform_context("/host/repo")

    assert "postgresql://praxis:***@192.168.86.249:5432/praxis" in ctx
    assert "secret" not in ctx


def test_assemble_full_prompt_matches_execution_core_concatenation_order():
    """Parity guarantee: the order must be prompt + platform + shard + bundle,
    double-newline joined, no blank parts. This is the exact order execution
    uses in runtime/workflow/_execution_core.py."""
    out = assemble_full_prompt(
        prompt="DO THE THING",
        platform_context="[PLATFORM]",
        execution_context_shard_text="[SHARD]",
        execution_bundle_text="[BUNDLE]",
    )
    assert out == "DO THE THING\n\n[PLATFORM]\n\n[SHARD]\n\n[BUNDLE]"


def test_assemble_full_prompt_skips_empty_parts():
    """If a section is empty (e.g. no execution_bundle_text), it should not
    produce a trailing \\n\\n — this matches execution's behavior and
    prevents preview from showing ghost separators."""
    out = assemble_full_prompt(
        prompt="P",
        platform_context="",
        execution_context_shard_text="S",
        execution_bundle_text="",
    )
    assert out == "P\n\nS"


def test_assemble_full_prompt_empty_inputs_yields_empty_string():
    assert assemble_full_prompt(
        prompt="",
        platform_context="",
        execution_context_shard_text="",
        execution_bundle_text="",
    ) == ""


# ----------------------------------------------------- preview output contract

# These tests exercise the full preview path. They reuse the existing
# _FakeConn + monkeypatch pattern from test_unified_workflow.py but remain
# self-contained so they don't couple to that file's fixtures.


class _FakeConn:
    def execute(self, *a, **k):
        return None

    def fetchone(self, *a, **k):
        return None

    def fetchall(self, *a, **k):
        return []

    def fetch(self, *a, **k):
        return []


def _inline_spec():
    return {
        "name": "preview-parity-spec",
        "workflow_id": "workflow.preview_parity",
        "phase": "build",
        "workdir": "/repo",
        "jobs": [
            {
                "label": "build_a",
                "prompt": "Implement the preview parity fix.",
                "agent": "auto/build",
                "task_type": "code_generation",
                "write_scope": ["runtime/workflow/preview.py"],
                "verify_refs": ["verify.preview"],
            }
        ],
    }


def _run_preview(monkeypatch):
    from runtime.workflow import _admission as _admission_mod
    from runtime.workflow import _context_building as _ctx_mod

    monkeypatch.setattr(
        _ctx_mod, "_runtime_profile_sandbox_payload", lambda *a, **k: None
    )
    monkeypatch.setattr(
        _ctx_mod,
        "resolve_job_decision_pack",
        lambda *a, **k: {
            "pack_version": 1,
            "authority_domains": ["workspace_boundary"],
            "decision_keys": [],
            "decisions": [],
        },
    )

    return _admission_mod.preview_workflow_execution(
        _FakeConn(), inline_spec=_inline_spec(), repo_root="/repo"
    )


def test_preview_exposes_rendered_full_prompt_with_platform_context(monkeypatch):
    """BUG-D3CD86B8 fix: rendered_full_prompt contains the platform context
    block that execution prepends. Operators see the true backend-bound
    payload, not the helper-only parity string."""
    preview = _run_preview(monkeypatch)
    job = preview["jobs"][0]
    assert "rendered_full_prompt" in job
    assert "--- PLATFORM CONTEXT ---" in job["rendered_full_prompt"]
    assert "rendered_platform_context" in job
    assert "--- PLATFORM CONTEXT ---" in job["rendered_platform_context"]


def test_preview_rendered_full_prompt_starts_with_job_prompt(monkeypatch):
    preview = _run_preview(monkeypatch)
    job = preview["jobs"][0]
    assert job["rendered_full_prompt"].startswith("Implement the preview parity fix.")


def test_preview_keeps_legacy_rendered_user_prompt_without_platform_context(monkeypatch):
    """Back-compat: rendered_user_prompt is still the pre-platform form for
    callers that rely on the existing helper-parity contract. Only the
    NEW rendered_full_prompt contains platform context."""
    preview = _run_preview(monkeypatch)
    job = preview["jobs"][0]
    assert "--- PLATFORM CONTEXT ---" not in job["rendered_user_prompt"]
    # rendered_prompt is the legacy alias; must also stay pre-platform.
    assert "--- PLATFORM CONTEXT ---" not in job["rendered_prompt"]


def test_preview_job_workspace_marks_materialized_as_unresolved(monkeypatch):
    """BUG-31C147A8 fix: each job's workspace payload declares host_* fields
    truthfully and flags materialized_* as unresolved_until_execution."""
    preview = _run_preview(monkeypatch)
    ws = preview["jobs"][0]["workspace"]
    # Truthful host-side labels.
    assert ws["host_repo_root"] == "/repo"
    assert ws["host_workdir"] == "/repo"
    # Back-compat fields still present.
    assert ws["repo_root"] == "/repo"
    assert ws["workdir"] == "/repo"
    # Explicit materialized-unresolved marker.
    assert ws["materialized"]["status"] == "unresolved_until_execution"
    assert "fork/worktree" in ws["materialized"]["note"]


def test_preview_top_level_workspace_also_marks_materialized_unresolved(monkeypatch):
    preview = _run_preview(monkeypatch)
    top_ws = preview["workspace"]
    assert top_ws["host_repo_root"] == "/repo"
    assert top_ws["repo_root"] == "/repo"  # back-compat alias
    assert top_ws["materialized"]["status"] == "unresolved_until_execution"


def test_preview_rendered_full_prompt_matches_shared_assembler(monkeypatch):
    """Parity: preview's rendered_full_prompt must be byte-equal to calling
    assemble_full_prompt directly with the preview-computed parts. This
    proves preview isn't sneaking a second assembly path."""
    preview = _run_preview(monkeypatch)
    job = preview["jobs"][0]
    expected = assemble_full_prompt(
        prompt="Implement the preview parity fix.",
        platform_context=job["rendered_platform_context"],
        execution_context_shard_text=job["rendered_execution_context_shard"],
        execution_bundle_text=job["rendered_execution_bundle"],
    )
    assert job["rendered_full_prompt"] == expected
