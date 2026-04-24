"""Preflight helpers that catch friction-class errors *before* submission.

Each check below corresponds to a class of error that silently wasted
operator time this session: builder typos surfaced as phantom-work green
receipts, admission gaps surfaced as `adapter.transport_unsupported`
mid-run, workflow_id collisions surfaced as raw psycopg UniqueViolations.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from runtime.workflow_validation import (
    _preflight_deterministic_builders,
    _preflight_provider_admissions,
    _preflight_provider_availability,
    _preflight_workdir_drift,
    _preflight_workflow_id_collision,
)


class _FakeSpec:
    """Minimal spec double — tests exercise the preflight helpers directly."""

    def __init__(self, *, jobs: list[dict[str, Any]] | None = None,
                 workflow_id: str | None = None,
                 raw: dict[str, Any] | None = None) -> None:
        self.jobs = jobs or []
        self.workflow_id = workflow_id
        self._raw = raw or {}


# ----- Deterministic builder checks --------------------------------------


def test_deterministic_missing_builder_is_error_without_explicit_passthrough() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "step_smoke",
        "adapter_type": "deterministic_task",
        "inputs": {"input_path": "data.json"},
    }])

    warnings = _preflight_deterministic_builders(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "deterministic_builder_missing"
    assert warnings[0]["severity"] == "error"
    assert warnings[0]["label"] == "step_smoke"
    assert "add a deterministic_builder" in warnings[0]["message"]


def test_deterministic_missing_builder_can_opt_into_smoke_passthrough_warning() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "step_smoke",
        "adapter_type": "deterministic_task",
        "inputs": {"input_path": "data.json", "allow_passthrough_echo": True},
    }])

    warnings = _preflight_deterministic_builders(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "deterministic_builder_passthrough_echo"
    assert warnings[0]["severity"] == "warning"
    assert warnings[0]["label"] == "step_smoke"
    assert "allow_passthrough_echo=true" in warnings[0]["message"]


def test_deterministic_builder_import_failure_is_error() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "step_bad",
        "adapter_type": "deterministic_task",
        "inputs": {"deterministic_builder": "nonexistent_module_xyz.build"},
    }])

    warnings = _preflight_deterministic_builders(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "deterministic_builder_import_failed"
    assert warnings[0]["severity"] == "error"
    assert "cannot import builder module" in warnings[0]["message"]


def test_deterministic_builder_malformed_path_is_error() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "step_mf",
        "adapter_type": "deterministic_task",
        "inputs": {"deterministic_builder": "noDotHere"},
    }])

    warnings = _preflight_deterministic_builders(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "deterministic_builder_malformed"
    assert warnings[0]["severity"] == "error"


def test_deterministic_builder_not_callable_is_error() -> None:
    # json.__name__ is a string attribute, not callable.
    spec = _FakeSpec(jobs=[{
        "label": "step_attr",
        "adapter_type": "deterministic_task",
        "inputs": {"deterministic_builder": "json.__name__"},
    }])

    warnings = _preflight_deterministic_builders(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "deterministic_builder_not_callable"
    assert warnings[0]["severity"] == "error"


def test_deterministic_builder_resolving_callable_passes_clean() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "step_ok",
        "adapter_type": "deterministic_task",
        "inputs": {"deterministic_builder": "json.dumps"},
    }])

    assert _preflight_deterministic_builders(spec) == []


def test_non_deterministic_jobs_are_skipped() -> None:
    spec = _FakeSpec(jobs=[
        {"label": "cli_step", "adapter_type": "cli_llm", "agent": "anthropic/claude-x"},
        {"label": "llm_step", "adapter_type": "llm_task", "agent": "anthropic/claude-y"},
        {"label": "ctrl_step", "adapter_type": "control_operator"},
    ])

    assert _preflight_deterministic_builders(spec) == []


# ----- Provider admission checks -----------------------------------------


class _FakeCursor:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def execute(self, _sql: str, _params: tuple) -> None:
        # The real query filters by (provider_slug, adapter_type); the fake
        # returns whatever was preloaded regardless.
        pass

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._rows)

    def execute(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
        return self._rows


class _ExplodingConn:
    def cursor(self):  # type: ignore[no-untyped-def]
        raise RuntimeError("DB is on fire")


def test_provider_admission_admitted_is_quiet() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "adapter_type": "cli_llm",
        "agent": "anthropic/claude-haiku",
    }])
    conn = _FakeConn([{
        "provider_slug": "anthropic",
        "adapter_type": "cli_llm",
        "admitted_by_policy": True,
        "policy_reason": "",
    }])

    warnings = _preflight_provider_admissions(spec, pg_conn=conn)

    assert warnings == []


def test_provider_admission_denied_is_error() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "adapter_type": "llm_task",
        "agent": "openai/gpt-x",
    }])
    conn = _FakeConn([{
        "provider_slug": "openai",
        "adapter_type": "llm_task",
        "admitted_by_policy": False,
        "policy_reason": "credential missing",
    }])

    warnings = _preflight_provider_admissions(spec, pg_conn=conn)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "provider_admission_denied"
    assert warnings[0]["severity"] == "error"
    assert "credential missing" in warnings[0]["message"]
    assert "praxis_provider_onboard" in warnings[0]["message"]


def test_provider_admission_missing_row_is_error() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "adapter_type": "cli_llm",
        "agent": "unknownprovider/some-model",
    }])
    conn = _FakeConn([])

    warnings = _preflight_provider_admissions(spec, pg_conn=conn)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "provider_admission_missing"
    assert warnings[0]["severity"] == "error"


def test_provider_admission_db_failure_is_non_fatal_warning() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "adapter_type": "cli_llm",
        "agent": "anthropic/x",
    }])

    warnings = _preflight_provider_admissions(spec, pg_conn=_ExplodingConn())

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "provider_admission_query_failed"
    # DB-level preflight outages must not block submission.
    assert warnings[0]["severity"] == "warning"


def test_provider_admission_ignores_non_agent_jobs() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "deterministic_step",
        "adapter_type": "deterministic_task",
    }])
    conn = _FakeConn([])

    assert _preflight_provider_admissions(spec, pg_conn=conn) == []


# ----- Provider availability checks --------------------------------------


class _FakeCircuitBreakers:
    def __init__(self, states: dict[str, dict[str, Any]] | None = None) -> None:
        self._states = states or {}

    def all_states(self) -> dict[str, dict[str, Any]]:
        return self._states


def test_provider_availability_blocks_degraded_provider_usage_snapshot() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "agent": "anthropic/claude-sonnet-4-6",
    }])
    conn = _FakeConn([{
        "subject_id": "anthropic",
        "subject_sub": "cli_llm",
        "status": "degraded",
        "summary": "anthropic/cli_llm: degraded",
        "details": {"rate_limited": True, "stderr_excerpt": "quota exhausted"},
        "captured_at": "2026-04-23T10:00:00Z",
    }])

    warnings = _preflight_provider_availability(
        spec,
        pg_conn=conn,
        circuit_breakers=_FakeCircuitBreakers(),
    )

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "provider_unavailable"
    assert warnings[0]["severity"] == "error"
    assert "anthropic" in warnings[0]["message"]
    assert "quota exhausted" in warnings[0]["message"]


def test_provider_availability_allows_ok_provider_usage_snapshot() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "agent": "openai/gpt-5.4",
    }])
    conn = _FakeConn([{
        "subject_id": "openai",
        "subject_sub": "cli_llm",
        "status": "ok",
        "summary": "openai/cli_llm: ok",
        "details": {},
        "captured_at": "2026-04-23T10:00:00Z",
    }])

    assert _preflight_provider_availability(
        spec,
        pg_conn=conn,
        circuit_breakers=_FakeCircuitBreakers(),
    ) == []


def test_provider_availability_blocks_open_circuit_breaker() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "agent": "openai/gpt-5.4",
    }])
    conn = _FakeConn([])
    circuit_breakers = _FakeCircuitBreakers({
        "openai": {
            "state": "OPEN",
            "manual_override": {"rationale": "operator forced outage"},
        }
    })

    warnings = _preflight_provider_availability(
        spec,
        pg_conn=conn,
        circuit_breakers=circuit_breakers,
    )

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "provider_circuit_open"
    assert warnings[0]["severity"] == "error"
    assert "operator forced outage" in warnings[0]["message"]


def test_provider_availability_query_failure_is_warning() -> None:
    spec = _FakeSpec(jobs=[{
        "label": "agent_step",
        "agent": "openai/gpt-5.4",
    }])

    warnings = _preflight_provider_availability(
        spec,
        pg_conn=_ExplodingConn(),
        circuit_breakers=_FakeCircuitBreakers(),
    )

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "provider_availability_query_failed"
    assert warnings[0]["severity"] == "warning"


# ----- workflow_id collision check ---------------------------------------


def test_workflow_id_collision_emits_warning() -> None:
    spec = _FakeSpec(workflow_id="e2e_sample")
    conn = _FakeConn([{"definition_version": 1, "status": "active"}])

    warnings = _preflight_workflow_id_collision(spec, pg_conn=conn)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "workflow_id_already_registered"
    # Pre-submit advisory, not a hard block — the submit-time translator
    # already surfaces an actionable error.
    assert warnings[0]["severity"] == "warning"
    assert "bump workflow_id" in warnings[0]["message"]


def test_workflow_id_without_existing_row_is_quiet() -> None:
    spec = _FakeSpec(workflow_id="brand_new_id")
    conn = _FakeConn([])

    assert _preflight_workflow_id_collision(spec, pg_conn=conn) == []


def test_workflow_id_from_raw_dict_is_detected() -> None:
    # Some spec loaders leave workflow_id only in the raw dict.
    spec = _FakeSpec(workflow_id=None, raw={"workflow_id": "from_raw"})
    conn = _FakeConn([{"definition_version": 7, "status": "active"}])

    warnings = _preflight_workflow_id_collision(spec, pg_conn=conn)

    assert len(warnings) == 1
    assert "from_raw" in warnings[0]["message"]


# ----- workdir drift -----------------------------------------------------


def test_workdir_drift_warns_on_missing_host_path_with_suggestion(monkeypatch, tmp_path) -> None:
    host_root = tmp_path / "host"
    container_root = tmp_path / "container"
    missing = str(host_root / "artifacts" / "e2e_exercise_99999999")
    monkeypatch.setattr(
        "runtime.workflow_validation.authority_workspace_roots",
        lambda: (host_root,),
    )
    monkeypatch.setattr(
        "runtime.workflow_validation.container_workspace_root",
        lambda: container_root,
    )
    spec = _FakeSpec(jobs=[], raw={"workdir": missing})

    warnings = _preflight_workdir_drift(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "workdir_path_missing"
    assert warnings[0]["severity"] == "warning"
    assert str(container_root / "artifacts" / "e2e_exercise_99999999") in warnings[0]["message"]


def test_workdir_drift_quiet_when_path_exists(tmp_path) -> None:
    spec = _FakeSpec(raw={"workdir": str(tmp_path)})

    assert _preflight_workdir_drift(spec) == []


def test_workdir_drift_warns_on_host_path_outside_workspace_authority(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "runtime.workflow_validation.authority_workspace_roots",
        lambda: (tmp_path / "current-checkout",),
    )
    spec = _FakeSpec(raw={"workdir": "/Users/nate/Praxis"})

    warnings = _preflight_workdir_drift(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "workspace_path_outside_authority"
    assert warnings[0]["severity"] == "warning"
    assert "PRAXIS_HOST_WORKSPACE_ROOT" in warnings[0]["message"]


def test_workdir_drift_checks_target_repo_authority(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "runtime.workflow_validation.authority_workspace_roots",
        lambda: (tmp_path / "current-checkout",),
    )
    spec = _FakeSpec(raw={"target_repo": "/Volumes/Users/natha/Documents/Builds/Praxis"})

    warnings = _preflight_workdir_drift(spec)

    assert len(warnings) == 1
    assert warnings[0]["kind"] == "workspace_path_outside_authority"
    assert "target_repo" in warnings[0]["message"]


def test_workdir_drift_quiet_when_relative_path() -> None:
    # Relative paths are resolved at submission time by the surface layer;
    # the preflight intentionally does not speculate about them.
    spec = _FakeSpec(raw={"workdir": "artifacts/foo"})

    assert _preflight_workdir_drift(spec) == []


def test_workdir_drift_covers_per_job_workdirs(monkeypatch, tmp_path) -> None:
    spec_workdir = str(tmp_path)  # exists
    missing_job_workdir = str(Path(tempfile.gettempdir()) / "praxis-workspace" / "elsewhere" / "nope")
    monkeypatch.setattr(
        "runtime.workflow_validation.authority_workspace_roots",
        lambda: (),
    )
    spec = _FakeSpec(
        raw={"workdir": spec_workdir},
        jobs=[
            {"label": "step_ok", "workdir": spec_workdir},
            {"label": "step_bad", "workdir": missing_job_workdir},
        ],
    )

    warnings = _preflight_workdir_drift(spec)

    assert len(warnings) == 1
    assert warnings[0]["label"] == "step_bad"
    assert "job.workdir" in warnings[0]["message"]


def test_workdir_drift_warns_when_workspace_authority_unavailable(monkeypatch) -> None:
    def _boom():
        raise RuntimeError("authority unavailable")

    monkeypatch.setattr(
        "runtime.workflow_validation.authority_workspace_roots",
        _boom,
    )
    missing_workdir = str(Path(tempfile.gettempdir()) / "praxis-workspace" / "elsewhere" / "nope")
    spec = _FakeSpec(raw={"workdir": missing_workdir})

    warnings = _preflight_workdir_drift(spec)

    assert [warning["kind"] for warning in warnings] == [
        "workdir_authority_unavailable",
        "workdir_path_missing",
    ]
    assert "authority unavailable" in warnings[0]["message"]
