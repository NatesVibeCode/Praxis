"""Tests for the new authoring spec format in workflow_spec.py."""

import json
import os
import tempfile

import pytest

# Ensure the DB loader doesn't block tests
os.environ.setdefault("WORKFLOW_DATABASE_URL", "")

from adapters.task_profiles import TaskProfile

from runtime.workflow_spec import (
    WorkflowSpec,
    WorkflowSpecError,
    _is_new_authoring_format,
    validate_authoring_spec,
)


def _profile_for(task_type: str) -> TaskProfile | None:
    profiles = {
        "code_generation": TaskProfile(
            task_type="code_generation",
            allowed_tools=("Read", "Edit", "Write", "Bash"),
            default_tier="mid",
            file_attach=False,
            system_prompt_hint="Write clean, tested code.",
            default_scope_read=("src/", "lib/", "tests/"),
            default_scope_write=("src/", "tests/"),
        ),
        "code_review": TaskProfile(
            task_type="code_review",
            allowed_tools=("Read", "Grep", "Glob"),
            default_tier="mid",
            file_attach=False,
            system_prompt_hint="Review code for issues. Be specific.",
            default_scope_read=("src/", "lib/", "tests/"),
            default_scope_write=(),
        ),
    }
    return profiles.get(task_type)


@pytest.fixture(autouse=True)
def _task_profile_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("adapters.task_profiles.try_resolve_profile", _profile_for)


def _write_spec(spec: dict) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".queue.json", delete=False)
    json.dump(spec, f)
    f.close()
    return f.name


def _minimal_spec(**overrides) -> dict:
    base = {
        "name": "test-workflow",
        "outcome_goal": "Test the new authoring format",
        "task_type": "code_generation",
        "authoring_contract": {"artifact_kind": "code"},
        "acceptance_contract": {"verify_refs": ["pytest tests/"]},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def test_detects_new_format():
    assert _is_new_authoring_format(_minimal_spec()) is True


def test_does_not_detect_legacy_as_new():
    legacy = {"name": "legacy", "jobs": [{"label": "j1", "prompt": "do stuff"}]}
    assert _is_new_authoring_format(legacy) is False


# ---------------------------------------------------------------------------
# from_dict — raw dict coercion path (BUG-C50252DD regression)
# ---------------------------------------------------------------------------

def test_from_dict_new_format_builds_spec_without_file_io():
    """Callers holding a parsed dict can coerce directly — no tempfile needed."""
    raw = _minimal_spec()
    spec = WorkflowSpec.from_dict(raw)
    assert spec.name == "test-workflow"
    assert spec.task_type == "code_generation"
    assert spec.jobs, "from_dict must build at least one job"
    # _raw should round-trip so downstream consumers (graph compiler, etc.) work.
    assert spec._raw.get("name") == "test-workflow"


def test_from_dict_rejects_non_dict_input_with_clear_error():
    with pytest.raises(WorkflowSpecError) as exc_info:
        WorkflowSpec.from_dict("not-a-dict")  # type: ignore[arg-type]
    assert "dict" in str(exc_info.value).lower()


def test_validate_workflow_spec_accepts_raw_dict_and_does_not_crash():
    """BUG-C50252DD regression: passing a raw dict must not raise AttributeError.

    The front-door validator is used from standalone scripts and MCP surfaces
    that may hold a dict (not a WorkflowSpec). It must coerce cleanly.
    """
    from runtime.workflow_validation import validate_workflow_spec

    class _StubConn:
        """Stand-in pg_conn; validate may short-circuit before touching it."""

        def execute(self, *args, **kwargs):
            raise RuntimeError("stub conn should not be reached for this test")

        def fetch(self, *args, **kwargs):
            raise RuntimeError("stub conn should not be reached for this test")

    raw = _minimal_spec()
    # Should not raise AttributeError (the BUG-C50252DD symptom).
    # We accept *any* dict result — validity depends on live DB authority,
    # which we're not mocking here. The regression is specifically about
    # crashing on dict input, not about the full validation pipeline.
    try:
        result = validate_workflow_spec(raw, pg_conn=_StubConn())
    except AttributeError as exc:
        pytest.fail(f"validate_workflow_spec crashed on raw dict: {exc}")
    except Exception:
        # Downstream authority errors are fine for this regression; the only
        # forbidden outcome is AttributeError on 'dict' has no attribute 'summary'.
        pass
    else:
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Loading — basic
# ---------------------------------------------------------------------------

def test_load_new_format_single_job():
    path = _write_spec(_minimal_spec())
    spec = WorkflowSpec.load(path)
    assert spec.name == "test-workflow"
    assert spec.outcome_goal == "Test the new authoring format"
    assert spec.task_type == "code_generation"
    assert len(spec.jobs) == 1
    assert "## Objective" in spec.jobs[0]["prompt"]
    assert spec.jobs[0]["label"] == "job_1"
    assert spec.jobs[0]["agent"] == "auto/build"
    os.unlink(path)


def test_load_new_format_no_jobs_array():
    """Single-job shorthand: no jobs array, spec-level fields are the job."""
    path = _write_spec(_minimal_spec(agent="google/gemini-2.5-flash", prefer_cost=True))
    spec = WorkflowSpec.load(path)
    assert len(spec.jobs) == 1
    assert spec.jobs[0]["agent"] == "google/gemini-2.5-flash"
    assert spec.jobs[0]["prefer_cost"] is True
    os.unlink(path)


def test_load_new_format_multi_job_sequential():
    """Multi-job without sprint → sequential by array order."""
    raw = _minimal_spec(jobs=[
        {"task_type": "code_review"},
        {"task_type": "code_generation"},
    ])
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert len(spec.jobs) == 2
    # Second job depends on first
    assert spec.jobs[1].get("depends_on") == ["job_1"]
    os.unlink(path)


def test_load_new_format_multi_job_sprint():
    """Sprint ordering groups parallel jobs."""
    raw = _minimal_spec(jobs=[
        {"task_type": "code_review", "sprint": 1},
        {"task_type": "code_review", "sprint": 1},
        {"task_type": "code_generation", "sprint": 2},
    ])
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert len(spec.jobs) == 3
    # Sprint 1 jobs have no deps; sprint 2 depends on both sprint 1 jobs
    assert spec.jobs[0].get("depends_on") is None
    assert spec.jobs[1].get("depends_on") is None
    assert set(spec.jobs[2].get("depends_on", [])) == {"job_1", "job_2"}
    os.unlink(path)


# ---------------------------------------------------------------------------
# Prompt generation
# ---------------------------------------------------------------------------

def test_prompt_contains_outcome_goal():
    path = _write_spec(_minimal_spec())
    spec = WorkflowSpec.load(path)
    assert "Test the new authoring format" in spec.jobs[0]["prompt"]
    os.unlink(path)


def test_prompt_contains_anti_requirements():
    raw = _minimal_spec(anti_requirements=["No external deps"])
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert "No external deps" in spec.jobs[0]["prompt"]
    os.unlink(path)


# ---------------------------------------------------------------------------
# Scope inference
# ---------------------------------------------------------------------------

def test_scope_inferred_from_task_type():
    path = _write_spec(_minimal_spec(task_type="code_generation"))
    spec = WorkflowSpec.load(path)
    scope = spec.jobs[0].get("scope", {})
    # code_generation profile has default_scope_read and default_scope_write
    assert "read" in scope or "write" in scope
    os.unlink(path)


# ---------------------------------------------------------------------------
# Contract inheritance
# ---------------------------------------------------------------------------

def test_contract_inherited_from_spec():
    raw = _minimal_spec(jobs=[{}])
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert spec.jobs[0]["authoring_contract"] == {"artifact_kind": "code"}
    os.unlink(path)


def test_contract_override_replaces():
    raw = _minimal_spec(jobs=[
        {"authoring_contract": {"artifact_kind": "report"}},
    ])
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert spec.jobs[0]["authoring_contract"] == {"artifact_kind": "report"}
    os.unlink(path)


def test_job_level_prefer_cost_overrides_spec_default():
    raw = _minimal_spec(
        prefer_cost=True,
        jobs=[{"prefer_cost": False}],
    )
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert spec.jobs[0]["prefer_cost"] is False
    os.unlink(path)


# ---------------------------------------------------------------------------
# Replicate
# ---------------------------------------------------------------------------

def test_replicate_in_new_format():
    raw = _minimal_spec(jobs=[
        {"replicate": 3},
    ])
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert len(spec.jobs) == 3
    os.unlink(path)


def test_replicate_with_in_new_format():
    raw = _minimal_spec(jobs=[
        {"replicate_with": ["alpha", "beta"]},
    ])
    path = _write_spec(raw)
    spec = WorkflowSpec.load(path)
    assert len(spec.jobs) == 2
    os.unlink(path)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_validation_rejects_missing_required():
    ok, errors = validate_authoring_spec({"name": "test"})
    assert ok is False
    assert any("acceptance_contract" in e for e in errors)
    assert any("authoring_contract" in e for e in errors)
    assert any("outcome_goal" in e for e in errors)
    assert any("task_type" in e for e in errors)


def test_validation_rejects_removed_fields():
    raw = _minimal_spec(prompt="should not be here", timeout=300)
    ok, errors = validate_authoring_spec(raw)
    assert ok is False
    assert any("prompt" in e and "removed" in e for e in errors)
    assert any("timeout" in e and "removed" in e for e in errors)


def test_validation_rejects_removed_job_fields():
    raw = _minimal_spec(jobs=[{"prompt": "bad", "label": "bad"}])
    ok, errors = validate_authoring_spec(raw)
    assert ok is False
    assert any("prompt" in e and "removed" in e for e in errors)
    assert any("label" in e and "removed" in e for e in errors)


def test_validation_passes_valid_spec():
    ok, errors = validate_authoring_spec(_minimal_spec())
    assert ok is True
    assert errors == []


def test_validation_passes_with_jobs():
    raw = _minimal_spec(jobs=[
        {"task_type": "code_review", "sprint": 1},
        {"sprint": 2},
    ])
    ok, errors = validate_authoring_spec(raw)
    assert ok is True


def test_validation_rejects_non_boolean_prefer_cost():
    raw = _minimal_spec(prefer_cost="yes")
    ok, errors = validate_authoring_spec(raw)
    assert ok is False
    assert "prefer_cost must be a boolean or null" in errors


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

def test_load_rejects_empty_outcome_goal():
    path = _write_spec(_minimal_spec(outcome_goal=""))
    with pytest.raises(WorkflowSpecError, match="outcome_goal"):
        WorkflowSpec.load(path)
    os.unlink(path)


def test_load_rejects_empty_jobs_array():
    path = _write_spec(_minimal_spec(jobs=[]))
    with pytest.raises(WorkflowSpecError, match="non-empty"):
        WorkflowSpec.load(path)
    os.unlink(path)


# ---------------------------------------------------------------------------
# Legacy format still works
# ---------------------------------------------------------------------------

def test_legacy_format_still_loads():
    legacy = {
        "name": "legacy-test",
        "jobs": [
            {"label": "j1", "prompt": "do stuff", "agent": "auto/build"},
        ],
    }
    path = _write_spec(legacy)
    spec = WorkflowSpec.load(path)
    assert spec.name == "legacy-test"
    assert spec.jobs[0]["prompt"] == "do stuff"
    os.unlink(path)
