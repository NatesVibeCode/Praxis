"""Regression tests: scope normalization must resolve identically across
all three authoring keys (write_scope / scope.write / write).

Root cause of BUG: pull_crm_snapshot (and any first-job / resume path that
recomputed its shard from the raw spec_snapshot) saw empty write_scope
because the raw snapshot only carries the top-level `write` key — the
normalized key ``scope.write`` is only set by WorkflowSpec loader, not by
the inline submit path or by _runtime_execution_context_shard. The shard
builder must accept all three so authoring format does not silently flip
the sandbox policy to deny-all.
"""

from __future__ import annotations

from runtime.workflow._context_building import (
    _normalized_job_read_scope,
    _normalized_job_write_scope,
)


def test_write_scope_reads_top_level_write_key():
    job = {"label": "j", "write": ["artifacts/out/"]}
    assert _normalized_job_write_scope(job) == ["artifacts/out/"]


def test_write_scope_reads_scope_write_key():
    job = {"label": "j", "scope": {"write": ["artifacts/out/"]}}
    assert _normalized_job_write_scope(job) == ["artifacts/out/"]


def test_write_scope_reads_write_scope_key():
    job = {"label": "j", "write_scope": ["artifacts/out/"]}
    assert _normalized_job_write_scope(job) == ["artifacts/out/"]


def test_write_scope_precedence_write_scope_beats_scope_write_beats_write():
    # All three present; explicit write_scope wins.
    job = {
        "label": "j",
        "write_scope": ["artifacts/a/"],
        "scope": {"write": ["artifacts/b/"]},
        "write": ["artifacts/c/"],
    }
    assert _normalized_job_write_scope(job) == ["artifacts/a/"]

    # Drop write_scope; scope.write wins over top-level write.
    job2 = {
        "label": "j",
        "scope": {"write": ["artifacts/b/"]},
        "write": ["artifacts/c/"],
    }
    assert _normalized_job_write_scope(job2) == ["artifacts/b/"]


def test_write_scope_empty_when_no_keys_declared():
    assert _normalized_job_write_scope({"label": "j"}) == []


def test_read_scope_reads_top_level_read_key():
    job = {"label": "j", "read": ["artifacts/in.json"]}
    assert _normalized_job_read_scope(job) == ["artifacts/in.json"]


def test_read_scope_reads_scope_read_key():
    job = {"label": "j", "scope": {"read": ["artifacts/in.json"]}}
    assert _normalized_job_read_scope(job) == ["artifacts/in.json"]


def test_read_scope_precedence_read_scope_beats_scope_read_beats_read():
    job = {
        "label": "j",
        "read_scope": ["artifacts/a.json"],
        "scope": {"read": ["artifacts/b.json"]},
        "read": ["artifacts/c.json"],
    }
    assert _normalized_job_read_scope(job) == ["artifacts/a.json"]

    job2 = {
        "label": "j",
        "scope": {"read": ["artifacts/b.json"]},
        "read": ["artifacts/c.json"],
    }
    assert _normalized_job_read_scope(job2) == ["artifacts/b.json"]
