"""Unit tests for runtime/dataset_exporter.py."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from typing import Any

import pytest

from runtime import dataset_exporter as exporter_module
from runtime.dataset_exporter import (
    DatasetExportError,
    aexport_dataset,
)


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_a) -> bool:
        return False


@dataclass
class _FakeConn:
    sft_rows: list[dict[str, Any]] = field(default_factory=list)
    preference_rows: list[dict[str, Any]] = field(default_factory=list)
    eval_rows: list[dict[str, Any]] = field(default_factory=list)
    leakage_count: int = 0
    executed: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    closed: bool = False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "OK"

    async def fetch(self, query: str, *_args: Any) -> list[Any]:
        if "FROM dataset_curated_examples e" in query:
            return self.sft_rows
        if "FROM dataset_curated_preference_pairs" in query:
            return self.preference_rows
        if "FROM dataset_curated_eval_cases" in query:
            return self.eval_rows
        return []

    async def fetchrow(self, query: str, *_args: Any) -> Any:
        if "AS n" in query:
            return {"n": self.leakage_count}
        return None

    async def close(self) -> None:
        self.closed = True


@dataclass
class _EmitRecorder:
    emitted: list[dict[str, Any]] = field(default_factory=list)
    invalidated: list[dict[str, Any]] = field(default_factory=list)

    async def aemit(self, _conn, *, channel, event_type, entity_id, entity_kind, payload, emitted_by):
        self.emitted.append(
            {"channel": channel, "event_type": event_type, "entity_id": entity_id}
        )
        return 1

    async def aemit_cache_invalidation(self, _conn, *, cache_kind, cache_key, reason, invalidated_by):
        self.invalidated.append({"cache_kind": cache_kind, "cache_key": cache_key})


def _install(monkeypatch, conn: _FakeConn) -> tuple[_EmitRecorder, Any]:
    recorder = _EmitRecorder()
    monkeypatch.setattr(exporter_module, "aemit", recorder.aemit)
    monkeypatch.setattr(
        exporter_module, "aemit_cache_invalidation", recorder.aemit_cache_invalidation
    )

    async def _connect(_env):
        return conn

    return recorder, _connect


def test_unknown_family_raises() -> None:
    with pytest.raises(DatasetExportError, match="unknown dataset_family"):
        asyncio.run(
            aexport_dataset(
                dataset_family="bogus",
                specialist_target="slm/review",
                split_tag="train",
                output_path="/tmp/x.jsonl",
                exported_by="nathan",
            )
        )


def test_eval_family_cannot_export_train_split() -> None:
    with pytest.raises(DatasetExportError, match="eval-family"):
        asyncio.run(
            aexport_dataset(
                dataset_family="eval",
                specialist_target="slm/review",
                split_tag="train",
                output_path="/tmp/x.jsonl",
                exported_by="nathan",
            )
        )


def test_blank_exporter_rejected() -> None:
    with pytest.raises(DatasetExportError, match="exported_by"):
        asyncio.run(
            aexport_dataset(
                dataset_family="sft",
                specialist_target="slm/review",
                split_tag="train",
                output_path="/tmp/x.jsonl",
                exported_by="   ",
            )
        )


def test_train_eval_leakage_raises_before_writing(monkeypatch, tmp_path) -> None:
    conn = _FakeConn(leakage_count=2)
    _, connect = _install(monkeypatch, conn)
    target = str(tmp_path / "leak.jsonl")
    with pytest.raises(DatasetExportError, match="train/eval leakage"):
        asyncio.run(
            aexport_dataset(
                dataset_family="sft",
                specialist_target="slm/review",
                split_tag="train",
                output_path=target,
                exported_by="nathan",
                connect_database=connect,
            )
        )
    assert not os.path.exists(target)


def test_sft_export_writes_jsonl_and_records_manifest(monkeypatch, tmp_path) -> None:
    conn = _FakeConn(
        sft_rows=[
            {
                "promotion_id": "prom_1",
                "prompt": json.dumps({"task": "review"}),
                "target_output": json.dumps({"verdict": "approve"}),
                "candidate_id": "cand_1",
                "dedupe_signature": "sig_1",
                "staleness_status": "fresh",
                "redaction_status": "clean",
                "admitted_definition_hash": "sha256:abc",
                "workflow_definition_id": "wfdef_1",
                "source_receipt_id": "rcpt_1",
                "route_slug": "slm/review",
                "policy_id": "pol_1",
            },
            {
                "promotion_id": "prom_2",
                "prompt": {"task": "review"},
                "target_output": {"verdict": "reject"},
                "candidate_id": "cand_2",
                "dedupe_signature": "sig_2",
                "staleness_status": "fresh",
                "redaction_status": "clean",
                "admitted_definition_hash": "sha256:abc",
                "workflow_definition_id": "wfdef_1",
                "source_receipt_id": "rcpt_2",
                "route_slug": "slm/review",
                "policy_id": "pol_1",
            },
        ]
    )
    recorder, connect = _install(monkeypatch, conn)
    target = str(tmp_path / "review_train.jsonl")

    result = asyncio.run(
        aexport_dataset(
            dataset_family="sft",
            specialist_target="slm/review",
            split_tag="train",
            output_path=target,
            exported_by="nathan",
            connect_database=connect,
        )
    )

    assert result["row_count"] == 2
    assert result["dataset_family"] == "sft"
    assert result["promotion_ids"] == ["prom_1", "prom_2"]
    assert os.path.exists(target)
    with open(target, encoding="utf-8") as fh:
        lines = [json.loads(line) for line in fh if line.strip()]
    assert len(lines) == 2
    assert lines[0]["prompt"] == {"task": "review"}
    assert lines[0]["completion"] == {"verdict": "approve"}
    assert lines[0]["meta"]["promotion_id"] == "prom_1"
    assert lines[0]["meta"]["dedupe_signature"] == "sig_1"
    # sha matches what we just wrote.
    with open(target, "rb") as fh:
        actual_sha = "sha256:" + hashlib.sha256(fh.read()).hexdigest()
    assert result["output_sha256"] == actual_sha

    # A manifest INSERT and an emit happened.
    assert any("dataset_export_manifests" in q for q, _ in conn.executed)
    assert any(e["event_type"] == "dataset_exported" for e in recorder.emitted)
    assert recorder.invalidated  # cache invalidation queued


def test_eval_export_emits_no_leakage_check(monkeypatch, tmp_path) -> None:
    # leakage_count > 0 would raise — but eval family skips the leakage assertion.
    conn = _FakeConn(
        leakage_count=99,
        eval_rows=[
            {
                "promotion_id": "prom_eval_1",
                "case_input": {"q": "explain"},
                "expected_output": {"a": "because circuit"},
                "rubric": {"must_mention": ["circuit"]},
                "difficulty_tags": ["smoke"],
                "domain_tags": ["routing"],
                "revision_scope": {"definition_hash": "sha256:abc"},
                "excluded_from_training": True,
                "policy_id": "pol_op_1",
                "candidate_ids": ["cand_op_1"],
                "split_tag": "eval",
            }
        ],
    )
    _, connect = _install(monkeypatch, conn)
    target = str(tmp_path / "eval_set.jsonl")

    result = asyncio.run(
        aexport_dataset(
            dataset_family="eval",
            specialist_target="slm/operator_explain",
            split_tag="eval",
            output_path=target,
            exported_by="nathan",
            connect_database=connect,
        )
    )

    assert result["row_count"] == 1
    with open(target, encoding="utf-8") as fh:
        row = json.loads(fh.readline())
    assert row["input"] == {"q": "explain"}
    assert row["expected"] == {"a": "because circuit"}
    assert row["meta"]["excluded_from_training"] is True
    assert row["revision_scope"] == {"definition_hash": "sha256:abc"}
