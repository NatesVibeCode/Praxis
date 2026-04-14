from __future__ import annotations

from pathlib import Path

import pytest


WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
WAVE_C_SOURCE_FILES = (
    WORKFLOW_ROOT / "runtime" / "compile_artifacts.py",
    WORKFLOW_ROOT / "runtime" / "persistent_evidence.py",
    WORKFLOW_ROOT / "runtime" / "receipt_store.py",
    WORKFLOW_ROOT / "runtime" / "workflow" / "job_runtime_context.py",
    WORKFLOW_ROOT / "runtime" / "workflow" / "receipt_writer.py",
)
FORBIDDEN_SQL_SNIPPETS = (
    "INSERT ",
    "UPDATE ",
    "DELETE FROM",
    "CREATE TABLE",
)


@pytest.mark.parametrize("source_path", WAVE_C_SOURCE_FILES, ids=lambda path: path.name)
def test_wave_c_runtime_sources_do_not_inline_write_sql(source_path: Path) -> None:
    source_text = source_path.read_text(encoding="utf-8")

    for forbidden_snippet in FORBIDDEN_SQL_SNIPPETS:
        assert forbidden_snippet not in source_text
