"""Portability smoke for committed queue specs.

Confirms BUG-A4CE07C5 / BUG-ACF1F41A: the swept queue JSON files under
``artifacts/workflow`` and ``config/cascade/specs`` no longer carry retired
DSNs or operator-local host paths and are still parseable.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]

ARTIFACT_DIR = REPO_ROOT / "Code&DBs/Workflow/artifacts/workflow"
CASCADE_DIR = REPO_ROOT / "config/cascade/specs"

RETIRED_PATTERNS = (
    "postgresql://nate@127.0.0.1:5432/dag_workflow",
    "postgresql://localhost:5432/praxis",
    "/Users/nate/Praxis",
    "/Volumes/Users/natha/Documents/Builds/Praxis",
    "/opt/homebrew/bin/python3",
)


def _queue_files() -> list[Path]:
    files: list[Path] = []
    for directory in (ARTIFACT_DIR, CASCADE_DIR):
        if directory.is_dir():
            files.extend(sorted(directory.glob("*.queue.json")))
    return files


@pytest.mark.parametrize("queue_path", _queue_files(), ids=lambda p: p.name)
def test_queue_json_is_portable(queue_path: Path) -> None:
    text = queue_path.read_text(encoding="utf-8")
    assert text.strip(), f"queue file is empty: {queue_path}"
    json.loads(text)
    for retired in RETIRED_PATTERNS:
        assert retired not in text, (
            f"{queue_path} still embeds retired/operator-local marker {retired!r}"
        )
