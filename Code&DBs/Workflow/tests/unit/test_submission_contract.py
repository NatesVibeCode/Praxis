from __future__ import annotations

import pytest

from runtime.workflow.submission_contract import (
    SubmissionContractError,
    normalize_declared_operations,
    normalize_scope_paths,
    optional_datetime,
)


def test_declared_operations_normalize_actions_and_scope_paths() -> None:
    operations = normalize_declared_operations(
        [
            {"action": " create ", "path": "./docs/readme.md"},
            {"action": "update", "path": "docs/readme.md"},
            {"action": "rename", "path": "docs/archive.md", "from_path": "./docs/readme.md"},
        ]
    )

    assert operations == [
        {"path": "docs/readme.md", "action": "create"},
        {"path": "docs/readme.md", "action": "update"},
        {"path": "docs/archive.md", "action": "rename", "from_path": "docs/readme.md"},
    ]
    assert normalize_scope_paths(["./docs", "docs", " ./notes "]) == ["docs", "notes"]


def test_optional_datetime_normalizes_and_rejects_invalid_text() -> None:
    parsed = optional_datetime("2026-04-24T12:34:56Z", field_name="requested_at")

    assert parsed is not None
    assert parsed.isoformat() == "2026-04-24T12:34:56+00:00"

    with pytest.raises(SubmissionContractError) as exc_info:
        optional_datetime("not-a-timestamp", field_name="requested_at")

    assert exc_info.value.field_name == "requested_at"
