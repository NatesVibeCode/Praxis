from __future__ import annotations

from pathlib import Path
import sys

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.mcp.catalog import get_tool_catalog
from surfaces.mcp.tools import submission as submission_tools


def test_submission_tools_are_discoverable() -> None:
    catalog = get_tool_catalog()
    for tool_name in (
        "praxis_submit_code_change",
        "praxis_submit_research_result",
        "praxis_submit_artifact_bundle",
        "praxis_get_submission",
        "praxis_review_submission",
    ):
        assert tool_name in catalog


def test_tool_wrappers_forward_parameters(monkeypatch) -> None:
    captured = {}

    def _submit_code_change(**kwargs):
        captured["code_change"] = kwargs
        return {"ok": True, "tool": "praxis_submit_code_change", "submission": {"submission_id": "sub-1"}}

    monkeypatch.setattr(submission_tools.workflow_submission, "submit_code_change", _submit_code_change)

    payload = submission_tools.tool_praxis_submit_code_change(
        {
            "summary": "Done",
            "primary_paths": ["runtime/workflow/submission_capture.py"],
            "result_kind": "code_change",
            "tests_ran": ["pytest"],
        }
    )

    assert payload["ok"] is True
    assert captured["code_change"]["result_kind"] == "code_change"
    assert captured["code_change"]["primary_paths"] == ["runtime/workflow/submission_capture.py"]
