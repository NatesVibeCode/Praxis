from __future__ import annotations

import json
from pathlib import Path

from runtime.workflow_eval import build_agent_handoff_probe_review


def test_build_agent_handoff_probe_review_writes_authoritative_review_contract(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    handoff_path = workspace_root / "artifacts" / "probe.handoff.md"
    handoff_path.parent.mkdir(parents=True, exist_ok=True)
    handoff_path.write_text(
        "\n".join(
            [
                "# Search Proof",
                "- discover_local_code returned live code matches",
                "# Authority Gaps",
                "- none",
                "# Next Verification",
                "- inspect the saved review artifact",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_agent_handoff_probe_review(
        {
            "workspace_root": str(workspace_root),
            "handoff_path": str(handoff_path.relative_to(workspace_root)),
            "review_path": "artifacts/probe.review.json",
            "write_side_bug_anchor": "BUG-AE2B9669",
            "discover_local_code": {"tool_result": {"matches": ["runtime/workflow_spec.py"]}},
            "query_bug_db": {"tool_result": {"open_count": 12}},
        }
    )

    review_payload = result["review_payload"]
    assert review_payload["verdict"].startswith("Authoritative:")
    assert review_payload["agents_exercised"].startswith("Yes:")
    assert review_payload["information_handoff_proven"].startswith("Yes:")
    assert review_payload["search_exercised"].startswith("Yes:")
    assert review_payload["db_action_exercised"].startswith("Yes:")
    assert result["review_artifact_path"] == "artifacts/probe.review.json"
    code_block = result["code_blocks"][0]
    assert code_block["file_path"] == "artifacts/probe.review.json"
    assert json.loads(code_block["content"]) == review_payload


def test_build_agent_handoff_probe_review_marks_missing_handoff_non_authoritative(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"

    result = build_agent_handoff_probe_review(
        {
            "workspace_root": str(workspace_root),
            "handoff_path": "artifacts/missing.handoff.md",
            "review_path": "artifacts/probe.review.json",
            "write_side_bug_anchor": "BUG-AE2B9669",
            "discover_local_code": {},
            "query_bug_db": {},
        }
    )

    review_payload = result["review_payload"]
    assert review_payload["verdict"].startswith("Non-authoritative:")
    assert "BUG-AE2B9669" in review_payload["verdict"]
    assert review_payload["information_handoff_proven"].startswith("No:")
    assert review_payload["search_exercised"].startswith("No authoritative")
    assert review_payload["db_action_exercised"].startswith("No authoritative")
