from __future__ import annotations

import json
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]
_SPEC_PATH = (
    _REPO_ROOT
    / "config"
    / "cascade"
    / "specs"
    / "W_mobile_setup_authority_spine_20260422.queue.json"
)


def test_mobile_authority_spine_jobs_declare_sealed_completion_contracts() -> None:
    spec = json.loads(_SPEC_PATH.read_text(encoding="utf-8"))
    jobs = spec["jobs"]

    assert jobs
    for job in jobs:
        contract = job.get("completion_contract")
        assert isinstance(contract, dict), job["label"]
        assert contract["submission_required"] is True
        assert contract["verification_required"] is True
        assert contract["result_kind"] in {
            "artifact_bundle",
            "code_change",
            "research_result",
        }
        assert contract["submit_tool_names"]
        assert all(str(name).startswith("praxis_submit_") for name in contract["submit_tool_names"])


def test_mobile_authority_spine_preserves_artifact_vs_code_change_boundary() -> None:
    spec = json.loads(_SPEC_PATH.read_text(encoding="utf-8"))
    by_label = {job["label"]: job["completion_contract"] for job in spec["jobs"]}

    assert by_label["audit_mobile_state"]["result_kind"] == "artifact_bundle"
    assert by_label["command_bus_integration_plan"]["result_kind"] == "artifact_bundle"
    assert by_label["build_plan_envelope"]["result_kind"] == "code_change"
    assert by_label["build_grant_resolver"]["result_kind"] == "code_change"
    assert by_label["build_approval_lifecycle"]["result_kind"] == "code_change"
