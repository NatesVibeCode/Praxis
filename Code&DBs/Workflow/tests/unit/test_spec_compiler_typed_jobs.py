"""Phase 2.1.C regression: spec_compiler runtime jobs carry typed contracts.

Before this change, ``_packet_to_job`` produced workflow-spec jobs with
``agent``, ``prompt``, ``task_type``, ``write_scope`` etc but no
``consumes`` / ``produces``. Workers received typeless jobs and could not
emit typed produces back to the graph. Downstream consumers
(data_dictionary_lineage, release gates, typed_gap surfacing) had nothing
to bind to.

The fix: ``_packet_to_job`` resolves a typed contract from agent route +
label + description and adds the typed shape to every emitted job dict.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.spec_compiler import CompiledSpec, PlanPacket, _packet_to_job


def _stub_compiled(prompt: str = "PROMPT", verify_refs=("verify.x",)) -> CompiledSpec:
    return CompiledSpec(
        prompt=prompt,
        scope_write=["src/foo.py"],
        scope_read=None,
        capabilities=["capability.code.python"],
        tier="mid",
        label="build:foo",
        task_type="build",
        verify_refs=list(verify_refs),
        workspace_ref="workspace.default",
        runtime_profile_ref="runtime.default",
    )


def test_packet_to_job_carries_typed_contract_for_auto_build():
    packet = PlanPacket(
        description="Implement the new feature",
        write=["src/foo.py"],
        stage="build",
        label="implement-feature",
    )
    job = _packet_to_job(packet, compiled=_stub_compiled(), workdir="/repo", index=0)
    assert "consumes" in job
    assert "consumes_any" in job
    assert "produces" in job
    # build family produces code_change / diff / execution_receipt
    assert "code_change" in job["produces"]
    assert "execution_receipt" in job["produces"]


def test_packet_to_job_carries_typed_contract_for_auto_review():
    packet = PlanPacket(
        description="Review the implementation",
        write=["src/foo.py"],
        stage="review",
        label="review-step",
        agent="auto/review",
    )
    job = _packet_to_job(packet, compiled=_stub_compiled(), workdir="/repo", index=0)
    assert job["produces"] == ["review_result"]
    assert "code_change" in job["consumes_any"]


def test_packet_to_job_typed_contract_uses_description_text():
    """Description text feeds the inference search; a research-titled
    packet on auto/build still ends up with research produces because
    the description tokens win the lookup."""
    packet = PlanPacket(
        description="Research the existing API surface and gather sources",
        write=["docs/findings.md"],
        stage="build",
        label="research-step",
    )
    job = _packet_to_job(packet, compiled=_stub_compiled(), workdir="/repo", index=0)
    # research/gather tokens in description match the research family
    assert "research_findings" in job["produces"] or "evidence_pack" in job["produces"]


def test_packet_to_job_typed_contract_for_unknown_agent_falls_back_to_default():
    packet = PlanPacket(
        description="Do something nobody else does",
        write=["misc/x"],
        stage="build",
        label="weird-step",
        agent="auto/unicorn",
    )
    job = _packet_to_job(packet, compiled=_stub_compiled(), workdir="/repo", index=0)
    # Unknown route + generic description → either research/build via
    # description tokens or default ("result",). Either way, the job
    # carries the typed shape.
    assert isinstance(job["produces"], list) and len(job["produces"]) >= 1


def test_packet_to_job_legacy_fields_still_present():
    """Adding typed contract must not break the existing job shape."""
    packet = PlanPacket(
        description="Build something",
        write=["src/x.py"],
        stage="build",
        label="legacy-test",
        depends_on=["other-step"],
    )
    job = _packet_to_job(packet, compiled=_stub_compiled(), workdir="/repo", index=0)
    # All the legacy fields are still in place
    assert job["label"] == "legacy-test"
    assert job["agent"] == "auto/build"
    assert job["prompt"]
    assert job["task_type"] == "build"
    assert job["write_scope"] == ["src/x.py"]
    assert job["workdir"] == "/repo"
    assert job["depends_on"] == ["other-step"]
    assert job["verify_refs"] == ["verify.x"]
    assert job["capabilities"] == ["capability.code.python"]
