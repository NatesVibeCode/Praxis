"""End-to-end LLM-authored compile path: synthesis-then-fork.

  1. atoms (suggest_plan_atoms) — Layer 0
  2. skeleton (synthesize_skeleton) — Layer 0.5 (deterministic depends_on)
  3. synthesis (synthesize_plan_statement) — Layer 3 (one LLM call, ~3-5 sentences)
  4. fork-out N parallel author calls (fork_author_packets) — Layer 4
  5. validate (validate_authored_plan) — Layer 5
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from runtime.intent_dependency import SkeletalPlan, skeleton_from_seeds, synthesize_skeleton
from runtime.intent_suggestion import SuggestedAtoms, suggest_plan_atoms
from runtime.plan_fork_author import AuthoredPlan, fork_author_packets
from runtime.plan_pill_triage import PillTriageResult, triage_plan_pills
from runtime.plan_section_author import AuthoredPacket
from runtime.plan_section_validator import ValidationReport, validate_authored_plan
from runtime.plan_synthesis import PlanSynthesis, synthesize_plan_statement


@dataclass(frozen=True)
class ComposeViaLLMResult:
    ok: bool
    intent: str
    atoms: SuggestedAtoms
    skeleton: SkeletalPlan
    synthesis: PlanSynthesis | None
    authored: AuthoredPlan
    validation: ValidationReport
    plan_packets: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    reason_code: str | None = None
    error: str | None = None
    pill_triage: PillTriageResult | None = None

    def usage_summary(self) -> dict[str, int]:
        """Aggregate prompt/completion/cached-token totals across synth + N fork-outs."""
        total = {"prompt_tokens": 0, "completion_tokens": 0,
                 "total_tokens": 0, "cached_tokens": 0, "calls": 0}
        if self.synthesis and self.synthesis.usage:
            for k, v in self.synthesis.usage.items():
                total[k] = total.get(k, 0) + int(v or 0)
            total["calls"] += 1
        for packet in self.authored.packets:
            for k, v in (packet.usage or {}).items():
                total[k] = total.get(k, 0) + int(v or 0)
            total["calls"] += 1
        if total["prompt_tokens"]:
            total["cache_hit_ratio"] = round(
                total["cached_tokens"] / total["prompt_tokens"], 3
            )
        else:
            total["cache_hit_ratio"] = 0
        return total

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok, "intent": self.intent,
            "atoms": self.atoms.to_dict(),
            "skeleton": self.skeleton.to_dict(),
            "synthesis": self.synthesis.to_dict() if self.synthesis else None,
            "authored": self.authored.to_dict(),
            "validation": self.validation.to_dict(),
            "plan_packets": list(self.plan_packets),
            "notes": list(self.notes),
            "reason_code": self.reason_code, "error": self.error,
            "usage_summary": self.usage_summary(),
            "pill_triage": self.pill_triage.to_dict() if self.pill_triage else None,
        }


def _packet_to_translatable(packet: AuthoredPacket) -> dict[str, Any]:
    out: dict[str, Any] = {
        "label": packet.label, "stage": packet.stage,
        "description": packet.description, "write": list(packet.write),
        "depends_on": list(packet.depends_on),
        "agent": packet.agent, "task_type": packet.task_type,
        "capabilities": list(packet.capabilities),
        "consumes": list(packet.consumes), "produces": list(packet.produces),
        "gates": [dict(gate) for gate in packet.gates],
        "parameters": dict(packet.parameters), "workdir": packet.workdir,
        "on_failure": packet.on_failure, "on_success": packet.on_success,
        "timeout": packet.timeout,
        "budget": dict(packet.budget) if packet.budget else None,
        "prompt": packet.prompt,
    }
    if packet.integration_id:
        out["integration_id"] = packet.integration_id
    if packet.integration_action:
        out["integration_action"] = packet.integration_action
    if packet.integration_args:
        out["integration_args"] = dict(packet.integration_args)
    return out


def _empty_report() -> ValidationReport:
    return ValidationReport(
        findings=[], every_required_filled=False,
        no_forbidden_placeholders=False, no_workspace_root=False,
        no_dropped_floors=False, every_required_gate_scaffolded=False,
    )


def compose_plan_via_llm(
    intent: str,
    *,
    conn: Any,
    plan_name: str | None = None,
    why: str | None = None,
    concurrency: int = 20,
    hydrate_env: Any | None = None,
    llm_overrides: dict[str, Any] | None = None,
) -> ComposeViaLLMResult:
    """Compose a plan via the synthesis + fork-author chain.

    ``llm_overrides`` (optional) lets the caller override LLM call knobs on
    the synthesis + every fork-out for THIS compose run. Honoured keys:
    provider_slug, model_slug, temperature, max_tokens. The override is
    applied uniformly to both layers of the chain. Default ``None`` keeps
    today's behavior (route resolution + per-task default cap of 4096).
    """
    notes: list[str] = []

    atoms = suggest_plan_atoms(intent, conn=conn)
    if not atoms.intent:
        empty_skel = SkeletalPlan(
            parameters=[], packets=[], notes=[],
            stage_contracts={}, gate_contracts={},
        )
        return ComposeViaLLMResult(
            ok=False, intent="", atoms=atoms, skeleton=empty_skel, synthesis=None,
            authored=AuthoredPlan(packets=[], errors=[]),
            validation=_empty_report(), reason_code="intent.empty",
            error="intent is empty",
        )

    # Bootstrap skeleton drives the synthesis prompt's stage_io and pill view;
    # work-volume decomposition is the LLM's job in the synthesis call.
    bootstrap_skeleton = synthesize_skeleton(atoms, conn=conn)

    try:
        synthesis = synthesize_plan_statement(
            atoms=atoms, skeleton=bootstrap_skeleton, conn=conn,
            hydrate_env=hydrate_env, llm_overrides=llm_overrides,
        )
    except Exception as exc:  # noqa: BLE001 - surface a typed materialization blocker.
        return ComposeViaLLMResult(
            ok=False, intent=intent, atoms=atoms, skeleton=bootstrap_skeleton,
            synthesis=None,
            authored=AuthoredPlan(packets=[], errors=[]),
            validation=_empty_report(),
            reason_code="synthesis.llm_call_failed",
            error=str(exc),
            notes=notes + bootstrap_skeleton.notes,
        )

    # If the synthesis call emitted packet seeds, those drive fan-out (work
    # volume). Compile recognition is only context; deterministic fallback is
    # intentionally not allowed to masquerade as an agent-authored plan.
    if synthesis.packet_seeds:
        skeleton = skeleton_from_seeds(
            seeds=synthesis.packet_seeds, atoms=atoms, conn=conn,
        )
    else:
        return ComposeViaLLMResult(
            ok=False, intent=intent, atoms=atoms, skeleton=bootstrap_skeleton,
            synthesis=synthesis,
            authored=AuthoredPlan(packets=[], errors=[]),
            validation=_empty_report(),
            reason_code="synthesis.empty",
            error="synthesis emitted no usable packet seeds",
            notes=notes + bootstrap_skeleton.notes + list(synthesis.notes or []),
        )

    if not skeleton.packets:
        return ComposeViaLLMResult(
            ok=False, intent=intent, atoms=atoms, skeleton=skeleton,
            synthesis=synthesis,
            authored=AuthoredPlan(packets=[], errors=[]),
            validation=_empty_report(), reason_code="skeleton.empty",
            error="synthesizer + LLM produced no packets",
            notes=notes + skeleton.notes,
        )

    authored = fork_author_packets(
        atoms=atoms, skeleton=skeleton, synthesis=synthesis,
        conn=conn, concurrency=concurrency, hydrate_env=hydrate_env,
        llm_overrides=llm_overrides,
    )

    # On-demand pill triage: only fires LLM calls when packets disagree.
    # Triage shares the fork-out shared_prefix for cache hits, and carries
    # each disagreeing author's reasoning so triage doesn't re-derive.
    pill_triage = triage_plan_pills(
        atoms=atoms, packets=authored.packets, conn=conn,
    )

    validation = validate_authored_plan(authored, skeleton=skeleton, conn=conn)

    # Partial-success policy (2026-04-26, autonomous-first standing order):
    # if the majority of packets authored cleanly, drop the failed ones and
    # continue. Wholesale failure only when every packet failed. The user
    # gets a usable plan with the majority decomposition; failed packets
    # surface as warnings on compose_provenance.
    total_attempted = len(authored.packets) + len(authored.errors)
    if authored.errors and len(authored.packets) == 0:
        return ComposeViaLLMResult(
            ok=False, intent=intent, atoms=atoms, skeleton=skeleton,
            synthesis=synthesis, authored=authored, validation=validation,
            pill_triage=pill_triage,
            reason_code="fork_author.failed",
            error=f"{len(authored.errors)} of {total_attempted} packet(s) failed authoring",
            notes=notes,
        )
    if authored.errors:
        notes.append(
            f"fork_author partial: {len(authored.errors)} of {total_attempted} packet(s) failed; continuing with the {len(authored.packets)} successful packets",
        )

    if not validation.passed:
        return ComposeViaLLMResult(
            ok=False, intent=intent, atoms=atoms, skeleton=skeleton,
            synthesis=synthesis, authored=authored, validation=validation,
            pill_triage=pill_triage,
            reason_code="validation.failed",
            error=f"{sum(1 for f in validation.findings if f.severity == 'error')} validation error(s)",
            notes=notes,
        )

    plan_packets = [_packet_to_translatable(p) for p in authored.packets]
    if plan_name:
        notes.append(f"plan_name (caller-provided): {plan_name}")
    if why:
        notes.append(f"why: {why}")

    return ComposeViaLLMResult(
        ok=True, intent=intent, atoms=atoms, skeleton=skeleton,
        synthesis=synthesis, authored=authored, validation=validation,
        plan_packets=plan_packets, notes=notes,
    )
