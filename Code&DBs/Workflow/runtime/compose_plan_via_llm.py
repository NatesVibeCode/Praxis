"""End-to-end LLM-authored compile path for free prose.

Wires the layers together so a user's prose with no markers, no order,
no per-step detail produces a launch-ready spec:

  1. Layer 0    suggest_plan_atoms       — atoms (pills, step types, parameters)
  2. Layer 0.5  synthesize_skeleton      — depends_on / floors / gate scaffolds
  3. Layer 4    author_plan_sections_parallel — N parallel LLM calls fill menu fields
  4. Layer 5    validate_authored_plan   — every-field-set check against plan_field schema
  5. Layer 6    translate                — packet list → workflow spec (preview)

Call this when the prose has no explicit step markers. For prose that
already has numbered/bulleted/first-then-finally markers, the
deterministic ``compose_plan_from_intent`` path is faster and stricter.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from runtime.intent_dependency import SkeletalPlan, synthesize_skeleton
from runtime.intent_suggestion import SuggestedAtoms, suggest_plan_atoms
from runtime.plan_cluster_author import AuthoredPlan, author_plan_clusters_in_waves
from runtime.plan_section_author import AuthoredPacket
from runtime.plan_section_validator import ValidationReport, validate_authored_plan


@dataclass(frozen=True)
class ComposeViaLLMResult:
    ok: bool
    intent: str
    atoms: SuggestedAtoms
    skeleton: SkeletalPlan
    authored: AuthoredPlan
    validation: ValidationReport
    plan_packets: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    reason_code: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "intent": self.intent,
            "atoms": self.atoms.to_dict(),
            "skeleton": self.skeleton.to_dict(),
            "authored": self.authored.to_dict(),
            "validation": self.validation.to_dict(),
            "plan_packets": list(self.plan_packets),
            "notes": list(self.notes),
            "reason_code": self.reason_code,
            "error": self.error,
        }


def _packet_to_translatable(packet: AuthoredPacket) -> dict[str, Any]:
    """Shape an AuthoredPacket as a dict the existing launch_plan path accepts."""
    return {
        "label": packet.label,
        "stage": packet.stage,
        "description": packet.description,
        "write": list(packet.write),
        "depends_on": list(packet.depends_on),
        "agent": packet.agent,
        "task_type": packet.task_type,
        "capabilities": list(packet.capabilities),
        "consumes": list(packet.consumes),
        "produces": list(packet.produces),
        "gates": [dict(gate) for gate in packet.gates],
        "parameters": dict(packet.parameters),
        "workdir": packet.workdir,
        "on_failure": packet.on_failure,
        "on_success": packet.on_success,
        "timeout": packet.timeout,
        "budget": dict(packet.budget) if packet.budget else None,
        "prompt": packet.prompt,
    }


def compose_plan_via_llm(
    intent: str,
    *,
    conn: Any,
    plan_name: str | None = None,
    why: str | None = None,
    concurrency: int = 20,
    hydrate_env: Any | None = None,
) -> ComposeViaLLMResult:
    """Run the full LLM-authored compile pipeline on free prose.

    Returns a ``ComposeViaLLMResult`` carrying every layer's output so
    callers can inspect failures (e.g. the validator may surface dropped
    floors that block translation). When ``ok=True``, ``plan_packets``
    holds the launch-ready packet dicts; the caller passes them to
    ``launch_plan`` to translate into a workflow spec.
    """
    notes: list[str] = []

    atoms = suggest_plan_atoms(intent, conn=conn)
    if not atoms.intent:
        return ComposeViaLLMResult(
            ok=False,
            intent="",
            atoms=atoms,
            skeleton=SkeletalPlan(parameters=[], packets=[], notes=[], stage_contracts={}, gate_contracts={}),
            authored=AuthoredPlan(packets=[], errors=[]),
            validation=ValidationReport(
                findings=[], every_required_filled=False, no_forbidden_placeholders=False,
                no_workspace_root=False, no_dropped_floors=False, every_required_gate_scaffolded=False,
            ),
            reason_code="intent.empty",
            error="intent is empty",
        )

    skeleton = synthesize_skeleton(atoms, conn=conn)
    if not skeleton.packets:
        return ComposeViaLLMResult(
            ok=False,
            intent=intent,
            atoms=atoms,
            skeleton=skeleton,
            authored=AuthoredPlan(packets=[], errors=[]),
            validation=ValidationReport(
                findings=[], every_required_filled=False, no_forbidden_placeholders=False,
                no_workspace_root=False, no_dropped_floors=False, every_required_gate_scaffolded=False,
            ),
            reason_code="skeleton.empty",
            error="synthesizer produced no packets — reword the prose with more concrete verbs",
            notes=notes + skeleton.notes,
        )

    authored = author_plan_clusters_in_waves(
        atoms=atoms,
        skeleton=skeleton,
        conn=conn,
        max_concurrency=concurrency,
        hydrate_env=hydrate_env,
    )

    validation = validate_authored_plan(authored, skeleton=skeleton, conn=conn)

    if authored.errors:
        return ComposeViaLLMResult(
            ok=False,
            intent=intent,
            atoms=atoms,
            skeleton=skeleton,
            authored=authored,
            validation=validation,
            reason_code="section_author.failed",
            error=f"{len(authored.errors)} section(s) failed authoring",
            notes=notes,
        )

    if not validation.passed:
        return ComposeViaLLMResult(
            ok=False,
            intent=intent,
            atoms=atoms,
            skeleton=skeleton,
            authored=authored,
            validation=validation,
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
        ok=True,
        intent=intent,
        atoms=atoms,
        skeleton=skeleton,
        authored=authored,
        validation=validation,
        plan_packets=plan_packets,
        notes=notes,
    )
