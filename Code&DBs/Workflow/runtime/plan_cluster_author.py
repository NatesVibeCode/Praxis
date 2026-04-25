"""Layer 4 (Cluster Author): per-stage-cluster LLM filling of skeletal packets.

Replaces ``runtime.plan_section_author``'s per-packet fan-out. Authors N
packets in ONE LLM call when they share a stage, so the LLM sees siblings
and keeps prompt voice / parameter shapes / write_scope conventions
consistent across the cluster. Clusters are dispatched in topological
waves (research → review → build, etc.) so a downstream cluster sees
already-authored upstream packets and can bind precisely instead of
hallucinating output keys.

Concurrency: each wave runs ``min(max_concurrency, len(wave))`` workers
in parallel. Default cap is 20 so wide plans aren't pinned to 4 lanes,
but workers spin up only as needed (a 3-cluster wave uses 3 workers,
not 20).

Honest scope: this layer never fills cross-cluster structure
(``depends_on``, gate ids, stage assignment, floors). Those come from
the synthesizer. The cluster author only fills inside packets.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_dependency import SkeletalPacket, SkeletalPlan
from runtime.intent_suggestion import SuggestedAtoms
from runtime.plan_section_author import (
    AuthoredPacket,
    AuthorError,
    SectionSandbox,
    _coerce_packet_response,
    _section_author_timeout_seconds,
    _strip_json_fences,
    build_section_sandbox,
)

logger = logging.getLogger(__name__)


_DEFAULT_MAX_CONCURRENCY = 20


@dataclass(frozen=True)
class AuthoredPlan:
    """Cluster-author output."""

    packets: list[AuthoredPacket]
    errors: list[AuthorError]
    notes: list[str] = field(default_factory=list)
    waves: list[list[str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "packets": [p.to_dict() for p in self.packets],
            "errors": [e.to_dict() for e in self.errors],
            "notes": list(self.notes),
            "waves": [list(wave) for wave in self.waves],
        }


def _group_by_stage(packets: list[SkeletalPacket]) -> dict[str, list[SkeletalPacket]]:
    """Stage → packets in clause order."""
    by_stage: dict[str, list[SkeletalPacket]] = defaultdict(list)
    for packet in packets:
        by_stage[packet.stage].append(packet)
    for stage in by_stage:
        by_stage[stage].sort(key=lambda p: p.clause_offset)
    return by_stage


def _stage_dag(packets: list[SkeletalPacket]) -> dict[str, set[str]]:
    """Stage A → set of stages it depends on (any packet in A depends on a packet in B → A→B)."""
    label_to_stage = {p.label: p.stage for p in packets}
    edges: dict[str, set[str]] = defaultdict(set)
    for packet in packets:
        for upstream_label in packet.depends_on:
            upstream_stage = label_to_stage.get(upstream_label)
            if upstream_stage and upstream_stage != packet.stage:
                edges[packet.stage].add(upstream_stage)
    for packet in packets:
        edges.setdefault(packet.stage, set())
    return edges


def _topological_waves(stage_edges: dict[str, set[str]]) -> list[list[str]]:
    """Kahn's-algorithm waves: each wave is the set of stages with no unresolved deps."""
    indegree: dict[str, int] = {stage: len(deps) for stage, deps in stage_edges.items()}
    reverse: dict[str, set[str]] = defaultdict(set)
    for stage, deps in stage_edges.items():
        for dep in deps:
            reverse[dep].add(stage)
    waves: list[list[str]] = []
    pending = set(indegree.keys())
    while pending:
        wave = sorted(stage for stage in pending if indegree[stage] == 0)
        if not wave:
            # Cycle — fall back to single-wave to avoid infinite loop. Cycles
            # in the synthesizer's stage DAG would be a synthesizer bug.
            waves.append(sorted(pending))
            break
        waves.append(wave)
        for stage in wave:
            pending.remove(stage)
            for downstream in reverse[stage]:
                indegree[downstream] -= 1
    return waves


def _packet_skeleton_view(packet: SkeletalPacket) -> dict[str, Any]:
    return {
        "label": packet.label,
        "stage": packet.stage,
        "description": packet.description,
        "consumes_floor": list(packet.consumes_floor),
        "produces_floor": list(packet.produces_floor),
        "capabilities_floor": list(packet.capabilities_floor),
        "depends_on": list(packet.depends_on),
        "scaffolded_gate_ids": [gate.gate_id for gate in packet.gates_scaffold],
    }


def _authored_upstream_view(authored: list[AuthoredPacket]) -> list[dict[str, Any]]:
    """Compact view of already-authored upstream packets a cluster may bind against."""
    return [
        {
            "label": p.label,
            "stage": p.stage,
            "produces": list(p.produces),
            "prompt_excerpt": (p.prompt or "")[:240],
        }
        for p in authored
    ]


def _build_cluster_prompt(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    cluster_stage: str,
    cluster_packets: list[SkeletalPacket],
    upstream_authored: list[AuthoredPacket],
    sandbox: SectionSandbox,
) -> str:
    """One prompt that asks the LLM to author every packet in this stage cluster."""

    pills_view = [
        {
            "ref": pill.ref,
            "summary": (pill.description or "")[:140],
            "score": round(pill.score, 1),
        }
        for pill in atoms.pills.suggested[:6]
    ]
    parameters_view = [
        {"name": p.name, "type_hint": p.type_hint} for p in atoms.parameters
    ]

    field_lines: list[str] = []
    for entry in sandbox.plan_field_schema:
        meta = entry.get("metadata", {})
        flags: list[str] = []
        if meta.get("required"):
            flags.append("REQUIRED")
        if meta.get("type"):
            flags.append(f"type={meta['type']}")
        if meta.get("picker_source"):
            flags.append(f"pick={meta['picker_source']}")
        if meta.get("floor_from"):
            flags.append(f"floor={meta['floor_from']}")
        if meta.get("forbid_workspace_root"):
            flags.append("no-workspace-root")
        if meta.get("forbid_placeholders"):
            flags.append(f"no-placeholders={meta['forbid_placeholders']}")
        field_lines.append(
            f"  - {entry['field']}: {entry['summary']} [{', '.join(flags)}]"
        )

    cluster_view = [_packet_skeleton_view(packet) for packet in cluster_packets]
    upstream_view = _authored_upstream_view(upstream_authored)
    catalog_view = {
        "tools": [tool["name"] for tool in sandbox.tools[:30]],
        "skills": [
            f"{skill['name']}: {skill['summary'][:120]}" for skill in sandbox.skills[:20]
        ],
        "stage_io": sandbox.stage_io,
    }

    sections: list[str] = [
        f"TASK: Author every packet in the '{cluster_stage}' stage cluster of "
        f"a workflow plan. There are {len(cluster_packets)} packet(s) in this cluster.",
        "",
        "Cross-packet structure (depends_on, gate ids, consumes/produces floor) "
        "is already wired by the deterministic synthesizer. Keep what is given. "
        "You fill the menu-level fields inside every packet of this cluster.",
        "",
        "WITHIN-CLUSTER CONSISTENCY: because you see all sibling packets at once, "
        "use the SAME parameter-binding shape across all packets you author, the "
        "SAME prompt voice, and non-overlapping write_scope globs.",
        "",
        "SOURCE PROSE:",
        atoms.intent,
        "",
        "PACKETS TO AUTHOR (one PlanPacket JSON per entry, in this order):",
        json.dumps(cluster_view, indent=2),
        "",
        "ALREADY-AUTHORED UPSTREAM PACKETS (bind your parameters to these by label; "
        "use only outputs that appear in their produces or prompt_excerpt):",
        json.dumps(upstream_view, indent=2),
        "",
        "PILLS YOU MAY REFERENCE (top 6 from data dictionary):",
        json.dumps(pills_view, indent=2),
        "",
        "WORKFLOW PARAMETERS:",
        json.dumps(parameters_view, indent=2),
        "",
        "FIELDS TO FILL (every REQUIRED field must be set on every packet):",
        "\n".join(field_lines),
        "",
        "CATALOG (sandbox):",
        json.dumps(catalog_view, indent=2),
        "",
        f"STANDING ORDER (provider routing): {sandbox.standing_orders}",
        "",
        "RULES:",
        "  - Reference parameters by {name} (e.g. {app_name}).",
        "  - Reference upstream outputs by their packet label.",
        "  - Never write TBD / TODO / FIXME / 'auto' (unless 'pick' admits 'auto').",
        "  - write must be precise globs; never ['.'] (workspace-root is forbidden).",
        "  - Keep depends_on as given; the synthesizer wired it.",
        "  - capabilities / consumes / produces begin at the floor; you may add "
        "but never drop floor entries.",
        "  - gates MUST be a list of OBJECTS, one per scaffolded gate id. "
        "Required shape: {\"gate_id\": \"<one of scaffolded_gate_ids>\", "
        "\"params\": {<gate-specific keys derived from THIS packet's intent>}}. "
        "If you have no real params, emit an empty params dict. Bare strings are "
        "not acceptable. Never fabricate ref ids you did not see in the skeleton "
        "or pills.",
        "  - Prefer auto/<task_type> for agent unless catalog forces otherwise.",
        "  - Across the cluster, use ONE shape for the parameters dict and ONE "
        "voice for prompt text. Do not let sibling packets diverge.",
        "",
        f"OUTPUT: a JSON OBJECT with exactly one key 'packets' whose value is a "
        f"JSON array of EXACTLY {len(cluster_packets)} PlanPacket objects, in "
        f"the same order as PACKETS TO AUTHOR. No markdown fences. No prose "
        f"outside the JSON. No commentary.",
    ]
    return "\n".join(sections)


def _call_cluster_llm(prompt: str, *, hydrate_env: Any | None = None) -> tuple[str, str, str]:
    """Send the cluster prompt; return (raw_content, provider_slug, model_slug)."""
    from adapters.keychain import resolve_secret
    from adapters.llm_client import LLMRequest, call_llm
    from registry.provider_execution_registry import (
        resolve_api_endpoint,
        resolve_api_key_env_vars,
        resolve_api_protocol_family,
    )
    from runtime.plan_section_author import _resolve_section_author_routes

    if hydrate_env is not None:
        hydrate_env()

    timeout = int(_section_author_timeout_seconds())
    last_error: Exception | None = None
    for provider_slug, model_slug in _resolve_section_author_routes():
        try:
            endpoint = resolve_api_endpoint(provider_slug, model_slug)
            if not endpoint:
                raise RuntimeError(f"no registered endpoint for {provider_slug}/{model_slug}")
            protocol_family = resolve_api_protocol_family(provider_slug)
            if not protocol_family:
                raise RuntimeError(f"no protocol_family for {provider_slug}")

            env = dict(os.environ)
            api_key: str | None = None
            for env_var in resolve_api_key_env_vars(provider_slug):
                candidate = resolve_secret(env_var, env=env)
                if candidate and candidate.strip():
                    api_key = candidate.strip()
                    break
            if not api_key:
                raise RuntimeError(f"no API key for {provider_slug} (Keychain + env both empty)")

            request = LLMRequest(
                endpoint_uri=str(endpoint),
                api_key=api_key,
                provider_slug=provider_slug,
                model_slug=model_slug,
                messages=({"role": "user", "content": prompt},),
                protocol_family=str(protocol_family),
                timeout_seconds=timeout,
                retry_attempts=0,
            )
            response = call_llm(request)
            return response.content, provider_slug, model_slug
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Cluster-author route failed for %s/%s: %s",
                provider_slug,
                model_slug,
                exc,
            )
    if last_error is not None:
        raise last_error
    raise RuntimeError("task_type_routing returned no llm_task routes for plan_section_author")


def author_plan_cluster(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    cluster_stage: str,
    cluster_packets: list[SkeletalPacket],
    upstream_authored: list[AuthoredPacket],
    sandbox: SectionSandbox,
    hydrate_env: Any | None = None,
) -> tuple[list[AuthoredPacket], list[AuthorError]]:
    """Author every packet in one stage cluster via a single LLM call."""
    prompt = _build_cluster_prompt(
        atoms=atoms,
        skeleton=skeleton,
        cluster_stage=cluster_stage,
        cluster_packets=cluster_packets,
        upstream_authored=upstream_authored,
        sandbox=sandbox,
    )

    try:
        raw, provider_slug, model_slug = _call_cluster_llm(prompt, hydrate_env=hydrate_env)
    except Exception as exc:
        return [], [
            AuthorError(
                label=packet.label,
                error=str(exc),
                reason_code="cluster_author.llm_call_failed",
                raw_llm_response=None,
            )
            for packet in cluster_packets
        ]

    try:
        parsed_envelope = json.loads(_strip_json_fences(raw))
    except json.JSONDecodeError as exc:
        return [], [
            AuthorError(
                label=packet.label,
                error=f"cluster author returned non-JSON: {exc}",
                reason_code="cluster_author.parse_failed",
                raw_llm_response=raw,
            )
            for packet in cluster_packets
        ]

    parsed_packets = (
        parsed_envelope.get("packets")
        if isinstance(parsed_envelope, dict)
        else parsed_envelope
    )
    if not isinstance(parsed_packets, list):
        return [], [
            AuthorError(
                label=packet.label,
                error="cluster author did not return a 'packets' array",
                reason_code="cluster_author.shape_invalid",
                raw_llm_response=raw,
            )
            for packet in cluster_packets
        ]

    successes: list[AuthoredPacket] = []
    failures: list[AuthorError] = []

    by_label: dict[str, dict[str, Any]] = {}
    for entry in parsed_packets:
        if isinstance(entry, dict):
            label = str(entry.get("label") or "").strip()
            if label:
                by_label[label] = entry

    for index, packet in enumerate(cluster_packets):
        entry = by_label.get(packet.label)
        if entry is None and index < len(parsed_packets) and isinstance(
            parsed_packets[index], dict
        ):
            entry = parsed_packets[index]
        if entry is None:
            failures.append(
                AuthorError(
                    label=packet.label,
                    error="cluster author returned no entry for this packet",
                    reason_code="cluster_author.packet_missing",
                    raw_llm_response=raw,
                )
            )
            continue
        successes.append(
            _coerce_packet_response(
                target=packet,
                parsed=entry,
                raw=raw,
                provider_slug=provider_slug,
                model_slug=model_slug,
            )
        )

    return successes, failures


def author_plan_clusters_in_waves(
    *,
    atoms: SuggestedAtoms,
    skeleton: SkeletalPlan,
    conn: Any,
    max_concurrency: int = _DEFAULT_MAX_CONCURRENCY,
    hydrate_env: Any | None = None,
) -> AuthoredPlan:
    """Group packets by stage, walk the stage DAG in waves, fan out parallel clusters."""
    sandbox = build_section_sandbox(conn)
    notes: list[str] = []
    if not sandbox.plan_field_schema:
        notes.append(
            "no plan_field rows registered; apply migration 247 — cluster author "
            "validates against an empty schema until then"
        )
    if not skeleton.packets:
        notes.append("skeleton has no packets — nothing to author")
        return AuthoredPlan(packets=[], errors=[], notes=notes, waves=[])

    by_stage = _group_by_stage(skeleton.packets)
    stage_edges = _stage_dag(skeleton.packets)
    waves = _topological_waves(stage_edges)

    authored_so_far: list[AuthoredPacket] = []
    failures: list[AuthorError] = []
    completed_waves: list[list[str]] = []

    for wave in waves:
        wave_size = len(wave)
        worker_count = max(1, min(max_concurrency, wave_size))
        wave_results: dict[str, tuple[list[AuthoredPacket], list[AuthorError]]] = {}

        def _run_one(stage: str) -> tuple[str, list[AuthoredPacket], list[AuthorError]]:
            cluster_packets = by_stage.get(stage, [])
            if not cluster_packets:
                return stage, [], []
            upstream_stages = stage_edges.get(stage, set())
            upstream_authored = [p for p in authored_so_far if p.stage in upstream_stages]
            successes, errors = author_plan_cluster(
                atoms=atoms,
                skeleton=skeleton,
                cluster_stage=stage,
                cluster_packets=cluster_packets,
                upstream_authored=upstream_authored,
                sandbox=sandbox,
                hydrate_env=hydrate_env,
            )
            return stage, successes, errors

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = {pool.submit(_run_one, stage): stage for stage in wave}
            for future in concurrent.futures.as_completed(futures):
                stage = futures[future]
                try:
                    _, successes, errors = future.result()
                except Exception as exc:
                    failures.extend(
                        AuthorError(
                            label=packet.label,
                            error=f"{type(exc).__name__}: {exc}",
                            reason_code="cluster_author.unhandled",
                            raw_llm_response=None,
                        )
                        for packet in by_stage.get(stage, [])
                    )
                    continue
                wave_results[stage] = (successes, errors)

        for stage in wave:
            successes, errors = wave_results.get(stage, ([], []))
            authored_so_far.extend(successes)
            failures.extend(errors)

        completed_waves.append(wave)

    label_order = {p.label: i for i, p in enumerate(skeleton.packets)}
    authored_so_far.sort(key=lambda p: label_order.get(p.label, 999_999))

    return AuthoredPlan(
        packets=authored_so_far,
        errors=failures,
        notes=notes,
        waves=completed_waves,
    )
