"""Repo-local native operator CLI surface.

This module stays thin over existing authority and derived read surfaces:

- repo-local instance resolution
- native frontdoor health and status reads
- repo-local smoke execution
- derived inspection reads
- graph topology and lineage reads
- bounded route/workflow/cutover truth reads
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import TextIO

from observability import graph_lineage_run, graph_topology_run
from runtime.execution.orchestrator import RuntimeOrchestrator
from runtime.instance import resolve_native_instance
from registry.provider_onboarding import load_provider_onboarding_spec_from_file, run_provider_onboarding
from storage.postgres import PostgresEvidenceReader
from surfaces.api import frontdoor
from surfaces.api import native_operator_surface
from surfaces.api import operator_read
from surfaces.api import operator_write
from surfaces.api.operator_read import run_native_self_hosted_smoke

from .render import render_graph_lineage, render_graph_topology, render_inspection

__all__ = ["main"]


@dataclass(frozen=True, slots=True)
class InstanceCommand:
    """Show the resolved repo-local native instance contract."""


@dataclass(frozen=True, slots=True)
class DbHealthCommand:
    """Show the repo-local Postgres health snapshot."""


@dataclass(frozen=True, slots=True)
class DbBootstrapCommand:
    """Bootstrap the repo-local Postgres control plane schema."""


@dataclass(frozen=True, slots=True)
class SmokeCommand:
    """Run the repo-local native smoke sequence."""


@dataclass(frozen=True, slots=True)
class InspectCommand:
    """Render the derived inspect view for one run."""

    run_id: str


@dataclass(frozen=True, slots=True)
class FrontdoorStatusCommand:
    """Read native frontdoor status for one run."""

    run_id: str


@dataclass(frozen=True, slots=True)
class GraphTopologyCommand:
    """Read graph topology for one run."""

    run_id: str


@dataclass(frozen=True, slots=True)
class GraphLineageCommand:
    """Read graph lineage for one run."""

    run_id: str


@dataclass(frozen=True, slots=True)
class CockpitCommand:
    """Read the bounded route/workflow/cutover truth surface for one run."""

    run_id: str


@dataclass(frozen=True, slots=True)
class RouteDisableCommand:
    """Apply one bounded route-disable window for a provider or provider/model scope."""

    provider_slug: str
    effective_to: datetime
    task_type: str | None
    model_slug: str | None
    reason_code: str
    rationale: str | None
    decision_ref: str | None


@dataclass(frozen=True, slots=True)
class RoadmapWriteCommand:
    """Preview, validate, or commit roadmap rows through the shared operator write gate."""

    action: str
    title: str
    intent_brief: str
    template: str
    priority: str
    parent_roadmap_item_id: str | None
    slug: str | None
    depends_on: tuple[str, ...]
    source_bug_id: str | None
    registry_paths: tuple[str, ...]
    decision_ref: str | None
    item_kind: str | None
    tier: str | None
    phase_ready: bool | None
    approval_tag: str | None
    reference_doc: str | None
    outcome_gate: str | None


@dataclass(frozen=True, slots=True)
class WorkItemCloseoutCommand:
    """Preview or commit proof-backed bug and roadmap closeout through the shared gate."""

    action: str
    bug_ids: tuple[str, ...]
    roadmap_item_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RoadmapViewCommand:
    """Render one roadmap subtree from DB-backed authority."""

    root_roadmap_item_id: str
    as_json: bool


@dataclass(frozen=True, slots=True)
class ProviderOnboardingCommand:
    """Seed a provider profile, model catalog rows, benchmark metadata, and verification."""

    spec_path: str
    dry_run: bool


@dataclass(frozen=True, slots=True)
class NativePrimaryCutoverGateCommand:
    """Admit a native-primary cutover gate through operator control."""

    decided_by: str
    decision_source: str
    rationale: str
    roadmap_item_id: str | None
    workflow_class_id: str | None
    schedule_definition_id: str | None
    title: str | None
    gate_name: str | None
    gate_policy: Mapping[str, object] | None
    required_evidence: Mapping[str, object] | None
    decided_at: datetime | None
    opened_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None


def _usage() -> str:
    return (
        "usage: workflow native-operator "
        "<instance|health|db-health|bootstrap|db-bootstrap|smoke|inspect|"
        "status|graph-topology|graph-lineage|cockpit|route-disable|roadmap-write|"
        "work-item-closeout|roadmap-view|provider-onboard|native-primary-cutover-gate> [args]"
    )


def _unsupported_start_message() -> str:
    return (
        "workflow native-operator start has been removed; "
        "use workflow native-operator instance for the native contract read"
    )


def _parse_datetime(value: str, *, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 datetime with timezone") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed


def _parse_route_disable(args: list[str]) -> RouteDisableCommand:
    if len(args) < 3:
        raise ValueError(
            "usage: workflow native-operator route-disable <provider_slug> <effective_to_iso> "
            "[--task-type <task_type>] [--model <model_slug>] [--reason <reason_code>] "
            "[--rationale <text>] [--decision-ref <ref>]"
        )
    provider_slug = args[1]
    effective_to = _parse_datetime(args[2], field_name="effective_to")
    task_type: str | None = None
    model_slug: str | None = None
    reason_code = "provider_disabled"
    rationale: str | None = None
    decision_ref: str | None = None
    index = 3
    while index < len(args):
        flag = args[index]
        if index + 1 >= len(args):
            raise ValueError(f"missing value for {flag}")
        value = args[index + 1]
        if flag == "--task-type":
            task_type = value
        elif flag == "--model":
            model_slug = value
        elif flag == "--reason":
            reason_code = value
        elif flag == "--rationale":
            rationale = value
        elif flag == "--decision-ref":
            decision_ref = value
        else:
            raise ValueError(_usage())
        index += 2
    return RouteDisableCommand(
        provider_slug=provider_slug,
        effective_to=effective_to,
        task_type=task_type,
        model_slug=model_slug,
        reason_code=reason_code,
        rationale=rationale,
        decision_ref=decision_ref,
    )


def _parse_roadmap_write(args: list[str]) -> RoadmapWriteCommand:
    title: str | None = None
    intent_brief: str | None = None
    template = "single_capability"
    priority = "p2"
    parent_roadmap_item_id: str | None = None
    slug: str | None = None
    depends_on: list[str] = []
    source_bug_id: str | None = None
    registry_paths: list[str] = []
    decision_ref: str | None = None
    item_kind: str | None = None
    tier: str | None = None
    phase_ready: bool | None = None
    approval_tag: str | None = None
    reference_doc: str | None = None
    outcome_gate: str | None = None
    action = "preview"
    index = 1
    while index < len(args):
        flag = args[index]
        if flag == "--commit":
            action = "commit"
            index += 1
            continue
        if flag == "--validate":
            action = "validate"
            index += 1
            continue
        if flag == "--phase-ready":
            phase_ready = True
            index += 1
            continue
        if flag == "--not-phase-ready":
            phase_ready = False
            index += 1
            continue
        if index + 1 >= len(args):
            raise ValueError(f"missing value for {flag}")
        value = args[index + 1]
        if flag == "--title":
            title = value
        elif flag == "--brief":
            intent_brief = value
        elif flag == "--template":
            template = value
        elif flag == "--priority":
            priority = value
        elif flag == "--parent":
            parent_roadmap_item_id = value
        elif flag == "--slug":
            slug = value
        elif flag == "--depends-on":
            depends_on.append(value)
        elif flag == "--source-bug":
            source_bug_id = value
        elif flag == "--registry-path":
            registry_paths.append(value)
        elif flag == "--decision-ref":
            decision_ref = value
        elif flag == "--item-kind":
            item_kind = value
        elif flag == "--tier":
            tier = value
        elif flag == "--approval-tag":
            approval_tag = value
        elif flag == "--reference-doc":
            reference_doc = value
        elif flag == "--outcome-gate":
            outcome_gate = value
        else:
            raise ValueError(_usage())
        index += 2
    if title is None or intent_brief is None:
        raise ValueError(
            "usage: workflow native-operator roadmap-write "
            "--title <title> --brief <intent_brief> [--template <template>] "
            "[--priority <p1|p2>] [--parent <roadmap_item_id>] [--slug <slug>] "
            "[--depends-on <roadmap_item_id>]... [--source-bug <bug_id>] "
            "[--registry-path <repo_relative_path>]... "
            "[--decision-ref <ref>] [--item-kind <capability|initiative>] "
            "[--tier <tier>] [--phase-ready|--not-phase-ready] "
            "[--approval-tag <tag>] [--reference-doc <path>] "
            "[--outcome-gate <text>] [--validate|--commit]"
        )
    return RoadmapWriteCommand(
        action=action,
        title=title,
        intent_brief=intent_brief,
        template=template,
        priority=priority,
        parent_roadmap_item_id=parent_roadmap_item_id,
        slug=slug,
        depends_on=tuple(depends_on),
        source_bug_id=source_bug_id,
        registry_paths=tuple(registry_paths),
        decision_ref=decision_ref,
        item_kind=item_kind,
        tier=tier,
        phase_ready=phase_ready,
        approval_tag=approval_tag,
        reference_doc=reference_doc,
        outcome_gate=outcome_gate,
    )


def _parse_roadmap_view(args: list[str]) -> RoadmapViewCommand:
    if len(args) < 2:
        raise ValueError(
            "usage: workflow native-operator roadmap-view <root_roadmap_item_id> [--json]"
        )
    root_roadmap_item_id = args[1]
    as_json = False
    index = 2
    while index < len(args):
        flag = args[index]
        if flag == "--json":
            as_json = True
            index += 1
            continue
        raise ValueError(_usage())
    return RoadmapViewCommand(
        root_roadmap_item_id=root_roadmap_item_id,
        as_json=as_json,
    )


def _parse_provider_onboard(args: list[str]) -> ProviderOnboardingCommand:
    if len(args) < 2:
        raise ValueError(
            "usage: workflow native-operator provider-onboard <spec.json> [--dry-run]"
        )
    spec_path = args[1]
    dry_run = False
    index = 2
    while index < len(args):
        flag = args[index]
        if flag == "--dry-run":
            dry_run = True
            index += 1
            continue
        raise ValueError(_usage())
    return ProviderOnboardingCommand(spec_path=spec_path, dry_run=dry_run)


def _parse_mapping(value: str, *, field_name: str) -> Mapping[str, object]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return parsed


def _parse_native_primary_cutover_gate(args: list[str]) -> NativePrimaryCutoverGateCommand:
    if len(args) < 4:
        raise ValueError(
            "usage: workflow native-operator native-primary-cutover-gate "
            "--decided-by <name> --decision-source <source> --rationale <text> "
            "(--roadmap-item-id <id> | --workflow-class-id <id> | --schedule-definition-id <id>) "
            "[--title <text>] [--gate-name <name>] [--gate-policy <json>] [--required-evidence <json>] "
            "[--decided-at <iso>] [--opened-at <iso>] [--created-at <iso>] [--updated-at <iso>]"
        )

    decided_by: str | None = None
    decision_source: str | None = None
    rationale: str | None = None
    roadmap_item_id: str | None = None
    workflow_class_id: str | None = None
    schedule_definition_id: str | None = None
    title: str | None = None
    gate_name: str | None = None
    gate_policy: Mapping[str, object] | None = None
    required_evidence: Mapping[str, object] | None = None
    decided_at: datetime | None = None
    opened_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    index = 1
    while index < len(args):
        flag = args[index]
        if index + 1 >= len(args):
            raise ValueError(f"missing value for {flag}")
        value = args[index + 1]
        if flag == "--decided-by":
            decided_by = value
        elif flag == "--decision-source":
            decision_source = value
        elif flag == "--rationale":
            rationale = value
        elif flag == "--roadmap-item-id":
            roadmap_item_id = value
        elif flag == "--workflow-class-id":
            workflow_class_id = value
        elif flag == "--schedule-definition-id":
            schedule_definition_id = value
        elif flag == "--title":
            title = value
        elif flag == "--gate-name":
            gate_name = value
        elif flag == "--gate-policy":
            gate_policy = _parse_mapping(value, field_name="--gate-policy")
        elif flag == "--required-evidence":
            required_evidence = _parse_mapping(value, field_name="--required-evidence")
        elif flag == "--decided-at":
            decided_at = _parse_datetime(value, field_name="decided_at")
        elif flag == "--opened-at":
            opened_at = _parse_datetime(value, field_name="opened_at")
        elif flag == "--created-at":
            created_at = _parse_datetime(value, field_name="created_at")
        elif flag == "--updated-at":
            updated_at = _parse_datetime(value, field_name="updated_at")
        else:
            raise ValueError(_usage())
        index += 2

    if decided_by is None or not decided_by.strip():
        raise ValueError("missing required field --decided-by")
    if decision_source is None or not decision_source.strip():
        raise ValueError("missing required field --decision-source")
    if rationale is None or not rationale.strip():
        raise ValueError("missing required field --rationale")

    targets = (roadmap_item_id, workflow_class_id, schedule_definition_id)
    if sum(1 for value in targets if value is not None) != 1:
        raise ValueError(
            "usage: workflow native-operator native-primary-cutover-gate "
            "requires exactly one target: --roadmap-item-id, --workflow-class-id, or --schedule-definition-id"
        )

    return NativePrimaryCutoverGateCommand(
        decided_by=decided_by.strip(),
        decision_source=decision_source.strip(),
        rationale=rationale.strip(),
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        title=title,
        gate_name=gate_name,
        gate_policy=gate_policy,
        required_evidence=required_evidence,
        decided_at=decided_at,
        opened_at=opened_at,
        created_at=created_at,
        updated_at=updated_at,
    )


def _parse_work_item_closeout(args: list[str]) -> WorkItemCloseoutCommand:
    action = "preview"
    bug_ids: list[str] = []
    roadmap_item_ids: list[str] = []
    index = 1
    while index < len(args):
        flag = args[index]
        if flag == "--commit":
            action = "commit"
            index += 1
            continue
        if index + 1 >= len(args):
            raise ValueError(f"missing value for {flag}")
        value = args[index + 1]
        if flag == "--bug-id":
            bug_ids.append(value)
        elif flag == "--roadmap-item-id":
            roadmap_item_ids.append(value)
        else:
            raise ValueError(_usage())
        index += 2
    return WorkItemCloseoutCommand(
        action=action,
        bug_ids=tuple(bug_ids),
        roadmap_item_ids=tuple(roadmap_item_ids),
    )


def _parse(argv: Sequence[str]) -> (
    InstanceCommand
    | DbHealthCommand
    | DbBootstrapCommand
    | SmokeCommand
    | InspectCommand
    | FrontdoorStatusCommand
    | GraphTopologyCommand
    | GraphLineageCommand
    | CockpitCommand
    | RouteDisableCommand
    | RoadmapWriteCommand
    | WorkItemCloseoutCommand
    | RoadmapViewCommand
    | ProviderOnboardingCommand
    | NativePrimaryCutoverGateCommand
):
    args = list(argv)
    if not args:
        raise ValueError(_usage())

    command_name = args[0]
    if command_name == "instance" and len(args) == 1:
        return InstanceCommand()
    if command_name == "start":
        raise ValueError(_unsupported_start_message())
    if command_name in {"health", "db-health"} and len(args) == 1:
        return DbHealthCommand()
    if command_name in {"bootstrap", "db-bootstrap"} and len(args) == 1:
        return DbBootstrapCommand()
    if command_name == "smoke" and len(args) == 1:
        return SmokeCommand()
    if command_name == "inspect" and len(args) == 2:
        return InspectCommand(run_id=args[1])
    if command_name == "status" and len(args) == 2:
        return FrontdoorStatusCommand(run_id=args[1])
    if command_name == "graph-topology" and len(args) == 2:
        return GraphTopologyCommand(run_id=args[1])
    if command_name == "graph-lineage" and len(args) == 2:
        return GraphLineageCommand(run_id=args[1])
    if command_name == "cockpit" and len(args) == 2:
        return CockpitCommand(run_id=args[1])
    if command_name == "route-disable":
        return _parse_route_disable(args)
    if command_name == "roadmap-write":
        return _parse_roadmap_write(args)
    if command_name == "work-item-closeout":
        return _parse_work_item_closeout(args)
    if command_name == "roadmap-view":
        return _parse_roadmap_view(args)
    if command_name == "provider-onboard":
        return _parse_provider_onboard(args)
    if command_name == "native-primary-cutover-gate":
        return _parse_native_primary_cutover_gate(args)
    raise ValueError(_usage())


def _emit_json(stdout: TextIO, payload: Mapping[str, object]) -> None:
    stdout.write(json.dumps(payload, indent=2, sort_keys=True))
    stdout.write("\n")


def _emit_text(stdout: TextIO, payload: str) -> None:
    stdout.write(f"{payload}\n")


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
) -> int:
    """Parse argv and relay to the repo-local operator surfaces."""

    stdout = sys.stdout if stdout is None else stdout
    source = env if env is not None else os.environ
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        command = _parse(args)
    except ValueError as exc:
        stdout.write(f"{exc}\n")
        return 2

    if isinstance(command, InstanceCommand):
        native_instance = resolve_native_instance(env=source)
        _emit_json(stdout, native_instance.to_contract())
        return 0
    if isinstance(command, DbHealthCommand):
        _emit_json(stdout, frontdoor.health(env=source))
        return 0
    if isinstance(command, DbBootstrapCommand):
        _emit_json(stdout, frontdoor.health(env=source, bootstrap=True))
        return 0
    if isinstance(command, SmokeCommand):
        _emit_json(stdout, run_native_self_hosted_smoke())
        return 0
    if isinstance(command, RouteDisableCommand):
        _emit_json(
            stdout,
            operator_write.set_task_route_eligibility_window(
                provider_slug=command.provider_slug,
                eligibility_status="rejected",
                effective_to=command.effective_to,
                task_type=command.task_type,
                model_slug=command.model_slug,
                reason_code=command.reason_code,
                rationale=command.rationale,
                decision_ref=command.decision_ref,
                env=source,
            ),
        )
        return 0
    if isinstance(command, RoadmapWriteCommand):
        _emit_json(
            stdout,
            operator_write.roadmap_write(
                action=command.action,
                title=command.title,
                intent_brief=command.intent_brief,
                template=command.template,
                priority=command.priority,
                parent_roadmap_item_id=command.parent_roadmap_item_id,
                slug=command.slug,
                depends_on=command.depends_on,
                source_bug_id=command.source_bug_id,
                registry_paths=command.registry_paths,
                decision_ref=command.decision_ref,
                item_kind=command.item_kind,
                tier=command.tier,
                phase_ready=command.phase_ready,
                approval_tag=command.approval_tag,
                reference_doc=command.reference_doc,
                outcome_gate=command.outcome_gate,
                env=source,
            ),
        )
        return 0
    if isinstance(command, WorkItemCloseoutCommand):
        _emit_json(
            stdout,
            operator_write.reconcile_work_item_closeout(
                action=command.action,
                bug_ids=command.bug_ids,
                roadmap_item_ids=command.roadmap_item_ids,
                env=source,
            ),
        )
        return 0
    if isinstance(command, RoadmapViewCommand):
        payload = operator_read.query_roadmap_tree(
            root_roadmap_item_id=command.root_roadmap_item_id,
            env=source,
        )
        if command.as_json:
            _emit_json(stdout, payload)
        else:
            _emit_text(stdout, str(payload["rendered_markdown"]))
        return 0
    if isinstance(command, ProviderOnboardingCommand):
        try:
            spec = load_provider_onboarding_spec_from_file(command.spec_path)
            database_url = str(source.get("WORKFLOW_DATABASE_URL") or "").strip()
            if not database_url:
                raise RuntimeError("WORKFLOW_DATABASE_URL is required for provider onboarding")
            _emit_json(
                stdout,
                asdict(
                    run_provider_onboarding(
                        database_url=database_url,
                        spec=spec,
                        dry_run=command.dry_run,
                    )
                ),
            )
            return 0
        except Exception as exc:
            stdout.write(f"ERROR: {exc}\n")
            return 1
    if isinstance(command, NativePrimaryCutoverGateCommand):
        _emit_json(
            stdout,
            operator_write.admit_native_primary_cutover_gate(
                decided_by=command.decided_by,
                decision_source=command.decision_source,
                rationale=command.rationale,
                roadmap_item_id=command.roadmap_item_id,
                workflow_class_id=command.workflow_class_id,
                schedule_definition_id=command.schedule_definition_id,
                title=command.title,
                gate_name=command.gate_name,
                gate_policy=command.gate_policy,
                required_evidence=command.required_evidence,
                decided_at=command.decided_at,
                opened_at=command.opened_at,
                created_at=command.created_at,
                updated_at=command.updated_at,
                env=source,
            ),
        )
        return 0
    if isinstance(command, InspectCommand):
        _emit_text(
            stdout,
            render_inspection(
                RuntimeOrchestrator(
                    evidence_reader=PostgresEvidenceReader(env=source),
                ).inspect_run(run_id=command.run_id)
            ),
        )
        return 0
    if isinstance(command, FrontdoorStatusCommand):
        _emit_json(stdout, frontdoor.status(run_id=command.run_id, env=source))
        return 0
    if isinstance(command, CockpitCommand):
        _emit_json(
            stdout,
            native_operator_surface.query_native_operator_surface(
                run_id=command.run_id,
                env=source,
            ),
        )
        return 0

    frontdoor.status(run_id=command.run_id, env=source)
    evidence_reader = PostgresEvidenceReader(env=source)
    canonical_evidence = evidence_reader.evidence_timeline(command.run_id)
    if isinstance(command, GraphTopologyCommand):
        view = graph_topology_run(
            run_id=command.run_id,
            canonical_evidence=canonical_evidence,
        )
        _emit_text(stdout, render_graph_topology(view))
        return 0

    inspection = RuntimeOrchestrator(evidence_reader=evidence_reader).inspect_run(
        run_id=command.run_id,
    )
    view = graph_lineage_run(
        run_id=command.run_id,
        canonical_evidence=canonical_evidence,
        operator_frame_source=inspection.operator_frame_source,
        operator_frames=inspection.operator_frames,
    )
    _emit_text(stdout, render_graph_lineage(view))
    return 0


if __name__ == "__main__":  # pragma: no cover - manual operator entrypoint
    raise SystemExit(main())
