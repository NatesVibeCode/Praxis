"""Shadow execution packet assembly for migrated runtime LLM jobs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore
from runtime.compile_reuse import module_surface_revision, stable_hash
from runtime.execution_packet_authority import (
    build_execution_packet_lineage_payload,
    finalize_execution_packet,
    inspect_execution_packets,
)
from runtime.prompt_renderer import RenderedPrompt, render_prompt_as_messages
from runtime.workflow.execution_bundle import (
    build_execution_bundle,
    inject_execution_bundle_into_messages,
)


class ShadowExecutionPacketError(RuntimeError):
    """Raised when shadow packet authority or persistence is incomplete."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _json_clone(value: object) -> object:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _mapping_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        return []
    values: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            values.append(text)
    return values


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def _json_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return []
        if isinstance(parsed, list):
            return list(parsed)
    return []


def _job_row(
    source: Mapping[str, Any],
    *,
    job_label: str,
) -> dict[str, Any]:
    jobs = source.get("jobs")
    if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes, bytearray)):
        return {}
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        label = str(job.get("label") or "").strip()
        if label and label == job_label:
            return _mapping(_json_clone(job))
    return {}


def _job_reference_slugs(*job_rows: Mapping[str, Any]) -> list[str]:
    slugs: list[str] = []
    for row in job_rows:
        slugs.extend(_string_list(row.get("reference_slugs")))
        for reference in _mapping_list(row.get("references")):
            slug = str(reference.get("slug") or reference.get("raw") or "").strip()
            if slug:
                slugs.append(slug)
    return _dedupe_strings(slugs)


def _job_capabilities(*job_rows: Mapping[str, Any]) -> list[str]:
    caps: list[str] = []
    for row in job_rows:
        caps.extend(_string_list(row.get("capabilities")))
        for capability in _mapping_list(row.get("capabilities")):
            slug = str(capability.get("slug") or "").strip()
            if slug:
                caps.append(slug)
    return _dedupe_strings(caps)


def _job_allowed_tools(*job_rows: Mapping[str, Any]) -> list[str]:
    tools: list[str] = []
    for row in job_rows:
        tools.extend(_string_list(row.get("allowed_tools")))
    return _dedupe_strings(tools)


def _job_verify_refs(*job_rows: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    for row in job_rows:
        refs.extend(_string_list(row.get("verify_refs")))
    return _dedupe_strings(refs)


def _has_reference_authority(
    *,
    provenance: Mapping[str, Any],
    compiled_spec_row: Mapping[str, Any],
    definition_row: Mapping[str, Any],
    compiled_job_row: Mapping[str, Any],
    definition_job_row: Mapping[str, Any],
) -> bool:
    return any(
        (
            "reference_bindings" in provenance,
            "references" in compiled_spec_row,
            "references" in definition_row,
            "reference_slugs" in compiled_job_row,
            "reference_slugs" in definition_job_row,
            "references" in compiled_job_row,
            "references" in definition_job_row,
        )
    )


def _has_capability_authority(
    *,
    config: Mapping[str, Any],
    payload: Mapping[str, Any],
    provenance: Mapping[str, Any],
    compiled_spec_row: Mapping[str, Any],
    definition_row: Mapping[str, Any],
    compiled_job_row: Mapping[str, Any],
    definition_job_row: Mapping[str, Any],
) -> bool:
    return any(
        (
            "capability_bindings" in provenance,
            "capabilities" in config,
            "allowed_tools" in config,
            "capabilities" in payload,
            "allowed_tools" in payload,
            "capabilities" in compiled_spec_row,
            "capabilities" in definition_row,
            "capabilities" in compiled_job_row,
            "capabilities" in definition_job_row,
            "allowed_tools" in compiled_job_row,
            "allowed_tools" in definition_job_row,
        )
    )


def _has_verify_authority(
    *,
    config: Mapping[str, Any],
    payload: Mapping[str, Any],
    provenance: Mapping[str, Any],
    compiled_spec_row: Mapping[str, Any],
    definition_row: Mapping[str, Any],
    compiled_job_row: Mapping[str, Any],
    definition_job_row: Mapping[str, Any],
) -> bool:
    return any(
        (
            "verify_refs" in config,
            "verify_refs" in payload,
            "verify_refs" in provenance,
            "verify_refs" in compiled_spec_row,
            "verify_refs" in definition_row,
            "verify_refs" in compiled_job_row,
            "verify_refs" in definition_job_row,
        )
    )


def _require_text(
    value: object,
    *,
    field_name: str,
    reason_code: str,
) -> str:
    text = str(value or "").strip()
    if text:
        return text
    raise ShadowExecutionPacketError(
        reason_code,
        f"{field_name} is required for shadow execution packets",
        details={"field_name": field_name},
    )


def _resolve_revision(
    *,
    config: Mapping[str, Any],
    provenance: Mapping[str, Any],
    field_name: str,
    reason_code: str,
) -> str:
    provenance_compiled_spec = _mapping(provenance.get("compiled_spec_row"))
    provenance_definition = _mapping(provenance.get("definition_row"))
    for candidate in (
        config.get(field_name),
        provenance_compiled_spec.get(field_name),
        provenance_definition.get(field_name),
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    raise ShadowExecutionPacketError(
        reason_code,
        f"{field_name} is required for shadow execution packets",
        details={"field_name": field_name},
    )


def _reference_bindings(
    *,
    provenance: Mapping[str, Any],
    job_label: str,
    adapter_type: str,
    provider_slug: str,
    model_slug: str,
    task_type: str,
    scope_read: list[str],
    scope_write: list[str],
    workdir: str,
    context_sections: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if "reference_bindings" in provenance:
        provided = _mapping_list(provenance.get("reference_bindings"))
        return [_mapping(_json_clone(item)) for item in provided]

    compiled_spec_row = _mapping(provenance.get("compiled_spec_row"))
    definition_row = _mapping(provenance.get("definition_row"))
    compiled_job_row = _job_row(compiled_spec_row, job_label=job_label)
    definition_job_row = _job_row(definition_row, job_label=job_label)

    if not _has_reference_authority(
        provenance=provenance,
        compiled_spec_row=compiled_spec_row,
        definition_row=definition_row,
        compiled_job_row=compiled_job_row,
        definition_job_row=definition_job_row,
    ):
        raise ShadowExecutionPacketError(
            "shadow_packet.reference_authority_missing",
            "reference binding authority is required for shadow execution packets",
            details={"job_label": job_label},
        )

    reference_rows: list[dict[str, Any]] = []
    for source in (
        compiled_job_row,
        definition_job_row,
        compiled_spec_row,
        definition_row,
    ):
        reference_rows.extend(_mapping_list(source.get("references")))

    reference_slugs = _dedupe_strings(
        _job_reference_slugs(compiled_job_row, definition_job_row)
        + [
            str(row.get("slug") or row.get("raw") or "").strip()
            for row in reference_rows
            if isinstance(row, Mapping)
        ]
    )
    return [
        {
            "job_label": job_label,
            "adapter_type": adapter_type,
            "provider_slug": provider_slug,
            "model_slug": model_slug,
            "task_type": task_type,
            "depends_on": _dedupe_strings(
                _string_list(compiled_job_row.get("depends_on"))
                + _string_list(definition_job_row.get("depends_on"))
            ),
            "scope_read": list(scope_read),
            "scope_write": list(scope_write),
            "workdir": workdir,
            "reference_slugs": reference_slugs,
            "references": [_mapping(_json_clone(row)) for row in reference_rows],
            "context_section_names": [
                str(section.get("name") or "").strip()
                for section in context_sections
                if str(section.get("name") or "").strip()
            ],
        }
    ]


def _capability_bindings(
    *,
    provenance: Mapping[str, Any],
    config: Mapping[str, Any],
    payload: Mapping[str, Any],
    job_label: str,
    adapter_type: str,
    provider_slug: str,
    model_slug: str,
    task_type: str,
) -> list[dict[str, Any]]:
    if "capability_bindings" in provenance:
        provided = _mapping_list(provenance.get("capability_bindings"))
        return [_mapping(_json_clone(item)) for item in provided]

    compiled_spec_row = _mapping(provenance.get("compiled_spec_row"))
    definition_row = _mapping(provenance.get("definition_row"))
    compiled_job_row = _job_row(compiled_spec_row, job_label=job_label)
    definition_job_row = _job_row(definition_row, job_label=job_label)
    if not _has_capability_authority(
        config=config,
        payload=payload,
        provenance=provenance,
        compiled_spec_row=compiled_spec_row,
        definition_row=definition_row,
        compiled_job_row=compiled_job_row,
        definition_job_row=definition_job_row,
    ):
        raise ShadowExecutionPacketError(
            "shadow_packet.capability_authority_missing",
            "capability binding authority is required for shadow execution packets",
            details={"job_label": job_label},
        )
    capabilities = _dedupe_strings(
        _string_list(config.get("capabilities"))
        + _string_list(payload.get("capabilities"))
        + _job_capabilities(compiled_job_row, definition_job_row)
        + _string_list(compiled_spec_row.get("capabilities"))
        + [
            str(cap.get("slug") or "").strip()
            for cap in _mapping_list(definition_row.get("capabilities"))
            if str(cap.get("slug") or "").strip()
        ]
    )
    allowed_tools = _dedupe_strings(
        _string_list(config.get("allowed_tools"))
        + _string_list(payload.get("allowed_tools"))
        + _job_allowed_tools(compiled_job_row, definition_job_row)
    )
    return [
        {
            "job_label": job_label,
            "adapter_type": adapter_type,
            "provider_slug": provider_slug,
            "model_slug": model_slug,
            "task_type": task_type,
            "capabilities": capabilities,
            "allowed_tools": allowed_tools,
        }
    ]


def build_shadow_execution_packet(
    *,
    rendered: RenderedPrompt,
    payload: Mapping[str, Any],
    shadow_packet_config: Mapping[str, Any],
    scope_read: Sequence[str],
    scope_write: Sequence[str],
    test_scope: Sequence[str] = (),
    blast_radius: Sequence[str] = (),
) -> dict[str, Any]:
    config = dict(shadow_packet_config)
    provenance = _mapping(config.get("packet_provenance"))
    runtime = _mapping(payload.get("shadow_packet_runtime"))
    if not runtime:
        raise ShadowExecutionPacketError(
            "shadow_packet.runtime_missing",
            "shadow packet runtime authority is missing",
        )

    workflow_id = _require_text(
        runtime.get("workflow_id"),
        field_name="workflow_id",
        reason_code="shadow_packet.workflow_id_missing",
    )
    run_id = _require_text(
        runtime.get("run_id"),
        field_name="run_id",
        reason_code="shadow_packet.run_id_missing",
    )
    definition_revision = _resolve_revision(
        config=config,
        provenance=provenance,
        field_name="definition_revision",
        reason_code="shadow_packet.definition_revision_missing",
    )
    plan_revision = _resolve_revision(
        config=config,
        provenance=provenance,
        field_name="plan_revision",
        reason_code="shadow_packet.plan_revision_missing",
    )

    adapter_type = str(config.get("adapter_type") or payload.get("adapter_type") or "cli_llm").strip()
    job_label = str(config.get("job_label") or payload.get("label") or "workflow").strip() or "workflow"
    provider_slug = str(payload.get("provider_slug") or "").strip()
    model_slug = str(payload.get("model_slug") or "").strip()
    task_type = str(config.get("task_type") or payload.get("task_type") or "").strip()
    workdir = str(payload.get("workdir") or "").strip()
    compiled_spec_row = _mapping(provenance.get("compiled_spec_row"))
    definition_row = _mapping(provenance.get("definition_row"))
    compiled_job_row = _job_row(compiled_spec_row, job_label=job_label)
    definition_job_row = _job_row(definition_row, job_label=job_label)
    if not str(provenance.get("source_kind") or "").strip():
        raise ShadowExecutionPacketError(
            "shadow_packet.source_kind_missing",
            "packet provenance must include source_kind",
            details={"job_label": job_label},
        )
    if not _has_verify_authority(
        config=config,
        payload=payload,
        provenance=provenance,
        compiled_spec_row=compiled_spec_row,
        definition_row=definition_row,
        compiled_job_row=compiled_job_row,
        definition_job_row=definition_job_row,
    ):
        raise ShadowExecutionPacketError(
            "shadow_packet.verify_authority_missing",
            "verification authority is required for shadow execution packets",
            details={"job_label": job_label},
    )
    verify_refs = _dedupe_strings(
        _string_list(config.get("verify_refs"))
        + _string_list(payload.get("verify_refs"))
        + _string_list(provenance.get("verify_refs"))
        + _string_list(compiled_spec_row.get("verify_refs"))
        + _string_list(definition_row.get("verify_refs"))
        + _job_verify_refs(compiled_job_row, definition_job_row)
    )

    normalized_scope_read = _dedupe_strings(list(scope_read))
    normalized_scope_write = _dedupe_strings(list(scope_write))
    rendered_context_sections = [
        _mapping(_json_clone(section))
        for section in rendered.context_sections
        if isinstance(section, Mapping)
    ]
    reference_bindings = _reference_bindings(
        provenance=provenance,
        job_label=job_label,
        adapter_type=adapter_type,
        provider_slug=provider_slug,
        model_slug=model_slug,
        task_type=task_type,
        scope_read=normalized_scope_read,
        scope_write=normalized_scope_write,
        workdir=workdir,
        context_sections=rendered_context_sections,
    )
    capability_bindings = _capability_bindings(
        provenance=provenance,
        config=config,
        payload=payload,
        job_label=job_label,
        adapter_type=adapter_type,
        provider_slug=provider_slug,
        model_slug=model_slug,
        task_type=task_type,
    )
    capability_binding = capability_bindings[0] if capability_bindings else {}
    execution_bundle = build_execution_bundle(
        job_label=job_label,
        prompt=str(rendered.user_message or ""),
        task_type=task_type or None,
        capabilities=_string_list(capability_binding.get("capabilities")),
        allowed_tools=_string_list(capability_binding.get("allowed_tools")),
        explicit_mcp_tools=_string_list(config.get("mcp_tools")) + _string_list(payload.get("mcp_tools")),
        explicit_skill_refs=_string_list(config.get("skill_refs")) + _string_list(payload.get("skill_refs")),
        write_scope=normalized_scope_write,
        declared_read_scope=normalized_scope_read,
        resolved_read_scope=normalized_scope_read,
        blast_radius=_dedupe_strings(list(blast_radius)),
        test_scope=_dedupe_strings(list(test_scope)),
        verify_refs=verify_refs,
        context_sections=rendered_context_sections,
    )
    if capability_binding:
        capability_binding["allowed_tools"] = _string_list(execution_bundle.get("allowed_tools"))
        capability_binding["tool_bucket"] = str(execution_bundle.get("tool_bucket") or "").strip()
        capability_binding["mcp_tools"] = _string_list(execution_bundle.get("mcp_tool_names"))
        capability_binding["skill_refs"] = _string_list(execution_bundle.get("skill_refs"))
    model_messages = [
        {
            "job_label": job_label,
            "adapter_type": adapter_type,
            "provider_slug": provider_slug,
            "model_slug": model_slug,
            "messages": inject_execution_bundle_into_messages(
                list(render_prompt_as_messages(rendered)),
                execution_bundle=execution_bundle,
            ),
        }
    ]
    authority_inputs = {
        "shadow_runtime": _mapping(_json_clone(runtime)),
        "packet_provenance": _mapping(_json_clone(provenance)),
        "source_authority": _mapping(_json_clone(provenance.get("authority_inputs"))),
        "workflow_row": _mapping(_json_clone(provenance.get("workflow_row"))),
        "definition_row": _mapping(_json_clone(definition_row)),
        "compiled_spec_row": _mapping(_json_clone(compiled_spec_row)),
        "definition_job_row": _mapping(_json_clone(definition_job_row)),
        "compiled_job_row": _mapping(_json_clone(compiled_job_row)),
    }
    file_inputs = {
        "workdir": workdir,
        "scope_read": normalized_scope_read,
        "scope_write": normalized_scope_write,
        "test_scope": _dedupe_strings(list(test_scope)),
        "blast_radius": _dedupe_strings(list(blast_radius)),
        "context_sections": rendered_context_sections,
        "execution_bundle": _mapping(_json_clone(execution_bundle)),
        "packet_file_inputs": _mapping(_json_clone(provenance.get("file_inputs"))),
    }
    packet_payload: dict[str, Any] = {
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "packet_version": 1,
        "workflow_id": workflow_id,
        "run_id": run_id,
        "spec_name": job_label,
        "source_kind": str(provenance.get("source_kind") or "").strip(),
        "authority_refs": [definition_revision, plan_revision],
        "model_messages": model_messages,
        "scope_reads": normalized_scope_read,
        "scope_writes": normalized_scope_write,
        "reference_bindings": reference_bindings,
        "capability_bindings": capability_bindings,
        "verify_refs": verify_refs,
        "authority_inputs": authority_inputs,
        "file_inputs": file_inputs,
        "compile_provenance": _shadow_packet_compile_provenance(
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            plan_revision=plan_revision,
            spec_name=job_label,
            source_kind=str(provenance.get("source_kind") or "").strip(),
            model_messages=model_messages,
            reference_bindings=reference_bindings,
            capability_bindings=capability_bindings,
            verify_refs=verify_refs,
            file_inputs=file_inputs,
            authority_inputs=authority_inputs,
        ),
    }
    return packet_payload


def persist_shadow_execution_packet(
    packet: Mapping[str, Any],
    *,
    conn_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    try:
        conn = conn_factory() if conn_factory is not None else _default_connection()
    except Exception as exc:
        raise ShadowExecutionPacketError(
            "shadow_packet.persistence_unavailable",
            f"failed to resolve packet persistence authority: {exc}",
        ) from exc
    if conn is None:
        raise ShadowExecutionPacketError(
            "shadow_packet.persistence_unavailable",
            "packet persistence authority returned no connection",
        )
    try:
        packet_dict = dict(packet)
        artifact_store = CompileArtifactStore(conn)
        lineage_payload = build_execution_packet_lineage_payload(
            packet_dict,
            parent_artifact_ref=packet_dict["plan_revision"],
        )
        compile_provenance = packet_dict.get("compile_provenance")
        input_fingerprint = (
            str(compile_provenance.get("input_fingerprint"))
            if isinstance(compile_provenance, Mapping)
            else ""
        ).strip()
        try:
            reusable_lineage = artifact_store.load_reusable_artifact(
                artifact_kind="packet_lineage",
                input_fingerprint=input_fingerprint,
            )
        except CompileArtifactError as exc:
            raise ShadowExecutionPacketError(
                "shadow_packet.reuse_failed_closed",
                f"shadow packet lineage reuse failed closed: {exc}",
                details={"input_fingerprint": input_fingerprint},
            ) from exc

        if reusable_lineage is not None:
            lineage_payload = dict(_json_clone(reusable_lineage.payload))
            reuse_metadata = {
                "decision": "reused",
                "reason_code": "packet.compile.exact_input_match",
                "artifact_ref": reusable_lineage.artifact_ref,
                "revision_ref": reusable_lineage.revision_ref,
                "content_hash": reusable_lineage.content_hash,
                "decision_ref": reusable_lineage.decision_ref,
            }
        else:
            artifact_store.record_packet_lineage(
                packet=lineage_payload,
                authority_refs=[packet_dict["definition_revision"], packet_dict["plan_revision"]],
                decision_ref=str(lineage_payload["decision_ref"]),
                parent_artifact_ref=str(lineage_payload["parent_artifact_ref"]),
                input_fingerprint=input_fingerprint,
            )
            reuse_metadata = {
                "decision": "compiled",
                "reason_code": "packet.compile.miss",
                "artifact_ref": str(lineage_payload["packet_revision"]),
                "revision_ref": str(lineage_payload["packet_revision"]),
                "content_hash": str(lineage_payload["packet_hash"]),
                "decision_ref": str(lineage_payload["decision_ref"]),
            }

        finalized_packet = finalize_execution_packet(
            packet_dict,
            lineage_payload=lineage_payload,
            reuse_metadata=reuse_metadata,
        )
        artifact_store.record_execution_packet(
            packet=finalized_packet,
            authority_refs=[finalized_packet["definition_revision"], finalized_packet["plan_revision"]],
            decision_ref=str(finalized_packet["decision_ref"]),
            parent_artifact_ref=str(finalized_packet["parent_artifact_ref"]),
        )
    except ShadowExecutionPacketError:
        raise
    except Exception as exc:
        raise ShadowExecutionPacketError(
            "shadow_packet.persistence_failed",
            f"failed to persist shadow execution packet: {exc}",
        ) from exc
    return finalized_packet


def _shadow_packet_surface_revision() -> str:
    return module_surface_revision(__file__)


def _shadow_runtime_reuse_authority(runtime: Mapping[str, Any]) -> dict[str, Any]:
    allowed_fields = (
        "workflow_id",
        "workflow_definition_id",
        "definition_hash",
        "context_bundle_hash",
        "workspace_ref",
        "runtime_profile_ref",
    )
    return {
        field_name: _json_clone(runtime.get(field_name))
        for field_name in allowed_fields
        if field_name in runtime
    }


def _shadow_packet_compile_provenance(
    *,
    workflow_id: str,
    definition_revision: str,
    plan_revision: str,
    spec_name: str,
    source_kind: str,
    model_messages: list[dict[str, Any]],
    reference_bindings: list[dict[str, Any]],
    capability_bindings: list[dict[str, Any]],
    verify_refs: list[str],
    file_inputs: Mapping[str, Any],
    authority_inputs: Mapping[str, Any],
) -> dict[str, Any]:
    shadow_runtime = _mapping(authority_inputs.get("shadow_runtime"))
    reuse_authority = {
        "shadow_runtime": _shadow_runtime_reuse_authority(shadow_runtime),
        "packet_provenance": _mapping(_json_clone(authority_inputs.get("packet_provenance"))),
        "source_authority": _mapping(_json_clone(authority_inputs.get("source_authority"))),
        "definition_row": _mapping(_json_clone(authority_inputs.get("definition_row"))),
        "compiled_spec_row": _mapping(_json_clone(authority_inputs.get("compiled_spec_row"))),
        "definition_job_row": _mapping(_json_clone(authority_inputs.get("definition_job_row"))),
        "compiled_job_row": _mapping(_json_clone(authority_inputs.get("compiled_job_row"))),
    }
    input_payload = {
        "artifact_kind": "packet_lineage",
        "surface_revision": _shadow_packet_surface_revision(),
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "workflow_id": workflow_id,
        "spec_name": spec_name,
        "source_kind": source_kind,
        "model_messages": model_messages,
        "reference_bindings": reference_bindings,
        "capability_bindings": capability_bindings,
        "verify_refs": verify_refs,
        "file_inputs": _mapping(_json_clone(file_inputs)),
        "authority_inputs": reuse_authority,
    }
    return {
        "artifact_kind": "packet_lineage",
        "input_fingerprint": stable_hash(input_payload),
        "surface_revision": input_payload["surface_revision"],
        "definition_revision": definition_revision,
        "plan_revision": plan_revision,
        "workflow_id": workflow_id,
        "spec_name": spec_name,
        "source_kind": source_kind,
        "file_inputs": _mapping(_json_clone(file_inputs)),
        "authority_inputs": reuse_authority,
    }


def _packet_payload(packet: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_mapping(packet.get("payload"))
    if payload:
        merged = dict(payload)
        merged.update(dict(packet))
        return merged
    return dict(packet)


def _packet_provenance(packet: Mapping[str, Any]) -> dict[str, Any]:
    packet_provenance = _json_mapping(packet.get("packet_provenance"))
    if packet_provenance:
        return packet_provenance
    authority_inputs = _json_mapping(packet.get("authority_inputs"))
    return _json_mapping(authority_inputs.get("packet_provenance"))


def _execution_snapshot(run_row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if run_row is None:
        return None
    row = _json_mapping(run_row)
    request_envelope = _json_mapping(row.get("request_envelope"))
    spec_snapshot = _json_mapping(request_envelope.get("spec_snapshot"))
    return {
        "run_id": str(row.get("run_id") or "").strip(),
        "workflow_id": str(row.get("workflow_id") or "").strip(),
        "request_id": str(row.get("request_id") or "").strip(),
        "workflow_definition_id": str(row.get("workflow_definition_id") or "").strip(),
        "current_state": str(row.get("current_state") or row.get("status") or "").strip(),
        "requested_at": _json_clone(row.get("requested_at")) if row.get("requested_at") is not None else None,
        "admitted_at": _json_clone(row.get("admitted_at")) if row.get("admitted_at") is not None else None,
        "started_at": _json_clone(row.get("started_at")) if row.get("started_at") is not None else None,
        "finished_at": _json_clone(row.get("finished_at")) if row.get("finished_at") is not None else None,
        "spec_name": str(
            request_envelope.get("name")
            or request_envelope.get("spec_name")
            or row.get("spec_name")
            or row.get("workflow_id")
            or ""
        ).strip(),
        "request_envelope": request_envelope,
        "spec_snapshot": spec_snapshot,
    }


def _packet_differences(
    *,
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []
    for field_name in (
        "run_id",
        "workflow_id",
        "spec_name",
        "source_kind",
        "definition_revision",
        "plan_revision",
        "authority_refs",
        "verify_refs",
        "packet_provenance",
    ):
        expected_value = _json_clone(expected.get(field_name))
        actual_value = _json_clone(actual.get(field_name))
        if expected_value != actual_value:
            differences.append(
                {
                    "field": field_name,
                    "expected": expected_value,
                    "actual": actual_value,
                }
            )
    return differences


def inspect_shadow_execution_packets(
    packets: Sequence[Mapping[str, Any]] | None,
    *,
    run_row: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return inspect_execution_packets(
        [
            _packet_payload(packet)
            for packet in (packets or ())
            if isinstance(packet, Mapping)
        ],
        run_row=run_row,
    )


def _default_connection():
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    return SyncPostgresConnection(get_workflow_pool())


__all__ = [
    "ShadowExecutionPacketError",
    "build_shadow_execution_packet",
    "inspect_shadow_execution_packets",
    "persist_shadow_execution_packet",
]
