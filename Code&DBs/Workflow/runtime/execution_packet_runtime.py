"""Execution-packet authority helpers for packet-only migrated runtime paths."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore


class ExecutionPacketRuntimeError(RuntimeError):
    """Raised when packet-only runtime execution cannot prove packet authority."""

    def __init__(
        self,
        reason_code: str,
        message: str,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class ExecutionPacketBinding:
    packet_revision: str
    packet_hash: str
    definition_revision: str
    plan_revision: str
    workflow_id: str
    run_id: str
    spec_name: str
    source_kind: str
    messages: tuple[dict[str, str], ...]
    execution_bundle: dict[str, Any] | None = None
    execution_context_shard: dict[str, Any] | None = None


def packet_required(input_payload: Mapping[str, Any]) -> bool:
    return bool(input_payload.get("packet_required"))


def load_execution_packet_binding(
    input_payload: Mapping[str, Any],
    *,
    conn: Any | None = None,
    conn_factory: Callable[[], Any] | None = None,
) -> ExecutionPacketBinding | None:
    required = packet_required(input_payload)
    packet_revision = str(input_payload.get("execution_packet_ref") or "").strip()
    if not packet_revision:
        if required:
            raise ExecutionPacketRuntimeError(
                "execution_packet.ref_missing",
                "execution packet ref is required for packet-only runtime execution",
            )
        return None

    expected_hash = str(input_payload.get("execution_packet_hash") or "").strip()
    expected_definition_revision = str(input_payload.get("definition_revision") or "").strip()
    expected_plan_revision = str(input_payload.get("plan_revision") or "").strip()

    conn = _resolve_connection(conn=conn, conn_factory=conn_factory)

    store = CompileArtifactStore(conn)
    try:
        record = store.load_execution_packet(packet_revision=packet_revision)
    except CompileArtifactError as exc:
        raise ExecutionPacketRuntimeError(
            "execution_packet.authority_invalid",
            f"execution packet authority failed closed: {exc}",
        ) from exc
    if record is None:
        raise ExecutionPacketRuntimeError(
            "execution_packet.missing",
            "execution packet authority is missing for packet-only runtime execution",
        )

    if expected_hash and record.packet_hash != expected_hash:
        raise ExecutionPacketRuntimeError(
            "execution_packet.hash_mismatch",
            "execution packet hash does not match the compiled packet authority",
        )
    if expected_definition_revision and record.definition_revision != expected_definition_revision:
        raise ExecutionPacketRuntimeError(
            "execution_packet.definition_revision_mismatch",
            "execution packet definition revision does not match runtime authority",
        )
    if expected_plan_revision and record.plan_revision != expected_plan_revision:
        raise ExecutionPacketRuntimeError(
            "execution_packet.plan_revision_mismatch",
            "execution packet plan revision does not match runtime authority",
        )
    return _binding_from_record(record=record, conn=conn)


def load_execution_packet_job_binding(
    *,
    run_id: str,
    job_label: str,
    expected_definition_revision: str | None = None,
    expected_plan_revision: str | None = None,
    conn: Any | None = None,
    conn_factory: Callable[[], Any] | None = None,
) -> ExecutionPacketBinding:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ExecutionPacketRuntimeError(
            "execution_packet.run_id_missing",
            "run id is required for packet-only migrated runtime execution",
        )

    normalized_job_label = str(job_label or "").strip()
    if not normalized_job_label:
        raise ExecutionPacketRuntimeError(
            "execution_packet.job_label_missing",
            "job label is required for packet-only migrated runtime execution",
        )

    expected_definition_revision = str(expected_definition_revision or "").strip()
    expected_plan_revision = str(expected_plan_revision or "").strip()
    conn = _resolve_connection(conn=conn, conn_factory=conn_factory)

    store = CompileArtifactStore(conn)
    try:
        records = store.load_execution_packets(run_id=normalized_run_id)
    except CompileArtifactError as exc:
        raise ExecutionPacketRuntimeError(
            "execution_packet.authority_invalid",
            f"execution packet authority failed closed: {exc}",
        ) from exc
    if not records:
        raise ExecutionPacketRuntimeError(
            "execution_packet.missing",
            "execution packet authority is missing for packet-only migrated runtime execution",
        )

    record = _select_run_execution_packet_record(
        records,
        run_id=normalized_run_id,
        expected_definition_revision=expected_definition_revision,
        expected_plan_revision=expected_plan_revision,
    )
    return _binding_from_record(record=record, conn=conn, job_label=normalized_job_label)


def packet_prompt_fields(binding: ExecutionPacketBinding) -> tuple[str | None, str]:
    system_parts: list[str] = []
    prompt_parts: list[str] = []
    for message in binding.messages:
        role = str(message.get("role") or "user").strip().lower()
        content = str(message.get("content") or "")
        if not content:
            continue
        if role == "system":
            system_parts.append(content)
            continue
        prompt_parts.append(content if role == "user" else f"[{role}]\n{content}")
    prompt = "\n\n".join(part for part in prompt_parts if part)
    if not prompt:
        raise ExecutionPacketRuntimeError(
            "execution_packet.user_prompt_missing",
            "execution packet is missing a runnable user prompt",
        )
    system_prompt = "\n\n".join(part for part in system_parts if part) or None
    return system_prompt, prompt


def _packet_messages(model_messages: Sequence[Mapping[str, Any]]) -> tuple[dict[str, str], ...]:
    return _packet_messages_for_job(model_messages, job_label=None)


def _packet_messages_for_job(
    model_messages: Sequence[Mapping[str, Any]],
    *,
    job_label: str | None,
) -> tuple[dict[str, str], ...]:
    normalized_job_label = str(job_label or "").strip()
    grouped = []
    for item in model_messages:
        if normalized_job_label:
            item_job_label = str(item.get("job_label") or "").strip()
            if item_job_label != normalized_job_label:
                continue
        messages = item.get("messages")
        if isinstance(messages, Sequence) and not isinstance(messages, (str, bytes, bytearray)):
            normalized = tuple(
                {
                    "role": str(message.get("role") or "user"),
                    "content": str(message.get("content") or ""),
                }
                for message in messages
                if isinstance(message, Mapping)
            )
            if normalized:
                grouped.append(normalized)
    if not grouped:
        if normalized_job_label:
            raise ExecutionPacketRuntimeError(
                "execution_packet.messages_missing",
                f"execution packet is missing compiled model messages for job '{normalized_job_label}'",
            )
        return ()
    if len(grouped) != 1:
        raise ExecutionPacketRuntimeError(
            "execution_packet.messages_ambiguous",
            (
                "execution packet contains multiple message groups for the same migrated task"
                if normalized_job_label
                else "execution packet contains multiple message groups and cannot be executed as one migrated task"
            ),
        )
    return grouped[0]


def _binding_from_record(
    *,
    record,
    conn: Any,
    job_label: str | None = None,
) -> ExecutionPacketBinding:
    _validate_compile_index_authority(record=record, conn=conn)

    messages = _packet_messages_for_job(record.model_messages, job_label=job_label)
    if not messages:
        raise ExecutionPacketRuntimeError(
            "execution_packet.messages_missing",
            "execution packet is missing compiled model messages",
        )

    return ExecutionPacketBinding(
        packet_revision=record.packet_revision,
        packet_hash=record.packet_hash,
        definition_revision=record.definition_revision,
        plan_revision=record.plan_revision,
        workflow_id=record.workflow_id,
        run_id=record.run_id,
        spec_name=record.spec_name,
        source_kind=record.source_kind,
        messages=messages,
        execution_bundle=_execution_bundle_for_job(record=record, job_label=job_label),
        execution_context_shard=_execution_context_shard_for_job(record=record, job_label=job_label),
    )


def _execution_bundle_for_job(*, record, job_label: str | None) -> dict[str, Any] | None:
    file_inputs = dict(record.file_inputs or {})
    if job_label:
        bundles = file_inputs.get("execution_bundles")
        if isinstance(bundles, Mapping):
            bundle = bundles.get(job_label)
            if isinstance(bundle, Mapping):
                return dict(bundle)
    bundle = file_inputs.get("execution_bundle")
    if isinstance(bundle, Mapping):
        return dict(bundle)
    return None


def _execution_context_shard_for_job(*, record, job_label: str | None) -> dict[str, Any] | None:
    file_inputs = dict(record.file_inputs or {})
    if job_label:
        shards = file_inputs.get("execution_context_shards")
        if isinstance(shards, Mapping):
            shard = shards.get(job_label)
            if isinstance(shard, Mapping):
                return dict(shard)
    shard = file_inputs.get("execution_context_shard")
    if isinstance(shard, Mapping):
        return dict(shard)
    return None


def _resolve_connection(
    *,
    conn: Any | None,
    conn_factory: Callable[[], Any] | None,
) -> Any:
    if conn is not None:
        return conn
    try:
        resolved = conn_factory() if conn_factory is not None else _default_connection()
    except Exception as exc:
        raise ExecutionPacketRuntimeError(
            "execution_packet.authority_unavailable",
            f"execution packet authority is unavailable: {exc}",
        ) from exc
    if resolved is None:
        raise ExecutionPacketRuntimeError(
            "execution_packet.authority_unavailable",
            "execution packet authority returned no connection",
        )
    return resolved


def _select_run_execution_packet_record(
    records: Sequence[Any],
    *,
    run_id: str,
    expected_definition_revision: str,
    expected_plan_revision: str,
):
    candidates = list(records)
    if expected_definition_revision:
        candidates = [
            record
            for record in candidates
            if record.definition_revision == expected_definition_revision
        ]
    if expected_plan_revision:
        candidates = [
            record
            for record in candidates
            if record.plan_revision == expected_plan_revision
        ]

    if not candidates:
        if expected_plan_revision:
            raise ExecutionPacketRuntimeError(
                "execution_packet.plan_revision_mismatch",
                "execution packet plan revision does not match runtime authority",
            )
        if expected_definition_revision:
            raise ExecutionPacketRuntimeError(
                "execution_packet.definition_revision_mismatch",
                "execution packet definition revision does not match runtime authority",
            )
        raise ExecutionPacketRuntimeError(
            "execution_packet.missing",
            f"execution packet authority is missing for run '{run_id}'",
        )

    packet_revisions = {record.packet_revision for record in candidates}
    if len(packet_revisions) != 1:
        raise ExecutionPacketRuntimeError(
            "execution_packet.ambiguous",
            f"execution packet authority is ambiguous for run '{run_id}'",
        )
    return candidates[0]


def _validate_compile_index_authority(
    *,
    record,
    conn: Any,
) -> None:
    workflow_definition = _workflow_definition_authority(record.authority_inputs)
    if not _definition_requires_compile_index_authority(workflow_definition):
        return

    compile_provenance = workflow_definition.get("compile_provenance")
    if not isinstance(compile_provenance, Mapping):
        raise ExecutionPacketRuntimeError(
            "execution_packet.compile_index_missing",
            "workflow-backed execution packet is missing compile-index authority",
        )

    compile_index_ref = str(compile_provenance.get("compile_index_ref") or "").strip()
    compile_surface_revision = str(
        compile_provenance.get("compile_surface_revision") or ""
    ).strip()
    if not compile_index_ref or not compile_surface_revision:
        raise ExecutionPacketRuntimeError(
            "execution_packet.compile_index_missing",
            "workflow-backed execution packet is missing compile-index revision authority",
        )

    from runtime.compile_index import (
        CompileIndexAuthorityError,
        load_compile_index_snapshot,
    )

    try:
        load_compile_index_snapshot(
            conn,
            snapshot_ref=compile_index_ref,
            surface_revision=compile_surface_revision,
            surface_name="compiler",
            require_fresh=True,
            repo_root=Path(__file__).resolve().parents[3],
        )
    except CompileIndexAuthorityError as exc:
        raise ExecutionPacketRuntimeError(
            _compile_index_reason_code(exc.reason_code),
            f"execution packet compile-index authority failed closed: {exc}",
        ) from exc


def _workflow_definition_authority(authority_inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    for field_name in ("workflow_definition", "definition_row"):
        value = authority_inputs.get(field_name)
        if isinstance(value, Mapping) and value:
            return value
    return {}


def _definition_requires_compile_index_authority(definition_row: Mapping[str, Any]) -> bool:
    if not definition_row:
        return False
    definition_type = str(definition_row.get("type") or "").strip()
    if definition_type == "operating_model":
        return True
    return isinstance(definition_row.get("compile_provenance"), Mapping)


def _compile_index_reason_code(reason_code: str) -> str:
    if reason_code == "compile_index.snapshot_missing":
        return "execution_packet.compile_index_missing"
    if reason_code == "compile_index.snapshot_stale":
        return "execution_packet.compile_index_stale"
    return "execution_packet.compile_index_invalid"


def _default_connection():
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    return SyncPostgresConnection(get_workflow_pool())


__all__ = [
    "ExecutionPacketBinding",
    "ExecutionPacketRuntimeError",
    "load_execution_packet_job_binding",
    "load_execution_packet_binding",
    "packet_prompt_fields",
    "packet_required",
]
