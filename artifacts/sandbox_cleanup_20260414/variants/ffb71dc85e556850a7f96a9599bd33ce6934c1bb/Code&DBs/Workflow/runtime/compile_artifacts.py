"""DB-backed compile artifact store for definition, plan, and packet lineage."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from runtime.execution_packet_authority import rebuild_workflow_run_packet_inspection


class CompileArtifactError(RuntimeError):
    """Raised when compile artifact persistence is unavailable or malformed."""


@dataclass(frozen=True, slots=True)
class CompileArtifactRecord:
    compile_artifact_id: str
    artifact_kind: str
    artifact_ref: str
    revision_ref: str
    parent_artifact_ref: str | None
    input_fingerprint: str
    content_hash: str
    authority_refs: tuple[str, ...]
    payload: dict[str, Any]
    decision_ref: str


@dataclass(frozen=True, slots=True)
class ExecutionPacketRecord:
    execution_packet_id: str
    packet_revision: str
    definition_revision: str
    plan_revision: str
    parent_artifact_ref: str | None
    packet_version: int
    packet_hash: str
    workflow_id: str
    run_id: str
    spec_name: str
    source_kind: str
    authority_refs: tuple[str, ...]
    model_messages: tuple[dict[str, Any], ...]
    reference_bindings: tuple[dict[str, Any], ...]
    capability_bindings: tuple[dict[str, Any], ...]
    verify_refs: tuple[str, ...]
    authority_inputs: dict[str, Any]
    file_inputs: dict[str, Any]
    payload: dict[str, Any]
    decision_ref: str


class CompileArtifactStore:
    """Explicit Postgres-backed compile artifact persistence."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def record_definition(
        self,
        *,
        definition: dict[str, Any],
        authority_refs: list[str] | tuple[str, ...] = (),
        decision_ref: str,
        input_fingerprint: str | None = None,
    ) -> CompileArtifactRecord:
        revision_ref = _require_text(definition.get("definition_revision"), field_name="definition_revision")
        return self._upsert(
            artifact_kind="definition",
            artifact_ref=revision_ref,
            revision_ref=revision_ref,
            parent_artifact_ref=None,
            input_fingerprint=_resolve_input_fingerprint(definition, input_fingerprint=input_fingerprint),
            authority_refs=authority_refs,
            payload=definition,
            decision_ref=decision_ref,
        )

    def record_plan(
        self,
        *,
        plan: dict[str, Any],
        authority_refs: list[str] | tuple[str, ...] = (),
        decision_ref: str,
        parent_artifact_ref: str | None = None,
        input_fingerprint: str | None = None,
    ) -> CompileArtifactRecord:
        revision_ref = _require_text(plan.get("plan_revision"), field_name="plan_revision")
        return self._upsert(
            artifact_kind="plan",
            artifact_ref=revision_ref,
            revision_ref=revision_ref,
            parent_artifact_ref=parent_artifact_ref,
            input_fingerprint=_resolve_input_fingerprint(plan, input_fingerprint=input_fingerprint),
            authority_refs=authority_refs,
            payload=plan,
            decision_ref=decision_ref,
        )

    def record_packet_lineage(
        self,
        *,
        packet: dict[str, Any],
        authority_refs: list[str] | tuple[str, ...] = (),
        decision_ref: str,
        parent_artifact_ref: str | None = None,
        input_fingerprint: str | None = None,
    ) -> CompileArtifactRecord:
        revision_ref = _packet_revision(packet)
        return self._upsert(
            artifact_kind="packet_lineage",
            artifact_ref=revision_ref,
            revision_ref=revision_ref,
            parent_artifact_ref=parent_artifact_ref,
            input_fingerprint=_resolve_input_fingerprint(packet, input_fingerprint=input_fingerprint),
            authority_refs=authority_refs,
            payload=packet,
            decision_ref=decision_ref,
        )

    def load_reusable_artifact(
        self,
        *,
        artifact_kind: str,
        input_fingerprint: str,
    ) -> CompileArtifactRecord | None:
        if self._conn is None:
            raise CompileArtifactError("compile artifact reads require Postgres authority")

        normalized_input_fingerprint = _require_text(
            input_fingerprint,
            field_name="input_fingerprint",
        )
        rows = self._conn.execute(
            """
            SELECT
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref
            FROM compile_artifacts
            WHERE artifact_kind = $1
              AND input_fingerprint = $2
            ORDER BY created_at ASC, compile_artifact_id ASC
            """,
            artifact_kind,
            normalized_input_fingerprint,
        )
        if not rows:
            return None

        records = tuple(
            _compile_artifact_record_from_row(
                row,
                expected_artifact_kind=artifact_kind,
                expected_input_fingerprint=normalized_input_fingerprint,
            )
            for row in rows
        )
        revision_refs = {record.revision_ref for record in records}
        content_hashes = {record.content_hash for record in records}
        if len(revision_refs) != 1 or len(content_hashes) != 1:
            raise CompileArtifactError(
                "conflicting reusable compile artifacts detected for the same exact input fingerprint",
            )
        return records[0]

    def record_execution_packet(
        self,
        *,
        packet: dict[str, Any],
        authority_refs: list[str] | tuple[str, ...] = (),
        decision_ref: str,
        parent_artifact_ref: str | None = None,
    ) -> ExecutionPacketRecord:
        definition_revision = _require_text(
            packet.get("definition_revision"),
            field_name="definition_revision",
        )
        plan_revision = _require_text(packet.get("plan_revision"), field_name="plan_revision")
        packet_revision = _require_text(packet.get("packet_revision"), field_name="packet_revision")
        packet_version = int(packet.get("packet_version") or 0)
        if packet_version < 1:
            raise CompileArtifactError("packet_version must be a positive integer")
        return self._upsert_execution_packet(
            packet=packet,
            definition_revision=definition_revision,
            plan_revision=plan_revision,
            packet_revision=packet_revision,
            packet_version=packet_version,
            parent_artifact_ref=parent_artifact_ref,
            authority_refs=authority_refs,
            decision_ref=decision_ref,
        )

    def _upsert(
        self,
        *,
        artifact_kind: str,
        artifact_ref: str,
        revision_ref: str,
        parent_artifact_ref: str | None,
        input_fingerprint: str,
        authority_refs: list[str] | tuple[str, ...],
        payload: dict[str, Any],
        decision_ref: str,
    ) -> CompileArtifactRecord:
        if self._conn is None:
            raise CompileArtifactError("compile artifact persistence requires Postgres authority")

        normalized_payload = _freeze_jsonish(payload)
        if not isinstance(normalized_payload, dict):
            raise CompileArtifactError("compile artifact payload must be mapping-shaped")

        payload_json = json.dumps(normalized_payload, sort_keys=True, separators=(",", ":"), default=str)
        content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        compile_artifact_id = f"compile_artifact.{artifact_kind}.{content_hash[:16]}"
        authority_refs_json = json.dumps(list(authority_refs))

        self._conn.execute(
            """
            INSERT INTO compile_artifacts (
                compile_artifact_id,
                artifact_kind,
                artifact_ref,
                revision_ref,
                parent_artifact_ref,
                input_fingerprint,
                content_hash,
                authority_refs,
                payload,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10
            )
            ON CONFLICT (artifact_kind, revision_ref) DO UPDATE SET
                artifact_ref = EXCLUDED.artifact_ref,
                parent_artifact_ref = EXCLUDED.parent_artifact_ref,
                input_fingerprint = EXCLUDED.input_fingerprint,
                content_hash = EXCLUDED.content_hash,
                authority_refs = EXCLUDED.authority_refs,
                payload = EXCLUDED.payload,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            compile_artifact_id,
            artifact_kind,
            artifact_ref,
            revision_ref,
            parent_artifact_ref,
            input_fingerprint,
            content_hash,
            authority_refs_json,
            payload_json,
            decision_ref,
        )

        return CompileArtifactRecord(
            compile_artifact_id=compile_artifact_id,
            artifact_kind=artifact_kind,
            artifact_ref=artifact_ref,
            revision_ref=revision_ref,
            parent_artifact_ref=parent_artifact_ref,
            input_fingerprint=input_fingerprint,
            content_hash=content_hash,
            authority_refs=tuple(str(ref) for ref in authority_refs),
            payload=normalized_payload,
            decision_ref=decision_ref,
        )

    def _upsert_execution_packet(
        self,
        *,
        packet: dict[str, Any],
        definition_revision: str,
        plan_revision: str,
        packet_revision: str,
        packet_version: int,
        parent_artifact_ref: str | None,
        authority_refs: list[str] | tuple[str, ...],
        decision_ref: str,
    ) -> ExecutionPacketRecord:
        if self._conn is None:
            raise CompileArtifactError("execution packet persistence requires Postgres authority")

        normalized_payload = _freeze_jsonish(packet)
        if not isinstance(normalized_payload, dict):
            raise CompileArtifactError("execution packet payload must be mapping-shaped")

        payload_json = json.dumps(normalized_payload, sort_keys=True, separators=(",", ":"), default=str)
        packet_hash = _require_text(packet.get("packet_hash"), field_name="packet_hash")
        execution_packet_id = f"execution_packet.{_require_text(normalized_payload.get('run_id'), field_name='run_id')}.{packet_revision}"
        authority_refs_json = json.dumps(list(authority_refs))
        model_messages = normalized_payload.get("model_messages", [])
        reference_bindings = normalized_payload.get("reference_bindings", [])
        capability_bindings = normalized_payload.get("capability_bindings", [])
        verify_refs = normalized_payload.get("verify_refs", [])
        authority_inputs = normalized_payload.get("authority_inputs", {})
        file_inputs = normalized_payload.get("file_inputs", {})

        self._conn.execute(
            """
            INSERT INTO execution_packets (
                execution_packet_id,
                definition_revision,
                plan_revision,
                packet_revision,
                parent_artifact_ref,
                packet_version,
                packet_hash,
                workflow_id,
                run_id,
                spec_name,
                source_kind,
                authority_refs,
                model_messages,
                reference_bindings,
                capability_bindings,
                verify_refs,
                authority_inputs,
                file_inputs,
                payload,
                decision_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, $16::jsonb,
                $17::jsonb, $18::jsonb, $19::jsonb, $20
            )
            ON CONFLICT (definition_revision, plan_revision, packet_revision) DO UPDATE SET
                parent_artifact_ref = EXCLUDED.parent_artifact_ref,
                packet_version = EXCLUDED.packet_version,
                packet_hash = EXCLUDED.packet_hash,
                workflow_id = EXCLUDED.workflow_id,
                run_id = EXCLUDED.run_id,
                spec_name = EXCLUDED.spec_name,
                source_kind = EXCLUDED.source_kind,
                authority_refs = EXCLUDED.authority_refs,
                model_messages = EXCLUDED.model_messages,
                reference_bindings = EXCLUDED.reference_bindings,
                capability_bindings = EXCLUDED.capability_bindings,
                verify_refs = EXCLUDED.verify_refs,
                authority_inputs = EXCLUDED.authority_inputs,
                file_inputs = EXCLUDED.file_inputs,
                payload = EXCLUDED.payload,
                decision_ref = EXCLUDED.decision_ref,
                updated_at = now()
            """,
            execution_packet_id,
            definition_revision,
            plan_revision,
            packet_revision,
            parent_artifact_ref,
            packet_version,
            packet_hash,
            _require_text(normalized_payload.get("workflow_id"), field_name="workflow_id"),
            _require_text(normalized_payload.get("run_id"), field_name="run_id"),
            _require_text(normalized_payload.get("spec_name"), field_name="spec_name"),
            _require_text(normalized_payload.get("source_kind"), field_name="source_kind"),
            authority_refs_json,
            json.dumps(model_messages, default=str),
            json.dumps(reference_bindings, default=str),
            json.dumps(capability_bindings, default=str),
            json.dumps(verify_refs, default=str),
            json.dumps(authority_inputs, default=str),
            json.dumps(file_inputs, default=str),
            payload_json,
            decision_ref,
        )

        rebuild_workflow_run_packet_inspection(
            self._conn,
            run_id=_require_text(normalized_payload.get("run_id"), field_name="run_id"),
        )

        return ExecutionPacketRecord(
            execution_packet_id=execution_packet_id,
            packet_revision=packet_revision,
            definition_revision=definition_revision,
            plan_revision=plan_revision,
            parent_artifact_ref=parent_artifact_ref,
            packet_version=packet_version,
            packet_hash=packet_hash,
            workflow_id=_require_text(normalized_payload.get("workflow_id"), field_name="workflow_id"),
            run_id=_require_text(normalized_payload.get("run_id"), field_name="run_id"),
            spec_name=_require_text(normalized_payload.get("spec_name"), field_name="spec_name"),
            source_kind=_require_text(normalized_payload.get("source_kind"), field_name="source_kind"),
            authority_refs=tuple(str(ref) for ref in authority_refs),
            model_messages=tuple(dict(item) for item in model_messages if isinstance(item, dict)),
            reference_bindings=tuple(dict(item) for item in reference_bindings if isinstance(item, dict)),
            capability_bindings=tuple(dict(item) for item in capability_bindings if isinstance(item, dict)),
            verify_refs=tuple(str(ref) for ref in verify_refs if isinstance(ref, str)),
            authority_inputs=dict(authority_inputs) if isinstance(authority_inputs, dict) else {},
            file_inputs=dict(file_inputs) if isinstance(file_inputs, dict) else {},
            payload=normalized_payload,
            decision_ref=decision_ref,
        )

    def load_execution_packets(
        self,
        *,
        run_id: str,
    ) -> tuple[ExecutionPacketRecord, ...]:
        if self._conn is None:
            raise CompileArtifactError("execution packet reads require Postgres authority")

        rows = self._conn.execute(
            """
            SELECT
                execution_packet_id,
                definition_revision,
                plan_revision,
                packet_revision,
                parent_artifact_ref,
                packet_version,
                packet_hash,
                workflow_id,
                run_id,
                spec_name,
                source_kind,
                authority_refs,
                model_messages,
                reference_bindings,
                capability_bindings,
                verify_refs,
                authority_inputs,
                file_inputs,
                payload,
                decision_ref
            FROM execution_packets
            WHERE run_id = $1
            ORDER BY created_at ASC, execution_packet_id ASC
            """,
            run_id,
        )
        return tuple(_execution_packet_record_from_row(row) for row in (rows or []))

    def load_execution_packet(
        self,
        *,
        packet_revision: str,
    ) -> ExecutionPacketRecord | None:
        if self._conn is None:
            raise CompileArtifactError("execution packet reads require Postgres authority")

        normalized_packet_revision = _require_text(
            packet_revision,
            field_name="packet_revision",
        )
        rows = self._conn.execute(
            """
            SELECT
                execution_packet_id,
                definition_revision,
                plan_revision,
                packet_revision,
                parent_artifact_ref,
                packet_version,
                packet_hash,
                workflow_id,
                run_id,
                spec_name,
                source_kind,
                authority_refs,
                model_messages,
                reference_bindings,
                capability_bindings,
                verify_refs,
                authority_inputs,
                file_inputs,
                payload,
                decision_ref
            FROM execution_packets
            WHERE packet_revision = $1
            ORDER BY created_at DESC, execution_packet_id DESC
            """,
            normalized_packet_revision,
        )
        if not rows:
            return None

        records = tuple(_execution_packet_record_from_row(row) for row in rows)
        packet_hashes = {record.packet_hash for record in records}
        definition_revisions = {record.definition_revision for record in records}
        plan_revisions = {record.plan_revision for record in records}
        if (
            len(packet_hashes) != 1
            or len(definition_revisions) != 1
            or len(plan_revisions) != 1
        ):
            raise CompileArtifactError(
                "conflicting execution packet authority detected for the same packet_revision",
            )
        return records[0]


def _packet_revision(packet: dict[str, Any]) -> str:
    packet_hash = _require_text(packet.get("packet_hash"), field_name="packet_hash")
    packet_version = packet.get("packet_version")
    return f"packet_{_require_text(packet_hash, field_name='packet_hash')[:16]}:{int(packet_version or 0)}"


def _freeze_jsonish(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _freeze_jsonish(subvalue) for key, subvalue in value.items()}
    if isinstance(value, list):
        return [_freeze_jsonish(item) for item in value]
    if isinstance(value, tuple):
        return [_freeze_jsonish(item) for item in value]
    return value


def _jsonish_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []


def _jsonish_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _require_text(value: object, *, field_name: str) -> str:
    text = value if isinstance(value, str) else ""
    if not text.strip():
        raise CompileArtifactError(f"{field_name} is required")
    return text.strip()


def _execution_packet_record_from_row(row: Any) -> ExecutionPacketRecord:
    return ExecutionPacketRecord(
        execution_packet_id=str(row["execution_packet_id"]),
        packet_revision=str(row["packet_revision"]),
        definition_revision=str(row["definition_revision"]),
        plan_revision=str(row["plan_revision"]),
        parent_artifact_ref=(
            None if row.get("parent_artifact_ref") is None else str(row["parent_artifact_ref"])
        ),
        packet_version=int(row["packet_version"] or 0),
        packet_hash=str(row["packet_hash"]),
        workflow_id=str(row["workflow_id"]),
        run_id=str(row["run_id"]),
        spec_name=str(row["spec_name"]),
        source_kind=str(row["source_kind"]),
        authority_refs=tuple(str(ref) for ref in _jsonish_list(row["authority_refs"])),
        model_messages=tuple(
            dict(item) for item in _jsonish_list(row["model_messages"]) if isinstance(item, dict)
        ),
        reference_bindings=tuple(
            dict(item) for item in _jsonish_list(row["reference_bindings"]) if isinstance(item, dict)
        ),
        capability_bindings=tuple(
            dict(item) for item in _jsonish_list(row["capability_bindings"]) if isinstance(item, dict)
        ),
        verify_refs=tuple(str(ref) for ref in _jsonish_list(row["verify_refs"]) if isinstance(ref, str)),
        authority_inputs=_jsonish_object(row["authority_inputs"]),
        file_inputs=_jsonish_object(row["file_inputs"]),
        payload=_jsonish_object(row["payload"]),
        decision_ref=str(row["decision_ref"]),
    )


def _compile_artifact_record_from_row(
    row: Any,
    *,
    expected_artifact_kind: str,
    expected_input_fingerprint: str,
) -> CompileArtifactRecord:
    payload = _jsonish_object(row.get("payload"))
    if not payload:
        raise CompileArtifactError("reusable compile artifact payload must be a non-empty object")
    artifact_kind = _require_text(row.get("artifact_kind"), field_name="artifact_kind")
    if artifact_kind != expected_artifact_kind:
        raise CompileArtifactError(
            f"reusable compile artifact kind mismatch: expected {expected_artifact_kind}, got {artifact_kind}",
        )
    revision_ref = _require_text(row.get("revision_ref"), field_name="revision_ref")
    payload_revision = _payload_revision_ref(payload, artifact_kind=artifact_kind)
    if payload_revision != revision_ref:
        raise CompileArtifactError(
            f"reusable compile artifact revision mismatch for {artifact_kind}: "
            f"payload={payload_revision} row={revision_ref}",
        )
    input_fingerprint = _require_text(row.get("input_fingerprint"), field_name="input_fingerprint")
    if input_fingerprint != expected_input_fingerprint:
        raise CompileArtifactError(
            "reusable compile artifact input_fingerprint does not match the requested exact fingerprint",
        )
    payload_input_fingerprint = _resolve_input_fingerprint(payload, input_fingerprint=None)
    if payload_input_fingerprint != expected_input_fingerprint:
        raise CompileArtifactError(
            "reusable compile artifact payload is missing the expected compile_provenance input_fingerprint",
        )
    normalized_payload = _freeze_jsonish(payload)
    payload_json = json.dumps(normalized_payload, sort_keys=True, separators=(",", ":"), default=str)
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    recorded_content_hash = _require_text(row.get("content_hash"), field_name="content_hash")
    if content_hash != recorded_content_hash:
        raise CompileArtifactError(
            "reusable compile artifact payload hash does not match the recorded content_hash",
        )
    return CompileArtifactRecord(
        compile_artifact_id=_require_text(row.get("compile_artifact_id"), field_name="compile_artifact_id"),
        artifact_kind=artifact_kind,
        artifact_ref=_require_text(row.get("artifact_ref"), field_name="artifact_ref"),
        revision_ref=revision_ref,
        parent_artifact_ref=(
            None if row.get("parent_artifact_ref") is None else str(row.get("parent_artifact_ref"))
        ),
        input_fingerprint=input_fingerprint,
        content_hash=content_hash,
        authority_refs=tuple(str(ref) for ref in _jsonish_list(row.get("authority_refs"))),
        payload=normalized_payload if isinstance(normalized_payload, dict) else {},
        decision_ref=_require_text(row.get("decision_ref"), field_name="decision_ref"),
    )


def _payload_revision_ref(payload: dict[str, Any], *, artifact_kind: str) -> str:
    field_name_by_kind = {
        "definition": "definition_revision",
        "plan": "plan_revision",
        "packet_lineage": "packet_revision",
    }
    field_name = field_name_by_kind.get(artifact_kind)
    if field_name is None:
        raise CompileArtifactError(f"unsupported compile artifact kind: {artifact_kind}")
    return _require_text(payload.get(field_name), field_name=field_name)


def _resolve_input_fingerprint(
    payload: dict[str, Any],
    *,
    input_fingerprint: str | None,
) -> str:
    if input_fingerprint is not None and str(input_fingerprint).strip():
        return str(input_fingerprint).strip()
    compile_provenance = payload.get("compile_provenance")
    if isinstance(compile_provenance, dict):
        candidate = compile_provenance.get("input_fingerprint")
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    packet_hash = payload.get("packet_hash")
    if isinstance(packet_hash, str) and packet_hash.strip():
        return packet_hash.strip()
    raise CompileArtifactError("input_fingerprint is required")


__all__ = [
    "CompileArtifactError",
    "CompileArtifactRecord",
    "CompileArtifactStore",
    "ExecutionPacketRecord",
]
