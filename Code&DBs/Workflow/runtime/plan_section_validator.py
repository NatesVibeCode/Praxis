"""Layer 5 (Validate): every-field-set + schema-conform check on authored packets.

Reads the plan_field rows from the data dictionary (category='plan_field')
and confirms every authored packet:

  - has every required field populated (no missing keys, no empty strings,
    no empty arrays where a value is required)
  - never contains forbidden placeholders (TBD, TODO, FIXME) inside
    fields whose plan_field row sets ``forbid_placeholders``
  - never sets write_scope to workspace root (``["."]`` or ``["./"]``)
    for plan_field rows with ``forbid_workspace_root=true``
  - drops nothing from the synthesizer's ``floor_from`` contracts
    (consumes / produces / capabilities / gates may grow, never shrink)
  - produces a typed gate scaffold for every required gate the stage
    contract demanded

Returns ``ValidationFinding`` records the operator (or compose pipeline)
can fail on. The validator NEVER mutates packets — it reports.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from runtime.intent_dependency import SkeletalPlan
from runtime.plan_section_author import AuthoredPacket, AuthoredPlan


_FORBIDDEN_PLACEHOLDERS_DEFAULT: tuple[str, ...] = ("TBD", "TODO", "FIXME", "XXX")


@dataclass(frozen=True)
class ValidationFinding:
    label: str
    field: str
    severity: str  # 'error' | 'warning'
    code: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "field": self.field,
            "severity": self.severity,
            "code": self.code,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ValidationReport:
    findings: list[ValidationFinding]
    every_required_filled: bool
    no_forbidden_placeholders: bool
    no_workspace_root: bool
    no_dropped_floors: bool
    every_required_gate_scaffolded: bool

    @property
    def passed(self) -> bool:
        return not any(f.severity == "error" for f in self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "findings": [f.to_dict() for f in self.findings],
            "every_required_filled": self.every_required_filled,
            "no_forbidden_placeholders": self.no_forbidden_placeholders,
            "no_workspace_root": self.no_workspace_root,
            "no_dropped_floors": self.no_dropped_floors,
            "every_required_gate_scaffolded": self.every_required_gate_scaffolded,
        }


def _load_plan_field_schema(conn: Any) -> dict[str, dict[str, Any]]:
    from runtime.data_dictionary import DataDictionaryBoundaryError, list_object_kinds

    out: dict[str, dict[str, Any]] = {}
    try:
        rows = list_object_kinds(conn, category="plan_field")
    except DataDictionaryBoundaryError:
        return out
    for row in rows:
        object_kind = str(row.get("object_kind") or "")
        field_name = object_kind.split(":", 1)[1] if ":" in object_kind else object_kind
        out[field_name] = {
            "object_kind": object_kind,
            "label": row.get("label"),
            "summary": row.get("summary"),
            "metadata": row.get("metadata") or {},
        }
    return out


def _is_filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _packet_value(packet: AuthoredPacket, field_name: str) -> Any:
    return packet.to_dict().get(field_name)


def _check_placeholders(value: Any, forbidden: tuple[str, ...]) -> str | None:
    if isinstance(value, str):
        upper = value.upper()
        for token in forbidden:
            if token in upper:
                return token
    elif isinstance(value, list):
        for item in value:
            hit = _check_placeholders(item, forbidden)
            if hit:
                return hit
    elif isinstance(value, dict):
        for sub in value.values():
            hit = _check_placeholders(sub, forbidden)
            if hit:
                return hit
    return None


def _floor_from_skeleton(
    skeleton: SkeletalPlan, label: str, source: str
) -> list[str] | None:
    """Look up the synthesizer's floor for a packet field."""
    target = next((p for p in skeleton.packets if p.label == label), None)
    if target is None:
        return None
    if source == "stage.consumes":
        return list(target.consumes_floor)
    if source == "stage.produces":
        return list(target.produces_floor)
    if source == "stage.capabilities":
        return list(target.capabilities_floor)
    if source == "stage.required_gates":
        return [gate.gate_id for gate in target.gates_scaffold]
    return None


def validate_authored_plan(
    plan: AuthoredPlan,
    *,
    skeleton: SkeletalPlan,
    conn: Any,
) -> ValidationReport:
    """Check every packet against the plan_field schema and synthesizer floors."""
    schema = _load_plan_field_schema(conn)
    findings: list[ValidationFinding] = []

    every_required_filled = True
    no_forbidden_placeholders = True
    no_workspace_root = True
    no_dropped_floors = True
    every_required_gate_scaffolded = True

    if not schema:
        findings.append(
            ValidationFinding(
                label="*",
                field="*",
                severity="error",
                code="plan_field.schema_missing",
                detail=(
                    "no plan_field rows in the data dictionary; cannot validate. "
                    "Apply migration 247."
                ),
            )
        )
        return ValidationReport(
            findings=findings,
            every_required_filled=False,
            no_forbidden_placeholders=False,
            no_workspace_root=False,
            no_dropped_floors=False,
            every_required_gate_scaffolded=False,
        )

    for packet in plan.packets:
        for field_name, entry in schema.items():
            metadata = entry.get("metadata", {})
            required = bool(metadata.get("required"))
            value = _packet_value(packet, field_name)
            forbid_placeholders = tuple(
                metadata.get("forbid_placeholders") or _FORBIDDEN_PLACEHOLDERS_DEFAULT
            )

            if required and not _is_filled(value):
                every_required_filled = False
                findings.append(
                    ValidationFinding(
                        label=packet.label,
                        field=field_name,
                        severity="error",
                        code="plan_field.required_missing",
                        detail=f"required field '{field_name}' is empty or missing",
                    )
                )

            placeholder_hit = _check_placeholders(value, forbid_placeholders)
            if placeholder_hit:
                no_forbidden_placeholders = False
                findings.append(
                    ValidationFinding(
                        label=packet.label,
                        field=field_name,
                        severity="error",
                        code="plan_field.placeholder",
                        detail=f"field '{field_name}' contains forbidden token {placeholder_hit!r}",
                    )
                )

            if metadata.get("forbid_workspace_root") and isinstance(value, list):
                if value in ([".", ], ["./"], []):
                    no_workspace_root = False
                    findings.append(
                        ValidationFinding(
                            label=packet.label,
                            field=field_name,
                            severity="error",
                            code="plan_field.workspace_root",
                            detail=(
                                f"field '{field_name}' is workspace-root or empty; "
                                "must be precise globs"
                            ),
                        )
                    )

            floor_from = metadata.get("floor_from")
            if floor_from and isinstance(value, list):
                floor = _floor_from_skeleton(skeleton, packet.label, floor_from)
                if floor:
                    if field_name == "gates":
                        present_ids = {
                            gate.get("gate_id")
                            for gate in value
                            if isinstance(gate, dict)
                        }
                        missing = [g for g in floor if g not in present_ids]
                    else:
                        missing = [g for g in floor if g not in value]
                    if missing:
                        no_dropped_floors = False
                        if field_name == "gates":
                            every_required_gate_scaffolded = False
                        findings.append(
                            ValidationFinding(
                                label=packet.label,
                                field=field_name,
                                severity="error",
                                code="plan_field.floor_dropped",
                                detail=(
                                    f"field '{field_name}' dropped floor entries "
                                    f"required by {floor_from}: {missing}"
                                ),
                            )
                        )

    return ValidationReport(
        findings=findings,
        every_required_filled=every_required_filled,
        no_forbidden_placeholders=no_forbidden_placeholders,
        no_workspace_root=no_workspace_root,
        no_dropped_floors=no_dropped_floors,
        every_required_gate_scaffolded=every_required_gate_scaffolded,
    )
