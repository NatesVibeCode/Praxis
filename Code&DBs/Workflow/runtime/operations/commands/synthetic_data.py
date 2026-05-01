"""CQRS commands for Synthetic Data authority."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from runtime.synthetic_data import SyntheticDataError, generate_synthetic_dataset
from storage.postgres.synthetic_data_repository import persist_synthetic_dataset


DomainPack = Literal["saas_b2b", "support_ops", "finance_ops", "healthcare_ops", "logistics_ops"]
PrivacyMode = Literal["synthetic_only", "schema_only", "anonymized_operational_seeded"]


class GenerateSyntheticDataCommand(BaseModel):
    """Generate and persist a synthetic dataset revision."""

    intent: str = Field(description="Purpose and behavioral shape for the dataset.")
    namespace: str = "default"
    workflow_ref: str | None = None
    source_context_ref: str | None = None
    source_object_truth_refs: list[str] = Field(default_factory=list)
    scenario_pack_refs: list[str] = Field(default_factory=list)
    object_counts: dict[str, int] = Field(default_factory=dict)
    records_per_object: int = Field(default=25, ge=1, le=50_000)
    seed: str | None = None
    domain_pack: DomainPack = "saas_b2b"
    locale_ref: str = "en-US"
    uniqueness_scope: str = "dataset"
    privacy_mode: PrivacyMode = "synthetic_only"
    reserved_terms: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("intent", "namespace", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("intent and namespace must be non-empty strings")
        return value.strip()

    @field_validator(
        "workflow_ref",
        "source_context_ref",
        "seed",
        "locale_ref",
        "uniqueness_scope",
        "observed_by_ref",
        "source_ref",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @field_validator("source_object_truth_refs", "scenario_pack_refs", "reserved_terms", mode="before")
    @classmethod
    def _normalize_string_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        if not isinstance(value, list):
            raise ValueError("ref fields must be lists of strings")
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("object_counts", "metadata", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: object) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        raise ValueError("object_counts and metadata must be JSON objects")


def handle_synthetic_data_generate(
    command: GenerateSyntheticDataCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Generate and persist one synthetic dataset through authority.synthetic_data."""

    try:
        dataset = generate_synthetic_dataset(
            intent=command.intent,
            namespace=command.namespace,
            workflow_ref=command.workflow_ref,
            source_context_ref=command.source_context_ref,
            source_object_truth_refs=command.source_object_truth_refs,
            scenario_pack_refs=command.scenario_pack_refs,
            object_counts=command.object_counts,
            records_per_object=command.records_per_object,
            seed=command.seed,
            domain_pack=command.domain_pack,
            locale_ref=command.locale_ref or "en-US",
            uniqueness_scope=command.uniqueness_scope or "dataset",
            privacy_mode=command.privacy_mode,
            reserved_terms=command.reserved_terms,
            metadata=command.metadata,
        )
        persisted = persist_synthetic_dataset(
            subsystems.get_pg_conn(),
            dataset=dataset,
            observed_by_ref=command.observed_by_ref,
            source_ref=command.source_ref,
        )
    except SyntheticDataError as exc:
        return {
            "ok": False,
            "operation": "synthetic_data_generate",
            "error_code": exc.reason_code,
            "error": str(exc),
            "details": exc.details,
        }
    event_payload = {
        "dataset_ref": persisted["dataset_ref"],
        "namespace": persisted["namespace"],
        "record_count": persisted["record_count"],
        "quality_state": persisted["quality_state"],
        "quality_score": persisted["quality_score"],
        "name_plan_ref": persisted["name_plan"]["plan_ref"],
        "scenario_pack_refs": persisted["scenario_pack_refs"],
        "privacy_mode": persisted["privacy_mode"],
    }
    return {
        "ok": True,
        "operation": "synthetic_data_generate",
        "dataset_ref": persisted["dataset_ref"],
        "dataset": persisted,
        "quality_report": persisted["quality_report"],
        "event_payload": event_payload,
    }


__all__ = [
    "GenerateSyntheticDataCommand",
    "handle_synthetic_data_generate",
]
