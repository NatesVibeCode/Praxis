"""Typed contracts for Phase 1 client system discovery authority."""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any, Literal


SurfaceKind = Literal["capability", "object", "api", "event"]
AutomationClass = Literal["automation_bearing", "observe_only", "unknown"]
CredentialStatus = Literal[
    "valid",
    "expired",
    "revoked",
    "missing",
    "missing_scope",
    "probe_failed",
    "error",
    "unknown",
]
DeploymentModel = Literal["saas", "self_hosted", "desktop", "spreadsheet", "file_drop", "internal_tool", "unknown"]
DiscoveryStatus = Literal["declared", "captured", "verified", "blocked", "unknown"]
Directionality = Literal["uni", "bi", "unknown"]
EvidenceStatus = Literal["declared", "observed", "verified", "blocked", "unknown"]
GapSeverity = Literal["critical", "high", "medium", "low"]
DiscoveryGapKind = Literal[
    "missing_connector",
    "missing_capability",
    "missing_object_surface",
    "missing_event_surface",
    "credential_health_unknown",
    "missing_access",
    "invalid_credential",
    "insufficient_scope",
    "unknown_object_model",
    "unknown_rate_limit",
    "unknown_event_surface",
    "connector_unavailable",
    "connector_capability_unverified",
    "owner_unassigned",
    "doc_conflict",
    "probe_blocked",
    "environment_ambiguity",
]

_AUTOMATION_ACTION_HINTS = (
    "create",
    "update",
    "delete",
    "write",
    "send",
    "post",
    "put",
    "patch",
    "invoke",
    "dispatch",
    "trigger",
    "sync",
)
_AUTOMATION_HTTP_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_VALID_SURFACE_KINDS = {"capability", "object", "api", "event"}
_VALID_CREDENTIAL_STATUSES = {
    "valid",
    "expired",
    "revoked",
    "missing",
    "missing_scope",
    "probe_failed",
    "error",
    "unknown",
}
_REDACTED_VALUES = {"", "[redacted]", "redacted", "***", "<redacted>", "[REDACTED]"}
_SENSITIVE_KEY_RE = re.compile(
    r"(^|[_\-.])(password|passwd|secret|token|api[_\-.]?key|private[_\-.]?key|session|cookie)([_\-.]|$)",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = re.compile(
    r"(Bearer\s+[A-Za-z0-9._\-]{12,}|sk-[A-Za-z0-9]{12,}|xox[baprs]-[A-Za-z0-9-]{12,}|"
    r"gh[pousr]_[A-Za-z0-9_]{12,}|AKIA[0-9A-Z]{12,}|-----BEGIN [A-Z ]*PRIVATE KEY-----)",
    re.IGNORECASE,
)
_NON_SECRET_KEY_EXCEPTIONS = {"token_url", "authorize_url", "authorization_url", "auth_url"}


class ClientSystemDiscoveryError(ValueError):
    """Raised when Phase 1 discovery evidence violates the packet contract."""


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_digest(value: Any) -> str:
    return hashlib.sha256(stable_json_dumps(value).encode("utf-8")).hexdigest()


def assert_no_secret_material(value: Any, *, path: str = "payload") -> None:
    """Fail closed when evidence appears to carry raw credential material."""
    secret_paths = _find_secret_material(value, path=path)
    if secret_paths:
        raise ClientSystemDiscoveryError(
            "client_system_discovery.secret_material",
            f"raw secret material is not allowed in discovery evidence: {', '.join(secret_paths)}",
        )


def _find_secret_material(value: Any, *, path: str) -> list[str]:
    findings: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            item_path = f"{path}.{key_text}" if path else key_text
            if _is_sensitive_key(key_text) and not _is_redacted_secret_placeholder(item):
                findings.append(item_path)
                continue
            findings.extend(_find_secret_material(item, path=item_path))
        return findings
    if isinstance(value, (list, tuple, set)):
        for index, item in enumerate(value):
            findings.extend(_find_secret_material(item, path=f"{path}[{index}]"))
        return findings
    if isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        findings.append(path)
    return findings


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower()
    if normalized in _NON_SECRET_KEY_EXCEPTIONS or normalized.endswith("_url"):
        return False
    return bool(_SENSITIVE_KEY_RE.search(key))


def _is_redacted_secret_placeholder(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in _REDACTED_VALUES
    if isinstance(value, dict):
        return all(_is_redacted_secret_placeholder(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return all(_is_redacted_secret_placeholder(item) for item in value)
    return False


def _parse_jsonish(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _normal_capabilities(value: Any) -> list[dict[str, Any]]:
    raw = _parse_jsonish(value)
    if not isinstance(raw, list):
        return []
    capabilities: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, str):
            action = item.strip()
            if action:
                capabilities.append({"action": action})
        elif isinstance(item, dict) and str(item.get("action") or "").strip():
            capabilities.append(dict(item))
    assert_no_secret_material(capabilities, path="capabilities")
    return capabilities


def classify_automation_bearing_tool(
    capabilities: list[dict[str, Any]],
    surfaces: list["ConnectorSurfaceEvidence"],
) -> AutomationClass:
    for capability in capabilities:
        action = str(capability.get("action") or "").strip().lower()
        if action.startswith(_AUTOMATION_ACTION_HINTS):
            return "automation_bearing"
    for surface in surfaces:
        if surface.surface_kind == "api" and (surface.http_method or "").upper() in _AUTOMATION_HTTP_METHODS:
            return "automation_bearing"
        if surface.surface_kind == "event" and str(surface.evidence.get("direction") or "").lower() in {
            "emit",
            "publish",
            "write",
        }:
            return "automation_bearing"
    if capabilities or surfaces:
        return "observe_only"
    return "unknown"


@dataclass(frozen=True)
class CredentialHealthRef:
    credential_ref: str | None
    env_var_ref: str | None
    status: CredentialStatus
    checked_at: str | None = None
    expires_at: str | None = None
    detail: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in _VALID_CREDENTIAL_STATUSES:
            raise ClientSystemDiscoveryError(
                "client_system_discovery.credential_status",
                f"unsupported credential health status: {self.status}",
            )
        assert_no_secret_material(
            {
                "credential_ref": self.credential_ref,
                "env_var_ref": self.env_var_ref,
                "detail": self.detail,
                "metadata": self.metadata,
            },
            path="credential_health_ref",
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "credential_ref": self.credential_ref,
            "env_var_ref": self.env_var_ref,
            "status": self.status,
            "checked_at": self.checked_at,
            "expires_at": self.expires_at,
            "detail": self.detail,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class ConnectorSurfaceEvidence:
    surface_kind: SurfaceKind
    surface_ref: str
    evidence: dict[str, Any] = field(default_factory=dict)
    operation_name: str | None = None
    object_name: str | None = None
    http_method: str | None = None
    path_template: str | None = None
    event_name: str | None = None

    def __post_init__(self) -> None:
        if self.surface_kind not in _VALID_SURFACE_KINDS:
            raise ClientSystemDiscoveryError(
                "client_system_discovery.surface_kind",
                f"unsupported connector surface kind: {self.surface_kind}",
            )
        if not str(self.surface_ref or "").strip():
            raise ClientSystemDiscoveryError(
                "client_system_discovery.surface_ref_required",
                "connector surface evidence requires a surface_ref",
            )
        assert_no_secret_material(self.evidence, path=f"surface_evidence.{self.surface_ref}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "surface_kind": self.surface_kind,
            "surface_ref": self.surface_ref,
            "operation_name": self.operation_name,
            "object_name": self.object_name,
            "http_method": self.http_method,
            "path_template": self.path_template,
            "event_name": self.event_name,
            "evidence": dict(self.evidence),
        }


def capability_surface(
    *,
    surface_ref: str,
    operation_name: str,
    evidence: dict[str, Any] | None = None,
) -> ConnectorSurfaceEvidence:
    return ConnectorSurfaceEvidence(
        surface_kind="capability",
        surface_ref=surface_ref,
        operation_name=operation_name,
        evidence=dict(evidence or {}),
    )


def object_surface(
    *,
    surface_ref: str,
    object_name: str,
    evidence: dict[str, Any] | None = None,
) -> ConnectorSurfaceEvidence:
    return ConnectorSurfaceEvidence(
        surface_kind="object",
        surface_ref=surface_ref,
        object_name=object_name,
        evidence=dict(evidence or {}),
    )


def api_surface(
    *,
    surface_ref: str,
    operation_name: str,
    http_method: str,
    path_template: str,
    evidence: dict[str, Any] | None = None,
) -> ConnectorSurfaceEvidence:
    return ConnectorSurfaceEvidence(
        surface_kind="api",
        surface_ref=surface_ref,
        operation_name=operation_name,
        http_method=http_method.upper(),
        path_template=path_template,
        evidence=dict(evidence or {}),
    )


def event_surface(
    *,
    surface_ref: str,
    event_name: str,
    evidence: dict[str, Any] | None = None,
) -> ConnectorSurfaceEvidence:
    return ConnectorSurfaceEvidence(
        surface_kind="event",
        surface_ref=surface_ref,
        event_name=event_name,
        evidence=dict(evidence or {}),
    )


@dataclass(frozen=True)
class ConnectorCensusRecord:
    connector_census_id: str
    integration_id: str | None
    connector_slug: str
    display_name: str
    provider: str
    auth_kind: str
    auth_status: str
    capabilities: list[dict[str, Any]] = field(default_factory=list)
    surfaces: list[ConnectorSurfaceEvidence] = field(default_factory=list)
    credential_health_refs: list[CredentialHealthRef] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    automation_classification: AutomationClass = "unknown"

    def __post_init__(self) -> None:
        assert_no_secret_material(self.capabilities, path=f"connector.{self.connector_slug}.capabilities")
        assert_no_secret_material(self.metadata, path=f"connector.{self.connector_slug}.metadata")

    def with_inferred_classification(self) -> "ConnectorCensusRecord":
        inferred = classify_automation_bearing_tool(self.capabilities, self.surfaces)
        return ConnectorCensusRecord(
            connector_census_id=self.connector_census_id,
            integration_id=self.integration_id,
            connector_slug=self.connector_slug,
            display_name=self.display_name,
            provider=self.provider,
            auth_kind=self.auth_kind,
            auth_status=self.auth_status,
            capabilities=list(self.capabilities),
            surfaces=list(self.surfaces),
            credential_health_refs=list(self.credential_health_refs),
            metadata=dict(self.metadata),
            automation_classification=inferred,
        )

    def counts(self) -> dict[str, int]:
        counts = {"capability": 0, "object": 0, "api": 0, "event": 0}
        for surface in self.surfaces:
            counts[surface.surface_kind] += 1
        return counts

    def as_dict(self) -> dict[str, Any]:
        counts = self.counts()
        return {
            "connector_census_id": self.connector_census_id,
            "integration_id": self.integration_id,
            "connector_slug": self.connector_slug,
            "display_name": self.display_name,
            "provider": self.provider,
            "auth_kind": self.auth_kind,
            "auth_status": self.auth_status,
            "automation_classification": self.automation_classification,
            "capability_count": len(self.capabilities),
            "object_surface_count": counts["object"],
            "api_surface_count": counts["api"],
            "event_surface_count": counts["event"],
            "capabilities": [dict(item) for item in self.capabilities],
            "surfaces": [item.as_dict() for item in self.surfaces],
            "credential_health_refs": [item.as_dict() for item in self.credential_health_refs],
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class IntegrationEvidenceRecord:
    integration_id: str
    source_system_id: str
    target_system_id: str
    integration_type: str
    transport: str
    directionality: Directionality = "unknown"
    trigger_mode: str = "unknown"
    integration_owner: str | None = None
    observed_status: EvidenceStatus = "unknown"
    evidence_ref: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        assert_no_secret_material(self.metadata, path=f"integration.{self.integration_id}.metadata")

    def as_dict(self) -> dict[str, Any]:
        return {
            "integration_id": self.integration_id,
            "source_system_id": self.source_system_id,
            "target_system_id": self.target_system_id,
            "integration_type": self.integration_type,
            "transport": self.transport,
            "directionality": self.directionality,
            "trigger_mode": self.trigger_mode,
            "integration_owner": self.integration_owner,
            "observed_status": self.observed_status,
            "evidence_ref": self.evidence_ref,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class DiscoveryGap:
    gap_kind: DiscoveryGapKind
    reason_code: str
    source_ref: str
    detail: str
    severity: GapSeverity = "medium"
    is_blocker: bool = False
    expected_evidence: str | None = None
    current_evidence: str | None = None
    next_action: str | None = None
    owner: str | None = None
    opened_at: str | None = None
    resolved_at: str | None = None
    legal_repair_actions: list[str] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)
    gap_id: str | None = None

    def __post_init__(self) -> None:
        assert_no_secret_material(self.context, path=f"gap.{self.reason_code}.context")

    def resolved_gap_id(self) -> str:
        if self.gap_id:
            return self.gap_id
        payload = {
            "gap_kind": self.gap_kind,
            "reason_code": self.reason_code,
            "source_ref": self.source_ref,
            "detail": self.detail,
            "severity": self.severity,
            "is_blocker": self.is_blocker,
            "expected_evidence": self.expected_evidence,
            "current_evidence": self.current_evidence,
            "next_action": self.next_action,
            "owner": self.owner,
            "opened_at": self.opened_at,
            "resolved_at": self.resolved_at,
            "legal_repair_actions": list(self.legal_repair_actions),
            "context": dict(self.context),
        }
        return f"typed_gap.client_system_discovery.{stable_digest(payload)[:16]}"

    def as_event_payload(self) -> dict[str, Any]:
        return {
            "gap_id": self.resolved_gap_id(),
            "gap_kind": self.gap_kind,
            "missing_type": "client_system_discovery",
            "reason_code": self.reason_code,
            "source_ref": self.source_ref,
            "detail": self.detail,
            "severity": self.severity,
            "is_blocker": self.is_blocker,
            "expected_evidence": self.expected_evidence,
            "current_evidence": self.current_evidence,
            "next_action": self.next_action,
            "owner": self.owner,
            "opened_at": self.opened_at,
            "resolved_at": self.resolved_at,
            "legal_repair_actions": [str(item) for item in self.legal_repair_actions],
            "context": dict(self.context),
        }


@dataclass(frozen=True)
class SystemCensusRecord:
    census_id: str
    tenant_ref: str
    workspace_ref: str
    system_slug: str
    system_name: str
    discovery_source: str
    captured_at: str
    status: str = "captured"
    category: str = "unknown"
    vendor: str | None = None
    deployment_model: DeploymentModel = "unknown"
    environment: str = "unknown"
    business_owner: str | None = None
    technical_owner: str | None = None
    criticality: str = "unknown"
    declared_purpose: str | None = None
    discovery_status: DiscoveryStatus = "captured"
    last_verified_at: str | None = None
    integrations: list[IntegrationEvidenceRecord] = field(default_factory=list)
    connectors: list[ConnectorCensusRecord] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        assert_no_secret_material(self.metadata, path=f"system.{self.system_slug}.metadata")

    def evidence_hash(self) -> str:
        return stable_digest(
            {
                "census_id": self.census_id,
                "tenant_ref": self.tenant_ref,
                "workspace_ref": self.workspace_ref,
                "system_slug": self.system_slug,
                "system_name": self.system_name,
                "discovery_source": self.discovery_source,
                "captured_at": self.captured_at,
                "status": self.status,
                "category": self.category,
                "vendor": self.vendor,
                "deployment_model": self.deployment_model,
                "environment": self.environment,
                "business_owner": self.business_owner,
                "technical_owner": self.technical_owner,
                "criticality": self.criticality,
                "declared_purpose": self.declared_purpose,
                "discovery_status": self.discovery_status,
                "last_verified_at": self.last_verified_at,
                "integrations": [item.as_dict() for item in self.integrations],
                "connectors": [item.as_dict() for item in self.connectors],
                "metadata": dict(self.metadata),
            }
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "census_id": self.census_id,
            "tenant_ref": self.tenant_ref,
            "workspace_ref": self.workspace_ref,
            "system_slug": self.system_slug,
            "system_name": self.system_name,
            "discovery_source": self.discovery_source,
            "captured_at": self.captured_at,
            "status": self.status,
            "category": self.category,
            "vendor": self.vendor,
            "deployment_model": self.deployment_model,
            "environment": self.environment,
            "business_owner": self.business_owner,
            "technical_owner": self.technical_owner,
            "criticality": self.criticality,
            "declared_purpose": self.declared_purpose,
            "discovery_status": self.discovery_status,
            "last_verified_at": self.last_verified_at,
            "connector_count": len(self.connectors),
            "integration_count": len(self.integrations),
            "evidence_hash": self.evidence_hash(),
            "metadata": dict(self.metadata),
            "integrations": [item.as_dict() for item in self.integrations],
            "connectors": [item.as_dict() for item in self.connectors],
        }


def connector_record_from_payload(*, census_id: str, payload: dict[str, Any]) -> ConnectorCensusRecord:
    assert_no_secret_material(payload.get("metadata") or {}, path="connector_payload.metadata")
    raw_surfaces = payload.get("surfaces") or []
    surfaces = [
        ConnectorSurfaceEvidence(
            surface_kind=str(item.get("surface_kind") or "capability"),
            surface_ref=str(item.get("surface_ref") or ""),
            operation_name=item.get("operation_name"),
            object_name=item.get("object_name"),
            http_method=item.get("http_method"),
            path_template=item.get("path_template"),
            event_name=item.get("event_name"),
            evidence=dict(item.get("evidence") or {}),
        )
        for item in raw_surfaces
        if isinstance(item, dict) and str(item.get("surface_ref") or "").strip()
    ]
    credential_health_refs = [
        CredentialHealthRef(
            credential_ref=item.get("credential_ref"),
            env_var_ref=item.get("env_var_ref"),
            status=str(item.get("status") or "unknown"),
            checked_at=item.get("checked_at"),
            expires_at=item.get("expires_at"),
            detail=item.get("detail"),
            metadata=dict(item.get("metadata") or {}),
        )
        for item in (payload.get("credential_health_refs") or [])
        if isinstance(item, dict)
    ]
    capabilities = _normal_capabilities(payload.get("capabilities") or [])
    basis = {
        "census_id": census_id,
        "integration_id": payload.get("integration_id"),
        "connector_slug": payload.get("connector_slug"),
        "provider": payload.get("provider"),
        "display_name": payload.get("display_name"),
    }
    record = ConnectorCensusRecord(
        connector_census_id=str(payload.get("connector_census_id") or f"connector_census.{stable_digest(basis)[:16]}"),
        integration_id=str(payload.get("integration_id") or "").strip() or None,
        connector_slug=str(payload.get("connector_slug") or "").strip(),
        display_name=str(payload.get("display_name") or payload.get("connector_slug") or "").strip(),
        provider=str(payload.get("provider") or "").strip(),
        auth_kind=str(payload.get("auth_kind") or "unknown").strip(),
        auth_status=str(payload.get("auth_status") or "unknown").strip(),
        capabilities=capabilities,
        surfaces=surfaces,
        credential_health_refs=credential_health_refs,
        metadata=dict(payload.get("metadata") or {}),
        automation_classification=str(payload.get("automation_classification") or "unknown"),
    )
    return record.with_inferred_classification()


@dataclass(frozen=True)
class DiscoveryValidationReport:
    census_id: str
    ok: bool
    summary: dict[str, Any]
    gaps: list[DiscoveryGap] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "census_id": self.census_id,
            "ok": self.ok,
            "summary": dict(self.summary),
            "gap_count": len(self.gaps),
            "blocker_count": sum(1 for gap in self.gaps if gap.is_blocker),
            "critical_gap_count": sum(1 for gap in self.gaps if gap.severity == "critical"),
            "gaps": [gap.as_event_payload() for gap in self.gaps],
        }


def integration_record_from_payload(payload: dict[str, Any]) -> IntegrationEvidenceRecord:
    return IntegrationEvidenceRecord(
        integration_id=str(payload.get("integration_id") or "").strip(),
        source_system_id=str(payload.get("source_system_id") or "").strip(),
        target_system_id=str(payload.get("target_system_id") or "").strip(),
        integration_type=str(payload.get("integration_type") or "unknown").strip(),
        transport=str(payload.get("transport") or "unknown").strip(),
        directionality=str(payload.get("directionality") or "unknown"),
        trigger_mode=str(payload.get("trigger_mode") or "unknown").strip(),
        integration_owner=payload.get("integration_owner"),
        observed_status=str(payload.get("observed_status") or "unknown"),
        evidence_ref=payload.get("evidence_ref"),
        metadata=dict(payload.get("metadata") or {}),
    )


def system_record_from_payload(payload: dict[str, Any]) -> SystemCensusRecord:
    tenant_ref = str(payload.get("tenant_ref") or "").strip()
    workspace_ref = str(payload.get("workspace_ref") or "").strip()
    system_slug = str(payload.get("system_slug") or "").strip()
    captured_at = str(payload.get("captured_at") or "").strip()
    census_id = str(
        payload.get("census_id")
        or f"client_system_census.{stable_digest({'tenant_ref': tenant_ref, 'workspace_ref': workspace_ref, 'system_slug': system_slug, 'captured_at': captured_at})[:16]}"
    )
    connectors = [
        connector_record_from_payload(census_id=census_id, payload=item)
        for item in (payload.get("connectors") or [])
        if isinstance(item, dict)
    ]
    integrations = [
        integration_record_from_payload(item)
        for item in (payload.get("integrations") or [])
        if isinstance(item, dict)
    ]
    return SystemCensusRecord(
        census_id=census_id,
        tenant_ref=tenant_ref,
        workspace_ref=workspace_ref,
        system_slug=system_slug,
        system_name=str(payload.get("system_name") or system_slug).strip(),
        discovery_source=str(payload.get("discovery_source") or "repo_inspection").strip(),
        captured_at=captured_at,
        status=str(payload.get("status") or "captured").strip(),
        category=str(payload.get("category") or "unknown").strip(),
        vendor=payload.get("vendor"),
        deployment_model=str(payload.get("deployment_model") or "unknown"),
        environment=str(payload.get("environment") or "unknown").strip(),
        business_owner=payload.get("business_owner"),
        technical_owner=payload.get("technical_owner"),
        criticality=str(payload.get("criticality") or "unknown").strip(),
        declared_purpose=payload.get("declared_purpose"),
        discovery_status=str(payload.get("discovery_status") or payload.get("status") or "captured"),
        last_verified_at=payload.get("last_verified_at"),
        integrations=integrations,
        connectors=connectors,
        metadata=dict(payload.get("metadata") or {}),
    )


def connector_record_from_manifest(
    *,
    census_id: str,
    manifest: Any,
    credential_status: CredentialStatus = "unknown",
) -> ConnectorCensusRecord:
    """Build connector census evidence from runtime.integration_manifest objects."""
    auth_shape = getattr(manifest, "auth_shape", None)
    auth_dict = _auth_shape_as_dict(auth_shape)
    capabilities = [
        {
            "action": getattr(capability, "action", ""),
            "description": getattr(capability, "description", ""),
            "method": getattr(capability, "method", ""),
            "path": getattr(capability, "path", ""),
            "source": "integration_manifest",
        }
        for capability in tuple(getattr(manifest, "capabilities", ()) or ())
        if str(getattr(capability, "action", "")).strip()
    ]
    payload = {
        "integration_id": getattr(manifest, "id", None),
        "connector_slug": getattr(manifest, "id", ""),
        "display_name": getattr(manifest, "name", getattr(manifest, "id", "")),
        "provider": getattr(manifest, "provider", ""),
        "auth_kind": auth_dict.get("kind") or "unknown",
        "auth_status": "declared",
        "capabilities": capabilities,
        "surfaces": [
            surface.as_dict()
            for surface in _surfaces_from_capabilities(
                connector_ref=str(getattr(manifest, "id", "")),
                capabilities=capabilities,
                evidence_source="integration_manifest",
                auth_kind=str(auth_dict.get("kind") or "unknown"),
            )
        ],
        "credential_health_refs": [
            ref.as_dict()
            for ref in _credential_refs_from_auth_shape(auth_dict, status=credential_status)
        ],
        "metadata": {"manifest_provider": getattr(manifest, "provider", "")},
    }
    return connector_record_from_payload(census_id=census_id, payload=payload)


def connector_record_from_registry_row(
    *,
    census_id: str,
    row: dict[str, Any],
    credential_status: CredentialStatus = "unknown",
) -> ConnectorCensusRecord:
    """Build connector census evidence from integration_registry rows or descriptions."""
    auth_shape = _auth_shape_as_dict(row.get("auth_shape") or row.get("auth"))
    capabilities = _normal_capabilities(row.get("capabilities") or row.get("actions") or [])
    connector_ref = str(row.get("connector_slug") or row.get("id") or row.get("slug") or "").strip()
    payload = {
        "integration_id": row.get("id") or row.get("integration_id"),
        "connector_slug": connector_ref,
        "display_name": row.get("display_name") or row.get("name") or connector_ref,
        "provider": row.get("provider") or connector_ref,
        "auth_kind": row.get("auth_kind") or auth_shape.get("kind") or "unknown",
        "auth_status": row.get("auth_status") or "unknown",
        "capabilities": capabilities,
        "surfaces": [
            surface.as_dict()
            for surface in _surfaces_from_capabilities(
                connector_ref=connector_ref,
                capabilities=capabilities,
                evidence_source=str(row.get("source") or row.get("manifest_source") or "integration_registry"),
                auth_kind=str(row.get("auth_kind") or auth_shape.get("kind") or "unknown"),
            )
        ],
        "credential_health_refs": [
            ref.as_dict()
            for ref in _credential_refs_from_auth_shape(auth_shape, status=credential_status)
        ],
        "metadata": {
            "registry_source": row.get("source") or row.get("manifest_source"),
            "health": row.get("health"),
        },
    }
    return connector_record_from_payload(census_id=census_id, payload=payload)


def summarize_system_census(record: SystemCensusRecord) -> dict[str, Any]:
    surface_counts = {"capability": 0, "object": 0, "api": 0, "event": 0}
    credential_statuses: dict[str, int] = {}
    automation_counts = {"automation_bearing": 0, "observe_only": 0, "unknown": 0}
    connector_summaries: list[dict[str, Any]] = []

    for connector in record.connectors:
        connector_counts = connector.counts()
        for key, value in connector_counts.items():
            surface_counts[key] += value
        automation_counts[connector.automation_classification] = (
            automation_counts.get(connector.automation_classification, 0) + 1
        )
        for credential in connector.credential_health_refs:
            credential_statuses[credential.status] = credential_statuses.get(credential.status, 0) + 1
        connector_summaries.append(
            {
                "connector_census_id": connector.connector_census_id,
                "connector_slug": connector.connector_slug,
                "automation_classification": connector.automation_classification,
                "capability_count": len(connector.capabilities),
                "surface_counts": connector_counts,
                "credential_ref_count": len(connector.credential_health_refs),
            }
        )

    return {
        "system": {
            "census_id": record.census_id,
            "tenant_ref": record.tenant_ref,
            "workspace_ref": record.workspace_ref,
            "system_slug": record.system_slug,
            "system_name": record.system_name,
            "category": record.category,
            "deployment_model": record.deployment_model,
            "environment": record.environment,
            "discovery_status": record.discovery_status,
            "evidence_hash": record.evidence_hash(),
        },
        "counts": {
            "connectors": len(record.connectors),
            "integrations": len(record.integrations),
            "surfaces": surface_counts,
            "credential_refs": sum(credential_statuses.values()),
            "automation": automation_counts,
        },
        "credential_statuses": credential_statuses,
        "connectors": connector_summaries,
    }


def validate_system_census(record: SystemCensusRecord) -> DiscoveryValidationReport:
    gaps: list[DiscoveryGap] = []
    if not record.connectors:
        gaps.append(
            _gap(
                gap_kind="missing_connector",
                reason_code="client_system.connector.missing",
                source_ref=f"census:{record.census_id}",
                detail=f"No connector evidence captured for {record.system_slug}",
                severity="critical",
                is_blocker=True,
                expected_evidence="At least one native, managed, custom, file, or manual connector record.",
                current_evidence="connector_count=0",
                next_action="Register a connector or open a scoped missing-access gap.",
                owner=record.technical_owner,
                legal_repair_actions=["register_connector", "record_missing_access_gap"],
            )
        )

    if record.environment == "unknown":
        gaps.append(
            _gap(
                gap_kind="environment_ambiguity",
                reason_code="client_system.environment.unknown",
                source_ref=f"census:{record.census_id}",
                detail=f"Environment is unknown for {record.system_slug}",
                severity="medium",
                expected_evidence="prod, sandbox, staging, mixed, or explicit unknown confirmation.",
                current_evidence="environment=unknown",
                next_action="Ask the client technical owner to confirm the environment boundary.",
                owner=record.technical_owner,
                legal_repair_actions=["confirm_environment_scope"],
            )
        )

    for connector in record.connectors:
        gaps.extend(_validate_connector(record, connector))

    return DiscoveryValidationReport(
        census_id=record.census_id,
        ok=not any(gap.is_blocker or gap.severity == "critical" for gap in gaps),
        summary=summarize_system_census(record),
        gaps=gaps,
    )


def _validate_connector(record: SystemCensusRecord, connector: ConnectorCensusRecord) -> list[DiscoveryGap]:
    gaps: list[DiscoveryGap] = []
    source_ref = f"connector:{connector.connector_census_id}"
    counts = connector.counts()
    owner = record.technical_owner

    if not connector.capabilities and counts["capability"] == 0:
        gaps.append(
            _gap(
                gap_kind="missing_capability",
                reason_code="connector.capability.missing",
                source_ref=source_ref,
                detail=f"No capability evidence captured for connector {connector.connector_slug}",
                severity="high",
                is_blocker=True,
                expected_evidence="Read/write/search/bulk/subscribe capability evidence or explicit unsupported evidence.",
                current_evidence="capability_count=0",
                next_action="Inspect manifest, registry, vendor metadata, or admin export for connector capabilities.",
                owner=owner,
                legal_repair_actions=["inspect_integration_manifest", "query_connector_registry"],
            )
        )

    if counts["object"] == 0:
        gaps.append(
            _gap(
                gap_kind="missing_object_surface",
                reason_code="connector.object_surface.missing",
                source_ref=source_ref,
                detail=f"No object surface evidence captured for connector {connector.connector_slug}",
                severity="medium",
                expected_evidence="Object/entity/file/message catalog evidence with key and cursor fields when available.",
                current_evidence="object_surface_count=0",
                next_action="Run metadata/schema discovery or record an unknown object-model gap.",
                owner=owner,
                legal_repair_actions=["discover_object_catalog", "record_unknown_object_model_gap"],
            )
        )

    if counts["api"] == 0:
        gaps.append(
            _gap(
                gap_kind="unknown_rate_limit",
                reason_code="connector.api_surface.missing",
                source_ref=source_ref,
                detail=f"No API surface evidence captured for connector {connector.connector_slug}",
                severity="medium",
                expected_evidence="API style, pagination, filtering, timeout, quota, and rate-limit evidence.",
                current_evidence="api_surface_count=0",
                next_action="Inspect vendor API metadata or client-enabled connector settings.",
                owner=owner,
                legal_repair_actions=["inspect_api_docs", "record_api_surface"],
            )
        )

    if connector.automation_classification == "automation_bearing" and counts["event"] == 0:
        gaps.append(
            _gap(
                gap_kind="unknown_event_surface",
                reason_code="connector.event_surface.unknown",
                source_ref=source_ref,
                detail=f"Automation-bearing connector {connector.connector_slug} has no event surface evidence",
                severity="high",
                expected_evidence="Webhook, polling, stream, audit log, CDC, or explicit no-event-support evidence.",
                current_evidence="event_surface_count=0",
                next_action="Discover webhook/audit/polling support before downstream automation planning.",
                owner=owner,
                legal_repair_actions=["discover_event_surface", "record_polling_fallback"],
            )
        )

    if not connector.credential_health_refs:
        gaps.append(
            _gap(
                gap_kind="credential_health_unknown",
                reason_code="credential.health.missing_reference",
                source_ref=source_ref,
                detail=f"No credential health reference captured for connector {connector.connector_slug}",
                severity="high",
                is_blocker=True,
                expected_evidence="Opaque credential ref or env var ref plus validation status; never raw secret material.",
                current_evidence="credential_ref_count=0",
                next_action="Record credential reference and read-only probe status.",
                owner=owner,
                legal_repair_actions=["record_credential_health_ref"],
            )
        )
    for credential in connector.credential_health_refs:
        if credential.status != "valid":
            gaps.append(_credential_gap(source_ref=source_ref, owner=owner, credential=credential))

    return gaps


def _credential_gap(
    *,
    source_ref: str,
    owner: str | None,
    credential: CredentialHealthRef,
) -> DiscoveryGap:
    if credential.status == "missing_scope":
        gap_kind = "insufficient_scope"
        reason_code = "credential.scope.missing"
        detail = "Credential is present but lacks required read-only discovery scope"
    elif credential.status in {"expired", "revoked", "error"}:
        gap_kind = "invalid_credential"
        reason_code = f"credential.{credential.status}"
        detail = f"Credential health status is {credential.status}"
    elif credential.status == "missing":
        gap_kind = "missing_access"
        reason_code = "credential.missing"
        detail = "Credential reference is missing"
    else:
        gap_kind = "credential_health_unknown"
        reason_code = f"credential.{credential.status}"
        detail = f"Credential health status is {credential.status}"
    return _gap(
        gap_kind=gap_kind,
        reason_code=reason_code,
        source_ref=source_ref,
        detail=detail,
        severity="high",
        is_blocker=True,
        expected_evidence="valid credential health from a safe read-only probe.",
        current_evidence=f"status={credential.status}",
        next_action="Validate or remediate the credential through the approved credential authority.",
        owner=owner,
        legal_repair_actions=["validate_credential_ref", "request_scoped_access"],
        context={"credential_ref": credential.credential_ref, "env_var_ref": credential.env_var_ref},
    )


def _gap(
    *,
    gap_kind: DiscoveryGapKind,
    reason_code: str,
    source_ref: str,
    detail: str,
    severity: GapSeverity,
    is_blocker: bool = False,
    expected_evidence: str | None = None,
    current_evidence: str | None = None,
    next_action: str | None = None,
    owner: str | None = None,
    legal_repair_actions: list[str] | None = None,
    context: dict[str, Any] | None = None,
) -> DiscoveryGap:
    return DiscoveryGap(
        gap_kind=gap_kind,
        reason_code=reason_code,
        source_ref=source_ref,
        detail=detail,
        severity=severity,
        is_blocker=is_blocker,
        expected_evidence=expected_evidence,
        current_evidence=current_evidence,
        next_action=next_action,
        owner=owner,
        legal_repair_actions=list(legal_repair_actions or []),
        context=dict(context or {}),
    )


def _surfaces_from_capabilities(
    *,
    connector_ref: str,
    capabilities: list[dict[str, Any]],
    evidence_source: str,
    auth_kind: str,
) -> list[ConnectorSurfaceEvidence]:
    surfaces: list[ConnectorSurfaceEvidence] = []
    for capability in capabilities:
        action = str(capability.get("action") or "").strip()
        if not action:
            continue
        surface_base = f"{connector_ref}.{action}" if connector_ref else action
        surfaces.append(
            capability_surface(
                surface_ref=surface_base,
                operation_name=action,
                evidence={
                    "source": evidence_source,
                    "description": capability.get("description", ""),
                    "status": "declared",
                },
            )
        )
        method = str(capability.get("method") or "").strip().upper()
        path = str(capability.get("path") or "").strip()
        if method or path:
            surfaces.append(
                api_surface(
                    surface_ref=f"{surface_base}.api",
                    operation_name=action,
                    http_method=method or "UNKNOWN",
                    path_template=path,
                    evidence={
                        "source": evidence_source,
                        "auth_kind": auth_kind,
                        "api_style": capability.get("api_style", "rest"),
                        "pagination_model": capability.get("pagination_model", "unknown"),
                        "rate_limit_model": capability.get("rate_limit_model", "unknown"),
                        "bulk_support": capability.get("bulk_support", "unknown"),
                    },
                )
            )
        object_name = str(capability.get("object_name") or capability.get("object") or "").strip()
        if object_name:
            surfaces.append(
                object_surface(
                    surface_ref=f"{surface_base}.object.{object_name}",
                    object_name=object_name,
                    evidence={
                        "source": evidence_source,
                        "read_capability": capability.get("read_capability", "unknown"),
                        "write_capability": capability.get("write_capability", "unknown"),
                        "search_capability": capability.get("search_capability", "unknown"),
                        "subscribe_capability": capability.get("subscribe_capability", "unknown"),
                        "key_fields": capability.get("key_fields", []),
                        "cursor_field": capability.get("cursor_field"),
                    },
                )
            )
        event_name = str(capability.get("event_name") or capability.get("event") or "").strip()
        if event_name:
            surfaces.append(
                event_surface(
                    surface_ref=f"{surface_base}.event.{event_name}",
                    event_name=event_name,
                    evidence={
                        "source": evidence_source,
                        "surface_type": capability.get("event_surface_type", "unknown"),
                        "delivery_semantics": capability.get("delivery_semantics", "unknown"),
                        "replay_support": capability.get("replay_support", "unknown"),
                        "direction": capability.get("event_direction", "unknown"),
                    },
                )
            )
    return surfaces


def _credential_refs_from_auth_shape(
    auth_shape: dict[str, Any],
    *,
    status: CredentialStatus,
) -> list[CredentialHealthRef]:
    credential_ref = str(auth_shape.get("credential_ref") or "").strip() or None
    env_var_ref = str(auth_shape.get("env_var") or auth_shape.get("env_var_ref") or "").strip() or None
    if not credential_ref and not env_var_ref:
        return []
    return [
        CredentialHealthRef(
            credential_ref=credential_ref,
            env_var_ref=env_var_ref,
            status=status,
            metadata={
                "auth_kind": auth_shape.get("kind", "unknown"),
                "scope_summary": sorted(str(item) for item in (auth_shape.get("scopes") or [])),
            },
        )
    ]


def _auth_shape_as_dict(auth_shape: Any) -> dict[str, Any]:
    parsed = _parse_jsonish(auth_shape)
    if isinstance(parsed, dict):
        return dict(parsed)
    if parsed is None:
        return {}
    return {
        "kind": getattr(parsed, "kind", "unknown"),
        "credential_ref": getattr(parsed, "credential_ref", ""),
        "env_var": getattr(parsed, "env_var", ""),
        "scopes": list(getattr(parsed, "scopes", ()) or ()),
        "token_url": getattr(parsed, "token_url", ""),
        "authorize_url": getattr(parsed, "authorize_url", ""),
    }
