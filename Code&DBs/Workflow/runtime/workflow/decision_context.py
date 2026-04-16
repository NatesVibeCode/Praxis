"""Scoped decision context for workflow execution bundles.

This module resolves active architecture-policy rows from operator authority
and packages them into a job-local decision pack. Workflow jobs use that pack
for prompt injection and workspace overlays so decisions are visible before a
model starts changing code.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any


_ARCHITECTURE_POLICY_PATH_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "sandbox_execution",
        (
            "runtime/sandbox_runtime.py",
            "runtime/execution_transport.py",
            "runtime/workflow/execution_backends.py",
            "runtime/workflow/mcp_bridge.py",
            "registry/agent_config.py",
            "registry/sandbox_profile_authority.py",
        ),
    ),
    (
        "embedding_runtime",
        (
            "runtime/embedding",
            "runtime/database_maintenance.py",
            "runtime/embedding_backend_server.py",
            "surfaces/mcp/invocation.py",
            "surfaces/_subsystems_base.py",
        ),
    ),
    (
        "compile_authority",
        (
            "runtime/compile",
            "runtime/execution_packet_authority.py",
            "runtime/receipt_store.py",
            "storage/migrations.py",
            "storage/postgres/receipt_repository.py",
            "storage/postgres/connection.py",
        ),
    ),
    (
        "decision_tables",
        (
            "authority/operator_control.py",
            "storage/postgres/operator_control_repository.py",
            "surfaces/api/operator_write.py",
            "surfaces/mcp/tools/operator.py",
            "surfaces/cli/native_operator.py",
            "docs/ARCHITECTURE.md",
        ),
    ),
)


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    items: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            items.append(text)
    return items


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _normalize_domain(value: object) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")


def normalize_authority_domains(value: object) -> list[str]:
    return _dedupe(
        [
            normalized
            for normalized in (_normalize_domain(item) for item in _string_list(value))
            if normalized
        ]
    )


def _path_variants(path: str) -> tuple[str, ...]:
    normalized = path.replace("\\", "/").strip()
    normalized = normalized.lstrip("./")
    if not normalized:
        return ()
    variants = [normalized]
    marker = "Code&DBs/Workflow/"
    if marker in normalized:
        variants.append(normalized.split(marker, 1)[1])
    return tuple(_dedupe(variants))


def _pattern_matches_path(*, pattern: str, path: str) -> bool:
    return path == pattern or path.startswith(f"{pattern}/") or path.startswith(pattern)


def infer_authority_domains_from_paths(paths: Sequence[str]) -> list[str]:
    matches: list[str] = []
    for raw_path in paths:
        for variant in _path_variants(str(raw_path or "")):
            for authority_domain, patterns in _ARCHITECTURE_POLICY_PATH_RULES:
                if any(_pattern_matches_path(pattern=pattern, path=variant) for pattern in patterns):
                    matches.append(authority_domain)
    return _dedupe(matches)


def explicit_authority_domains_for_job(
    *,
    job: Mapping[str, Any] | None,
    spec_snapshot: Mapping[str, Any] | None = None,
) -> list[str]:
    raw_job = dict(job or {})
    raw_snapshot = dict(spec_snapshot or {})
    explicit: list[str] = []
    for source in (raw_snapshot, raw_job):
        explicit.extend(normalize_authority_domains(source.get("authority_domains")))
        explicit.extend(normalize_authority_domains(source.get("decision_authority_domains")))
        explicit.extend(normalize_authority_domains(source.get("decision_scope_refs")))
        scope = source.get("scope")
        if isinstance(scope, Mapping):
            explicit.extend(normalize_authority_domains(scope.get("authority_domains")))
    return _dedupe(explicit)


def resolve_job_decision_pack(
    conn,
    *,
    write_scope: Sequence[str] | None = None,
    declared_read_scope: Sequence[str] | None = None,
    resolved_read_scope: Sequence[str] | None = None,
    blast_radius: Sequence[str] | None = None,
    explicit_authority_domains: Sequence[str] | None = None,
) -> dict[str, Any]:
    path_domains = infer_authority_domains_from_paths(
        [
            *_string_list(write_scope),
            *_string_list(declared_read_scope),
            *_string_list(resolved_read_scope),
            *_string_list(blast_radius),
        ]
    )
    normalized_explicit = normalize_authority_domains(explicit_authority_domains)
    authority_domains = _dedupe([*normalized_explicit, *path_domains])
    pack: dict[str, Any] = {
        "pack_version": 1,
        "authority_domains": authority_domains,
        "decision_keys": [],
        "decisions": [],
        "scope_evidence": {
            "explicit_authority_domains": normalized_explicit,
            "inferred_authority_domains": path_domains,
        },
    }
    if not authority_domains:
        return pack

    rows = conn.execute(
        """
        SELECT
            operator_decision_id,
            decision_key,
            decision_kind,
            decision_status,
            title,
            rationale,
            decided_by,
            decision_source,
            effective_from,
            effective_to,
            decided_at,
            updated_at,
            decision_scope_kind,
            decision_scope_ref
        FROM operator_decisions
        WHERE decision_kind = 'architecture_policy'
          AND decision_scope_kind = 'authority_domain'
          AND decision_scope_ref = ANY($1::text[])
          AND effective_from <= now()
          AND (effective_to IS NULL OR effective_to > now())
        ORDER BY decision_scope_ref, effective_from DESC, decided_at DESC, updated_at DESC, operator_decision_id
        """,
        authority_domains,
    )

    decisions: list[dict[str, Any]] = []
    for row in rows or []:
        record = {
            "operator_decision_id": str(row.get("operator_decision_id") or "").strip(),
            "decision_key": str(row.get("decision_key") or "").strip(),
            "decision_kind": str(row.get("decision_kind") or "").strip(),
            "decision_status": str(row.get("decision_status") or "").strip(),
            "title": str(row.get("title") or "").strip(),
            "rationale": str(row.get("rationale") or "").strip(),
            "decided_by": str(row.get("decided_by") or "").strip(),
            "decision_source": str(row.get("decision_source") or "").strip(),
            "effective_from": str(row.get("effective_from") or ""),
            "effective_to": str(row.get("effective_to") or ""),
            "decided_at": str(row.get("decided_at") or ""),
            "updated_at": str(row.get("updated_at") or ""),
            "decision_scope_kind": str(row.get("decision_scope_kind") or "").strip(),
            "decision_scope_ref": str(row.get("decision_scope_ref") or "").strip(),
        }
        if not record["decision_key"]:
            continue
        decisions.append(record)
    decisions.sort(
        key=lambda record: (
            record["decision_scope_ref"],
            record["decision_key"],
            record["operator_decision_id"],
        )
    )
    pack["decisions"] = decisions
    pack["decision_keys"] = [record["decision_key"] for record in decisions]
    return pack


def render_decision_pack(decision_pack: Mapping[str, Any] | None) -> str:
    if not isinstance(decision_pack, Mapping):
        return ""
    decisions = decision_pack.get("decisions")
    if not isinstance(decisions, list) or not decisions:
        return ""
    authority_domains = normalize_authority_domains(decision_pack.get("authority_domains"))
    parts = ["** APPLICABLE DECISIONS **", "Treat these as active constraints for this job."]
    if authority_domains:
        parts.append("authority_domains: " + ", ".join(authority_domains))
    for decision in decisions:
        if not isinstance(decision, Mapping):
            continue
        decision_key = str(decision.get("decision_key") or "").strip()
        title = str(decision.get("title") or "").strip()
        rationale = str(decision.get("rationale") or "").strip()
        scope_ref = str(decision.get("decision_scope_ref") or "").strip()
        heading = " - ".join(
            item
            for item in (f"[{scope_ref}]" if scope_ref else "", decision_key, title)
            if item
        )
        if heading:
            parts.append(f"- {heading}")
        if rationale:
            parts.append(rationale)
    return "\n".join(parts)


def decision_workspace_overlays(execution_bundle: Mapping[str, object] | None) -> list[dict[str, str]]:
    if not isinstance(execution_bundle, Mapping):
        return []
    decision_pack = execution_bundle.get("decision_pack")
    if not isinstance(decision_pack, Mapping) or not decision_pack.get("decisions"):
        return []
    rendered = render_decision_pack(decision_pack)
    return [
        {
            "relative_path": "_context/decision_pack.json",
            "content": json.dumps(dict(decision_pack), sort_keys=True, indent=2, default=str),
        },
        {
            "relative_path": "_context/decision_summary.md",
            "content": rendered + "\n" if rendered else "",
        },
    ]


__all__ = [
    "decision_workspace_overlays",
    "explicit_authority_domains_for_job",
    "infer_authority_domains_from_paths",
    "normalize_authority_domains",
    "render_decision_pack",
    "resolve_job_decision_pack",
]
