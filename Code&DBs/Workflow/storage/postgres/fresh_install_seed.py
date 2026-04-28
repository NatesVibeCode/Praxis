"""Fresh-install authority seed for the workflow database.

Schema bootstrap owns structure. This module owns the idempotent post-schema
authority rows that make a fresh clone runnable without relying on a private
operator database.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class FreshInstallSeedError(RuntimeError):
    """Raised when fresh-install authority rows cannot be seeded safely."""

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


@dataclass(frozen=True, slots=True)
class FreshInstallSeedSummary:
    """Machine-readable summary of rows managed by the fresh-install seed."""

    runtime_profiles: tuple[str, ...]
    sandbox_profiles: tuple[str, ...]
    operator_decisions: tuple[str, ...]
    functional_areas: tuple[str, ...]
    workflow_definitions: tuple[str, ...]
    synced_runtime_profiles: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "runtime_profiles": list(self.runtime_profiles),
            "sandbox_profiles": list(self.sandbox_profiles),
            "operator_decisions": list(self.operator_decisions),
            "functional_areas": list(self.functional_areas),
            "workflow_definitions": list(self.workflow_definitions),
            "synced_runtime_profiles": list(self.synced_runtime_profiles),
        }


_DEFAULT_RUNTIME_PROFILES_CONFIG = "config/runtime_profiles.json"
_DEFAULT_AUTHORITY_KEY = "default"
_FRESH_INSTALL_DECIDED_AT = datetime(2026, 4, 20, tzinfo=timezone.utc)
_SMOKE_WORKFLOW_DEFINITION_ID = "workflow_definition.native_self_hosted_smoke.v1"
_SMOKE_WORKFLOW_ID = "workflow.native-self-hosted-smoke"
_SMOKE_REQUEST_ID = "request.native-self-hosted-smoke"
_SMOKE_DEFINITION_HASH = "definition.native_self_hosted_smoke.v1"
_SMOKE_CREATED_AT = datetime(2026, 4, 16, tzinfo=timezone.utc)

_PUBLIC_OPERATOR_DECISIONS: tuple[dict[str, str], ...] = (
    {
        "operator_decision_id": (
            "operator_decision.architecture_policy.orient."
            "mandatory_authority_envelope"
        ),
        "decision_key": "architecture-policy::orient::mandatory-authority-envelope",
        "title": "Orient is the mandatory runtime authority envelope",
        "rationale": (
            "Fresh operator sessions must begin from the runtime authority "
            "envelope instead of sidecar docs. The envelope binds standing "
            "orders, workspace boundary, database authority, health, and live "
            "operator surfaces so future runs have one queryable source of truth."
        ),
        "decision_scope_ref": "orient",
    },
    {
        "operator_decision_id": (
            "operator_decision.architecture_policy.public_naming."
            "workflow_vocabulary_convention"
        ),
        "decision_key": (
            "architecture-policy::public-naming::workflow-vocabulary-convention"
        ),
        "title": "Public vocabulary uses Praxis, Praxis Engine, Praxis.db, workflow, and launch",
        "rationale": (
            "Public docs and surfaces should use Praxis for the product, "
            "Praxis Engine for the runtime, Praxis.db for durable authority, "
            "workflow for executable work, and launch for starting the system."
        ),
        "decision_scope_ref": "public_naming",
    },
    {
        "operator_decision_id": (
            "operator_decision.architecture_policy.test_isolation."
            "bug_surface_writes_require_rollback_isolation"
        ),
        "decision_key": (
            "architecture-policy::test-isolation::"
            "bug-surface-writes-require-rollback-isolation"
        ),
        "title": "Bug-surface test writes require rollback isolation",
        "rationale": (
            "Tests that exercise bug authority writes must run inside explicit "
            "rollback isolation. Durable test rows pollute operator queues and "
            "turn test fixtures into false production state."
        ),
        "decision_scope_ref": "test_isolation",
    },
    {
        "operator_decision_id": (
            "operator_decision.architecture_policy.decision_hygiene."
            "file_decisions_inline"
        ),
        "decision_key": "architecture-policy::decision-hygiene::file-decisions-inline",
        "title": "File durable decisions through operator authority as they are made",
        "rationale": (
            "Architecture decisions made in conversation must become DB-backed "
            "operator records immediately. Chat-only or doc-only decisions are "
            "not durable authority for future runs."
        ),
        "decision_scope_ref": "decision_hygiene",
    },
)

_PUBLIC_FUNCTIONAL_AREAS: tuple[dict[str, str], ...] = (
    {
        "area_slug": "compiler",
        "title": "Spec Compiler",
        "summary": "Normalizes workflow specs, validates topology, and lowers definitions into executable graph contracts.",
    },
    {
        "area_slug": "scheduler",
        "title": "Topological Scheduler",
        "summary": "Coordinates workflow runs, run nodes, leases, schedules, and recovery of executable work.",
    },
    {
        "area_slug": "sandbox",
        "title": "Sandbox Runtime",
        "summary": "Owns isolated execution sessions, execution manifests, workspace materialization, and runtime boundaries.",
    },
    {
        "area_slug": "routing",
        "title": "Provider Routing and Economics",
        "summary": "Resolves provider and model routes using model profiles, policies, eligibility, health, and cost signals.",
    },
    {
        "area_slug": "circuits",
        "title": "Circuit Breakers and Health",
        "summary": "Tracks quality, health, gates, failure categories, and route admission posture.",
    },
    {
        "area_slug": "outbox",
        "title": "Outbox and Projections",
        "summary": "Publishes durable events and projection state through outbox, event log, and subscription checkpoints.",
    },
    {
        "area_slug": "receipts",
        "title": "Receipts and Provenance",
        "summary": "Stores execution evidence, receipt search data, provenance, and probe receipts.",
    },
    {
        "area_slug": "memory",
        "title": "Knowledge Graph and Memory",
        "summary": "Maintains memory entities, memory edges, semantic assertions, context bundles, and reference catalog context.",
    },
    {
        "area_slug": "bugs",
        "title": "Bug Tracker and Evidence Bridge",
        "summary": "Owns bug and issue authority, evidence links, and operational defect tracking.",
    },
    {
        "area_slug": "roadmap",
        "title": "Roadmap and Closeout",
        "summary": "Owns roadmap items, dependencies, work item bindings, and cutover planning.",
    },
    {
        "area_slug": "authority",
        "title": "Operator Authority",
        "summary": "Owns operator decisions, object relations, authority checkpoints, objects, and registry authority rows.",
    },
    {
        "area_slug": "build",
        "title": "Build Planning and Review",
        "summary": "Owns build intents, candidate manifests, review sessions, review decisions, and app manifests.",
    },
    {
        "area_slug": "governance",
        "title": "Governance and Posture",
        "summary": "Owns credentials, policy, promotion decisions, verification registries, and posture controls.",
    },
    {
        "area_slug": "heal",
        "title": "Self-Healing and Retry",
        "summary": "Coordinates healing runs, repair loops, retry posture, and verifier-to-healer bindings.",
    },
    {
        "area_slug": "discover",
        "title": "Semantic Discovery",
        "summary": "Owns discovery indexes, compile context, search surfaces, and semantic retrieval support.",
    },
    {
        "area_slug": "debate",
        "title": "Adversarial Debate",
        "summary": "Owns debate frames, debate metrics, and structured adversarial review workflows.",
    },
    {
        "area_slug": "moon",
        "title": "Moon UI and Visual DAG",
        "summary": "Owns the React control plane, visual DAG authoring, and operator-facing workflow surfaces.",
    },
    {
        "area_slug": "mcp",
        "title": "MCP Surface Catalog",
        "summary": "Owns catalog-backed MCP tool exports, operation schemas, and tool surface metadata.",
    },
    {
        "area_slug": "cli",
        "title": "CLI Frontdoor",
        "summary": "Owns command-line operator surfaces, renderers, native operator commands, and CLI sessions.",
    },
    {
        "area_slug": "integrations",
        "title": "Integration Registry",
        "summary": "Owns connector registry, webhook intake, OAuth tokens, and external integration schemas.",
    },
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _runtime_profiles_path(repo_root: Path) -> Path:
    return repo_root / _DEFAULT_RUNTIME_PROFILES_CONFIG


def _load_runtime_profiles_config(repo_root: Path) -> dict[str, Any]:
    config_path = _runtime_profiles_path(repo_root)
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FreshInstallSeedError(
            "fresh_install_seed.config_missing",
            "fresh-install runtime profile config is missing",
            details={"path": str(config_path)},
        ) from exc
    except json.JSONDecodeError as exc:
        raise FreshInstallSeedError(
            "fresh_install_seed.config_invalid_json",
            "fresh-install runtime profile config is not valid JSON",
            details={"path": str(config_path), "error": str(exc)},
        ) from exc
    if not isinstance(payload, dict):
        raise FreshInstallSeedError(
            "fresh_install_seed.config_invalid",
            "fresh-install runtime profile config must be a JSON object",
            details={"path": str(config_path)},
        )
    return payload


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FreshInstallSeedError(
            "fresh_install_seed.config_invalid",
            f"{field_name} must be an object",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FreshInstallSeedError(
            "fresh_install_seed.config_invalid",
            f"{field_name} must be a non-empty string",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_text_array(value: object, *, field_name: str) -> str:
    if not isinstance(value, list) or not value:
        raise FreshInstallSeedError(
            "fresh_install_seed.config_invalid",
            f"{field_name} must be a non-empty array",
            details={"field_name": field_name, "value_type": type(value).__name__},
        )
    result: list[str] = []
    for index, item in enumerate(value):
        result.append(_require_text(item, field_name=f"{field_name}[{index}]"))
    return json.dumps(list(dict.fromkeys(result)))


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _native_smoke_node(
    *,
    node_id: str,
    display_name: str,
    step: int,
    expected_result: str,
    position_index: int,
) -> dict[str, Any]:
    return {
        "workflow_definition_node_id": (
            f"{_SMOKE_WORKFLOW_DEFINITION_ID}:{node_id}"
        ),
        "workflow_definition_id": _SMOKE_WORKFLOW_DEFINITION_ID,
        "node_id": node_id,
        "node_type": "deterministic_task",
        "schema_version": 1,
        "adapter_type": "deterministic_task",
        "display_name": display_name,
        "inputs": {
            "task_name": display_name,
            "input_payload": {
                "step": step,
                "allow_passthrough_echo": True,
            },
        },
        "expected_outputs": {"result": expected_result},
        "success_condition": {"kind": "always"},
        "failure_behavior": {"kind": "stop"},
        "authority_requirements": {
            "workspace_ref": "praxis",
            "runtime_profile_ref": "praxis",
        },
        "execution_boundary": {"workspace_ref": "praxis"},
        "position_index": position_index,
    }


def _native_smoke_edge() -> dict[str, Any]:
    return {
        "workflow_definition_edge_id": (
            f"{_SMOKE_WORKFLOW_DEFINITION_ID}:edge_0"
        ),
        "workflow_definition_id": _SMOKE_WORKFLOW_DEFINITION_ID,
        "edge_id": "edge_0",
        "edge_type": "after_success",
        "schema_version": 1,
        "from_node_id": "node_0",
        "to_node_id": "node_1",
        "release_condition": {"kind": "always"},
        "payload_mapping": {"prepared_result": "result"},
        "position_index": 0,
    }


def _native_smoke_request_envelope() -> dict[str, Any]:
    nodes = [
        _native_smoke_node(
            node_id="node_0",
            display_name="prepare",
            step=0,
            expected_result="prepared",
            position_index=0,
        ),
        _native_smoke_node(
            node_id="node_1",
            display_name="persist",
            step=1,
            expected_result="persisted",
            position_index=1,
        ),
    ]
    return {
        "schema_version": 1,
        "workflow_id": _SMOKE_WORKFLOW_ID,
        "request_id": _SMOKE_REQUEST_ID,
        "workflow_definition_id": _SMOKE_WORKFLOW_DEFINITION_ID,
        "definition_version": 1,
        "definition_hash": _SMOKE_DEFINITION_HASH,
        "workspace_ref": "praxis",
        "runtime_profile_ref": "praxis",
        "nodes": nodes,
        "edges": [_native_smoke_edge()],
    }


async def _seed_sandbox_profiles(
    conn: Any,
    sandbox_profiles: Mapping[str, Any],
) -> tuple[str, ...]:
    if not sandbox_profiles:
        raise FreshInstallSeedError(
            "fresh_install_seed.sandbox_profiles_missing",
            "runtime profile config must define at least one sandbox profile",
        )
    seeded: list[str] = []
    for sandbox_profile_ref, raw_profile in sandbox_profiles.items():
        profile = _require_mapping(
            raw_profile,
            field_name=f"sandbox_profiles.{sandbox_profile_ref}",
        )
        sandbox_ref = _require_text(
            sandbox_profile_ref,
            field_name="sandbox_profile_ref",
        )
        await conn.execute(
            """
            INSERT INTO registry_sandbox_profile_authority (
                sandbox_profile_ref,
                sandbox_provider,
                docker_image,
                docker_cpus,
                docker_memory,
                network_policy,
                workspace_materialization,
                secret_allowlist,
                auth_mount_policy,
                timeout_profile
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
            ON CONFLICT (sandbox_profile_ref) DO UPDATE
            SET sandbox_provider = EXCLUDED.sandbox_provider,
                docker_image = EXCLUDED.docker_image,
                docker_cpus = EXCLUDED.docker_cpus,
                docker_memory = EXCLUDED.docker_memory,
                network_policy = EXCLUDED.network_policy,
                workspace_materialization = EXCLUDED.workspace_materialization,
                secret_allowlist = EXCLUDED.secret_allowlist,
                auth_mount_policy = EXCLUDED.auth_mount_policy,
                timeout_profile = EXCLUDED.timeout_profile,
                recorded_at = now()
            """,
            sandbox_ref,
            _require_text(
                profile.get("sandbox_provider"),
                field_name=f"{sandbox_ref}.sandbox_provider",
            ),
            _optional_text(profile.get("docker_image")),
            _optional_text(profile.get("docker_cpus")),
            _optional_text(profile.get("docker_memory")),
            _require_text(
                profile.get("network_policy"),
                field_name=f"{sandbox_ref}.network_policy",
            ),
            _require_text(
                profile.get("workspace_materialization"),
                field_name=f"{sandbox_ref}.workspace_materialization",
            ),
            _json_text_array(
                profile.get("secret_allowlist", []),
                field_name=f"{sandbox_ref}.secret_allowlist",
            ),
            _require_text(
                profile.get("auth_mount_policy", "provider_scoped"),
                field_name=f"{sandbox_ref}.auth_mount_policy",
            ),
            _require_text(
                profile.get("timeout_profile", "default"),
                field_name=f"{sandbox_ref}.timeout_profile",
            ),
        )
        seeded.append(sandbox_ref)
    return tuple(seeded)


def _bootstrap_provider_projection(profile: Mapping[str, Any]) -> Mapping[str, Any]:
    """Seed-only provider projection for legacy native profile rows."""

    projection = profile.get("bootstrap_seed_projection")
    if projection is None:
        return profile
    return _require_mapping(
        projection,
        field_name="runtime_profile.bootstrap_seed_projection",
    )


async def _seed_runtime_profiles(
    conn: Any,
    config: Mapping[str, Any],
) -> tuple[str, ...]:
    runtime_profiles = _require_mapping(
        config.get("runtime_profiles"),
        field_name="runtime_profiles",
    )
    if not runtime_profiles:
        raise FreshInstallSeedError(
            "fresh_install_seed.runtime_profiles_missing",
            "runtime profile config must define at least one runtime profile",
        )
    seeded: list[str] = []
    for runtime_profile_ref, raw_profile in runtime_profiles.items():
        profile = _require_mapping(
            raw_profile,
            field_name=f"runtime_profiles.{runtime_profile_ref}",
        )
        profile_ref = _require_text(
            runtime_profile_ref,
            field_name="runtime_profile_ref",
        )
        workspace_ref = _require_text(
            profile.get("workspace_ref", profile_ref),
            field_name=f"{profile_ref}.workspace_ref",
        )
        await conn.execute(
            """
            INSERT INTO registry_workspace_authority (
                workspace_ref,
                repo_root,
                workdir
            ) VALUES ($1, $2, $3)
            ON CONFLICT (workspace_ref) DO UPDATE
            SET repo_root = EXCLUDED.repo_root,
                workdir = EXCLUDED.workdir,
                recorded_at = now()
            """,
            workspace_ref,
            _require_text(profile.get("repo_root"), field_name=f"{profile_ref}.repo_root"),
            _require_text(profile.get("workdir"), field_name=f"{profile_ref}.workdir"),
        )
        await conn.execute(
            """
            INSERT INTO registry_runtime_profile_authority (
                runtime_profile_ref,
                model_profile_id,
                provider_policy_id,
                sandbox_profile_ref
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT (runtime_profile_ref) DO UPDATE
            SET model_profile_id = EXCLUDED.model_profile_id,
                provider_policy_id = EXCLUDED.provider_policy_id,
                sandbox_profile_ref = EXCLUDED.sandbox_profile_ref,
                recorded_at = now()
            """,
            profile_ref,
            _require_text(
                profile.get("model_profile_id"),
                field_name=f"{profile_ref}.model_profile_id",
            ),
            _require_text(
                profile.get("provider_policy_id"),
                field_name=f"{profile_ref}.provider_policy_id",
            ),
            _require_text(
                profile.get("sandbox_profile_ref"),
                field_name=f"{profile_ref}.sandbox_profile_ref",
            ),
        )
        await conn.execute(
            """
            INSERT INTO registry_native_runtime_profile_authority (
                runtime_profile_ref,
                workspace_ref,
                instance_name,
                provider_name,
                provider_names,
                allowed_models,
                receipts_dir,
                topology_dir
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8)
            ON CONFLICT (runtime_profile_ref) DO UPDATE
            SET workspace_ref = EXCLUDED.workspace_ref,
                instance_name = EXCLUDED.instance_name,
                provider_name = EXCLUDED.provider_name,
                provider_names = EXCLUDED.provider_names,
                allowed_models = EXCLUDED.allowed_models,
                receipts_dir = EXCLUDED.receipts_dir,
                topology_dir = EXCLUDED.topology_dir,
                recorded_at = now()
            """,
            profile_ref,
            workspace_ref,
            _require_text(
                profile.get("instance_name"),
                field_name=f"{profile_ref}.instance_name",
            ),
            _require_text(
                _bootstrap_provider_projection(profile).get("provider_name"),
                field_name=f"{profile_ref}.bootstrap_seed_projection.provider_name",
            ),
            _json_text_array(
                _bootstrap_provider_projection(profile).get("provider_names"),
                field_name=f"{profile_ref}.bootstrap_seed_projection.provider_names",
            ),
            _json_text_array(
                _bootstrap_provider_projection(profile).get("allowed_models"),
                field_name=f"{profile_ref}.bootstrap_seed_projection.allowed_models",
            ),
            _require_text(profile.get("receipts_dir"), field_name=f"{profile_ref}.receipts_dir"),
            _require_text(profile.get("topology_dir"), field_name=f"{profile_ref}.topology_dir"),
        )
        seeded.append(profile_ref)

    default_runtime_profile = _require_text(
        config.get("default_runtime_profile"),
        field_name="default_runtime_profile",
    )
    await conn.execute(
        """
        INSERT INTO registry_native_runtime_defaults (
            authority_key,
            runtime_profile_ref
        ) VALUES ($1, $2)
        ON CONFLICT (authority_key) DO UPDATE
        SET runtime_profile_ref = EXCLUDED.runtime_profile_ref,
            recorded_at = now()
        """,
        _DEFAULT_AUTHORITY_KEY,
        default_runtime_profile,
    )
    return tuple(seeded)


async def _seed_public_operator_decisions(conn: Any) -> tuple[str, ...]:
    seeded: list[str] = []
    for decision in _PUBLIC_OPERATOR_DECISIONS:
        await conn.execute(
            """
            INSERT INTO operator_decisions (
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
                created_at,
                updated_at,
                decision_scope_kind,
                decision_scope_ref
            ) VALUES (
                $1, $2, 'architecture_policy', 'decided', $3, $4,
                'praxis', 'fresh_install_seed', $5, NULL, $5, $5, $5,
                'authority_domain', $6
            )
            ON CONFLICT (decision_key) DO UPDATE
            SET operator_decision_id = EXCLUDED.operator_decision_id,
                decision_kind = EXCLUDED.decision_kind,
                decision_status = EXCLUDED.decision_status,
                title = EXCLUDED.title,
                rationale = EXCLUDED.rationale,
                decided_by = EXCLUDED.decided_by,
                decision_source = EXCLUDED.decision_source,
                effective_from = EXCLUDED.effective_from,
                effective_to = EXCLUDED.effective_to,
                decided_at = EXCLUDED.decided_at,
                updated_at = EXCLUDED.updated_at,
                decision_scope_kind = EXCLUDED.decision_scope_kind,
                decision_scope_ref = EXCLUDED.decision_scope_ref
            WHERE operator_decisions.decision_source = 'fresh_install_seed'
            """,
            decision["operator_decision_id"],
            decision["decision_key"],
            decision["title"],
            decision["rationale"],
            _FRESH_INSTALL_DECIDED_AT,
            decision["decision_scope_ref"],
        )
        seeded.append(decision["decision_key"])
    return tuple(seeded)


_DECISIONS_SNAPSHOT_RELPATH = "policy/operator-decisions-snapshot.json"


def _load_decisions_snapshot(repo_root: Path) -> tuple[dict[str, Any], ...]:
    """Load policy/operator-decisions-snapshot.json if present.

    The snapshot is a byte-deterministic export of operator_decisions
    architecture_policy rows produced by scripts/refresh-decisions-snapshot.sh.
    Forks that strip private operator policy can simply remove or empty the
    file; this loader returns () in that case so the seed degrades cleanly.
    """
    path = repo_root / _DECISIONS_SNAPSHOT_RELPATH
    if not path.exists():
        return ()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ()
    rows = data.get("decisions") or ()
    if not isinstance(rows, list):
        return ()
    return tuple(r for r in rows if isinstance(r, dict))


async def _seed_operator_decisions_from_snapshot(
    conn: Any,
    repo_root: Path,
) -> tuple[str, ...]:
    """Seed operator_decisions from the committed snapshot.

    Sits AFTER `_seed_public_operator_decisions` so the public-safe baseline
    always wins when both define the same `decision_key`. Uses
    `ON CONFLICT (decision_key) DO NOTHING` so this seed never clobbers
    rows the operator has authored later (those carry their real
    `decision_source`, this seed only writes when the row is absent).

    Volatile timestamp fields are stripped from the snapshot for byte
    determinism — we synthesize stable defaults at seed time using
    `_FRESH_INSTALL_DECIDED_AT`. The downside is that re-running on a
    populated DB does not refresh content; the upside is the seed is a
    floor, not a source of truth. Operator authority lives in the live
    table once the agent starts writing.
    """
    rows = _load_decisions_snapshot(repo_root)
    if not rows:
        return ()
    seeded: list[str] = []
    seen_keys: set[str] = set()
    for r in rows:
        decision_key = r.get("decision_key") or ""
        if not decision_key or decision_key in seen_keys:
            continue
        scope_clamp = r.get("scope_clamp")
        if not isinstance(scope_clamp, dict):
            scope_clamp = {"applies_to": ["pending_review"], "does_not_apply_to": []}
        scope_kind = r.get("decision_scope_kind") or "authority_domain"
        scope_ref = r.get("decision_scope_ref") or "operator"
        operator_decision_id = (
            f"operator_decision.{r.get('decision_kind') or 'architecture_policy'}."
            f"snapshot.{decision_key.replace('::','.').replace('-','_')}"
        )[:240]
        await conn.execute(
            """
            INSERT INTO operator_decisions (
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
                created_at,
                updated_at,
                decision_scope_kind,
                decision_scope_ref,
                scope_clamp
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, NULL, $9, $9, $9,
                $10, $11, $12::jsonb
            )
            ON CONFLICT (decision_key) DO NOTHING
            """,
            operator_decision_id,
            decision_key,
            r.get("decision_kind") or "architecture_policy",
            r.get("decision_status") or "decided",
            r.get("title") or decision_key,
            r.get("rationale") or "",
            r.get("decided_by") or "praxis",
            r.get("decision_source") or "operator_decisions_snapshot",
            _FRESH_INSTALL_DECIDED_AT,
            scope_kind,
            scope_ref,
            json.dumps(scope_clamp),
        )
        seen_keys.add(decision_key)
        seeded.append(decision_key)
    return tuple(seeded)


async def _seed_public_functional_areas(conn: Any) -> tuple[str, ...]:
    seeded: list[str] = []
    for area in _PUBLIC_FUNCTIONAL_AREAS:
        area_slug = area["area_slug"]
        functional_area_id = f"functional_area.{area_slug}"
        await conn.execute(
            """
            INSERT INTO functional_areas (
                functional_area_id,
                area_slug,
                title,
                area_status,
                summary,
                created_at,
                updated_at
            ) VALUES ($1, $2, $3, 'active', $4, $5, $5)
            ON CONFLICT (functional_area_id) DO UPDATE
            SET area_slug = EXCLUDED.area_slug,
                title = EXCLUDED.title,
                area_status = EXCLUDED.area_status,
                summary = EXCLUDED.summary,
                updated_at = EXCLUDED.updated_at
            """,
            functional_area_id,
            area_slug,
            area["title"],
            area["summary"],
            _FRESH_INSTALL_DECIDED_AT,
        )
        seeded.append(functional_area_id)
    return tuple(seeded)


async def _seed_native_self_hosted_smoke_definition(conn: Any) -> tuple[str, ...]:
    """Reassert the canonical smoke workflow definition from DB authority."""

    envelope = _native_smoke_request_envelope()
    await conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at
        ) VALUES ($1, $2, 1, 1, $3, 'active', $4::jsonb, $4::jsonb, $5)
        ON CONFLICT (workflow_definition_id) DO UPDATE
        SET workflow_id = EXCLUDED.workflow_id,
            schema_version = EXCLUDED.schema_version,
            definition_version = EXCLUDED.definition_version,
            definition_hash = EXCLUDED.definition_hash,
            status = EXCLUDED.status,
            request_envelope = EXCLUDED.request_envelope,
            normalized_definition = EXCLUDED.normalized_definition
        """,
        _SMOKE_WORKFLOW_DEFINITION_ID,
        _SMOKE_WORKFLOW_ID,
        _SMOKE_DEFINITION_HASH,
        _jsonb(envelope),
        _SMOKE_CREATED_AT,
    )

    for node in envelope["nodes"]:
        await conn.execute(
            """
            INSERT INTO workflow_definition_nodes (
                workflow_definition_node_id,
                workflow_definition_id,
                node_id,
                node_type,
                schema_version,
                adapter_type,
                display_name,
                inputs,
                expected_outputs,
                success_condition,
                failure_behavior,
                authority_requirements,
                execution_boundary,
                position_index
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb,
                $12::jsonb, $13::jsonb, $14
            )
            ON CONFLICT (workflow_definition_node_id) DO UPDATE
            SET workflow_definition_id = EXCLUDED.workflow_definition_id,
                node_id = EXCLUDED.node_id,
                node_type = EXCLUDED.node_type,
                schema_version = EXCLUDED.schema_version,
                adapter_type = EXCLUDED.adapter_type,
                display_name = EXCLUDED.display_name,
                inputs = EXCLUDED.inputs,
                expected_outputs = EXCLUDED.expected_outputs,
                success_condition = EXCLUDED.success_condition,
                failure_behavior = EXCLUDED.failure_behavior,
                authority_requirements = EXCLUDED.authority_requirements,
                execution_boundary = EXCLUDED.execution_boundary,
                position_index = EXCLUDED.position_index
            """,
            node["workflow_definition_node_id"],
            node["workflow_definition_id"],
            node["node_id"],
            node["node_type"],
            node["schema_version"],
            node["adapter_type"],
            node["display_name"],
            _jsonb(node["inputs"]),
            _jsonb(node["expected_outputs"]),
            _jsonb(node["success_condition"]),
            _jsonb(node["failure_behavior"]),
            _jsonb(node["authority_requirements"]),
            _jsonb(node["execution_boundary"]),
            node["position_index"],
        )

    for edge in envelope["edges"]:
        await conn.execute(
            """
            INSERT INTO workflow_definition_edges (
                workflow_definition_edge_id,
                workflow_definition_id,
                edge_id,
                edge_type,
                schema_version,
                from_node_id,
                to_node_id,
                release_condition,
                payload_mapping,
                position_index
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10
            )
            ON CONFLICT (workflow_definition_edge_id) DO UPDATE
            SET workflow_definition_id = EXCLUDED.workflow_definition_id,
                edge_id = EXCLUDED.edge_id,
                edge_type = EXCLUDED.edge_type,
                schema_version = EXCLUDED.schema_version,
                from_node_id = EXCLUDED.from_node_id,
                to_node_id = EXCLUDED.to_node_id,
                release_condition = EXCLUDED.release_condition,
                payload_mapping = EXCLUDED.payload_mapping,
                position_index = EXCLUDED.position_index
            """,
            edge["workflow_definition_edge_id"],
            edge["workflow_definition_id"],
            edge["edge_id"],
            edge["edge_type"],
            edge["schema_version"],
            edge["from_node_id"],
            edge["to_node_id"],
            _jsonb(edge["release_condition"]),
            _jsonb(edge["payload_mapping"]),
            edge["position_index"],
        )

    return (_SMOKE_WORKFLOW_DEFINITION_ID,)


async def seed_fresh_install_authority_async(
    conn: Any,
    *,
    repo_root: Path | None = None,
) -> FreshInstallSeedSummary:
    """Seed idempotent first-run authority rows and project native routing.

    The seed intentionally omits operator-local policies, credentials, and
    Nate-specific Anthropic assumptions. Provider credentials are resolved at
    runtime through the credential resolver, not stored here.
    """

    resolved_repo_root = (repo_root or _repo_root()).resolve()
    config = _load_runtime_profiles_config(resolved_repo_root)
    sandbox_profiles = _require_mapping(
        config.get("sandbox_profiles"),
        field_name="sandbox_profiles",
    )
    sandbox_refs = await _seed_sandbox_profiles(conn, sandbox_profiles)
    runtime_refs = await _seed_runtime_profiles(conn, config)
    decision_keys = await _seed_public_operator_decisions(conn)
    snapshot_decision_keys = await _seed_operator_decisions_from_snapshot(
        conn,
        resolved_repo_root,
    )
    # Merge for the summary; keep public-seed keys first since those are the
    # authoritative baseline. The snapshot keys are advisory floor — the
    # operator's authoring is what fills in rationale/timestamps over time.
    merged_decision_keys = tuple(dict.fromkeys((*decision_keys, *snapshot_decision_keys)))
    functional_area_refs = await _seed_public_functional_areas(conn)
    workflow_definition_refs = await _seed_native_self_hosted_smoke_definition(conn)

    from registry.native_runtime_profile_sync import (
        NativeRuntimeProfileSyncError,
        sync_native_runtime_profile_authority_async,
    )

    try:
        synced_refs = await sync_native_runtime_profile_authority_async(conn)
    except NativeRuntimeProfileSyncError as exc:
        raise FreshInstallSeedError(
            "fresh_install_seed.native_runtime_projection_failed",
            "fresh-install runtime authority rows were seeded but projection failed",
            details={"error": str(exc)},
        ) from exc

    return FreshInstallSeedSummary(
        runtime_profiles=runtime_refs,
        sandbox_profiles=sandbox_refs,
        operator_decisions=merged_decision_keys,
        functional_areas=functional_area_refs,
        workflow_definitions=workflow_definition_refs,
        synced_runtime_profiles=tuple(synced_refs),
    )


__all__ = [
    "FreshInstallSeedError",
    "FreshInstallSeedSummary",
    "seed_fresh_install_authority_async",
]
