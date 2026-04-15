"""Sync repo-native runtime profile authority into Postgres.

The checked-in ``config/runtime_profiles.json`` file is the native authority
front door for default runtime/workspace refs. This module projects those
native refs into durable Postgres rows while sourcing live provider/model
health from the heartbeat-owned routing tables instead of freezing ephemeral
``model_profile.*`` / ``provider_policy.*`` ids in config.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from adapters.provider_registry import default_llm_adapter_type, get_profile, supports_adapter

from .domain import RuntimeProfileAuthorityRecord, WorkspaceAuthorityRecord

if TYPE_CHECKING:
    import asyncpg
    from storage.postgres.connection import SyncPostgresConnection


class NativeRuntimeProfileSyncError(RuntimeError):
    """Raised when repo-local runtime profiles cannot be synced safely."""


@dataclass(frozen=True, slots=True)
class NativeRuntimeProfileConfig:
    runtime_profile_ref: str
    workspace_ref: str
    model_profile_id: str
    provider_policy_id: str
    provider_name: str
    provider_names: tuple[str, ...]
    allowed_models: tuple[str, ...]
    repo_root: str
    workdir: str

    def workspace_record(self) -> WorkspaceAuthorityRecord:
        return WorkspaceAuthorityRecord(
            workspace_ref=self.workspace_ref,
            repo_root=self.repo_root,
            workdir=self.workdir,
        )

    def runtime_profile_record(self) -> RuntimeProfileAuthorityRecord:
        return RuntimeProfileAuthorityRecord(
            runtime_profile_ref=self.runtime_profile_ref,
            model_profile_id=self.model_profile_id,
            provider_policy_id=self.provider_policy_id,
        )


@dataclass(frozen=True, slots=True)
class _LiveCandidate:
    candidate_ref: str
    provider_ref: str
    provider_name: str
    provider_slug: str
    model_slug: str
    priority: int
    position_index: int


@dataclass(frozen=True, slots=True)
class _LiveRouteState:
    model_slug: str
    eligibility_status: str
    reason_code: str
    source_window_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _LiveBudgetWindow:
    provider_ref: str
    budget_scope: str
    budget_status: str
    window_started_at: object
    window_ended_at: object
    request_limit: object
    requests_used: object
    token_limit: object
    tokens_used: object
    spend_limit_usd: object
    spend_used_usd: object


def _default_live_budget_window(
    config: NativeRuntimeProfileConfig,
    *,
    candidates: tuple[_LiveCandidate, ...] | None = None,
) -> _LiveBudgetWindow:
    provider_ref = (
        str(candidates[0].provider_ref).strip()
        if candidates and str(candidates[0].provider_ref).strip()
        else f"provider.{config.provider_name}"
    )
    now = datetime.now(timezone.utc)
    return _LiveBudgetWindow(
        provider_ref=provider_ref,
        budget_scope="runtime",
        budget_status="available",
        window_started_at=now - timedelta(hours=1),
        window_ended_at=now + timedelta(days=1),
        request_limit=100000,
        requests_used=0,
        token_limit=100000000,
        tokens_used=0,
        spend_limit_usd="1000.000000",
        spend_used_usd="0.000000",
    )


def _native_transport_ready_refs(
    provider_slug: str,
) -> tuple[str, ...] | None:
    adapter_type = default_llm_adapter_type()
    if not supports_adapter(provider_slug, adapter_type):
        return None

    profile = get_profile(provider_slug)
    if profile is None:
        return None

    if adapter_type == "cli_llm":
        binary_path = shutil.which(profile.binary)
        if not binary_path:
            return None
        return (f"transport:{adapter_type}", f"binary:{binary_path}")

    if adapter_type == "llm_task":
        if not profile.api_endpoint or not profile.api_protocol_family:
            return None
        present_keys = tuple(
            env_name
            for env_name in profile.api_key_env_vars
            if os.environ.get(env_name, "").strip()
        )
        if not present_keys:
            return None
        return (f"transport:{adapter_type}", *[f"env:{name}" for name in present_keys])

    return None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _config_path() -> Path:
    return _repo_root() / "config" / "runtime_profiles.json"


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NativeRuntimeProfileSyncError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_string_list(value: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise NativeRuntimeProfileSyncError(f"{field_name} must be a non-empty array")
    normalized: list[str] = []
    for index, raw in enumerate(value):
        normalized.append(_require_text(raw, field_name=f"{field_name}[{index}]"))
    return tuple(dict.fromkeys(normalized))


def _resolve_repo_path(raw_value: object, *, field_name: str) -> str:
    raw_text = _require_text(raw_value, field_name=field_name)
    candidate = Path(raw_text)
    if not candidate.is_absolute():
        candidate = (_repo_root() / candidate).resolve()
    return str(candidate)


def _load_runtime_profiles_document() -> dict[str, Any]:
    path = _config_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise NativeRuntimeProfileSyncError(
            f"failed to read native runtime profile config: {path}",
        ) from exc
    except json.JSONDecodeError as exc:
        raise NativeRuntimeProfileSyncError(
            f"runtime profile config is not valid JSON: {path} ({exc.lineno}:{exc.colno})",
        ) from exc
    if not isinstance(payload, dict):
        raise NativeRuntimeProfileSyncError("runtime profile config must be a JSON object")
    return payload


def load_native_runtime_profile_configs() -> tuple[NativeRuntimeProfileConfig, ...]:
    payload = _load_runtime_profiles_document()
    runtime_profiles = payload.get("runtime_profiles")
    if not isinstance(runtime_profiles, dict) or not runtime_profiles:
        raise NativeRuntimeProfileSyncError("runtime_profiles config must define at least one profile")

    configs: list[NativeRuntimeProfileConfig] = []
    for runtime_profile_ref, profile in runtime_profiles.items():
        if not isinstance(profile, dict):
            raise NativeRuntimeProfileSyncError(
                f"runtime_profiles.{runtime_profile_ref} must be a JSON object",
            )
        primary_provider_name = _require_text(
            profile.get("provider_name"),
            field_name=f"{runtime_profile_ref}.provider_name",
        )
        raw_provider_names = profile.get("provider_names")
        provider_names = (
            _require_string_list(
                raw_provider_names,
                field_name=f"{runtime_profile_ref}.provider_names",
            )
            if raw_provider_names is not None
            else (primary_provider_name,)
        )
        if primary_provider_name not in provider_names:
            provider_names = (primary_provider_name, *provider_names)
        configs.append(
            NativeRuntimeProfileConfig(
                runtime_profile_ref=_require_text(
                    runtime_profile_ref,
                    field_name="runtime_profile_ref",
                ),
                workspace_ref=_require_text(
                    profile.get("workspace_ref", runtime_profile_ref),
                    field_name=f"{runtime_profile_ref}.workspace_ref",
                ),
                model_profile_id=_require_text(
                    profile.get("model_profile_id"),
                    field_name=f"{runtime_profile_ref}.model_profile_id",
                ),
                provider_policy_id=_require_text(
                    profile.get("provider_policy_id"),
                    field_name=f"{runtime_profile_ref}.provider_policy_id",
                ),
                provider_name=primary_provider_name,
                provider_names=provider_names,
                allowed_models=_require_string_list(
                    profile.get("allowed_models"),
                    field_name=f"{runtime_profile_ref}.allowed_models",
                ),
                repo_root=_resolve_repo_path(
                    profile.get("repo_root", "."),
                    field_name=f"{runtime_profile_ref}.repo_root",
                ),
                workdir=_resolve_repo_path(
                    profile.get("workdir", "."),
                    field_name=f"{runtime_profile_ref}.workdir",
                ),
            )
        )
    return tuple(configs)


def default_native_runtime_profile_ref() -> str:
    payload = _load_runtime_profiles_document()
    return _require_text(
        payload.get("default_runtime_profile"),
        field_name="default_runtime_profile",
    )


def resolve_native_runtime_profile_config(
    runtime_profile_ref: str | None = None,
) -> NativeRuntimeProfileConfig:
    target_ref = runtime_profile_ref or default_native_runtime_profile_ref()
    for config in load_native_runtime_profile_configs():
        if config.runtime_profile_ref == target_ref:
            return config
    raise NativeRuntimeProfileSyncError(
        f"runtime profile {target_ref!r} is not defined in { _config_path() }",
    )


def default_native_workspace_ref() -> str:
    return resolve_native_runtime_profile_config().workspace_ref


def is_native_runtime_profile_ref(runtime_profile_ref: str) -> bool:
    try:
        resolve_native_runtime_profile_config(runtime_profile_ref)
    except NativeRuntimeProfileSyncError:
        return False
    return True


def _slug_token(value: str) -> str:
    return value.lower().replace(".", "-").replace("/", "-")


def _live_candidates_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
) -> tuple[_LiveCandidate, ...]:
    rows = conn.execute(
        """
        SELECT DISTINCT ON (candidate.provider_slug, candidate.model_slug)
               candidate.candidate_ref,
               candidate.provider_ref,
               candidate.provider_name,
               candidate.provider_slug,
               candidate.model_slug,
               candidate.priority
        FROM provider_model_candidates candidate
        WHERE candidate.provider_name = ANY($1::text[])
          AND candidate.model_slug = ANY($2::text[])
          AND candidate.status = 'active'
        ORDER BY candidate.provider_slug,
                 candidate.model_slug,
                 CASE
                     WHEN candidate.candidate_ref = (
                         'candidate.' || candidate.provider_slug || '.' || candidate.model_slug
                     ) THEN 0
                     ELSE 1
                 END,
                 candidate.priority ASC,
                 candidate.created_at DESC,
                 candidate.candidate_ref ASC
        """,
        list(config.provider_names),
        list(config.allowed_models),
    )
    candidates = {
        str(row["model_slug"]): _LiveCandidate(
            candidate_ref=str(row["candidate_ref"]),
            provider_ref=str(row["provider_ref"]),
            provider_name=str(row["provider_name"]),
            provider_slug=str(row["provider_slug"]),
            model_slug=str(row["model_slug"]),
            priority=int(row.get("priority") or 999),
            position_index=index,
        )
        for index, row in enumerate(rows or [])
    }
    missing = [model for model in config.allowed_models if model not in candidates]
    if missing:
        raise NativeRuntimeProfileSyncError(
            (
                f"{config.runtime_profile_ref} has no active provider_model_candidates for "
                f"{', '.join(missing)}"
            ),
        )
    return tuple(candidates[model] for model in config.allowed_models)


async def _live_candidates_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
) -> tuple[_LiveCandidate, ...]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (candidate.provider_slug, candidate.model_slug)
               candidate.candidate_ref,
               candidate.provider_ref,
               candidate.provider_name,
               candidate.provider_slug,
               candidate.model_slug,
               candidate.priority
        FROM provider_model_candidates candidate
        WHERE candidate.provider_name = ANY($1::text[])
          AND candidate.model_slug = ANY($2::text[])
          AND candidate.status = 'active'
        ORDER BY candidate.provider_slug,
                 candidate.model_slug,
                 CASE
                     WHEN candidate.candidate_ref = (
                         'candidate.' || candidate.provider_slug || '.' || candidate.model_slug
                     ) THEN 0
                     ELSE 1
                 END,
                 candidate.priority ASC,
                 candidate.created_at DESC,
                 candidate.candidate_ref ASC
        """,
        list(config.provider_names),
        list(config.allowed_models),
    )
    candidates = {
        str(row["model_slug"]): _LiveCandidate(
            candidate_ref=str(row["candidate_ref"]),
            provider_ref=str(row["provider_ref"]),
            provider_name=str(row["provider_name"]),
            provider_slug=str(row["provider_slug"]),
            model_slug=str(row["model_slug"]),
            priority=int(row.get("priority") or 999),
            position_index=index,
        )
        for index, row in enumerate(rows or [])
    }
    missing = [model for model in config.allowed_models if model not in candidates]
    if missing:
        raise NativeRuntimeProfileSyncError(
            (
                f"{config.runtime_profile_ref} has no active provider_model_candidates for "
                f"{', '.join(missing)}"
            ),
        )
    return tuple(candidates[model] for model in config.allowed_models)


def _live_route_states_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
) -> dict[str, _LiveRouteState]:
    rows = conn.execute(
        """
        SELECT DISTINCT ON (candidate.model_slug)
               candidate.model_slug,
               eligibility.eligibility_status,
               eligibility.reason_code,
               eligibility.source_window_refs
        FROM route_eligibility_states eligibility
        JOIN provider_model_candidates candidate
          ON candidate.candidate_ref = eligibility.candidate_ref
        WHERE candidate.provider_name = ANY($1::text[])
          AND candidate.model_slug = ANY($2::text[])
        ORDER BY candidate.model_slug,
                 eligibility.evaluated_at DESC,
                 eligibility.route_eligibility_state_id DESC
        """,
        list(config.provider_names),
        list(config.allowed_models),
    )
    result: dict[str, _LiveRouteState] = {}
    for row in rows or []:
        source_refs = row.get("source_window_refs") or []
        if isinstance(source_refs, str):
            source_refs = json.loads(source_refs)
        result[str(row["model_slug"])] = _LiveRouteState(
            model_slug=str(row["model_slug"]),
            eligibility_status=str(row["eligibility_status"]),
            reason_code=str(row["reason_code"]),
            source_window_refs=tuple(
                str(ref)
                for ref in source_refs
                if isinstance(ref, str) and ref.strip()
            ),
        )
    return result


async def _live_route_states_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
) -> dict[str, _LiveRouteState]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (candidate.model_slug)
               candidate.model_slug,
               eligibility.eligibility_status,
               eligibility.reason_code,
               eligibility.source_window_refs
        FROM route_eligibility_states eligibility
        JOIN provider_model_candidates candidate
          ON candidate.candidate_ref = eligibility.candidate_ref
        WHERE candidate.provider_name = ANY($1::text[])
          AND candidate.model_slug = ANY($2::text[])
        ORDER BY candidate.model_slug,
                 eligibility.evaluated_at DESC,
                 eligibility.route_eligibility_state_id DESC
        """,
        list(config.provider_names),
        list(config.allowed_models),
    )
    result: dict[str, _LiveRouteState] = {}
    for row in rows or []:
        source_refs = row.get("source_window_refs") or []
        if isinstance(source_refs, str):
            source_refs = json.loads(source_refs)
        result[str(row["model_slug"])] = _LiveRouteState(
            model_slug=str(row["model_slug"]),
            eligibility_status=str(row["eligibility_status"]),
            reason_code=str(row["reason_code"]),
            source_window_refs=tuple(
                str(ref)
                for ref in source_refs
                if isinstance(ref, str) and ref.strip()
            ),
        )
    return result


def _latest_budget_window_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...] | None = None,
) -> _LiveBudgetWindow:
    rows = conn.execute(
        """
        SELECT budget_window.provider_ref,
               budget_window.budget_scope,
               budget_window.budget_status,
               budget_window.window_started_at,
               budget_window.window_ended_at,
               budget_window.request_limit,
               budget_window.requests_used,
               budget_window.token_limit,
               budget_window.tokens_used,
               budget_window.spend_limit_usd,
               budget_window.spend_used_usd
        FROM provider_budget_windows budget_window
        JOIN provider_policies policy
          ON policy.provider_policy_id = budget_window.provider_policy_id
        WHERE policy.provider_name = $1
          AND policy.status = 'active'
        ORDER BY COALESCE(budget_window.window_started_at, budget_window.created_at) DESC,
                 budget_window.provider_budget_window_id DESC
        LIMIT 1
        """,
        config.provider_name,
    )
    if not rows:
        return _default_live_budget_window(config, candidates=candidates)
    row = rows[0]
    return _LiveBudgetWindow(
        provider_ref=str(row["provider_ref"]),
        budget_scope=str(row["budget_scope"]),
        budget_status=str(row["budget_status"]),
        window_started_at=row["window_started_at"],
        window_ended_at=row["window_ended_at"],
        request_limit=row["request_limit"],
        requests_used=row["requests_used"],
        token_limit=row["token_limit"],
        tokens_used=row["tokens_used"],
        spend_limit_usd=row["spend_limit_usd"],
        spend_used_usd=row["spend_used_usd"],
    )


async def _latest_budget_window_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...] | None = None,
) -> _LiveBudgetWindow:
    row = await conn.fetchrow(
        """
        SELECT budget_window.provider_ref,
               budget_window.budget_scope,
               budget_window.budget_status,
               budget_window.window_started_at,
               budget_window.window_ended_at,
               budget_window.request_limit,
               budget_window.requests_used,
               budget_window.token_limit,
               budget_window.tokens_used,
               budget_window.spend_limit_usd,
               budget_window.spend_used_usd
        FROM provider_budget_windows budget_window
        JOIN provider_policies policy
          ON policy.provider_policy_id = budget_window.provider_policy_id
        WHERE policy.provider_name = $1
          AND policy.status = 'active'
        ORDER BY COALESCE(budget_window.window_started_at, budget_window.created_at) DESC,
                 budget_window.provider_budget_window_id DESC
        LIMIT 1
        """,
        config.provider_name,
    )
    if row is None:
        return _default_live_budget_window(config, candidates=candidates)
    return _LiveBudgetWindow(
        provider_ref=str(row["provider_ref"]),
        budget_scope=str(row["budget_scope"]),
        budget_status=str(row["budget_status"]),
        window_started_at=row["window_started_at"],
        window_ended_at=row["window_ended_at"],
        request_limit=row["request_limit"],
        requests_used=row["requests_used"],
        token_limit=row["token_limit"],
        tokens_used=row["tokens_used"],
        spend_limit_usd=row["spend_limit_usd"],
        spend_used_usd=row["spend_used_usd"],
    )


def _upsert_workspace_authority_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
) -> None:
    record = config.workspace_record()
    conn.execute(
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
        record.workspace_ref,
        record.repo_root,
        record.workdir,
    )


async def _upsert_workspace_authority_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
) -> None:
    record = config.workspace_record()
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
        record.workspace_ref,
        record.repo_root,
        record.workdir,
    )


def _upsert_profile_authority_rows_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...],
) -> None:
    profile_name = f"profile.{config.runtime_profile_ref}.native"
    allowed_provider_refs = tuple(
        dict.fromkeys(candidate.provider_ref for candidate in candidates if candidate.provider_ref)
    )
    preferred_provider_ref = next(
        (
            candidate.provider_ref
            for candidate in candidates
            if candidate.provider_name == config.provider_name and candidate.provider_ref
        ),
        allowed_provider_refs[0] if allowed_provider_refs else None,
    )
    conn.execute(
        """
        INSERT INTO model_profiles (
            model_profile_id,
            profile_name,
            provider_name,
            model_name,
            schema_version,
            status,
            budget_policy,
            routing_policy,
            default_parameters,
            effective_from,
            effective_to,
            supersedes_model_profile_id,
            created_at
        ) VALUES (
            $1, $2, $3, $4, 1, 'active',
            '{"tier":"native-runtime"}'::jsonb,
            '{"selection":"heartbeat-backed-native-runtime"}'::jsonb,
            '{"temperature":0}'::jsonb,
            now(), NULL, NULL, now()
        )
        ON CONFLICT (model_profile_id) DO UPDATE
        SET profile_name = EXCLUDED.profile_name,
            provider_name = EXCLUDED.provider_name,
            model_name = EXCLUDED.model_name,
            status = 'active',
            budget_policy = EXCLUDED.budget_policy,
            routing_policy = EXCLUDED.routing_policy,
            default_parameters = EXCLUDED.default_parameters,
            effective_to = NULL
        """,
        config.model_profile_id,
        profile_name,
        config.provider_name,
        config.allowed_models[0],
    )
    policy_name = f"policy.{config.runtime_profile_ref}.native"
    conn.execute(
        """
        INSERT INTO provider_policies (
            provider_policy_id,
            policy_name,
            provider_name,
            allowed_provider_refs,
            preferred_provider_ref,
            scope,
            schema_version,
            status,
            allowed_models,
            retry_policy,
            budget_policy,
            routing_rules,
            effective_from,
            effective_to,
            decision_ref
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, 'runtime', 1, 'active', $6::jsonb,
            '{"retry":0}'::jsonb,
            '{"budget":"heartbeat-backed-native-runtime"}'::jsonb,
            '{"mode":"provider_catalog"}'::jsonb,
            now(), NULL, $7
        )
        ON CONFLICT (provider_policy_id) DO UPDATE
        SET policy_name = EXCLUDED.policy_name,
            provider_name = EXCLUDED.provider_name,
            allowed_provider_refs = EXCLUDED.allowed_provider_refs,
            preferred_provider_ref = EXCLUDED.preferred_provider_ref,
            status = 'active',
            allowed_models = EXCLUDED.allowed_models,
            retry_policy = EXCLUDED.retry_policy,
            budget_policy = EXCLUDED.budget_policy,
            routing_rules = EXCLUDED.routing_rules,
            effective_to = NULL,
            decision_ref = EXCLUDED.decision_ref
        """,
        config.provider_policy_id,
        policy_name,
        config.provider_name,
        json.dumps(list(allowed_provider_refs)),
        preferred_provider_ref,
        json.dumps(list(config.allowed_models)),
        f"decision.provider_policy.{config.runtime_profile_ref}.native",
    )


async def _upsert_profile_authority_rows_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...],
) -> None:
    profile_name = f"profile.{config.runtime_profile_ref}.native"
    allowed_provider_refs = tuple(
        dict.fromkeys(candidate.provider_ref for candidate in candidates if candidate.provider_ref)
    )
    preferred_provider_ref = next(
        (
            candidate.provider_ref
            for candidate in candidates
            if candidate.provider_name == config.provider_name and candidate.provider_ref
        ),
        allowed_provider_refs[0] if allowed_provider_refs else None,
    )
    await conn.execute(
        """
        INSERT INTO model_profiles (
            model_profile_id,
            profile_name,
            provider_name,
            model_name,
            schema_version,
            status,
            budget_policy,
            routing_policy,
            default_parameters,
            effective_from,
            effective_to,
            supersedes_model_profile_id,
            created_at
        ) VALUES (
            $1, $2, $3, $4, 1, 'active',
            '{"tier":"native-runtime"}'::jsonb,
            '{"selection":"heartbeat-backed-native-runtime"}'::jsonb,
            '{"temperature":0}'::jsonb,
            now(), NULL, NULL, now()
        )
        ON CONFLICT (model_profile_id) DO UPDATE
        SET profile_name = EXCLUDED.profile_name,
            provider_name = EXCLUDED.provider_name,
            model_name = EXCLUDED.model_name,
            status = 'active',
            budget_policy = EXCLUDED.budget_policy,
            routing_policy = EXCLUDED.routing_policy,
            default_parameters = EXCLUDED.default_parameters,
            effective_to = NULL
        """,
        config.model_profile_id,
        profile_name,
        config.provider_name,
        config.allowed_models[0],
    )
    policy_name = f"policy.{config.runtime_profile_ref}.native"
    await conn.execute(
        """
        INSERT INTO provider_policies (
            provider_policy_id,
            policy_name,
            provider_name,
            allowed_provider_refs,
            preferred_provider_ref,
            scope,
            schema_version,
            status,
            allowed_models,
            retry_policy,
            budget_policy,
            routing_rules,
            effective_from,
            effective_to,
            decision_ref
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, 'runtime', 1, 'active', $6::jsonb,
            '{"retry":0}'::jsonb,
            '{"budget":"heartbeat-backed-native-runtime"}'::jsonb,
            '{"mode":"provider_catalog"}'::jsonb,
            now(), NULL, $7
        )
        ON CONFLICT (provider_policy_id) DO UPDATE
        SET policy_name = EXCLUDED.policy_name,
            provider_name = EXCLUDED.provider_name,
            allowed_provider_refs = EXCLUDED.allowed_provider_refs,
            preferred_provider_ref = EXCLUDED.preferred_provider_ref,
            status = 'active',
            allowed_models = EXCLUDED.allowed_models,
            retry_policy = EXCLUDED.retry_policy,
            budget_policy = EXCLUDED.budget_policy,
            routing_rules = EXCLUDED.routing_rules,
            effective_to = NULL,
            decision_ref = EXCLUDED.decision_ref
        """,
        config.provider_policy_id,
        policy_name,
        config.provider_name,
        json.dumps(list(allowed_provider_refs)),
        preferred_provider_ref,
        json.dumps(list(config.allowed_models)),
        f"decision.provider_policy.{config.runtime_profile_ref}.native",
    )


def _sync_candidate_bindings_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...],
) -> None:
    live_refs = [candidate.candidate_ref for candidate in candidates]
    for candidate in candidates:
        binding_role = "primary" if candidate.position_index == 0 else "fallback"
        conn.execute(
            """
            INSERT INTO model_profile_candidate_bindings (
                model_profile_candidate_binding_id,
                model_profile_id,
                candidate_ref,
                binding_role,
                position_index,
                effective_from,
                effective_to,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, now(), NULL, now()
            )
            ON CONFLICT (model_profile_candidate_binding_id) DO UPDATE
            SET candidate_ref = EXCLUDED.candidate_ref,
                binding_role = EXCLUDED.binding_role,
                position_index = EXCLUDED.position_index,
                effective_to = NULL
            """,
            f"binding.{config.runtime_profile_ref}.{_slug_token(candidate.model_slug)}",
            config.model_profile_id,
            candidate.candidate_ref,
            binding_role,
            candidate.position_index,
        )
    conn.execute(
        """
        DELETE FROM model_profile_candidate_bindings
        WHERE model_profile_id = $1
          AND NOT (candidate_ref = ANY($2::text[]))
        """,
        config.model_profile_id,
        live_refs,
    )


async def _sync_candidate_bindings_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...],
) -> None:
    live_refs = [candidate.candidate_ref for candidate in candidates]
    for candidate in candidates:
        binding_role = "primary" if candidate.position_index == 0 else "fallback"
        await conn.execute(
            """
            INSERT INTO model_profile_candidate_bindings (
                model_profile_candidate_binding_id,
                model_profile_id,
                candidate_ref,
                binding_role,
                position_index,
                effective_from,
                effective_to,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, now(), NULL, now()
            )
            ON CONFLICT (model_profile_candidate_binding_id) DO UPDATE
            SET candidate_ref = EXCLUDED.candidate_ref,
                binding_role = EXCLUDED.binding_role,
                position_index = EXCLUDED.position_index,
                effective_to = NULL
            """,
            f"binding.{config.runtime_profile_ref}.{_slug_token(candidate.model_slug)}",
            config.model_profile_id,
            candidate.candidate_ref,
            binding_role,
            candidate.position_index,
        )
    await conn.execute(
        """
        DELETE FROM model_profile_candidate_bindings
        WHERE model_profile_id = $1
          AND NOT (candidate_ref = ANY($2::text[]))
        """,
        config.model_profile_id,
        live_refs,
    )


def _sync_budget_window_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
    budget: _LiveBudgetWindow,
) -> None:
    conn.execute(
        """
        INSERT INTO provider_budget_windows (
            provider_budget_window_id,
            provider_policy_id,
            provider_ref,
            budget_scope,
            budget_status,
            window_started_at,
            window_ended_at,
            request_limit,
            requests_used,
            token_limit,
            tokens_used,
            spend_limit_usd,
            spend_used_usd,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now()
        )
        ON CONFLICT (provider_budget_window_id) DO UPDATE
        SET provider_ref = EXCLUDED.provider_ref,
            budget_scope = EXCLUDED.budget_scope,
            budget_status = EXCLUDED.budget_status,
            window_started_at = EXCLUDED.window_started_at,
            window_ended_at = EXCLUDED.window_ended_at,
            request_limit = EXCLUDED.request_limit,
            requests_used = EXCLUDED.requests_used,
            token_limit = EXCLUDED.token_limit,
            tokens_used = EXCLUDED.tokens_used,
            spend_limit_usd = EXCLUDED.spend_limit_usd,
            spend_used_usd = EXCLUDED.spend_used_usd,
            decision_ref = EXCLUDED.decision_ref
        """,
        f"budget.{config.runtime_profile_ref}.runtime",
        config.provider_policy_id,
        budget.provider_ref,
        budget.budget_scope,
        budget.budget_status,
        budget.window_started_at,
        budget.window_ended_at,
        budget.request_limit,
        budget.requests_used,
        budget.token_limit,
        budget.tokens_used,
        budget.spend_limit_usd,
        budget.spend_used_usd,
        f"decision.provider_policy.{config.runtime_profile_ref}.budget",
    )


async def _sync_budget_window_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
    budget: _LiveBudgetWindow,
) -> None:
    await conn.execute(
        """
        INSERT INTO provider_budget_windows (
            provider_budget_window_id,
            provider_policy_id,
            provider_ref,
            budget_scope,
            budget_status,
            window_started_at,
            window_ended_at,
            request_limit,
            requests_used,
            token_limit,
            tokens_used,
            spend_limit_usd,
            spend_used_usd,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now()
        )
        ON CONFLICT (provider_budget_window_id) DO UPDATE
        SET provider_ref = EXCLUDED.provider_ref,
            budget_scope = EXCLUDED.budget_scope,
            budget_status = EXCLUDED.budget_status,
            window_started_at = EXCLUDED.window_started_at,
            window_ended_at = EXCLUDED.window_ended_at,
            request_limit = EXCLUDED.request_limit,
            requests_used = EXCLUDED.requests_used,
            token_limit = EXCLUDED.token_limit,
            tokens_used = EXCLUDED.tokens_used,
            spend_limit_usd = EXCLUDED.spend_limit_usd,
            spend_used_usd = EXCLUDED.spend_used_usd,
            decision_ref = EXCLUDED.decision_ref
        """,
        f"budget.{config.runtime_profile_ref}.runtime",
        config.provider_policy_id,
        budget.provider_ref,
        budget.budget_scope,
        budget.budget_status,
        budget.window_started_at,
        budget.window_ended_at,
        budget.request_limit,
        budget.requests_used,
        budget.token_limit,
        budget.tokens_used,
        budget.spend_limit_usd,
        budget.spend_used_usd,
        f"decision.provider_policy.{config.runtime_profile_ref}.budget",
    )


def _sync_route_states_sync(
    conn: "SyncPostgresConnection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...],
    states_by_model: dict[str, _LiveRouteState],
) -> None:
    active_refs = [candidate.candidate_ref for candidate in candidates]
    for candidate in candidates:
        live_state = states_by_model.get(candidate.model_slug)
        eligibility_status = (
            live_state.eligibility_status
            if live_state is not None
            else "rejected"
        )
        reason_code = (
            live_state.reason_code
            if live_state is not None
            else "provider_route_authority.no_live_probe_state"
        )
        source_window_refs = (
            list(live_state.source_window_refs)
            if live_state is not None
            else [f"budget.{config.runtime_profile_ref}.runtime"]
        )
        transport_refs = _native_transport_ready_refs(candidate.provider_slug)
        if transport_refs is not None:
            for ref in transport_refs:
                if ref not in source_window_refs:
                    source_window_refs.append(ref)
        conn.execute(
            """
            INSERT INTO route_eligibility_states (
                route_eligibility_state_id,
                model_profile_id,
                provider_policy_id,
                candidate_ref,
                eligibility_status,
                reason_code,
                source_window_refs,
                evaluated_at,
                expires_at,
                decision_ref,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb, now(), NULL, $8, now()
            )
            ON CONFLICT (route_eligibility_state_id) DO UPDATE
            SET candidate_ref = EXCLUDED.candidate_ref,
                eligibility_status = EXCLUDED.eligibility_status,
                reason_code = EXCLUDED.reason_code,
                source_window_refs = EXCLUDED.source_window_refs,
                evaluated_at = now(),
                expires_at = NULL,
                decision_ref = EXCLUDED.decision_ref
            """,
            f"eligibility.{config.runtime_profile_ref}.{_slug_token(candidate.model_slug)}",
            config.model_profile_id,
            config.provider_policy_id,
            candidate.candidate_ref,
            eligibility_status,
            reason_code,
            json.dumps(source_window_refs),
            f"decision.route_eligibility.{config.runtime_profile_ref}.{_slug_token(candidate.model_slug)}",
        )
    conn.execute(
        """
        DELETE FROM route_eligibility_states
        WHERE model_profile_id = $1
          AND provider_policy_id = $2
          AND NOT (candidate_ref = ANY($3::text[]))
        """,
        config.model_profile_id,
        config.provider_policy_id,
        active_refs,
    )


async def _sync_route_states_async(
    conn: "asyncpg.Connection",
    config: NativeRuntimeProfileConfig,
    candidates: tuple[_LiveCandidate, ...],
    states_by_model: dict[str, _LiveRouteState],
) -> None:
    active_refs = [candidate.candidate_ref for candidate in candidates]
    for candidate in candidates:
        live_state = states_by_model.get(candidate.model_slug)
        eligibility_status = (
            live_state.eligibility_status
            if live_state is not None
            else "rejected"
        )
        reason_code = (
            live_state.reason_code
            if live_state is not None
            else "provider_route_authority.no_live_probe_state"
        )
        source_window_refs = (
            list(live_state.source_window_refs)
            if live_state is not None
            else [f"budget.{config.runtime_profile_ref}.runtime"]
        )
        transport_refs = _native_transport_ready_refs(candidate.provider_slug)
        if transport_refs is not None:
            for ref in transport_refs:
                if ref not in source_window_refs:
                    source_window_refs.append(ref)
        await conn.execute(
            """
            INSERT INTO route_eligibility_states (
                route_eligibility_state_id,
                model_profile_id,
                provider_policy_id,
                candidate_ref,
                eligibility_status,
                reason_code,
                source_window_refs,
                evaluated_at,
                expires_at,
                decision_ref,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb, now(), NULL, $8, now()
            )
            ON CONFLICT (route_eligibility_state_id) DO UPDATE
            SET candidate_ref = EXCLUDED.candidate_ref,
                eligibility_status = EXCLUDED.eligibility_status,
                reason_code = EXCLUDED.reason_code,
                source_window_refs = EXCLUDED.source_window_refs,
                evaluated_at = now(),
                expires_at = NULL,
                decision_ref = EXCLUDED.decision_ref
            """,
            f"eligibility.{config.runtime_profile_ref}.{_slug_token(candidate.model_slug)}",
            config.model_profile_id,
            config.provider_policy_id,
            candidate.candidate_ref,
            eligibility_status,
            reason_code,
            json.dumps(source_window_refs),
            f"decision.route_eligibility.{config.runtime_profile_ref}.{_slug_token(candidate.model_slug)}",
        )
    await conn.execute(
        """
        DELETE FROM route_eligibility_states
        WHERE model_profile_id = $1
          AND provider_policy_id = $2
          AND NOT (candidate_ref = ANY($3::text[]))
        """,
        config.model_profile_id,
        config.provider_policy_id,
        active_refs,
    )


def sync_native_runtime_profile_authority(
    conn: "SyncPostgresConnection",
    *,
    prune: bool = False,
) -> tuple[str, ...]:
    configs = load_native_runtime_profile_configs()
    for config in configs:
        _upsert_workspace_authority_sync(conn, config)
        candidates = _live_candidates_sync(conn, config)
        _upsert_profile_authority_rows_sync(conn, config, candidates)
        budget = _latest_budget_window_sync(conn, config, candidates=candidates)
        live_states = _live_route_states_sync(conn, config)
        _sync_candidate_bindings_sync(conn, config, candidates)
        _sync_budget_window_sync(conn, config, budget)
        _sync_route_states_sync(conn, config, candidates, live_states)
        record = config.runtime_profile_record()
        conn.execute(
            """
            INSERT INTO registry_runtime_profile_authority (
                runtime_profile_ref,
                model_profile_id,
                provider_policy_id,
                recorded_at
            ) VALUES ($1, $2, $3, now())
            ON CONFLICT (runtime_profile_ref) DO UPDATE
            SET model_profile_id = EXCLUDED.model_profile_id,
                provider_policy_id = EXCLUDED.provider_policy_id,
                recorded_at = now()
            """,
            record.runtime_profile_ref,
            record.model_profile_id,
            record.provider_policy_id,
        )

    refs = [config.runtime_profile_ref for config in configs]
    if prune:
        conn.execute(
            """
            DELETE FROM registry_runtime_profile_authority
            WHERE runtime_profile_ref = ANY($1::text[])
              AND NOT (runtime_profile_ref = ANY($2::text[]))
            """,
            refs,
            refs,
        )
    return tuple(refs)


async def sync_native_runtime_profile_authority_async(
    conn: "asyncpg.Connection",
    *,
    prune: bool = False,
) -> tuple[str, ...]:
    configs = load_native_runtime_profile_configs()
    for config in configs:
        await _upsert_workspace_authority_async(conn, config)
        candidates = await _live_candidates_async(conn, config)
        await _upsert_profile_authority_rows_async(conn, config, candidates)
        budget = await _latest_budget_window_async(conn, config, candidates=candidates)
        live_states = await _live_route_states_async(conn, config)
        await _sync_candidate_bindings_async(conn, config, candidates)
        await _sync_budget_window_async(conn, config, budget)
        await _sync_route_states_async(conn, config, candidates, live_states)
        record = config.runtime_profile_record()
        await conn.execute(
            """
            INSERT INTO registry_runtime_profile_authority (
                runtime_profile_ref,
                model_profile_id,
                provider_policy_id,
                recorded_at
            ) VALUES ($1, $2, $3, now())
            ON CONFLICT (runtime_profile_ref) DO UPDATE
            SET model_profile_id = EXCLUDED.model_profile_id,
                provider_policy_id = EXCLUDED.provider_policy_id,
                recorded_at = now()
            """,
            record.runtime_profile_ref,
            record.model_profile_id,
            record.provider_policy_id,
        )

    refs = [config.runtime_profile_ref for config in configs]
    if prune:
        await conn.execute(
            """
            DELETE FROM registry_runtime_profile_authority
            WHERE runtime_profile_ref = ANY($1::text[])
              AND NOT (runtime_profile_ref = ANY($2::text[]))
            """,
            refs,
            refs,
        )
    return tuple(refs)


__all__ = [
    "NativeRuntimeProfileConfig",
    "NativeRuntimeProfileSyncError",
    "default_native_runtime_profile_ref",
    "default_native_workspace_ref",
    "is_native_runtime_profile_ref",
    "load_native_runtime_profile_configs",
    "resolve_native_runtime_profile_config",
    "sync_native_runtime_profile_authority",
    "sync_native_runtime_profile_authority_async",
]
