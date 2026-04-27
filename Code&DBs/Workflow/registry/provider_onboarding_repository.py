"""Provider onboarding repository helpers for canonical registry rows.

This module owns the Postgres reads and writes behind provider onboarding's
durable authority:

- provider CLI profiles
- provider transport admissions and probe receipts
- model profiles, candidates, and bindings
- benchmark match-rule writes and market-registry delegation
- onboarding verification snapshots over the canonical tables
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import asyncpg

from registry.provider_onboarding._spec import (
    ProviderOnboardingModelSpec,
    ProviderOnboardingSpec,
    ProviderOnboardingStepResult,
    ProviderTransportAuthorityTemplate,
    _adapter_type_for_transport,
    _binding_id,
    _candidate_ref,
    _capability_tags_for,
    _cli_config_for,
    _execution_topology_for_transport,
    _jsonb,
    _model_profile_id,
    _model_profile_name,
    _normalize_unique,
    _priority_for,
    _balance_weight_for,
    _provider_cli_profile_payload,
    _provider_ref,
    _provider_transport_admission_id,
    _provider_transport_probe_receipt_id,
    _rule_id,
    _selected_lane_probe_contract,
    _transport_kind_for_transport,
    _utc_now,
)

__all__ = [
    "_query_model_visibility",
    "_query_model_profile_visibility",
    "_query_transport_admissions",
    "_query_transport_probe_receipts",
    "_upsert_provider_cli_profile",
    "_upsert_provider_transport_admission",
    "_record_provider_transport_probe_receipts",
    "_upsert_model_profile",
    "_upsert_provider_model_candidate",
    "_upsert_model_profile_binding",
    "_upsert_match_rule",
    "_apply_benchmark_plan",
]


async def _query_model_visibility(
    conn: asyncpg.Connection,
    provider_slug: str,
) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (provider_slug, model_slug)
               provider_slug,
               model_slug,
               route_tier,
               latency_class,
               benchmark_profile
          FROM provider_model_candidates
         WHERE provider_slug = $1
           AND status = 'active'
         ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
        """,
        provider_slug,
    )
    return {
        "count": len(rows),
        "models": [
            {
                "provider_slug": str(row["provider_slug"]),
                "model_slug": str(row["model_slug"]),
                "route_tier": row["route_tier"],
                "latency_class": row["latency_class"],
                "benchmark_profile": row["benchmark_profile"],
            }
            for row in rows
        ],
    }


async def _query_model_profile_visibility(
    conn: asyncpg.Connection,
    provider_slug: str,
) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT profile.model_profile_id,
               profile.provider_name,
               profile.model_name,
               profile.default_parameters,
               binding.candidate_ref
          FROM model_profiles AS profile
          LEFT JOIN model_profile_candidate_bindings AS binding
            ON binding.model_profile_id = profile.model_profile_id
           AND binding.effective_to IS NULL
         WHERE profile.provider_name = $1
           AND profile.status = 'active'
         ORDER BY profile.model_profile_id
        """,
        provider_slug,
    )
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        model_profile_id = str(row["model_profile_id"])
        entry = grouped.setdefault(
            model_profile_id,
            {
                "model_profile_id": model_profile_id,
                "provider_name": str(row["provider_name"]),
                "model_name": str(row["model_name"]),
                "default_parameters": row["default_parameters"],
                "candidate_refs": [],
            },
        )
        candidate_ref = row["candidate_ref"]
        if isinstance(candidate_ref, str) and candidate_ref.strip():
            entry["candidate_refs"].append(candidate_ref)
    return {
        "count": len(grouped),
        "profiles": list(grouped.values()),
    }


async def _query_transport_admissions(
    conn: asyncpg.Connection,
    provider_slug: str,
) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT provider_transport_admission_id,
               provider_slug,
               adapter_type,
               transport_kind,
               execution_topology,
               admitted_by_policy,
               policy_reason,
               lane_id,
               docs_urls,
               credential_sources,
               probe_contract,
               decision_ref,
               status
          FROM provider_transport_admissions
         WHERE provider_slug = $1
         ORDER BY adapter_type ASC
        """,
        provider_slug,
    )
    return {
        "count": len(rows),
        "lanes": [
            {
                "provider_transport_admission_id": str(row["provider_transport_admission_id"]),
                "provider_slug": str(row["provider_slug"]),
                "adapter_type": str(row["adapter_type"]),
                "transport_kind": str(row["transport_kind"]),
                "execution_topology": str(row["execution_topology"]),
                "admitted_by_policy": bool(row["admitted_by_policy"]),
                "policy_reason": str(row["policy_reason"]),
                "lane_id": str(row["lane_id"]),
                "docs_urls": row["docs_urls"],
                "credential_sources": row["credential_sources"],
                "probe_contract": row["probe_contract"],
                "decision_ref": str(row["decision_ref"]),
                "status": str(row["status"]),
            }
            for row in rows
        ],
    }


async def _query_transport_probe_receipts(
    conn: asyncpg.Connection,
    provider_slug: str,
    decision_ref: str,
) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT provider_transport_probe_receipt_id,
               provider_slug,
               adapter_type,
               decision_ref,
               probe_step,
               status,
               summary,
               details,
               recorded_at
          FROM provider_transport_probe_receipts
         WHERE provider_slug = $1
           AND decision_ref = $2
         ORDER BY recorded_at ASC, provider_transport_probe_receipt_id ASC
        """,
        provider_slug,
        decision_ref,
    )
    return {
        "count": len(rows),
        "receipts": [
            {
                "provider_transport_probe_receipt_id": str(row["provider_transport_probe_receipt_id"]),
                "provider_slug": str(row["provider_slug"]),
                "adapter_type": str(row["adapter_type"]),
                "decision_ref": str(row["decision_ref"]),
                "probe_step": str(row["probe_step"]),
                "status": str(row["status"]),
                "summary": str(row["summary"]),
                "details": row["details"],
                "recorded_at": row["recorded_at"],
            }
            for row in rows
        ],
    }


_UPSERT_PROVIDER_CLI_PROFILE_SQL = """
    INSERT INTO provider_cli_profiles (
        provider_slug,
        binary_name,
        base_flags,
        model_flag,
        system_prompt_flag,
        json_schema_flag,
        output_format,
        output_envelope_key,
        forbidden_flags,
        default_timeout,
        aliases,
        status,
        default_model,
        api_endpoint,
        api_protocol_family,
        api_key_env_vars,
        adapter_economics,
        prompt_mode
    ) VALUES (
        $1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9::jsonb, $10, $11::jsonb, $12,
        $13, $14, $15, $16::jsonb, $17::jsonb, $18
    )
    ON CONFLICT (provider_slug) DO UPDATE SET
        binary_name = EXCLUDED.binary_name,
        base_flags = EXCLUDED.base_flags,
        model_flag = EXCLUDED.model_flag,
        system_prompt_flag = EXCLUDED.system_prompt_flag,
        json_schema_flag = EXCLUDED.json_schema_flag,
        output_format = EXCLUDED.output_format,
        output_envelope_key = EXCLUDED.output_envelope_key,
        forbidden_flags = EXCLUDED.forbidden_flags,
        default_timeout = EXCLUDED.default_timeout,
        aliases = EXCLUDED.aliases,
        status = EXCLUDED.status,
        default_model = EXCLUDED.default_model,
        api_endpoint = EXCLUDED.api_endpoint,
        api_protocol_family = EXCLUDED.api_protocol_family,
        api_key_env_vars = EXCLUDED.api_key_env_vars,
        adapter_economics = EXCLUDED.adapter_economics,
        prompt_mode = EXCLUDED.prompt_mode,
        updated_at = now()
"""

_UPSERT_PROVIDER_TRANSPORT_ADMISSION_SQL = """
    INSERT INTO provider_transport_admissions (
        provider_transport_admission_id,
        provider_slug,
        adapter_type,
        transport_kind,
        execution_topology,
        admitted_by_policy,
        policy_reason,
        lane_id,
        docs_urls,
        credential_sources,
        probe_contract,
        decision_ref,
        status
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12, $13
    )
    ON CONFLICT (provider_slug, adapter_type) DO UPDATE SET
        transport_kind = EXCLUDED.transport_kind,
        execution_topology = EXCLUDED.execution_topology,
        admitted_by_policy = EXCLUDED.admitted_by_policy,
        policy_reason = EXCLUDED.policy_reason,
        lane_id = EXCLUDED.lane_id,
        docs_urls = EXCLUDED.docs_urls,
        credential_sources = EXCLUDED.credential_sources,
        -- MERGE probe_contract instead of replacing. Migration 263 wrote per-provider
        -- `auth_mounts` and `cli_home_tmpfs_dirs` keys (the sandbox runner consumes
        -- them to mount CLI credentials into thin sandbox images). A bare
        -- `EXCLUDED.probe_contract` from re-onboard would overwrite those keys with
        -- whatever the probe pipeline emitted (typically just probe shapes), silently
        -- breaking sandbox auth for every reonboard. Shallow JSONB merge with
        -- existing-row priority on keys NOT in EXCLUDED preserves the auth catalog.
        probe_contract = COALESCE(provider_transport_admissions.probe_contract, '{}'::jsonb)
                         || COALESCE(EXCLUDED.probe_contract, '{}'::jsonb),
        decision_ref = EXCLUDED.decision_ref,
        status = EXCLUDED.status,
        updated_at = now()
"""

_UPSERT_PROVIDER_TRANSPORT_PROBE_RECEIPT_SQL = """
    INSERT INTO provider_transport_probe_receipts (
        provider_transport_probe_receipt_id,
        provider_slug,
        adapter_type,
        decision_ref,
        probe_step,
        status,
        summary,
        details
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8::jsonb
    )
    ON CONFLICT (provider_transport_probe_receipt_id) DO UPDATE SET
        status = EXCLUDED.status,
        summary = EXCLUDED.summary,
        details = EXCLUDED.details,
        recorded_at = now()
"""

_UPSERT_MODEL_PROFILE_SQL = """
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
        $5::jsonb, $6::jsonb, $7::jsonb,
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
"""

_UPSERT_PROVIDER_MODEL_CANDIDATE_SQL = """
    INSERT INTO provider_model_candidates (
        candidate_ref,
        provider_ref,
        provider_name,
        provider_slug,
        model_slug,
        status,
        priority,
        balance_weight,
        capability_tags,
        default_parameters,
        effective_from,
        effective_to,
        decision_ref,
        created_at,
        cli_config,
        route_tier,
        route_tier_rank,
        latency_class,
        latency_rank,
        reasoning_control,
        task_affinities,
        benchmark_profile
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8,
        $9::jsonb, $10::jsonb, $11, NULL, $12, $13, $14::jsonb,
        $15, $16, $17, $18, $19::jsonb, $20::jsonb, $21::jsonb
    )
    ON CONFLICT (candidate_ref) DO UPDATE SET
        provider_ref = EXCLUDED.provider_ref,
        provider_name = EXCLUDED.provider_name,
        provider_slug = EXCLUDED.provider_slug,
        model_slug = EXCLUDED.model_slug,
        status = EXCLUDED.status,
        priority = EXCLUDED.priority,
        balance_weight = EXCLUDED.balance_weight,
        capability_tags = EXCLUDED.capability_tags,
        default_parameters = EXCLUDED.default_parameters,
        effective_from = EXCLUDED.effective_from,
        effective_to = EXCLUDED.effective_to,
        decision_ref = EXCLUDED.decision_ref,
        cli_config = EXCLUDED.cli_config,
        route_tier = EXCLUDED.route_tier,
        route_tier_rank = EXCLUDED.route_tier_rank,
        latency_class = EXCLUDED.latency_class,
        latency_rank = EXCLUDED.latency_rank,
        reasoning_control = EXCLUDED.reasoning_control,
        task_affinities = EXCLUDED.task_affinities,
        benchmark_profile = EXCLUDED.benchmark_profile
"""

_UPSERT_MODEL_PROFILE_BINDING_SQL = """
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
        $1, $2, $3, 'primary', 0, now(), NULL, now()
    )
    ON CONFLICT (model_profile_candidate_binding_id) DO UPDATE
    SET candidate_ref = EXCLUDED.candidate_ref,
        binding_role = EXCLUDED.binding_role,
        position_index = EXCLUDED.position_index,
        effective_to = NULL
"""

_UPSERT_MATCH_RULE_SQL = """
    INSERT INTO provider_model_market_match_rules (
        provider_model_market_match_rule_id,
        source_slug,
        provider_slug,
        candidate_model_slug,
        target_creator_slug,
        target_source_model_slug,
        match_kind,
        binding_confidence,
        selection_metadata,
        decision_ref,
        enabled
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, true
    )
    ON CONFLICT (source_slug, provider_slug, candidate_model_slug) DO UPDATE SET
        provider_model_market_match_rule_id = EXCLUDED.provider_model_market_match_rule_id,
        target_creator_slug = EXCLUDED.target_creator_slug,
        target_source_model_slug = EXCLUDED.target_source_model_slug,
        match_kind = EXCLUDED.match_kind,
        binding_confidence = EXCLUDED.binding_confidence,
        selection_metadata = EXCLUDED.selection_metadata,
        decision_ref = EXCLUDED.decision_ref,
        enabled = EXCLUDED.enabled,
        updated_at = now()
"""


def _provider_cli_profile_args(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        payload["provider_slug"],
        payload["binary_name"],
        _jsonb(payload["base_flags"]),
        payload["model_flag"],
        payload["system_prompt_flag"],
        payload["json_schema_flag"],
        payload["output_format"],
        payload["output_envelope_key"],
        _jsonb(payload["forbidden_flags"]),
        payload["default_timeout"],
        _jsonb(payload["aliases"]),
        payload["status"],
        payload["default_model"],
        payload["api_endpoint"],
        payload["api_protocol_family"],
        _jsonb(payload["api_key_env_vars"]),
        _jsonb(payload["adapter_economics"]),
        payload["prompt_mode"],
    )


def _provider_transport_admission_payload(
    *,
    spec: ProviderOnboardingSpec,
    transport_template: ProviderTransportAuthorityTemplate,
    transport_step: ProviderOnboardingStepResult,
    model_step: ProviderOnboardingStepResult | None,
    capacity_step: ProviderOnboardingStepResult | None,
    selected_models: Sequence[ProviderOnboardingModelSpec],
    decision_ref: str,
    admitted_by_policy: bool,
    policy_reason: str,
    router_supported: bool | None,
) -> dict[str, Any]:
    adapter_type = _adapter_type_for_transport(spec.selected_transport or "")
    return {
        "provider_transport_admission_id": _provider_transport_admission_id(
            spec.provider_slug,
            adapter_type,
        ),
        "provider_slug": spec.provider_slug,
        "adapter_type": adapter_type,
        "transport_kind": _transport_kind_for_transport(spec.selected_transport or ""),
        "execution_topology": _execution_topology_for_transport(spec.selected_transport or ""),
        "admitted_by_policy": admitted_by_policy,
        "policy_reason": policy_reason,
        "lane_id": f"{spec.provider_slug}:{adapter_type}",
        "docs_urls": {
            "provider": spec.provider_docs_url,
            "transport": spec.transport_docs_url,
        },
        "credential_sources": _normalize_unique(
            [
                *spec.api_key_env_vars,
                str(transport_step.details.get("credential_source") or ""),
            ]
        ),
        "probe_contract": _selected_lane_probe_contract(
            spec=spec,
            transport_template=transport_template,
            transport_step=transport_step,
            model_step=model_step,
            capacity_step=capacity_step,
            selected_models=selected_models,
            router_supported=router_supported,
        ),
        "decision_ref": decision_ref,
        "status": "active",
    }


def _provider_transport_admission_args(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        payload["provider_transport_admission_id"],
        payload["provider_slug"],
        payload["adapter_type"],
        payload["transport_kind"],
        payload["execution_topology"],
        payload["admitted_by_policy"],
        payload["policy_reason"],
        payload["lane_id"],
        _jsonb(payload["docs_urls"]),
        _jsonb(list(payload["credential_sources"])),
        _jsonb(payload["probe_contract"]),
        payload["decision_ref"],
        payload["status"],
    )


def _provider_transport_probe_receipt_args(
    *,
    spec: ProviderOnboardingSpec,
    adapter_type: str,
    decision_ref: str,
    index: int,
    step: ProviderOnboardingStepResult,
) -> tuple[Any, ...]:
    return (
        _provider_transport_probe_receipt_id(
            spec.provider_slug,
            adapter_type,
            decision_ref,
            index,
            step.step,
        ),
        spec.provider_slug,
        adapter_type,
        decision_ref,
        step.step,
        step.status,
        step.summary,
        _jsonb(dict(step.details)),
    )


def _model_profile_payload(
    *,
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
) -> dict[str, Any]:
    model_profile_id = _model_profile_id(spec.provider_slug, model.model_slug)
    routing_policy = {
        "selection": "direct_candidate",
        "transport": spec.selected_transport,
        "route_tier": model.route_tier,
        "latency_class": model.latency_class,
    }
    budget_policy = {
        "tier": "provider-onboarding",
        "billing_mode": (
            spec.adapter_economics.get(
                "cli_llm" if spec.selected_transport == "cli" else "llm_task",
                {},
            ).get("billing_mode")
        ),
    }
    default_parameters = {
        "context_window": model.context_window,
        "provider_slug": spec.provider_slug,
        "model_slug": model.model_slug,
        "selected_transport": spec.selected_transport,
    }
    default_parameters.update(dict(model.default_parameters))
    return {
        "model_profile_id": model_profile_id,
        "profile_name": _model_profile_name(spec.provider_slug, model.model_slug),
        "provider_name": spec.provider_slug,
        "model_name": model.model_slug,
        "context_window": model.context_window,
        "budget_policy": budget_policy,
        "routing_policy": routing_policy,
        "default_parameters": default_parameters,
    }


def _model_profile_args(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        payload["model_profile_id"],
        payload["profile_name"],
        payload["provider_name"],
        payload["model_name"],
        _jsonb(payload["budget_policy"]),
        _jsonb(payload["routing_policy"]),
        _jsonb(payload["default_parameters"]),
    )


def _provider_model_candidate_payload(
    *,
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
    decision_ref: str,
) -> dict[str, Any]:
    candidate_ref = _candidate_ref(spec.provider_slug, model.model_slug)
    cli_config = _cli_config_for(spec, model)
    capability_tags = _capability_tags_for(spec, model)
    default_parameters = {
        "provider_slug": spec.provider_slug,
        "model_slug": model.model_slug,
        "context_window": model.context_window,
        "onboarding_source": "provider_onboarding_wizard",
        "decision_ref": decision_ref,
        "provider_name": spec.provider_name,
        "selected_transport": spec.selected_transport,
    }
    default_parameters.update(dict(model.default_parameters))
    return {
        "candidate_ref": candidate_ref,
        "provider_ref": _provider_ref(spec.provider_slug),
        "provider_name": spec.provider_name,
        "provider_slug": spec.provider_slug,
        "model_slug": model.model_slug,
        "status": model.status,
        "priority": _priority_for(str(model.route_tier), int(model.route_tier_rank or 1)),
        "balance_weight": _balance_weight_for(str(model.route_tier)),
        "capability_tags": capability_tags,
        "default_parameters": default_parameters,
        "decision_ref": decision_ref,
        "synced_at": _utc_now(),
        "cli_config": cli_config,
        "route_tier": model.route_tier,
        "route_tier_rank": model.route_tier_rank,
        "latency_class": model.latency_class,
        "latency_rank": model.latency_rank,
        "reasoning_control": dict(model.reasoning_control),
        "task_affinities": dict(model.task_affinities),
        "benchmark_profile": dict(model.benchmark_profile),
        "context_window": model.context_window,
    }


def _provider_model_candidate_args(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        payload["candidate_ref"],
        payload["provider_ref"],
        payload["provider_name"],
        payload["provider_slug"],
        payload["model_slug"],
        payload["status"],
        payload["priority"],
        payload["balance_weight"],
        _jsonb(payload["capability_tags"]),
        _jsonb(payload["default_parameters"]),
        payload["synced_at"],
        payload["decision_ref"],
        payload["synced_at"],
        _jsonb(payload["cli_config"]),
        payload["route_tier"],
        payload["route_tier_rank"],
        payload["latency_class"],
        payload["latency_rank"],
        _jsonb(payload["reasoning_control"]),
        _jsonb(payload["task_affinities"]),
        _jsonb(payload["benchmark_profile"]),
    )


def _model_profile_binding_payload(
    *,
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
) -> dict[str, Any]:
    return {
        "model_profile_candidate_binding_id": _binding_id(spec.provider_slug, model.model_slug),
        "model_profile_id": _model_profile_id(spec.provider_slug, model.model_slug),
        "candidate_ref": _candidate_ref(spec.provider_slug, model.model_slug),
        "binding_role": "primary",
        "position_index": 0,
    }


def _model_profile_binding_args(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        payload["model_profile_candidate_binding_id"],
        payload["model_profile_id"],
        payload["candidate_ref"],
    )


def _match_rule_payload(
    *,
    spec: ProviderOnboardingSpec,
    decision_ref: str,
    source_slug: str,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "provider_model_market_match_rule_id": _rule_id(
            source_slug,
            spec.provider_slug,
            str(plan["model_slug"]),
        ),
        "source_slug": source_slug,
        "provider_slug": spec.provider_slug,
        "candidate_model_slug": plan["model_slug"],
        "target_creator_slug": plan["target_creator_slug"],
        "target_source_model_slug": plan["target_source_model_slug"],
        "match_kind": plan["match_kind"],
        "binding_confidence": plan["binding_confidence"],
        "selection_metadata": dict(plan["selection_metadata"]),
        "decision_ref": decision_ref,
        "model_slug": plan["model_slug"],
    }


def _match_rule_args(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        payload["provider_model_market_match_rule_id"],
        payload["source_slug"],
        payload["provider_slug"],
        payload["candidate_model_slug"],
        payload["target_creator_slug"],
        payload["target_source_model_slug"],
        payload["match_kind"],
        payload["binding_confidence"],
        _jsonb(payload["selection_metadata"]),
        payload["decision_ref"],
    )


def _benchmark_decision_payload(
    *,
    spec: ProviderOnboardingSpec,
    source_slug: str,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    existing_profile = next(
        (
            model.benchmark_profile
            for model in spec.models
            if model.model_slug == plan["model_slug"]
        ),
        {},
    )
    return {
        "candidate_ref": _candidate_ref(spec.provider_slug, str(plan["model_slug"])),
        "match_kind": plan["match_kind"],
        "binding_confidence": plan["binding_confidence"],
        "market_row": plan.get("market_row"),
        "existing_benchmark_profile": dict(existing_profile),
        "selection_metadata": dict(plan["selection_metadata"]),
        "rule_ref": _rule_id(source_slug, spec.provider_slug, str(plan["model_slug"])),
    }


async def _upsert_provider_cli_profile(
    conn: asyncpg.Connection,
    spec: ProviderOnboardingSpec,
) -> None:
    payload = _provider_cli_profile_payload(spec)
    await conn.execute(_UPSERT_PROVIDER_CLI_PROFILE_SQL, *_provider_cli_profile_args(payload))


async def _upsert_provider_transport_admission(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    transport_template: ProviderTransportAuthorityTemplate,
    transport_step: ProviderOnboardingStepResult,
    model_step: ProviderOnboardingStepResult | None,
    capacity_step: ProviderOnboardingStepResult | None,
    selected_models: Sequence[ProviderOnboardingModelSpec],
    decision_ref: str,
    admitted_by_policy: bool,
    policy_reason: str,
    router_supported: bool | None,
) -> dict[str, Any]:
    payload = _provider_transport_admission_payload(
        spec=spec,
        transport_template=transport_template,
        transport_step=transport_step,
        model_step=model_step,
        capacity_step=capacity_step,
        selected_models=selected_models,
        decision_ref=decision_ref,
        admitted_by_policy=admitted_by_policy,
        policy_reason=policy_reason,
        router_supported=router_supported,
    )
    await conn.execute(
        _UPSERT_PROVIDER_TRANSPORT_ADMISSION_SQL,
        *_provider_transport_admission_args(payload),
    )
    return payload


async def _record_provider_transport_probe_receipts(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    decision_ref: str,
    steps: Sequence[ProviderOnboardingStepResult],
) -> None:
    adapter_type = _adapter_type_for_transport(spec.selected_transport or "")
    for index, step in enumerate(steps, start=1):
        await conn.execute(
            _UPSERT_PROVIDER_TRANSPORT_PROBE_RECEIPT_SQL,
            *_provider_transport_probe_receipt_args(
                spec=spec,
                adapter_type=adapter_type,
                decision_ref=decision_ref,
                index=index,
                step=step,
            ),
        )


async def _upsert_model_profile(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
) -> dict[str, Any]:
    payload = _model_profile_payload(spec=spec, model=model)
    await conn.execute(_UPSERT_MODEL_PROFILE_SQL, *_model_profile_args(payload))
    return {
        "model_profile_id": payload["model_profile_id"],
        "profile_name": payload["profile_name"],
        "provider_name": payload["provider_name"],
        "model_name": payload["model_name"],
        "context_window": payload["context_window"],
    }


async def _upsert_provider_model_candidate(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
    decision_ref: str,
) -> dict[str, Any]:
    payload = _provider_model_candidate_payload(
        spec=spec,
        model=model,
        decision_ref=decision_ref,
    )
    await conn.execute(
        _UPSERT_PROVIDER_MODEL_CANDIDATE_SQL,
        *_provider_model_candidate_args(payload),
    )
    return {
        "candidate_ref": payload["candidate_ref"],
        "provider_slug": payload["provider_slug"],
        "model_slug": payload["model_slug"],
        "route_tier": payload["route_tier"],
        "latency_class": payload["latency_class"],
        "context_window": payload["context_window"],
        "cli_config": payload["cli_config"],
    }


async def _upsert_model_profile_binding(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    model: ProviderOnboardingModelSpec,
) -> dict[str, Any]:
    payload = _model_profile_binding_payload(spec=spec, model=model)
    await conn.execute(
        _UPSERT_MODEL_PROFILE_BINDING_SQL,
        *_model_profile_binding_args(payload),
    )
    return dict(payload)


async def _upsert_match_rule(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    decision_ref: str,
    source_slug: str,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _match_rule_payload(
        spec=spec,
        decision_ref=decision_ref,
        source_slug=source_slug,
        plan=plan,
    )
    await conn.execute(_UPSERT_MATCH_RULE_SQL, *_match_rule_args(payload))
    return {
        "provider_model_market_match_rule_id": payload["provider_model_market_match_rule_id"],
        "source_slug": payload["source_slug"],
        "model_slug": payload["model_slug"],
        "match_kind": payload["match_kind"],
        "binding_confidence": payload["binding_confidence"],
    }


async def _apply_benchmark_plan(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    benchmark_report: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    source_slug = str(benchmark_report["source"])
    plan_rows = list(benchmark_report.get("_plan") or [])
    if not plan_rows:
        return [], 0

    from scripts import sync_market_model_registry as market_sync

    source_config = dict(benchmark_report["_source_config"])
    decision_ref = f"decision.provider-onboarding.{spec.provider_slug}.benchmark"

    reports: list[dict[str, Any]] = []
    for plan in plan_rows:
        reports.append(
            await _upsert_match_rule(
                conn,
                spec=spec,
                decision_ref=decision_ref,
                source_slug=source_slug,
                plan=plan,
            )
        )

    decisions = [
        _benchmark_decision_payload(
            spec=spec,
            source_slug=source_slug,
            plan=plan,
        )
        for plan in plan_rows
    ]

    bound_rows, _gap = await market_sync.apply_benchmark_decisions(
        conn,
        decisions=decisions,
        source_slug=source_slug,
        source_config=source_config,
        decision_ref=decision_ref,
    )
    return reports, bound_rows
