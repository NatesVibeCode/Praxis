"""Provider onboarding verification, reporting, and orchestration."""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
import json
from dataclasses import replace
from typing import Any

import asyncpg

from registry import provider_execution_registry as provider_registry_mod
from . import _probe as provider_onboarding_probe
from registry.provider_onboarding_repository import (
    _apply_benchmark_plan as _apply_benchmark_plan_impl,
    _query_model_profile_visibility,
    _query_model_visibility,
    _query_transport_admissions,
    _query_transport_probe_receipts,
    _record_provider_transport_probe_receipts as _record_provider_transport_probe_receipts_impl,
    _upsert_model_profile as _upsert_model_profile_impl,
    _upsert_model_profile_binding as _upsert_model_profile_binding_impl,
    _upsert_provider_cli_profile as _upsert_provider_cli_profile_impl,
    _upsert_provider_model_candidate as _upsert_provider_model_candidate_impl,
    _upsert_provider_transport_admission as _upsert_provider_transport_admission_impl,
)

from ._spec import (
    ProviderOnboardingModelSpec,
    ProviderOnboardingResult,
    ProviderOnboardingSpec,
    ProviderOnboardingStepResult,
    _VALID_CLI_PROMPT_MODES,
    _adapter_type_for_transport,
    _execution_topology_for_transport,
    _planned_step,
    _resolve_spec,
    _skipped_step,
    _utc_now,
)
from ._benchmark import (
    _probe_benchmark as _probe_benchmark_impl,
)

__all__ = [
    "run_provider_onboarding",
    "_run_provider_onboarding",
    "_verification_report",
    "_query_model_visibility",
    "_query_model_profile_visibility",
    "_query_transport_admissions",
    "_query_transport_probe_receipts",
]


async def _verification_report(
    *,
    conn: asyncpg.Connection,
    spec: ProviderOnboardingSpec,
    decision_ref: str,
) -> dict[str, Any]:
    provider_registry_mod.reload_from_db()
    provider_report = provider_registry_mod.validate_profiles().get(spec.provider_slug, {})
    from runtime import health as health_mod

    probe_checks: dict[str, Any] = {}
    for adapter_type in ("cli_llm", "llm_task"):
        probe = health_mod.ProviderTransportProbe(spec.provider_slug, adapter_type).check()
        probe_checks[adapter_type] = {
            "supported": probe.passed,
            "status": probe.status,
            "message": probe.message,
            "details": probe.details,
        }
    model_visibility = await _query_model_visibility(conn, spec.provider_slug)
    model_profile_visibility = await _query_model_profile_visibility(conn, spec.provider_slug)
    transport_admissions = await _query_transport_admissions(conn, spec.provider_slug)
    probe_receipts = await _query_transport_probe_receipts(conn, spec.provider_slug, decision_ref)
    return {
        "provider_report": provider_report,
        "transport": probe_checks,
        "transport_admissions": transport_admissions,
        "probe_receipts": probe_receipts,
        "model_visibility": model_visibility,
        "model_profiles": model_profile_visibility,
        "selected_transport_supported": provider_registry_mod.supports_model_adapter(
            spec.provider_slug,
            spec.default_model or "",
            "cli_llm" if spec.selected_transport == "cli" else "llm_task",
        )
        if spec.default_model
        else False,
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def _run_provider_onboarding(
    *,
    database_url: str,
    spec: ProviderOnboardingSpec,
    dry_run: bool,
) -> ProviderOnboardingResult:
    decision_ref = (
        f"decision.provider-onboarding.{spec.provider_slug}.{_utc_now().strftime('%Y%m%dT%H%M%SZ')}"
    )
    steps: list[ProviderOnboardingStepResult] = []
    provider_report: dict[str, Any] = {}
    model_reports: list[dict[str, Any]] = []
    benchmark_report: dict[str, Any] = {}

    resolved_spec, _template, transport_template, authority_step = _resolve_spec(spec)
    steps.append(authority_step)

    conn = await asyncpg.connect(database_url)
    transport_step: ProviderOnboardingStepResult | None = None
    model_step: ProviderOnboardingStepResult | None = None
    capacity_step: ProviderOnboardingStepResult | None = None
    benchmark_step: ProviderOnboardingStepResult | None = None
    resolved_models: tuple[ProviderOnboardingModelSpec, ...] = ()

    async def _record_transport_authority(
        *,
        router_supported: bool | None,
        record_receipts: bool,
    ) -> None:
        if dry_run or transport_step is None:
            return
        failure_step = next((step for step in steps if step.status == "failed"), None)
        admitted_by_policy = bool(
            capacity_step is not None
            and capacity_step.status == "succeeded"
            and router_supported is not False
        )
        policy_reason = (
            f"Admitted {spec.provider_slug}/{_adapter_type_for_transport(resolved_spec.selected_transport or '')} "
            f"via {_execution_topology_for_transport(resolved_spec.selected_transport or '')} after wizard probes succeeded."
            if admitted_by_policy
            else str(
                (failure_step.summary if failure_step is not None else "")
                or f"Lane not admitted for {resolved_spec.provider_slug}."
            )
        )
        await _upsert_provider_transport_admission_impl(
            conn,
            spec=resolved_spec,
            transport_template=transport_template,
            transport_step=transport_step,
            model_step=model_step,
            capacity_step=capacity_step,
            selected_models=resolved_models,
            decision_ref=decision_ref,
            admitted_by_policy=admitted_by_policy,
            policy_reason=policy_reason,
            router_supported=router_supported,
        )
        if record_receipts:
            await _record_provider_transport_probe_receipts_impl(
                conn,
                spec=resolved_spec,
                decision_ref=decision_ref,
                steps=steps,
            )

    async def _finish() -> ProviderOnboardingResult:
        ok = all(step.status != "failed" for step in steps)
        router_supported = provider_report.get("selected_transport_supported")
        await _record_transport_authority(
            router_supported=bool(router_supported) if router_supported is not None else None,
            record_receipts=False,
        )
        return ProviderOnboardingResult(
            ok=ok,
            provider_slug=resolved_spec.provider_slug,
            provider_name=str(resolved_spec.provider_name or resolved_spec.provider_slug),
            decision_ref=decision_ref,
            dry_run=dry_run,
            steps=tuple(steps),
            provider_report=provider_report,
            model_reports=tuple(model_reports),
            benchmark_report={
                key: value
                for key, value in benchmark_report.items()
                if not str(key).startswith("_")
            },
        )

    try:
        transport_step, transport_env = provider_onboarding_probe._probe_transport(
            resolved_spec,
            transport_template,
        )
        steps.append(transport_step)
        if transport_step.status == "failed":
            steps.extend(
                [
                    _skipped_step("model_probe", "Transport probe failed"),
                    _skipped_step("capacity_probe", "Transport probe failed"),
                    _skipped_step("benchmark_probe", "Transport probe failed"),
                    _skipped_step("registry_write", "Transport probe failed"),
                    _skipped_step("verification", "Transport probe failed"),
                ]
            )
            return await _finish()

        model_step, resolved_models = provider_onboarding_probe._probe_models(
            resolved_spec,
            transport_template,
            env=transport_env,
            transport_details=transport_step.details,
        )
        steps.append(model_step)
        if model_step.status == "failed":
            steps.extend(
                [
                    _skipped_step("capacity_probe", "Model probe failed"),
                    _skipped_step("benchmark_probe", "Model probe failed"),
                    _skipped_step("registry_write", "Model probe failed"),
                    _skipped_step("verification", "Model probe failed"),
                ]
            )
            return await _finish()

        if resolved_spec.default_model != model_step.details.get("default_model"):
            resolved_spec = replace(
                resolved_spec,
                default_model=str(model_step.details["default_model"]),
                models=resolved_models,
            )
        else:
            resolved_spec = replace(resolved_spec, models=resolved_models)

        capacity_step = provider_onboarding_probe._probe_capacity(
            resolved_spec,
            transport_template,
            env=transport_env,
            transport_details=transport_step.details,
            models=resolved_models,
        )
        steps.append(capacity_step)
        allow_provisional_registry_write = bool(
            capacity_step.status == "failed"
            and resolved_spec.selected_transport == "cli"
            and resolved_models
        )
        if capacity_step.status == "failed" and not allow_provisional_registry_write:
            steps.extend(
                [
                    _skipped_step("benchmark_probe", "Capacity probe failed"),
                    _skipped_step("registry_write", "Capacity probe failed"),
                    _skipped_step("verification", "Capacity probe failed"),
                ]
            )
            return await _finish()
        if capacity_step.status == "failed":
            benchmark_step = _skipped_step(
                "benchmark_probe",
                "Capacity probe failed; continuing with provisional CLI registry write",
            )
            steps.append(benchmark_step)
        else:
            benchmark_step, benchmark_report = await _probe_benchmark_impl(
                conn,
                spec=resolved_spec,
                models=resolved_models,
            )
            steps.append(benchmark_step)

        if resolved_spec.selected_transport == "cli":
            discovered_prompt_mode = str(capacity_step.details.get("prompt_mode") or "").strip().lower()
            if discovered_prompt_mode in _VALID_CLI_PROMPT_MODES:
                resolved_spec = replace(resolved_spec, cli_prompt_mode=discovered_prompt_mode)

        if dry_run:
            steps.append(
                _planned_step(
                    "registry_write",
                    (
                        f"Would upsert provider profile, {len(resolved_models)} model_profiles, "
                        f"{len(resolved_models)} candidates, and direct bindings"
                        if not allow_provisional_registry_write
                        else (
                            f"Would provision provider profile, {len(resolved_models)} model_profiles, "
                            f"{len(resolved_models)} candidates, and direct bindings even though "
                            "the live capacity probe failed; lane admission would stay disabled until a later successful probe"
                        )
                    ),
                    details={
                        "provider_slug": resolved_spec.provider_slug,
                        "selected_transport": resolved_spec.selected_transport,
                        "default_model": resolved_spec.default_model,
                        "provisional_write": allow_provisional_registry_write,
                    },
                )
            )
            steps.append(
                _planned_step(
                    "verification",
                    "Would reload provider registry and verify transport and router visibility",
                )
            )
            model_reports[:] = [
                {
                    "model_slug": model.model_slug,
                    "context_window": model.context_window,
                    "route_tier": model.route_tier,
                    "latency_class": model.latency_class,
                }
                for model in resolved_models
            ]
            return await _finish()

        async with conn.transaction():
            await _upsert_provider_cli_profile_impl(conn, resolved_spec)
            binding_reports: list[dict[str, Any]] = []
            profile_reports: list[dict[str, Any]] = []
            candidate_reports: list[dict[str, Any]] = []
            for model in resolved_models:
                profile_reports.append(
                    await _upsert_model_profile_impl(conn, spec=resolved_spec, model=model)
                )
                candidate_reports.append(
                    await _upsert_provider_model_candidate_impl(
                        conn,
                        spec=resolved_spec,
                        model=model,
                        decision_ref=decision_ref,
                    )
                )
                binding_reports.append(
                    await _upsert_model_profile_binding_impl(conn, spec=resolved_spec, model=model)
                )

            # No route_eligibility_states write here on purpose: the
            # `_candidate_is_admitted_for_runtime_profile` Python resolver
            # already special-cases `eligibility_status='rejected' AND
            # reason_code='no_live_probe_state' AND
            # source_window_refs contains transport:*` as ADMITTED. Combined
            # with the matching write in native_runtime_profile_sync that
            # translates projected-admitted route_states to literal
            # eligibility_status='admitted' on the runtime_profile_admitted_routes
            # row, the catalog SQL JOIN now sees the candidate as admitted
            # without needing a separate route_eligibility_states write
            # (which would require provider_policies FK setup).
            benchmark_rule_reports: list[dict[str, Any]] = []
            bound_market_models = 0
            if benchmark_step is not None and benchmark_step.status == "succeeded":
                benchmark_rule_reports, bound_market_models = await _apply_benchmark_plan_impl(
                    conn,
                    spec=resolved_spec,
                    benchmark_report=benchmark_report,
                )
            model_reports = [
                {
                    **candidate,
                    "model_profile_id": profile["model_profile_id"],
                    "binding_id": binding["model_profile_candidate_binding_id"],
                }
                for profile, candidate, binding in zip(
                    profile_reports,
                    candidate_reports,
                    binding_reports,
                    strict=True,
                )
            ]
        steps.append(
            ProviderOnboardingStepResult(
                step="registry_write",
                status="warning" if allow_provisional_registry_write else "succeeded",
                summary=(
                    f"Wrote provider profile plus {len(resolved_models)} model profile/candidate/binding set(s)"
                    if not allow_provisional_registry_write
                    else (
                        f"Provisioned provider profile plus {len(resolved_models)} model profile/candidate/binding set(s); "
                        "lane admission remains disabled until the live capacity probe succeeds"
                    )
                ),
                details={
                    "provider_slug": resolved_spec.provider_slug,
                    "selected_transport": resolved_spec.selected_transport,
                    "default_model": resolved_spec.default_model,
                    "provisional_write": allow_provisional_registry_write,
                    "capacity_probe_status": capacity_step.status if capacity_step is not None else None,
                    "capacity_probe_summary": capacity_step.summary if capacity_step is not None else None,
                    "model_profiles": profile_reports,
                    "candidates": candidate_reports,
                    "bindings": binding_reports,
                    "benchmark_rule_rows": benchmark_rule_reports,
                    "bound_market_models": bound_market_models,
                },
            )
        )

        await _record_transport_authority(
            router_supported=None,
            record_receipts=True,
        )

        # Refresh runtime_profile_admitted_routes for every native profile
        # so the newly-onboarded candidate's admission state lands without
        # waiting for the next process boot. Boot was the only caller of
        # this sync historically — onboarding-without-boot left the catalog
        # stale and made workflow submits fail with not_admitted reasons.
        try:
            from registry.native_runtime_profile_sync import (
                sync_native_runtime_profile_authority_async,
            )
            await sync_native_runtime_profile_authority_async(conn)
        except Exception:  # noqa: BLE001 — verification step also reflects state
            pass

        verification = await _verification_report(conn=conn, spec=resolved_spec, decision_ref=decision_ref)
        provider_report = dict(verification.get("provider_report") or {})
        selected_transport_supported = bool(verification.get("selected_transport_supported"))
        provider_report["selected_transport_supported"] = selected_transport_supported
        steps.append(
            ProviderOnboardingStepResult(
                step="verification",
                status="succeeded" if selected_transport_supported else "warning",
                summary=(
                    f"Verified {verification['model_visibility']['count']} candidate row(s) and "
                    f"{verification['model_profiles']['count']} model profile row(s)"
                    if selected_transport_supported
                    else (
                        f"Verified {verification['model_visibility']['count']} candidate row(s) and "
                        f"{verification['model_profiles']['count']} model profile row(s); "
                        "selected transport is registered but not admitted yet"
                    )
                ),
                details=verification,
            )
        )
        return await _finish()
    finally:
        await conn.close()


def run_provider_onboarding(
    *,
    database_url: str,
    spec: ProviderOnboardingSpec,
    dry_run: bool = False,
) -> ProviderOnboardingResult:
    if not database_url.strip():
        raise ValueError("database_url is required")
    return run_sync_safe(
        _run_provider_onboarding(
            database_url=database_url.strip(),
            spec=spec,
            dry_run=dry_run,
        )
    )
