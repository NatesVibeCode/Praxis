"""Operating model compiler.

Compiles natural language prose into structured operating models with
resolvable references. Uses IntentMatcher for vector/semantic search
and LLM for structured prose generation.

Sublayer modules:
  compiler_semantic   -- IntentMatcher interaction, embedder resolution, timeout
  compiler_llm        -- LLM calls, response parsing, context building, guards
  compiler_references -- regex-based reference extraction, resolution, job generation
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime.compile_artifacts import CompileArtifactError, CompileArtifactStore
from runtime.compile_reuse import module_surface_revision, stable_hash
from runtime.build_authority import apply_authority_bundle, build_authority_bundle
import runtime.compiler_components as _compiler_components
import runtime.compiler_output_builders as _compiler_output_builders
from runtime.compile_index import (
    CompileIndexAuthorityError,
    CompileIndexSnapshot,
    _load_integrations as _compile_index_load_integrations,
    _load_object_types as _compile_index_load_object_types,
    _load_reference_catalog as _compile_index_load_reference_catalog,
    load_compile_index_snapshot,
    refresh_compile_index,
)
from runtime.capability_catalog import load_capability_catalog
from runtime.definition_compile_kernel import (
    build_definition as _kernel_build_definition,
    detect_triggers as _kernel_detect_triggers,
)

from runtime.compiler_semantic import (
    IntentMatchTimeoutError as _IntentMatchTimeoutError,
    resolve_compiler_embedder as _resolve_compiler_embedder,
    run_intent_match as _run_intent_match,
    flatten_match_result as _flatten_match_result,
    composition_to_dict as _composition_to_dict,
)
from runtime.compiler_llm import (
    compiler_llm_enabled as _compiler_llm_enabled,
    build_llm_context as _build_llm_context,
    call_llm_compile as _call_llm_compile,
    guard_llm_compiled_output as _guard_llm_compiled_output,
    build_refinement_summary as _build_refinement_summary,
    derive_title,
)
from runtime.compiler_references import (
    extract_references as _extract_references,
    resolve_references as _resolve_references,
    generate_jobs as _generate_jobs,
    infer_agent_route as _infer_agent_route_impl,
    workflow_id_for_title as _workflow_id_for_title_impl,
)
from runtime.integrations.display_names import display_name_for_integration
from storage.postgres.validators import PostgresConfigurationError

logger = logging.getLogger(__name__)

_REFRESHABLE_COMPILE_INDEX_REASON_CODES = {
    "compile_index.snapshot_missing",
    "compile_index.snapshot_stale",
}
_COMPILER_ROUTE_HINTS_CACHE: tuple[tuple[str, str], ...] = ()
_COMPILER_SURFACE_REVISION_CACHE: str | None = None


def compile_prose(
    prose: str,
    title: str | None = None,
    *,
    enable_llm: bool | None = None,
    compile_index_ref: str | None = None,
    compile_surface_revision: str | None = None,
    compile_index_snapshot: CompileIndexSnapshot | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Compile user prose into a structured operating model."""
    clean_prose = (prose or "").strip()
    if not clean_prose:
        return _empty_result("Empty prose")

    errors: list[str] = []
    compile_index = compile_index_snapshot
    if compile_index is not None:
        if compile_index_ref is not None and compile_index.compile_index_ref != compile_index_ref:
            raise RuntimeError(
                "compile_index.snapshot_mismatch: provided compile_index_snapshot does not match compile_index_ref"
            )
        if (
            compile_surface_revision is not None
            and compile_index.compile_surface_revision != compile_surface_revision
        ):
            raise RuntimeError(
                "compile_index.snapshot_surface_mismatch: provided compile_index_snapshot does not match compile_surface_revision"
            )

    try:
        if conn is None:
            conn = _get_connection()
        if compile_index is None:
            compile_index = _load_compile_index_snapshot_with_auto_refresh(
                conn,
                compile_index_ref=compile_index_ref,
                compile_surface_revision=compile_surface_revision,
            )
    except CompileIndexAuthorityError as exc:
        logger.warning("Failed to load compile index snapshot: %s", exc)
        raise RuntimeError(f"{exc.reason_code}: {exc}") from exc
    except PostgresConfigurationError as exc:
        logger.warning("Failed to load compiler context: %s", exc)
        raise RuntimeError(f"{exc.reason_code}: {exc}") from exc
    except Exception as exc:
        logger.warning("Failed to load compiler context: %s", exc)
        raise RuntimeError(f"compile_index.load_failed: {exc}") from exc

    llm_requested = _compiler_llm_enabled() if enable_llm is None else enable_llm
    compile_provenance = _definition_compile_provenance(
        source_prose=clean_prose,
        title=title,
        llm_requested=llm_requested,
        compile_index=compile_index,
    )

    # Check for reusable artifact
    if conn is not None:
        artifact_store = CompileArtifactStore(conn)
        try:
            reusable_definition = artifact_store.load_reusable_artifact(
                artifact_kind="definition",
                input_fingerprint=compile_provenance["input_fingerprint"],
            )
        except CompileArtifactError as exc:
            logger.warning("Skipping reusable definition artifact: %s", exc)
            reusable_definition = None
        if reusable_definition is not None:
            definition = json.loads(json.dumps(reusable_definition.payload, default=str))
            return _finalize_compile_result(
                definition=definition,
                title=title,
                conn=conn,
                errors=errors,
                compile_index=compile_index,
                semantic_retrieval={"mode": "reused", "reason": "definition.compile.exact_input_match"},
                refinement={"mode": "reused", "reason": "definition.compile.exact_input_match", "llm_requested": llm_requested},
                reuse_provenance={
                    "artifact_kind": "definition",
                    "decision": "reused",
                    "reason_code": "definition.compile.exact_input_match",
                    "input_fingerprint": compile_provenance["input_fingerprint"],
                    "artifact_ref": reusable_definition.artifact_ref,
                    "revision_ref": reusable_definition.revision_ref,
                    "content_hash": reusable_definition.content_hash,
                    "decision_ref": reusable_definition.decision_ref,
                },
            )

    # Unpack compile context
    compile_context = compile_index.to_compile_context()
    catalog = list(compile_context.get("catalog") or [])
    integrations = list(compile_context.get("integrations") or [])
    object_types = list(compile_context.get("object_types") or [])
    capability_catalog = list(compile_context.get("capabilities") or [])
    route_hints = tuple(tuple(item) for item in compile_context.get("route_hints") or ())

    # Semantic retrieval
    matched_refs: list[dict[str, Any]] = []
    composition: dict[str, Any] = {}
    semantic_retrieval = {"mode": "unavailable", "reason": "compiler_context_unavailable"}
    if conn is not None:
        try:
            from runtime.intent_matcher import IntentMatcher

            embedder, semantic_retrieval = _resolve_compiler_embedder()
            if semantic_retrieval["mode"] != "degraded":
                matcher = IntentMatcher(conn, embedder=embedder)
                match_result, match_plan = _run_intent_match(matcher, clean_prose)
                matched_refs = _flatten_match_result(match_result)
                composition = _composition_to_dict(match_plan)
        except _IntentMatchTimeoutError as exc:
            logger.warning("IntentMatcher timed out during compile: %s", exc)
            errors.append(f"intent_match_timeout: {exc}")
            semantic_retrieval = {"mode": "degraded", "reason": f"intent_match_timeout: {exc}"}
        except Exception as exc:
            logger.warning("IntentMatcher unavailable: %s", exc)
            errors.append(f"intent_match_failed: {exc}")
            semantic_retrieval = {"mode": "degraded", "reason": f"intent_match_failed: {exc}"}

    # Build LLM context
    context = _build_llm_context(
        catalog=catalog,
        integrations=integrations,
        object_types=object_types,
        matched_refs=matched_refs,
        composition=composition,
        capabilities=capability_catalog,
        route_hints=route_hints,
        route_hints_cache=_COMPILER_ROUTE_HINTS_CACHE,
    )

    compiled = {
        "title": _derive_title(clean_prose, clean_prose),
        "prose": clean_prose,
        "authority": "",
        "sla": {},
    }

    # LLM compilation
    llm_error: str | None = None
    llm_succeeded = False
    llm_guard_reason: str | None = None
    if llm_requested:
        try:
            try:
                compiled = _call_llm_compile(
                    clean_prose,
                    context,
                    conn=conn,
                    hydrate_env=_hydrate_env_from_dotenv,
                    get_connection=_get_connection,
                )
            except TypeError as exc:
                message = str(exc)
                if "unexpected keyword argument 'hydrate_env'" not in message and "unexpected keyword argument 'get_connection'" not in message:
                    raise
                compiled = _call_llm_compile(clean_prose, context, conn=conn)
            llm_succeeded = True
        except Exception as exc:
            logger.warning("LLM compilation failed: %s", exc)
            llm_error = str(exc)
            errors.append(f"llm_compile_failed: {exc}")

    if llm_requested and llm_succeeded:
        compiled, llm_guard_reason = _guard_llm_compiled_output(clean_prose, compiled)
        if llm_guard_reason:
            errors.append(f"llm_compile_guarded: {llm_guard_reason}")

    compiled_prose = _as_text(compiled.get("prose")) or clean_prose
    authority = _as_text(compiled.get("authority"))
    sla = compiled.get("sla") if isinstance(compiled.get("sla"), dict) else {}

    # Reference extraction and resolution
    references = _extract_references(compiled_prose)
    resolved_references, unresolved = _resolve_references(
        references, catalog,
        route_hints=route_hints,
        route_hints_cache=_COMPILER_ROUTE_HINTS_CACHE,
    )
    provisional_jobs = _generate_jobs(
        compiled_prose, resolved_references,
        route_hints=route_hints,
        route_hints_cache=_COMPILER_ROUTE_HINTS_CACHE,
    )

    # Capability selection (inline delegation)
    capabilities = _compiler_components.select_capabilities(
        original_prose=clean_prose,
        compiled_prose=compiled_prose,
        compiled_capability_slugs=_as_string_list(compiled.get("capabilities")),
        references=resolved_references,
        jobs=provisional_jobs,
        catalog=capability_catalog,
    )

    # Definition building (inline delegation to kernel)
    definition = _kernel_build_definition(
        source_prose=clean_prose,
        compiled_prose=compiled_prose,
        references=resolved_references,
        capabilities=capabilities,
        authority=authority,
        sla=sla,
    )

    # Execution setup, surface manifest, build receipt (inline delegation)
    setup_title = title or _as_text(compiled.get("title")) or _derive_title(clean_prose, compiled_prose)
    execution_setup = _compiler_output_builders.build_execution_setup(
        title=setup_title,
        definition=definition,
        jobs=provisional_jobs,
        unresolved=unresolved,
        conn=conn,
        route_hints=route_hints,
    )
    surface_manifest = _compiler_output_builders.build_surface_manifest(
        execution_setup=execution_setup,
        definition=definition,
        unresolved=unresolved,
    )
    build_receipt = _compiler_output_builders.build_build_receipt(
        execution_setup=execution_setup,
        surface_manifest=surface_manifest,
        definition=definition,
        unresolved=unresolved,
    )

    definition["execution_setup"] = execution_setup
    definition["surface_manifest"] = surface_manifest
    definition["build_receipt"] = build_receipt
    build_receipt["data_audit"] = _compiler_output_builders.build_data_audit(
        definition=definition,
        execution_setup=execution_setup,
        surface_manifest=surface_manifest,
        unresolved=unresolved,
    )
    build_receipt["data_gaps"] = _compiler_output_builders.build_data_gaps(
        execution_setup=execution_setup,
        definition=definition,
        unresolved=unresolved,
    )
    definition["compile_provenance"] = compile_provenance

    # Persist artifact
    if conn is not None:
        try:
            artifact_store = CompileArtifactStore(conn)
            artifact_store.record_definition(
                definition=definition,
                authority_refs=[cap.get("slug", "") for cap in capabilities if isinstance(cap, dict) and cap.get("slug")],
                decision_ref=f"decision.compile.definition.{definition['definition_revision']}",
                input_fingerprint=compile_provenance["input_fingerprint"],
            )
        except Exception as exc:
            logger.warning("Failed to persist definition compile artifact: %s", exc)
            errors.append(f"definition_artifact_persist_failed: {exc}")

    return _finalize_compile_result(
        definition=definition,
        title=title,
        conn=conn,
        errors=errors,
        compile_index=compile_index,
        semantic_retrieval=semantic_retrieval,
        refinement=_build_refinement_summary(
            source_prose=clean_prose,
            compiled=compiled,
            llm_requested=llm_requested,
            llm_succeeded=llm_succeeded,
            llm_error=llm_error,
            llm_guard_reason=llm_guard_reason,
        ),
        reuse_provenance={
            "artifact_kind": "definition",
            "decision": "compiled",
            "reason_code": "definition.compile.miss",
            "input_fingerprint": compile_provenance["input_fingerprint"],
        },
        matched_building_blocks=matched_refs,
        composition_plan=composition,
    )


def _empty_result(error: str, prose: str = "") -> dict[str, Any]:
    compiled_prose = prose
    definition = _kernel_build_definition(
        source_prose=prose,
        compiled_prose=compiled_prose,
        references=[],
        capabilities=[],
        authority="",
        sla={},
    )
    setup_title = _derive_title(prose, compiled_prose)
    execution_setup = _compiler_output_builders.build_execution_setup(
        title=setup_title,
        definition=definition,
        jobs=[],
        unresolved=[],
        conn=None,
    )
    surface_manifest = _compiler_output_builders.build_surface_manifest(
        execution_setup=execution_setup,
        definition=definition,
        unresolved=[],
    )
    build_receipt = _compiler_output_builders.build_build_receipt(
        execution_setup=execution_setup,
        surface_manifest=surface_manifest,
        definition=definition,
        unresolved=[],
    )
    definition["execution_setup"] = execution_setup
    definition["surface_manifest"] = surface_manifest
    definition["build_receipt"] = build_receipt
    build_receipt["data_audit"] = _compiler_output_builders.build_data_audit(
        definition=definition,
        execution_setup=execution_setup,
        surface_manifest=surface_manifest,
        unresolved=[],
    )
    build_receipt["data_gaps"] = _compiler_output_builders.build_data_gaps(
        execution_setup=execution_setup,
        definition=definition,
        unresolved=[],
    )
    return _finalize_compile_result(
        definition=definition,
        title=None,
        conn=None,
        errors=[error],
        compile_index=None,
        semantic_retrieval={"mode": "not_attempted", "reason": "empty_input"},
        refinement={
            "requested": False,
            "applied": False,
            "used_llm": False,
            "status": "deterministic",
            "message": "Compile returned the deterministic definition artifact.",
            "reason": "empty_input",
        },
        reuse_provenance=None,
    )


def _finalize_compile_result(
    *,
    definition: dict[str, Any],
    title: str | None,
    conn: Any | None,
    errors: list[str],
    compile_index: CompileIndexSnapshot | None,
    semantic_retrieval: dict[str, Any],
    refinement: dict[str, Any],
    reuse_provenance: dict[str, Any] | None,
    matched_building_blocks: list[dict[str, Any]] | None = None,
    composition_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from runtime.build_planning_contract import (
        build_candidate_resolution_manifest,
        build_intent_brief,
        build_reviewable_plan,
    )

    compiled_spec: dict[str, Any] | None = None
    planning_notes: list[str] = []
    authority_bundle = build_authority_bundle(definition)
    projection_state = _as_text(authority_bundle.get("projection_status", {}).get("state")) or "blocked"
    if projection_state == "ready":
        planning_notes.append(
            "Compile produced bootstrap planning state only. Review and harden the candidate manifest before generating execution authority."
        )
    else:
        planning_notes.append(
            "Compile produced bootstrap planning state with unresolved authority. Review and resolve blockers before hardening."
        )

    hydrated_definition = apply_authority_bundle(definition, compiled_spec=compiled_spec)
    authority_bundle = build_authority_bundle(hydrated_definition, compiled_spec=compiled_spec)
    projection_status = authority_bundle["projection_status"]
    blocking_issues = [
        issue
        for issue in authority_bundle["build_issues"]
        if _as_text(issue.get("severity")) == "blocking"
    ]

    # Emit compilation event to the service bus
    if conn is not None:
        try:
            from runtime.event_log import emit, CHANNEL_BUILD_STATE, EVENT_COMPILATION
            workflow_id = _as_text(hydrated_definition.get("workflow_id")) or ""
            emit(
                conn,
                channel=CHANNEL_BUILD_STATE,
                event_type=EVENT_COMPILATION,
                entity_id=workflow_id,
                entity_kind="workflow",
                payload={
                    "build_state": _as_text(projection_status.get("state")) or "blocked",
                    "blocker_count": len(blocking_issues),
                    "has_spec": compiled_spec is not None,
                },
                emitted_by="compiler",
            )
        except Exception:
            pass  # compilation itself must not fail because of event log

    enriched_ledger = _enrich_binding_ledger(authority_bundle["binding_ledger"], conn)
    candidate_resolution_manifest = build_candidate_resolution_manifest(
        definition=hydrated_definition,
        workflow_id=_as_text(hydrated_definition.get("workflow_id")) or None,
        conn=conn,
        compiled_spec=compiled_spec,
    )
    reviewable_plan = build_reviewable_plan(
        definition=hydrated_definition,
        workflow_id=_as_text(hydrated_definition.get("workflow_id")) or None,
        conn=conn,
        compiled_spec=compiled_spec,
        candidate_manifest=candidate_resolution_manifest,
    )
    intent_brief = build_intent_brief(
        definition=hydrated_definition,
        workflow_id=_as_text(hydrated_definition.get("workflow_id")) or None,
        conn=conn,
    )

    return {
        "intent_brief": intent_brief,
        "definition": hydrated_definition,
        "unresolved": _unresolved_reference_slugs(hydrated_definition),
        "error": "; ".join(error for error in errors if error) if errors else None,
        "compile_index": compile_index.summary() if compile_index is not None else None,
        "semantic_retrieval": semantic_retrieval,
        "refinement": refinement,
        "reuse_provenance": reuse_provenance,
        "build_state": _as_text(projection_status.get("state")) or "blocked",
        "build_blockers": blocking_issues,
        "build_graph": authority_bundle["build_graph"],
        "binding_ledger": enriched_ledger,
        "import_snapshots": authority_bundle["import_snapshots"],
        "authority_attachments": authority_bundle["authority_attachments"],
        "build_issues": authority_bundle["build_issues"],
        "projection_status": projection_status,
        "planning_notes": planning_notes,
        "compiled_spec": compiled_spec,
        "compiled_spec_projection": authority_bundle["compiled_spec_projection"],
        "candidate_resolution_manifest": candidate_resolution_manifest,
        "reviewable_plan": reviewable_plan,
        "matched_building_blocks": matched_building_blocks or [],
        "composition_plan": composition_plan or {},
    }


def _enrich_binding_ledger(
    ledger: list[dict[str, Any]],
    conn: Any | None,
) -> list[dict[str, Any]]:
    """Add integration/capability enrichment to binding targets."""
    if not ledger or conn is None:
        return ledger

    integration_lookup: dict[str, dict[str, Any]] = {}
    try:
        rows = conn.execute(
            "SELECT id, name, description, provider, auth_status FROM integration_registry"
        )
        for row in rows or []:
            iid = _as_text(row.get("id"))
            if iid:
                item = dict(row)
                integration_lookup[iid] = {
                    "integration_name": display_name_for_integration(item),
                    "provider": _as_text(item.get("provider")) or "",
                    "auth_status": _as_text(item.get("auth_status")) or "unknown",
                    "description": _as_text(item.get("description")) or "",
                }
                integration_lookup[f"@{iid}"] = integration_lookup[iid]
    except Exception:
        return ledger

    def _enrich_target(target: dict[str, Any]) -> dict[str, Any]:
        ref = _as_text(target.get("target_ref")) or ""
        parts = ref.lstrip("@").split("/", 1)
        base_ref = f"@{parts[0]}" if parts else ref
        enrichment = integration_lookup.get(ref) or integration_lookup.get(base_ref)
        if enrichment:
            return {**target, "enrichment": enrichment}
        return target

    enriched: list[dict[str, Any]] = []
    for entry in ledger:
        entry = dict(entry)
        if entry.get("candidate_targets"):
            entry["candidate_targets"] = [_enrich_target(t) for t in entry["candidate_targets"]]
        if entry.get("accepted_target"):
            entry["accepted_target"] = _enrich_target(entry["accepted_target"])
        enriched.append(entry)
    return enriched


def _definition_compile_provenance(
    *,
    source_prose: str,
    title: str | None,
    llm_requested: bool,
    compile_index: CompileIndexSnapshot,
) -> dict[str, Any]:
    surface_revision = _compiler_surface_revision()
    input_payload = {
        "artifact_kind": "definition",
        "surface_revision": surface_revision,
        "source_prose_hash": hashlib.sha256(source_prose.encode("utf-8")).hexdigest(),
        "title": title or "",
        "llm_requested": llm_requested,
        "compile_index_ref": compile_index.compile_index_ref,
        "compile_surface_revision": compile_index.compile_surface_revision,
        "repo_fingerprint": compile_index.repo_fingerprint,
        "source_fingerprints": dict(compile_index.source_fingerprints),
    }
    return {
        "artifact_kind": "definition",
        "input_fingerprint": stable_hash(input_payload),
        "surface_revision": surface_revision,
        "compile_index_ref": compile_index.compile_index_ref,
        "compile_surface_revision": compile_index.compile_surface_revision,
        "repo_fingerprint": compile_index.repo_fingerprint,
        "source_fingerprints": dict(compile_index.source_fingerprints),
        "source_prose_hash": input_payload["source_prose_hash"],
        "title": title or "",
        "llm_requested": llm_requested,
    }


def _fallback_compile_index_snapshot(*, reason: str) -> CompileIndexSnapshot:
    now = datetime.now(timezone.utc)
    repo_root = str(_compiler_repo_root())
    surface_revision = _compiler_surface_revision()
    return CompileIndexSnapshot(
        schema_version=1,
        compile_index_ref="compile_index:fallback",
        compile_surface_revision=surface_revision,
        compile_surface_name="compiler",
        repo_root=repo_root,
        repo_fingerprint="fallback",
        repo_info={"repo_root": repo_root, "repo_fingerprint": "fallback"},
        surface_manifest={"surface_revision": surface_revision},
        source_fingerprints={},
        source_counts={
            "catalog": 0,
            "integrations": 0,
            "object_types": 0,
            "capabilities": 0,
            "route_hints": 0,
        },
        decision_ref="compiler.fallback",
        refresh_count=0,
        refreshed_at=now,
        stale_after_at=now,
        freshness_state="fallback",
        freshness_reason=reason,
        reference_catalog=(),
        integration_registry=(),
        object_types=(),
        compiler_route_hints=(),
        capability_catalog=(),
        payload={"fallback": True, "reason": reason},
    )


def _unresolved_reference_slugs(definition: dict[str, Any]) -> list[str]:
    references = definition.get("references")
    if not isinstance(references, list):
        return []
    unresolved: list[str] = []
    for reference in references:
        if not isinstance(reference, dict):
            continue
        slug = _as_text(reference.get("slug"))
        if not slug:
            continue
        if reference.get("resolved") is False or not _as_text(reference.get("resolved_to")):
            unresolved.append(slug)
    return sorted(set(unresolved))


def _load_compile_index_snapshot_with_auto_refresh(
    conn: Any,
    *,
    compile_index_ref: str | None,
    compile_surface_revision: str | None,
) -> CompileIndexSnapshot:
    repo_root = _compiler_repo_root()
    try:
        return load_compile_index_snapshot(
            conn,
            snapshot_ref=compile_index_ref,
            surface_revision=compile_surface_revision,
            surface_name="compiler",
            require_fresh=True,
            repo_root=repo_root,
        )
    except CompileIndexAuthorityError as exc:
        if compile_index_ref is not None or compile_surface_revision is not None:
            raise
        if exc.reason_code not in _REFRESHABLE_COMPILE_INDEX_REASON_CODES:
            raise
        return refresh_compile_index(
            conn,
            repo_root=repo_root,
            surface_name="compiler",
        )


def _load_compiler_route_hints(conn: Any) -> tuple[tuple[str, str], ...]:
    global _COMPILER_ROUTE_HINTS_CACHE
    if conn is None:
        return _COMPILER_ROUTE_HINTS_CACHE
    rows = conn.execute(
        """
        SELECT hint_text, route_slug
          FROM compiler_route_hints
         WHERE enabled = TRUE
         ORDER BY priority ASC, hint_text ASC
        """
    )
    _COMPILER_ROUTE_HINTS_CACHE = tuple(
        (
            str(row["hint_text"]).strip().lower(),
            str(row["route_slug"]).strip(),
        )
        for row in rows or []
        if str(row.get("hint_text") or "").strip() and str(row.get("route_slug") or "").strip()
    )
    return _COMPILER_ROUTE_HINTS_CACHE


def _get_connection():
    _hydrate_env_from_dotenv()
    from storage.postgres.connection import ensure_postgres_available

    return ensure_postgres_available()


def _hydrate_env_from_dotenv() -> None:
    source = _read_compiler_env_file(_compiler_repo_root() / ".env")
    for key, value in source.items():
        if value and not os.environ.get(key, "").strip():
            os.environ[key] = value


def _compiler_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_compiler_env_file(path: Path) -> dict[str, str]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return {}

    parsed: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not key:
            continue
        cleaned = value.strip()
        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
            cleaned = cleaned[1:-1]
        parsed[key] = cleaned
    return parsed


def _build_capability_catalog(integrations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _compiler_components.build_capability_catalog(integrations)


def _load_reference_catalog(conn: Any) -> list[dict[str, Any]]:
    return _compile_index_load_reference_catalog(conn)


def _load_integrations(conn: Any) -> list[dict[str, Any]]:
    return _compile_index_load_integrations(conn)


def _load_object_types(conn: Any) -> list[dict[str, Any]]:
    return _compile_index_load_object_types(conn)


def _compiler_surface_revision() -> str:
    global _COMPILER_SURFACE_REVISION_CACHE
    if _COMPILER_SURFACE_REVISION_CACHE is None:
        _COMPILER_SURFACE_REVISION_CACHE = module_surface_revision(
            __file__,
            Path(__file__).with_name("definition_compile_kernel.py"),
        )
    return _COMPILER_SURFACE_REVISION_CACHE


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_as_text(item) for item in value if _as_text(item)]


def _slugify(value: Any) -> str:
    text = _as_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9/_-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-/")


def _derive_title(prose: str, compiled_prose: str) -> str:
    return derive_title(prose, compiled_prose)


def _workflow_id_for_title(title: str) -> str:
    return _workflow_id_for_title_impl(title)


def _infer_agent_route(
    slug: str,
    reference: dict[str, Any] | None = None,
    *,
    route_hints: tuple[tuple[str, str], ...] = (),
) -> str:
    return _infer_agent_route_impl(
        slug, reference,
        route_hints=route_hints,
        route_hints_cache=_COMPILER_ROUTE_HINTS_CACHE,
    )


# Backward-compatible aliases for test and external callers
_detect_triggers = _kernel_detect_triggers


__all__ = ["compile_prose"]
