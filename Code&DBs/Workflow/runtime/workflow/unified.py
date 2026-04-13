"""Unified workflow runtime facade.

All implementation lives in private submodules:
  _shared.py           — constants, utilities
  _routing.py          — route selection, touch-key conflict
  _context_building.py — execution context/bundle/packet building
  _workflow_state.py   — workflow authority, dependency state management
  _admission.py        — submission pipeline
  _claiming.py         — claim, complete, reap
  _status.py           — status, health, recovery queries
  _execution_core.py   — execute_job dispatch
  _worker_loop.py      — worker loop, notification listener
"""
from __future__ import annotations

# Standard library re-exports (tests patch these on the unified module)
import threading  # noqa: F401
import time  # noqa: F401

# ── Constants and utilities ──────────────────────────────────────────────
from ._shared import (  # noqa: F401
    STALE_REAPER_QUERY,
    _ACTIVE_JOB_STATUSES,
    _BLOCKING_PARENT_STATUSES,
    _CIRCUIT_BREAKERS,
    _CIRCUIT_BREAKERS_UNSET,
    _DEFAULT_NATIVE_RUNTIME_PROFILE_REF,
    _DEFAULT_NATIVE_WORKSPACE_REF,
    _MAX_INT32,
    _READ_ONLY_MODE,
    _TERMINAL_JOB_STATUSES,
    _WORKFLOW_REPLAYABLE_RUN_STATES,
    _WORKFLOW_TERMINAL_STATES,
    _WRITE_MODE,
    _circuit_breakers,
    _definition_version_for_hash,
    _job_artifact_basename,
    _json_loads_maybe,
    _json_safe,
    _normalize_paths,
    _normalize_string_list,
    _slugify,
    _workflow_id_for_spec,
    _workflow_run_envelope,
)

# ── Routing and touch-key ────────────────────────────────────────────────
from ._routing import (  # noqa: F401
    _build_request_envelope,
    _derive_touch_keys,
    _failure_zone_lookup,
    _job_has_touch_conflict,
    _job_touch_entries,
    _record_task_route_outcome,
    _route_candidates,
    _runtime_profile_admitted_route_candidates,
    _runtime_profile_ref_for_run,
    _runtime_profile_ref_from_spec,
    _select_claim_route,
    _touch_entry,
    _touches_conflict,
    _workspace_ref_from_spec,
)

# ── Context building and packets ─────────────────────────────────────────
from ._context_building import (  # noqa: F401
    _build_execution_packet,
    _build_job_execution_bundles,
    _build_job_execution_context_shards,
    _capture_submission_baseline_if_required,
    _execution_model_messages,
    _execution_packet_lineage_payload,
    _extract_verification_paths,
    _job_verify_refs,
    _persist_runtime_context_for_job,
    _render_execution_context_shard,
    _resolve_job_prompt_authority,
    _runtime_execution_bundle,
    _runtime_execution_context_shard,
    _shadow_packet_inspection_from_rows,
    _submission_required_for_bundle,
    _terminal_failure_classification,
    _verification_artifact_refs,
)

# ── Workflow state and authority ─────────────────────────────────────────
from ._workflow_state import (  # noqa: F401
    _block_descendants,
    _ensure_workflow_authority,
    _recompute_workflow_run_state,
    _release_ready_children,
    _reset_blocked_descendants_for_retry,
    _workflow_row_reuse_authority,
)

# ── Submission ───────────────────────────────────────────────────────────
from ._admission import (  # noqa: F401
    IdempotencyConflict,
    _retry_packet_reuse_provenance,
    load_execution_packets,
    submit_workflow,
    submit_workflow_inline,
)

# ── Claiming and completion ──────────────────────────────────────────────
from ._claiming import (  # noqa: F401
    _submission_list_latest_submission_summaries_for_run,
    _submission_state_by_job_label,
    claim_one,
    complete_job,
    mark_running,
    reap_stale_claims,
    reap_stale_runs,
)

# ── Status and observability ─────────────────────────────────────────────
from ._status import (  # noqa: F401
    cancel_run,
    get_run_status,
    inspect_job,
    retry_job,
    summarize_run_health,
    summarize_run_recovery,
    wait_for_run,
)

# ── Execution ────────────────────────────────────────────────────────────
from ._execution_core import (  # noqa: F401
    _build_platform_context,
    _execute_api,
    _execute_cli,
    _write_job_receipt,
    _write_output,
    execute_job,
)

# ── Worker loop ──────────────────────────────────────────────────────────
from ._worker_loop import (  # noqa: F401
    _WorkerNotificationListener,
    run_worker_loop,
)
