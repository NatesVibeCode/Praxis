"""Workflow capability discovery and shared singleton state."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ..route_outcomes import RouteOutcomeStore

__all__ = [
    "CIRCUIT_BREAKERS",
    "COST_TRACKER",
    "WORKFLOW_CAPABILITIES",
    "WORKFLOW_HISTORY",
    "WORKFLOW_METRICS_VIEW",
    "LOAD_BALANCER",
    "ROUTE_OUTCOMES",
    "TRUST_SCORER",
    "WorkflowCapabilities",
    "get_route_outcomes",
]

_log = logging.getLogger("workflow.capabilities")


@dataclass(frozen=True)
class WorkflowCapabilities:
    """Explicit registry of available workflow subsystems."""

    receipt_writer: Optional[Callable] = None
    workflow_history: Optional[Callable] = None
    cost_tracker: Optional[Callable] = None
    trust_scorer: Optional[Callable] = None
    metrics_view: Optional[Callable] = None
    route_resolver: Optional[Callable] = None
    route_decision_type: Optional[type] = None
    capability_inferrer: Optional[Callable] = None
    capability_resolver: Optional[Callable] = None
    circuit_breakers: Optional[Callable] = None
    completion_notifier: Optional[Callable] = None
    run_control: Optional[Callable] = None
    failure_classifier: Optional[Callable] = None
    load_balancer: Optional[Callable] = None
    friction_ledger: Optional[Callable] = None
    obs_hub: Optional[Callable] = None
    result_cache: Optional[Callable] = None
    event_logger: Optional[Callable] = None
    event_type_started: str = "workflow.started"
    event_type_completed: str = "workflow.completed"
    event_type_failed: str = "workflow.failed"


def _postgres_singleton(factory: Callable[[Any], Any]) -> Callable[[], Any]:
    from storage.postgres import ensure_postgres_available

    instance = None

    def _get_instance():
        nonlocal instance
        if instance is None:
            instance = factory(ensure_postgres_available())
        return instance

    return _get_instance


def _bootstrap_capabilities() -> WorkflowCapabilities:
    """Discover available workflow subsystems at startup."""

    kwargs: dict[str, Any] = {}
    available: list[str] = []
    missing: list[str] = []

    try:
        from ..receipt_store import write_receipt as _pg_write

        def _write_receipt(result):
            _pg_write(result.to_json())

        kwargs["receipt_writer"] = _write_receipt
        available.append("receipt_store")
    except ImportError:
        missing.append("receipt_store")

    try:
        from ..workflow_status import get_workflow_history

        kwargs["workflow_history"] = get_workflow_history
        available.append("workflow_status")
    except ImportError:
        missing.append("workflow_status")

    try:
        from ..cost_tracker import get_cost_tracker

        kwargs["cost_tracker"] = get_cost_tracker
        available.append("cost_tracker")
    except ImportError:
        missing.append("cost_tracker")

    try:
        from ..trust_scoring import get_trust_scorer

        kwargs["trust_scorer"] = get_trust_scorer
        available.append("trust_scoring")
    except ImportError:
        missing.append("trust_scoring")

    try:
        from ..observability import get_workflow_metrics_view

        kwargs["metrics_view"] = get_workflow_metrics_view
        available.append("observability")
    except ImportError:
        missing.append("observability")

    try:
        from ..auto_router import RouteDecision, resolve_route_from_db

        kwargs["route_resolver"] = resolve_route_from_db
        kwargs["route_decision_type"] = RouteDecision
        available.append("auto_router")
    except ImportError:
        missing.append("auto_router")

    try:
        from ..capability_router import infer_capabilities, resolve_by_capability

        kwargs["capability_inferrer"] = infer_capabilities
        kwargs["capability_resolver"] = resolve_by_capability
        available.append("capability_router")
    except ImportError:
        missing.append("capability_router")

    try:
        from ..circuit_breaker import get_circuit_breakers

        kwargs["circuit_breakers"] = get_circuit_breakers
        available.append("circuit_breaker")
    except ImportError:
        missing.append("circuit_breaker")

    try:
        from ..notifications import notify_workflow_complete

        kwargs["completion_notifier"] = notify_workflow_complete
        available.append("notifications")
    except ImportError:
        missing.append("notifications")

    try:
        from ..run_control import get_run_control

        kwargs["run_control"] = get_run_control
        available.append("run_control")
    except ImportError:
        missing.append("run_control")

    try:
        from ..failure_classifier import classify_failure

        kwargs["failure_classifier"] = classify_failure
        available.append("failure_classifier")
    except ImportError:
        missing.append("failure_classifier")

    try:
        from ..load_balancer import get_load_balancer

        kwargs["load_balancer"] = get_load_balancer
        available.append("load_balancer")
    except ImportError:
        missing.append("load_balancer")

    try:
        from ..friction_ledger import FrictionLedger

        kwargs["friction_ledger"] = _postgres_singleton(FrictionLedger)
        available.append("friction_ledger")
    except Exception:
        missing.append("friction_ledger")

    try:
        from ..observability_hub import ObservabilityHub

        kwargs["obs_hub"] = _postgres_singleton(ObservabilityHub)
        available.append("observability_hub")
    except Exception:
        missing.append("observability_hub")

    try:
        from ..result_cache import get_result_cache

        kwargs["result_cache"] = get_result_cache
        available.append("result_cache")
    except ImportError:
        missing.append("result_cache")

    try:
        from ..event_log import (
            EVENT_TYPE_WORKFLOW_COMPLETED,
            EVENT_TYPE_WORKFLOW_FAILED,
            EVENT_TYPE_WORKFLOW_STARTED,
            log_event,
        )

        kwargs["event_logger"] = log_event
        kwargs["event_type_started"] = EVENT_TYPE_WORKFLOW_STARTED
        kwargs["event_type_completed"] = EVENT_TYPE_WORKFLOW_COMPLETED
        kwargs["event_type_failed"] = EVENT_TYPE_WORKFLOW_FAILED
        available.append("event_log")
    except ImportError:
        missing.append("event_log")

    caps = WorkflowCapabilities(**kwargs)
    _log.info(
        "workflow capabilities: %d available, %d missing | available=%s | missing=%s",
        len(available),
        len(missing),
        ", ".join(available) or "(none)",
        ", ".join(missing) or "(none)",
    )
    return caps


def _bootstrap_singleton(factory: Callable[[], Any] | None) -> Any | None:
    if factory is None:
        return None
    try:
        return factory()
    except Exception:
        return None


WORKFLOW_CAPABILITIES = _bootstrap_capabilities()
ROUTE_OUTCOMES = RouteOutcomeStore()
CIRCUIT_BREAKERS = _bootstrap_singleton(WORKFLOW_CAPABILITIES.circuit_breakers)
WORKFLOW_HISTORY = _bootstrap_singleton(WORKFLOW_CAPABILITIES.workflow_history)
COST_TRACKER = _bootstrap_singleton(WORKFLOW_CAPABILITIES.cost_tracker)
TRUST_SCORER = _bootstrap_singleton(WORKFLOW_CAPABILITIES.trust_scorer)
LOAD_BALANCER = _bootstrap_singleton(WORKFLOW_CAPABILITIES.load_balancer)
WORKFLOW_METRICS_VIEW = _bootstrap_singleton(WORKFLOW_CAPABILITIES.metrics_view)


def get_route_outcomes() -> RouteOutcomeStore:
    """Return the shared route outcome store for status and diagnosis."""

    return ROUTE_OUTCOMES
