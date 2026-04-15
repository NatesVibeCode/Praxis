"""Shared lazy subsystem container for repo-local HTTP and MCP surfaces.

Delegates boot and lifecycle to _boot.py and _lifecycle.py while preserving
the full get_* API surface that inheritors depend on.
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
from pathlib import Path
from typing import Any

from storage.postgres import PostgresConfigurationError

from ._boot import create_pg_conn, ensure_workflow_on_path, sync_registries
from ._lifecycle import LifecycleManager
from ._workflow_database import workflow_database_env_for_repo


class _BaseSubsystems:
    """Lazily construct shared subsystems behind one authority seam."""

    def __init__(
        self,
        *,
        repo_root: Path,
        workflow_root: Path,
        receipts_dir: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._repo_root = repo_root
        self._workflow_root = workflow_root
        self._lifecycle = LifecycleManager()

        self._initialized = False
        self._obs_hub = None
        self._bug_tracker = None
        self._operator_panel = None
        self._knowledge_graph = None
        self._staleness_detector = None
        self._wave_orchestrator = None
        self._receipt_ingester = None
        self._quality_materializer = None
        self._health_mod = None
        self._constraint_ledger = None
        self._friction_ledger = None
        self._self_healer = None
        self._artifact_store = None
        self._governance_filter = None
        self._heartbeat_runner = None
        self._memory_engine = None
        self._session_carry_mgr = None
        self._intent_matcher = None
        self._manifest_generator = None
        self._module_indexer = None
        self._embedding_service = None
        self._notification_consumer = None
        self._pg_conn = None

        self.receipts_dir = receipts_dir
        self._maybe_startup_wiring()

    def _ensure_init(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        os.makedirs(self.receipts_dir, exist_ok=True)

    def _ensure_workflow_root_on_path(self) -> None:
        ensure_workflow_on_path(self._workflow_root)

    def _artifact_dir(self, name: str) -> str:
        path = self._repo_root / "artifacts" / name
        os.makedirs(path, exist_ok=True)
        return str(path)

    def _postgres_env(self) -> dict[str, str]:
        return workflow_database_env_for_repo(self._repo_root)

    def _handle_reference_catalog_sync_error(self, exc: Exception) -> None:
        del exc

    def _handle_integration_registry_sync_error(self, exc: Exception) -> None:
        del exc

    def _handle_startup_wiring_error(self, exc: Exception) -> None:
        if isinstance(exc, PermissionError):
            self._logger.debug("startup wiring skipped under sandbox constraints: %s", exc)
            return
        if (
            isinstance(exc, PostgresConfigurationError)
            and exc.reason_code == "postgres.authority_unavailable"
        ):
            self._logger.debug("startup wiring skipped under sandbox constraints: %s", exc)
            return
        self._logger.warning("startup wiring skipped: %s", exc)

    def _should_auto_startup_wiring(self) -> bool:
        if os.environ.get("PRAXIS_DISABLE_STARTUP_WIRING") == "1":
            return False
        return "pytest" not in sys.modules

    def _should_start_heartbeat_background(self) -> bool:
        return True

    def _maybe_startup_wiring(self) -> None:
        if self._lifecycle.started or not self._should_auto_startup_wiring():
            return
        try:
            self.get_pg_conn()
            if self._should_start_heartbeat_background():
                self._lifecycle.start_heartbeat(self.get_heartbeat_runner())
        except Exception as exc:
            self._handle_startup_wiring_error(exc)

    def _build_bug_tracker(self):
        from runtime.bug_tracker import BugTracker
        return BugTracker(self.get_pg_conn())

    def _build_heartbeat_runner(self):
        from runtime.heartbeat_runner import HeartbeatRunner
        return HeartbeatRunner(
            conn=self.get_pg_conn(),
            embedder=self.get_embedding_service(),
        )

    def get_pg_conn(self):
        if self._pg_conn is None:
            self._ensure_workflow_root_on_path()
            self._pg_conn = create_pg_conn(
                repo_root=self._repo_root,
                workflow_root=self._workflow_root,
                env=self._postgres_env(),
            )
            sync_registries(self._pg_conn)
        return self._pg_conn

    def get_obs_hub(self):
        self._ensure_init()
        if self._obs_hub is None:
            from runtime.observability_hub import ObservabilityHub
            self._obs_hub = ObservabilityHub(self.get_pg_conn())
        return self._obs_hub

    def get_bug_tracker(self):
        self._ensure_init()
        if self._bug_tracker is None:
            self._bug_tracker = self._build_bug_tracker()
        return self._bug_tracker

    def get_bug_tracker_mod(self):
        import runtime.bug_tracker as _mod
        return _mod

    def get_operator_panel(self):
        self._ensure_init()
        if self._operator_panel is None:
            from runtime.operator_panel import OperatorPanel
            self._operator_panel = OperatorPanel()
        return self._operator_panel

    def get_operator_panel_mod(self):
        import runtime.operator_panel as _mod
        return _mod

    def get_knowledge_graph(self):
        self._ensure_init()
        if self._knowledge_graph is None:
            self._ensure_workflow_root_on_path()
            from memory.knowledge_graph import KnowledgeGraph
            self._knowledge_graph = KnowledgeGraph(
                conn=self.get_pg_conn(),
                embedder=self.get_embedding_service(),
            )
        return self._knowledge_graph

    def get_staleness_detector(self):
        self._ensure_init()
        if self._staleness_detector is None:
            from runtime.staleness_detector import StalenessDetector
            self._staleness_detector = StalenessDetector()
        return self._staleness_detector

    def get_wave_orchestrator(self):
        self._ensure_init()
        if self._wave_orchestrator is None:
            from runtime.wave_orchestrator import WaveOrchestrator
            self._wave_orchestrator = WaveOrchestrator("default")
        return self._wave_orchestrator

    def get_wave_orchestrator_mod(self):
        import runtime.wave_orchestrator as _mod
        return _mod

    def get_receipt_ingester(self):
        self._ensure_init()
        if self._receipt_ingester is None:
            from runtime.observability_hub import ReceiptIngester
            self._receipt_ingester = ReceiptIngester(self.receipts_dir)
        return self._receipt_ingester

    def get_quality_materializer(self):
        self._ensure_init()
        if self._quality_materializer is None:
            from runtime.quality_views import QualityViewMaterializer
            self._quality_materializer = QualityViewMaterializer(self.get_pg_conn())
        return self._quality_materializer

    def get_quality_views_mod(self):
        import runtime.quality_views as _mod
        return _mod

    def get_constraint_ledger(self):
        self._ensure_init()
        if self._constraint_ledger is None:
            from runtime.constraint_ledger import ConstraintLedger
            self._constraint_ledger = ConstraintLedger(
                self.get_pg_conn(),
                self.get_embedding_service(),
            )
        return self._constraint_ledger

    def get_constraint_miner(self):
        from runtime.constraint_ledger import ConstraintMiner
        return ConstraintMiner()

    def get_friction_ledger(self):
        self._ensure_init()
        if self._friction_ledger is None:
            from runtime.friction_ledger import FrictionLedger
            self._friction_ledger = FrictionLedger(
                self.get_pg_conn(),
                self.get_embedding_service(),
            )
        return self._friction_ledger

    def get_self_healer(self):
        self._ensure_init()
        if self._self_healer is None:
            from runtime.self_healing import SelfHealingOrchestrator
            self._self_healer = SelfHealingOrchestrator()
        return self._self_healer

    def get_artifact_store(self):
        self._ensure_init()
        if self._artifact_store is None:
            from runtime.sandbox_artifacts import ArtifactStore
            self._artifact_store = ArtifactStore(self.get_pg_conn())
        return self._artifact_store

    def get_governance_filter(self):
        if self._governance_filter is None:
            from runtime.governance import GovernanceFilter
            self._governance_filter = GovernanceFilter()
        return self._governance_filter

    def get_heartbeat_runner(self):
        self._ensure_init()
        if self._heartbeat_runner is None:
            self._heartbeat_runner = self._build_heartbeat_runner()
        return self._heartbeat_runner

    def get_memory_engine(self):
        self._ensure_init()
        if self._memory_engine is None:
            self._ensure_workflow_root_on_path()
            from memory.engine import MemoryEngine
            self._memory_engine = MemoryEngine(conn=self.get_pg_conn())
        return self._memory_engine

    def get_session_carry_mgr(self):
        self._ensure_init()
        if self._session_carry_mgr is None:
            from runtime.session_carry import CarryForwardManager
            self._session_carry_mgr = CarryForwardManager(
                self._artifact_dir("session_carry")
            )
        return self._session_carry_mgr

    def get_intent_matcher(self):
        self._ensure_init()
        if self._intent_matcher is None:
            from runtime.intent_matcher import IntentMatcher
            self._intent_matcher = IntentMatcher(
                conn=self.get_pg_conn(),
                embedder=self.get_embedding_service(),
            )
        return self._intent_matcher

    def get_manifest_generator(self):
        self._ensure_init()
        if self._manifest_generator is None:
            from runtime.manifest_generator import ManifestGenerator
            self._manifest_generator = ManifestGenerator(self.get_pg_conn())
        return self._manifest_generator

    def get_module_indexer(self):
        self._ensure_init()
        if self._module_indexer is None:
            from runtime.module_indexer import ModuleIndexer
            self._module_indexer = ModuleIndexer(
                conn=self.get_pg_conn(),
                repo_root=str(self._repo_root),
            )
        return self._module_indexer

    def get_embedding_service(self):
        self._ensure_init()
        if self._embedding_service is None:
            from runtime.embedding_service import (
                EmbeddingService,
                resolve_embedding_runtime_authority,
            )
            self._embedding_service = EmbeddingService(
                authority=resolve_embedding_runtime_authority()
            )
        return self._embedding_service

    def get_notification_consumer(self):
        self._ensure_init()
        if self._notification_consumer is None:
            from runtime.workflow_notifications import WorkflowNotificationConsumer
            self._notification_consumer = WorkflowNotificationConsumer(
                self.get_pg_conn()
            )
        return self._notification_consumer

    def drain_notifications(self) -> str:
        consumer = self.get_notification_consumer()
        notifications = consumer.poll()
        if not notifications:
            return ""
        return consumer.format_batch(notifications)

    def get_health_mod(self):
        if self._health_mod is None:
            self._health_mod = importlib.import_module("runtime.health")
        return self._health_mod


__all__ = ["_BaseSubsystems"]
