"""Heartbeat module: run every registered TaskContract autonomously.

This is the closed loop: each cycle executes every contract's
verify→plan→apply→re-verify flow. Contracts that can't be satisfied
within their tier escalate to a governance bug (dedup-keyed on
`contract.<name>` so the next cycle doesn't re-file).

No human review queue. The primitive either resolves autonomously or
escalates into the scorecard surface.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from runtime.audit_primitive import execute_all_contracts
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

# Eager registration — ensures the contracts + patterns + audits are
# populated when the heartbeat constructs us.
from runtime.audit_primitive_wiring import register_all as _register_all
_register_all()

logger = logging.getLogger(__name__)


class AuditPrimitiveContractRunner(HeartbeatModule):
    """Run every registered contract on each heartbeat cycle."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "audit_primitive_contract_runner"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            result = execute_all_contracts(self._conn)
            # Heartbeat `_ok` returns a timestamped success; the full
            # result goes into logs / scan audit for introspection.
            logger.info(
                "audit_primitive contracts: satisfied=%d escalated=%d",
                result.get("satisfied", 0),
                result.get("escalated", 0),
            )
        except Exception as exc:
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = ["AuditPrimitiveContractRunner"]
