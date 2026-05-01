"""Gateway dispatch proof for action_fingerprint_record."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import _pg_test_conn
import runtime.operation_catalog_gateway as gateway
from runtime.friction_ledger import FrictionLedger


class _GatewayConn:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def fetch(self, query: str, *args: Any) -> Any:
        return self._conn.execute(query, *args)

    def fetchrow(self, query: str, *args: Any) -> Any:
        return self._conn.fetchrow(query, *args)

    def fetchval(self, query: str, *args: Any) -> Any:
        return self._conn.fetchval(query, *args)

    def execute(self, query: str, *args: Any) -> Any:
        return self._conn.execute(query, *args)

    @contextmanager
    def transaction(self):
        yield self


class _Subsystems:
    def __init__(self, conn: _GatewayConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _GatewayConn:
        return self._conn

    def get_friction_ledger(self) -> FrictionLedger:
        return FrictionLedger(self._conn, embedder=None)


def test_action_fingerprint_record_gateway_persists_shell_shape() -> None:
    isolated_conn = _pg_test_conn.get_isolated_conn()
    try:
        gateway_conn = _GatewayConn(isolated_conn)
        subsystems = _Subsystems(gateway_conn)

        result = gateway.execute_operation_from_subsystems(
            subsystems,
            operation_name="action_fingerprint_record",
            payload={
                "tool_name": "local_shell",
                "tool_input": {
                    "command": [
                        "pytest",
                        "Code&DBs/Workflow/tests/integration/test_action_fingerprint_record_gateway.py",
                        "-q",
                    ]
                },
                "source_surface": "codex:host",
                "session_ref": "codex-test-session",
                "payload_meta": {"harness": "codex_cli"},
            },
        )

        assert result["ok"] is True
        assert result["recorded"] is True
        assert result["action_kind"] == "shell"
        assert result["normalized_command"] == "pytest Code&DBs/Workflow/tests/integration/*.py -q"
        assert result["session_ref"] == "codex-test-session"
        receipt = result["operation_receipt"]
        assert receipt["operation_name"] == "action_fingerprint_record"
        assert receipt["execution_status"] == "completed"

        row = isolated_conn.fetchrow(
            """
            SELECT source_surface,
                   action_kind,
                   normalized_command,
                   path_shape,
                   shape_hash,
                   session_ref,
                   payload_meta::text AS payload_meta_json
              FROM action_fingerprints
             WHERE shape_hash = $1
             ORDER BY ts DESC
             LIMIT 1
            """,
            result["shape_hash"],
        )
        assert row is not None
        assert row["source_surface"] == "codex:host"
        assert row["action_kind"] == "shell"
        assert row["normalized_command"] == "pytest Code&DBs/Workflow/tests/integration/*.py -q"
        assert row["path_shape"] is None
        assert row["session_ref"] == "codex-test-session"
        assert "local_shell" in row["payload_meta_json"]
    finally:
        isolated_conn.close()
