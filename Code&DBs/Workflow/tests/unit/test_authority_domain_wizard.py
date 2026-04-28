from __future__ import annotations

from runtime.operations.commands.authority_domain_register import (
    RegisterAuthorityDomainCommand,
    handle_register_authority_domain,
)
from runtime.operations.queries.authority_domain_forge import (
    QueryAuthorityDomainForge,
    handle_query_authority_domain_forge,
)


class _ForgeConn:
    def __init__(self, *, existing=None, storage_target=None):
        self.existing = existing
        self.storage_target = storage_target or {"storage_target_ref": "praxis.primary_postgres"}
        self.fetchrow_calls = []
        self.fetch_calls = []

    def fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        if "FROM authority_domains" in query:
            return self.existing
        if "FROM authority_storage_targets" in query:
            return self.storage_target
        return None

    def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        if "FROM operation_catalog_registry" in query:
            return [
                {
                    "operation_ref": "object_truth.query.observe_record",
                    "operation_name": "object_truth_observe_record",
                    "operation_kind": "query",
                    "posture": "observe",
                    "idempotency_policy": "read_only",
                    "enabled": True,
                }
            ]
        if "FROM authority_object_registry" in query:
            return [
                {
                    "object_ref": "operation.object_truth_observe_record",
                    "object_kind": "query",
                    "object_name": "object_truth_observe_record",
                    "lifecycle_status": "active",
                    "data_dictionary_object_kind": "operation.object_truth_observe_record",
                }
            ]
        return []


class _RegisterConn:
    def __init__(self, *, storage_target=True):
        self.storage_target = storage_target
        self.executed = []

    def fetchrow(self, query, *args):
        if "FROM authority_storage_targets" in query:
            return {"storage_target_ref": args[0]} if self.storage_target else None
        if "FROM authority_domains" in query:
            return {
                "authority_domain_ref": args[0],
                "owner_ref": "praxis.engine",
                "event_stream_ref": f"stream.{args[0]}",
                "current_projection_ref": None,
                "storage_target_ref": "praxis.primary_postgres",
                "enabled": True,
                "decision_ref": "decision.test",
                "created_at": "now",
                "updated_at": "now",
            }
        return None

    def execute(self, query, *args):
        self.executed.append((query, args))


class _Subsystems:
    def __init__(self, conn):
        self.conn = conn

    def get_pg_conn(self):
        return self.conn


def test_authority_domain_forge_previews_new_domain_register_payload() -> None:
    result = handle_query_authority_domain_forge(
        QueryAuthorityDomainForge(
            authority_domain_ref="authority.object_truth",
            decision_ref="decision.object_truth",
        ),
        _Subsystems(_ForgeConn()),
    )

    assert result["view"] == "authority_domain_forge"
    assert result["state"] == "new_domain"
    assert result["ok_to_register"] is True
    assert result["proposed_domain"]["event_stream_ref"] == "stream.authority.object_truth"
    assert result["register_authority_domain_payload"]["decision_ref"] == "decision.object_truth"
    assert result["attached_operations"][0]["operation_name"] == "object_truth_observe_record"
    assert "praxis_register_authority_domain" in result["next_action_packet"]["register_command"]
    assert "Do not create operation rows before the owning authority domain exists." in result["reject_paths"]


def test_authority_domain_forge_blocks_new_domain_without_decision_ref() -> None:
    result = handle_query_authority_domain_forge(
        QueryAuthorityDomainForge(authority_domain_ref="authority.new_area"),
        _Subsystems(_ForgeConn()),
    )

    assert result["ok_to_register"] is False
    assert result["missing_inputs"] == ["decision_ref"]


def test_authority_domain_register_upserts_domain_and_returns_event_payload() -> None:
    conn = _RegisterConn()
    result = handle_register_authority_domain(
        RegisterAuthorityDomainCommand(
            authority_domain_ref="authority.object_truth",
            decision_ref="decision.object_truth",
        ),
        _Subsystems(conn),
    )

    assert result["ok"] is True
    assert result["action"] == "register"
    assert result["authority_domain"]["authority_domain_ref"] == "authority.object_truth"
    assert result["event_payload"]["event_stream_ref"] == "stream.authority.object_truth"
    assert conn.executed


def test_authority_domain_register_rejects_unknown_storage_target() -> None:
    result = handle_register_authority_domain(
        RegisterAuthorityDomainCommand(
            authority_domain_ref="authority.object_truth",
            decision_ref="decision.object_truth",
            storage_target_ref="praxis.missing",
        ),
        _Subsystems(_RegisterConn(storage_target=False)),
    )

    assert result == {
        "ok": False,
        "error_code": "authority_domain_register.storage_target_not_found",
        "error": "storage_target_ref 'praxis.missing' is not registered",
        "authority_domain_ref": "authority.object_truth",
        "storage_target_ref": "praxis.missing",
    }
