from __future__ import annotations

from storage.postgres.workflow_runtime_repository import (
    create_app_manifest,
    list_control_manifest_history_records,
    list_control_manifest_head_records,
    load_control_manifest_head_record,
    upsert_app_manifest,
    upsert_control_manifest_head,
)


class _Conn:
    def __init__(self, *, existing: bool = False) -> None:
        self.existing = existing
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.head_rows: list[dict[str, object]] = []
        self.history_rows: list[dict[str, object]] = []

    def execute(self, query: str, *args: object) -> list[dict[str, object]]:
        self.execute_calls.append((query, args))
        if "FROM control_manifest_heads h" in query:
            return list(self.head_rows)
        if "FROM app_manifest_history" in query and "manifest_snapshot->>'workspace_ref'" in query:
            return list(self.history_rows)
        return []

    def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        if "INSERT INTO control_manifest_heads" in query:
            return {
                "workspace_ref": args[0],
                "scope_ref": args[1],
                "manifest_type": args[2],
                "manifest_id": args[3],
                "head_status": args[4],
                "recorded_at": "2026-04-15T12:00:00+00:00",
            }
        if "FROM control_manifest_heads h" in query:
            return self.head_rows[0] if self.head_rows else None
        return None

    def fetchval(self, query: str, *args: object) -> object:
        if "SELECT 1 FROM app_manifests WHERE id = $1" in query:
            return 1 if self.existing else None
        if "SELECT EXTRACT(EPOCH FROM updated_at)::bigint" in query:
            return 1700000000
        return None


def test_create_app_manifest_stores_custom_status_and_parent_manifest_id() -> None:
    conn = _Conn()

    create_app_manifest(
        conn,
        manifest_id="manifest_123",
        name="Control Manifest",
        description="control-plane",
        manifest={"kind": "praxis_control_manifest"},
        parent_manifest_id="manifest_parent",
        status="approved",
    )

    query, args = conn.execute_calls[0]
    assert "parent_manifest_id" in query
    assert args[7] == "manifest_parent"
    assert args[8] == "approved"


def test_upsert_app_manifest_updates_status_and_parent_when_passed() -> None:
    conn = _Conn(existing=True)

    upsert_app_manifest(
        conn,
        manifest_id="manifest_123",
        name="Control Manifest",
        description="control-plane",
        manifest={"kind": "praxis_control_manifest"},
        version=7,
        parent_manifest_id="manifest_parent",
        status="applied",
    )

    query, args = conn.execute_calls[0]
    assert "parent_manifest_id =" in query
    assert "status =" in query
    assert "applied" in args
    assert "manifest_parent" in args


def test_create_app_manifest_defaults_status_to_active_when_omitted() -> None:
    conn = _Conn()

    create_app_manifest(
        conn,
        manifest_id="manifest_123",
        name="Default Manifest",
        description="",
        manifest={"kind": "helm_surface_bundle"},
    )

    _, args = conn.execute_calls[0]
    assert args[8] == "active"
    assert args[7] is None


def test_upsert_app_manifest_preserves_existing_metadata_when_optional_fields_omitted() -> None:
    conn = _Conn(existing=True)

    upsert_app_manifest(
        conn,
        manifest_id="manifest_123",
        name="Default Manifest",
        description="",
        manifest={"kind": "helm_surface_bundle"},
        version=8,
    )

    query, _args = conn.execute_calls[0]
    assert "parent_manifest_id =" not in query
    assert "status =" not in query


def test_upsert_control_manifest_head_returns_scope_keyed_row() -> None:
    conn = _Conn()

    row = upsert_control_manifest_head(
        conn,
        workspace_ref="workspace_root:/tmp/workspace",
        scope_ref="data_job:user-sync",
        manifest_type="data_plan",
        manifest_id="plan_manifest_123",
        head_status="draft",
    )

    assert row["workspace_ref"] == "workspace_root:/tmp/workspace"
    assert row["scope_ref"] == "data_job:user-sync"
    assert row["manifest_id"] == "plan_manifest_123"
    assert any("CREATE TABLE IF NOT EXISTS control_manifest_heads" in query for query, _ in conn.execute_calls)


def test_load_control_manifest_head_record_returns_joined_head_row() -> None:
    conn = _Conn()
    conn.head_rows = [
        {
            "workspace_ref": "workspace_root:/tmp/workspace",
            "scope_ref": "data_job:user-sync",
            "manifest_type": "data_plan",
            "head_manifest_id": "plan_manifest_123",
            "head_status": "approved",
            "recorded_at": "2026-04-15T12:00:00+00:00",
            "id": "plan_manifest_123",
            "name": "User Sync Plan",
            "description": "desc",
            "manifest": {"kind": "praxis_control_manifest"},
            "status": "approved",
            "version": 2,
            "parent_manifest_id": None,
            "created_at": "2026-04-15T11:00:00+00:00",
            "updated_at": "2026-04-15T12:00:00+00:00",
        }
    ]

    row = load_control_manifest_head_record(
        conn,
        workspace_ref="workspace_root:/tmp/workspace",
        scope_ref="data_job:user-sync",
        manifest_type="data_plan",
    )

    assert row is not None
    assert row["head_manifest_id"] == "plan_manifest_123"
    assert row["head_status"] == "approved"


def test_list_control_manifest_head_records_passes_filters_and_limit() -> None:
    conn = _Conn()
    conn.head_rows = [
        {
            "workspace_ref": "workspace_root:/tmp/workspace",
            "scope_ref": "data_job:user-sync",
            "manifest_type": "data_plan",
            "head_manifest_id": "plan_manifest_123",
            "head_status": "approved",
            "recorded_at": "2026-04-15T12:00:00+00:00",
            "id": "plan_manifest_123",
            "name": "User Sync Plan",
            "description": "desc",
            "manifest": {"kind": "praxis_control_manifest"},
            "status": "approved",
            "version": 2,
            "parent_manifest_id": None,
            "created_at": "2026-04-15T11:00:00+00:00",
            "updated_at": "2026-04-15T12:00:00+00:00",
        }
    ]

    rows = list_control_manifest_head_records(
        conn,
        workspace_ref="workspace_root:/tmp/workspace",
        manifest_type="data_plan",
        head_status="approved",
        limit=10,
    )

    assert len(rows) == 1
    query, args = conn.execute_calls[-1]
    assert "h.workspace_ref" in query
    assert "h.manifest_type" in query
    assert "h.head_status" in query
    assert args[-1] == 10


def test_list_control_manifest_history_records_filters_by_scope_type_and_status() -> None:
    conn = _Conn()
    conn.history_rows = [
        {
            "id": "hist_123",
            "manifest_id": "plan_manifest_123",
            "version": 2,
            "manifest_snapshot": {
                "kind": "praxis_control_manifest",
                "manifest_family": "control_plane",
                "manifest_type": "data_plan",
                "workspace_ref": "workspace_root:/tmp/workspace",
                "scope_ref": "data_job:user-sync",
                "status": "approved",
            },
            "change_description": "Approved control manifest",
            "changed_by": "ops",
            "created_at": "2026-04-15T12:00:00+00:00",
        }
    ]

    rows = list_control_manifest_history_records(
        conn,
        workspace_ref="workspace_root:/tmp/workspace",
        scope_ref="data_job:user-sync",
        manifest_type="data_plan",
        status="approved",
        limit=5,
    )

    assert len(rows) == 1
    query, args = conn.execute_calls[-1]
    assert "manifest_snapshot->>'workspace_ref'" in query
    assert "manifest_snapshot->>'scope_ref'" in query
    assert "manifest_snapshot->>'manifest_type'" in query
    assert "manifest_snapshot->>'status'" in query
    assert args[-1] == 5
