from __future__ import annotations

from storage.postgres.workflow_runtime_repository import create_app_manifest, upsert_app_manifest


class _Conn:
    def __init__(self, *, existing: bool = False) -> None:
        self.existing = existing
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args: object) -> list[dict[str, object]]:
        self.execute_calls.append((query, args))
        return []

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
