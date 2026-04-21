"""Postgres-backed connector registry with versioning and health tracking."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


def register_connector(
    conn: "SyncPostgresConnection",
    slug: str,
    display_name: str,
    version: str = "0.1.0",
    auth_type: str = "",
    base_url: str = "",
    module_path: str = "",
    schema_id: Optional[str] = None,
) -> str:
    rows = conn.execute(
        """INSERT INTO connector_registry (slug, display_name, version, auth_type, base_url, module_path, schema_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (slug) DO UPDATE SET
               display_name = EXCLUDED.display_name,
               version = EXCLUDED.version,
               auth_type = EXCLUDED.auth_type,
               base_url = EXCLUDED.base_url,
               module_path = EXCLUDED.module_path,
               schema_id = EXCLUDED.schema_id,
               updated_at = now()
           RETURNING connector_id""",
        slug, display_name, version, auth_type, base_url, module_path, schema_id,
    )
    return rows[0]["connector_id"]


def get_connector(conn: "SyncPostgresConnection", slug: str) -> Optional[dict]:
    rows = conn.execute(
        "SELECT * FROM connector_registry WHERE slug = $1", slug,
    )
    return dict(rows[0]) if rows else None


def list_connectors(conn: "SyncPostgresConnection", status: str = "active") -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM connector_registry WHERE status = $1 ORDER BY slug", status,
    )
    return [dict(r) for r in rows or []]


def update_health(
    conn: "SyncPostgresConnection",
    slug: str,
    health_status: str,
    total_calls_delta: int = 0,
    total_errors_delta: int = 0,
    *,
    error: bool = False,
) -> None:
    ts_col = "last_error_at" if error else "last_success_at"
    conn.execute(
        f"""UPDATE connector_registry SET
               health_status = $2,
               last_health_check = now(),
               last_call_at = now(),
               {ts_col} = now(),
               total_calls = total_calls + $3,
               total_errors = total_errors + $4,
               error_rate = CASE
                   WHEN (total_calls + $3) > 0
                   THEN (total_errors + $4)::real / (total_calls + $3)::real
                   ELSE 0.0
               END,
               updated_at = now()
           WHERE slug = $1""",
        slug, health_status, total_calls_delta, total_errors_delta,
    )


def deprecate_connector(conn: "SyncPostgresConnection", slug: str) -> None:
    conn.execute(
        "UPDATE connector_registry SET status = 'deprecated', updated_at = now() WHERE slug = $1",
        slug,
    )


def search_connectors(conn: "SyncPostgresConnection", query: str) -> list[dict]:
    rows = conn.execute(
        """SELECT * FROM connector_registry
           WHERE slug ILIKE '%' || $1 || '%' OR display_name ILIKE '%' || $1 || '%'
           ORDER BY slug""",
        query,
    )
    return [dict(r) for r in rows or []]


def get_verification_spec(conn: "SyncPostgresConnection", slug: str) -> list[dict]:
    rows = conn.execute(
        "SELECT verification_spec FROM connector_registry WHERE slug = $1", slug,
    )
    if not rows:
        return []
    spec = rows[0].get("verification_spec")
    if isinstance(spec, list):
        return spec
    if isinstance(spec, str):
        import json
        try:
            parsed = json.loads(spec)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def update_verification_status(
    conn: "SyncPostgresConnection",
    slug: str,
    status: str,
) -> None:
    conn.execute(
        """UPDATE connector_registry
           SET verification_status = $2, last_verified_at = now(), updated_at = now()
           WHERE slug = $1""",
        slug, status,
    )


def get_connector_with_schema(conn: "SyncPostgresConnection", slug: str) -> Optional[dict]:
    rows = conn.execute(
        """SELECT c.*, s.title as schema_title, s.version as schema_version, s.base_url as schema_base_url
           FROM connector_registry c
           LEFT JOIN api_schemas s ON s.schema_id = c.schema_id
           WHERE c.slug = $1""",
        slug,
    )
    return dict(rows[0]) if rows else None


def upsert_connector_schema(
    conn: "SyncPostgresConnection",
    slug: str,
    display_name: str,
    capabilities: list,
    auth_shape: dict,
) -> str:
    """Upsert api_schemas + api_endpoints from connector introspection. Returns schema_id."""
    import json as _json

    auth_kind = str((auth_shape or {}).get("kind") or "none").strip().lower()
    auth_type = {
        "api_key": "api_key",
        "env_var": "api_key",
        "oauth2": "oauth2",
        "unknown": "unknown",
    }.get(auth_kind, "none")
    paths = {
        f"/{cap['action']}": {
            "post": {
                "operationId": cap["action"],
                "summary": cap.get("description", ""),
                "responses": {"200": {"description": "ok"}},
            }
        }
        for cap in capabilities
        if cap.get("action")
    }
    raw_spec = _json.dumps({
        "openapi": "3.0.0",
        "info": {"title": display_name, "version": "0.1.0"},
        "paths": paths,
    })

    rows = conn.execute(
        """INSERT INTO api_schemas (provider_slug, version, title, auth_type, raw_spec)
           VALUES ($1, '0.1.0', $2, $3, $4::jsonb)
           ON CONFLICT (provider_slug, version) DO UPDATE SET
               title = EXCLUDED.title,
               auth_type = EXCLUDED.auth_type,
               raw_spec = EXCLUDED.raw_spec
           RETURNING schema_id""",
        slug, display_name, auth_type, raw_spec,
    )
    schema_id = rows[0]["schema_id"]

    for cap in capabilities:
        action = cap.get("action", "")
        if not action:
            continue
        conn.execute(
            """INSERT INTO api_endpoints (schema_id, path, method, operation_id, summary)
               VALUES ($1, $2, 'POST', $3, $4)
               ON CONFLICT (schema_id, path, method) DO UPDATE SET
                   operation_id = EXCLUDED.operation_id,
                   summary = EXCLUDED.summary""",
            schema_id, f"/{action}", action, cap.get("description", ""),
        )

    return schema_id
