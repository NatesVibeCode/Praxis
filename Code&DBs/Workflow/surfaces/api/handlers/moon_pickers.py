"""Read-only picker endpoints for Moon dropdowns.

Backs the source-ref, handoff-target, persistence-target, integration-provider,
and payload-field pickers in Moon. All endpoints are GET and return compact
lists Moon renders as ``<datalist>`` suggestions so free-text fields become
pickable without forcing strict validation.

Integration provider suggestions are derived from ``integration_registry`` (grouped
by ``provider``) and exclude rows whose provider slug matches an **OPEN** entry in
``effective_provider_circuit_breaker_state``.
"""

from __future__ import annotations

from typing import Any

from ._shared import RouteEntry, _exact, _query_params


KNOWN_PAYLOAD_FIELDS: list[dict[str, str]] = [
    {"key": "env", "label": "env (prod / staging / dev)", "sample": "prod"},
    {"key": "priority", "label": "priority (high / medium / low)", "sample": "high"},
    {"key": "source", "label": "source (where the event came from)", "sample": "api"},
    {"key": "dry_run", "label": "dry_run (true / false)", "sample": "false"},
    {"key": "event_type", "label": "event_type", "sample": ""},
    {"key": "user_id", "label": "user_id", "sample": ""},
    {"key": "tenant_id", "label": "tenant_id", "sample": ""},
]


def _handle_webhook_sources(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        rows = pg.fetch(
            """
            SELECT
                e.endpoint_id,
                e.slug,
                e.provider,
                e.enabled
            FROM webhook_endpoints e
            WHERE e.enabled = TRUE
              AND EXISTS (
                  SELECT 1
                  FROM workflow_triggers t
                  WHERE t.enabled = TRUE
                    AND t.trigger_type = 'workflow'
                    AND t.event_type = 'db.webhook_events.insert'
                    AND COALESCE(t.filter_policy->>'source_id', '') = e.endpoint_id
              )
            ORDER BY e.updated_at DESC NULLS LAST, e.created_at DESC NULLS LAST
            LIMIT 200
            """
        )
        items = [
            {
                "value": str(r["slug"] or r["endpoint_id"]),
                "label": f"{r['slug'] or r['endpoint_id']} ({r['provider']})" if r.get("provider") else str(r["slug"] or r["endpoint_id"]),
                "provider": r.get("provider") or "",
                "endpoint_id": r.get("endpoint_id") or "",
                "enabled": bool(r.get("enabled", True)),
            }
            for r in rows
        ]
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
        return
    request._send_json(200, {"sources": items, "count": len(items)})


def _handle_authorities(request: Any, path: str) -> None:
    kinds = _query_params(path).get("kind", [])
    try:
        pg = request.subsystems.get_pg_conn()
        if kinds:
            rows = pg.fetch(
                """
                SELECT decision_key, decision_kind, title
                FROM operator_decisions
                WHERE decision_status = 'active'
                  AND (effective_to IS NULL OR effective_to > now())
                  AND decision_kind = ANY($1::text[])
                ORDER BY decision_kind, decision_key
                LIMIT 500
                """,
                kinds,
            )
        else:
            rows = pg.fetch(
                """
                SELECT decision_key, decision_kind, title
                FROM operator_decisions
                WHERE decision_status = 'active'
                  AND (effective_to IS NULL OR effective_to > now())
                ORDER BY decision_kind, decision_key
                LIMIT 500
                """
            )
        items = [
            {
                "value": str(r["decision_key"]),
                "label": f"{r['decision_key']} — {r['title']}" if r.get("title") else str(r["decision_key"]),
                "kind": r.get("decision_kind") or "",
            }
            for r in rows
        ]
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
        return
    request._send_json(200, {"authorities": items, "count": len(items)})


def _handle_integration_providers(request: Any, path: str) -> None:
    del path
    try:
        pg = request.subsystems.get_pg_conn()
        rows = pg.fetch(
            """
            SELECT ir.provider, min(ir.name) AS name
            FROM integration_registry ir
            LEFT JOIN effective_provider_circuit_breaker_state b
              ON lower(btrim(b.provider_slug)) = lower(btrim(ir.provider))
            WHERE ir.provider IS NOT NULL
              AND btrim(ir.provider) <> ''
              AND (
                  b.provider_slug IS NULL
                  OR COALESCE(b.effective_state, 'CLOSED') <> 'OPEN'
              )
            GROUP BY ir.provider
            ORDER BY ir.provider
            """
        )
    except Exception as exc:
        request._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
        return
    providers: list[dict[str, str]] = []
    for r in rows:
        p = str(r["provider"] or "").strip()
        if not p:
            continue
        name = str(r.get("name") or "").strip()
        label = f"{name} ({p})" if name else p
        providers.append({"value": p, "label": label})
    request._send_json(200, {"providers": providers, "count": len(providers)})


def _handle_payload_fields(request: Any, path: str) -> None:
    params = _query_params(path)
    source_refs = params.get("source_ref", [])
    fields: dict[str, dict[str, Any]] = {
        f["key"]: {"key": f["key"], "label": f["label"], "samples": [f["sample"]] if f["sample"] else []}
        for f in KNOWN_PAYLOAD_FIELDS
    }
    try:
        pg = request.subsystems.get_pg_conn()
        if source_refs:
            rows = pg.fetch(
                """
                SELECT e.payload
                FROM webhook_events e
                JOIN webhook_endpoints w ON w.endpoint_id = e.endpoint_id
                WHERE w.slug = ANY($1::text[]) OR w.endpoint_id = ANY($1::text[])
                ORDER BY e.received_at DESC NULLS LAST
                LIMIT 100
                """,
                source_refs,
            )
        else:
            rows = pg.fetch(
                """
                SELECT payload FROM webhook_events
                ORDER BY received_at DESC NULLS LAST
                LIMIT 100
                """
            )
        import json as _json
        for r in rows:
            payload = r.get("payload")
            if isinstance(payload, str):
                try:
                    payload = _json.loads(payload)
                except Exception:
                    continue
            if not isinstance(payload, dict):
                continue
            for key, val in payload.items():
                entry = fields.setdefault(key, {"key": key, "label": key, "samples": []})
                if isinstance(val, (str, int, float, bool)):
                    sample = str(val)
                    if sample and sample not in entry["samples"] and len(entry["samples"]) < 10:
                        entry["samples"].append(sample)
    except Exception:
        pass
    items = sorted(fields.values(), key=lambda f: f["key"])
    request._send_json(200, {"fields": items, "count": len(items)})


MOON_PICKERS_GET_ROUTES: list[RouteEntry] = [
    (_exact("/api/moon/pickers/webhook-sources"), _handle_webhook_sources),
    (_exact("/api/moon/pickers/authorities"), _handle_authorities),
    (_exact("/api/moon/pickers/integration-providers"), _handle_integration_providers),
    (_exact("/api/moon/pickers/payload-fields"), _handle_payload_fields),
]


__all__ = ["MOON_PICKERS_GET_ROUTES"]
