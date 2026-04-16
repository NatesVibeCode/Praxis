"""Tools: praxis_query — the natural-language router."""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from ..subsystems import _subs, REPO_ROOT
from ..helpers import _serialize, _bug_to_dict, _matches
from surfaces.api.handlers import workflow_query_core


_DIAGNOSE_RUN_ID_RE = re.compile(
    r"(?:diagnose(?:\s+run)?(?:\s+id)?|run(?:\s+id)?)[:=#\s]+([A-Za-z0-9:_-]+)",
    re.IGNORECASE,
)


def _extract_run_id(question: str) -> str:
    match = _DIAGNOSE_RUN_ID_RE.search(question)
    if match:
        return match.group(1).strip()

    # Fall back to the last token if it looks like a run id suffix.
    tokens = [token.strip(".,;:()[]{}") for token in question.split() if token.strip()]
    for token in reversed(tokens):
        if len(token) >= 8 and any(ch.isdigit() for ch in token):
            return token
    return ""


def tool_praxis_query(params: dict) -> dict:
    """Natural language query surface — routes to the right subsystem."""
    return workflow_query_core.handle_query(_subs, dict(params))


def handle_legacy_query(subs, body: dict) -> dict | None:
    """Handle query phrases that still live in the legacy MCP router."""
    from .diagnose import tool_praxis_diagnose

    question = (body.get("question") or "").strip().lower()
    if not question:
        return None

    if _matches(question, ["diagnose", "diagnosis", "troubleshoot", "why did", "run id"]):
        run_id = _extract_run_id(question)
        if not run_id:
            return {
                "routed_to": "workflow_diagnose",
                "message": "Provide a run_id to diagnose a specific workflow run.",
            }
        return {
            "routed_to": "workflow_diagnose",
            "run_id": run_id,
            "diagnosis": tool_praxis_diagnose({"run_id": run_id}),
        }

    if _matches(question, ["operator status", "operator view", "cockpit"]):
        return workflow_query_core.handle_operator_view(
            subs,
            {"view": "status", "run_id": _extract_run_id(question) or body.get("run_id")},
        )

    if _matches(question, ["scoreboard", "cutover"]):
        return workflow_query_core.handle_operator_view(
            subs,
            {"view": "scoreboard", "run_id": _extract_run_id(question) or body.get("run_id")},
        )

    if _matches(question, ["operator graph", "graph topology", "workflow topology"]):
        return {
            "routed_to": "operator_view",
            "message": "Use praxis_operator_view(view='graph', run_id='...') to inspect one run graph.",
        }

    if _matches(question, ["graph lineage", "operator lineage", "workflow lineage"]):
        return {
            "routed_to": "operator_view",
            "message": "Use praxis_operator_view(view='lineage', run_id='...') to inspect one run lineage.",
        }

    if _matches(question, ["session", "carry forward", "carry-forward"]):
        from .session import tool_praxis_session

        return tool_praxis_session({"action": "latest"})

    if _matches(question, ["stale", "staleness", "inactive", "dormant"]):
        return _run_staleness_query(dict(body))

    if _matches(question, ["import path", "how to import", "where is", "from import", "import for", "defined in"]):
        return _import_resolver(question)

    if _matches(question, ["test command", "how to test", "pytest for", "verify command"]):
        return _test_command_resolver(question)

    if _matches(question, ["data dictionary", "what tables", "list tables", "schema for", "table schema", "valid values", "what columns", "what fields", "allowed values"]):
        return _data_dictionary(question)

    if _matches(question, ["calibrat", "tuned param", "auto-tune"]):
        try:
            from runtime.calibration import CalibrationEngine

            engine = CalibrationEngine({})
            cal_path = os.path.join(str(REPO_ROOT), "config", "calibration.json")
            if os.path.isfile(cal_path):
                engine.load(cal_path)
            params = engine.all_params()
            if not params:
                return {"routed_to": "calibration", "message": "No calibrated parameters yet."}
            return {
                "routed_to": "calibration",
                "params": {
                    name: {"value": round(p.value, 4), "min": p.lower, "max": p.upper}
                    for name, p in params.items()
                },
            }
        except Exception as exc:
            return {"routed_to": "calibration", "error": str(exc)}

    if _matches(question, ["route", "routing", "which model", "tier"]):
        try:
            import runtime.auto_router as auto_router_mod

            tiers = auto_router_mod.all_tiers()
            decisions = {}
            for tier in tiers:
                candidates = auto_router_mod.candidates_for_tier(tier)
                decisions[tier] = [
                    {"provider": c.provider_slug, "model": c.model_slug, "healthy": c.healthy}
                    for c in candidates
                ] if candidates else []
            return {"routed_to": "auto_router", "tiers": decisions}
        except Exception as exc:
            return {"routed_to": "auto_router", "error": str(exc)}

    if _matches(question, ["timeout", "dynamic timeout", "complexity"]):
        return {
            "routed_to": "dynamic_timeout",
            "message": "Timeouts are auto-computed per complexity tier. Use praxis_query 'calibration' to see tuned parameters.",
        }

    return None


def _parse_int(value: Any, default: int) -> int:
    """Parse a user-provided int with a safe fallback."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _parse_datetime(value: Any) -> datetime | None:
    """Parse timestamps used by stale candidate rows."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _collect_staleness_candidates_from_rows(
    source: str,
    rows: list[dict[str, Any]],
    candidates: list[dict],
    seen: set[tuple[str, str]],
    limit: int,
    key_column: str,
    type_value: str,
    last_activity_columns: list[str],
) -> int:
    collected = 0
    for row in rows or []:
        if len(candidates) >= limit:
            break
        item_id = str(row.get(key_column, "") or "").strip()
        if not item_id:
            continue
        last_activity = None
        for column in last_activity_columns:
            last_activity = _parse_datetime(row.get(column))
            if last_activity is not None:
                break
        if last_activity is None:
            continue
        key = (type_value, item_id)
        if key in seen:
            continue
        candidates.append({
            "item_id": item_id,
            "item_type": type_value,
            "last_activity": last_activity,
            "source": source,
        })
        seen.add(key)
        collected += 1
    return collected


def _collect_staleness_candidates_from_database(
    conn,
    *,
    per_source_limit: int,
) -> tuple[list[dict], list[dict], list[str]]:
    candidates: list[dict] = []
    sources: list[dict] = []
    warnings: list[str] = []
    seen: set[tuple[str, str]] = set()
    limit = max(per_source_limit, 1)

    if conn is None:
        return candidates, sources, warnings

    def _run_query(
        name: str,
        sql: str,
        args: tuple[Any, ...],
        key_column: str,
        type_value: str,
        last_activity_columns: list[str],
    ) -> None:
        try:
            rows = conn.execute(sql, *args)
            count = _collect_staleness_candidates_from_rows(
                source=name,
                rows=rows,
                candidates=candidates,
                seen=seen,
                limit=limit,
                key_column=key_column,
                type_value=type_value,
                last_activity_columns=last_activity_columns,
            )
            sources.append({"source": name, "count": count, "requested": limit})
        except Exception as exc:
            if name == "workflow_runs":
                details = "workflow_runs query unsupported"
            elif name == "workflow_jobs":
                details = "workflow_jobs query unsupported"
            elif name == "memory_entities":
                details = "memory_entities query unsupported"
            elif name == "dispatch_runs":
                details = "dispatch_runs query unsupported"
            else:
                details = f"{name} query unsupported"
            warnings.append(f"{details}: {exc}")

    _run_query(
        name="memory_entities",
        sql=(
            "SELECT id, entity_type, updated_at "
            "FROM memory_entities "
            "WHERE archived = false "
            "ORDER BY updated_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="id",
        type_value="memory_entities",
        last_activity_columns=["updated_at"],
    )

    _run_query(
        name="workflow_runs",
        sql=(
            "SELECT run_id, requested_at, started_at, finished_at "
            "FROM workflow_runs "
            "ORDER BY requested_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="run_id",
        type_value="work_items",
        last_activity_columns=["finished_at", "started_at", "requested_at"],
    )

    _run_query(
        name="workflow_jobs",
        sql=(
            "SELECT id, created_at, ready_at, claimed_at, started_at, finished_at "
            "FROM workflow_jobs "
            "ORDER BY created_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="id",
        type_value="work_items",
        last_activity_columns=["finished_at", "started_at", "claimed_at", "ready_at", "created_at"],
    )

    _run_query(
        name="dispatch_runs",
        sql=(
            "SELECT run_id, created_at, started_at, finished_at, terminal_reason "
            "FROM dispatch_runs "
            "ORDER BY created_at DESC "
            "LIMIT $1"
        ),
        args=(limit,),
        key_column="run_id",
        type_value="phases",
        last_activity_columns=["finished_at", "started_at", "created_at"],
    )

    return candidates, sources, warnings


def _run_staleness_query(params: dict) -> dict:
    detector = _subs.get_staleness_detector()
    per_source_limit = _parse_int(params.get("per_source_limit"), 200)
    max_items = _parse_int(params.get("max_items"), 20)

    direct_items: list[dict] = []
    if isinstance(params.get("items"), list):
        for item in params["items"]:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("item_id") or item.get("id") or "").strip()
            item_type = str(item.get("item_type") or "work_items").strip() or "work_items"
            last_activity = _parse_datetime(item.get("last_activity"))
            if item_id and last_activity is not None:
                direct_items.append({
                    "item_id": item_id,
                    "item_type": item_type,
                    "last_activity": last_activity,
                    "source": "direct",
                })

    candidates = direct_items
    sources = [{"source": "direct", "count": len(direct_items), "requested": len(direct_items)}] if direct_items else []
    warnings = []

    if not candidates:
        conn = getattr(_subs, "_pg_conn", None)
        db_candidates, db_sources, db_warnings = _collect_staleness_candidates_from_database(
            conn,
            per_source_limit=per_source_limit,
        )
        candidates.extend(db_candidates)
        sources.extend(db_sources)
        warnings.extend(db_warnings)

    if not candidates:
        return {
            "routed_to": "staleness_detector",
            "message": "No staleness candidates available. Provide 'items' or run this tool with active DB connectivity.",
            "sources": sources,
            "warnings": warnings,
        }

    try:
        stale = detector.scan(candidates)
    except Exception as exc:
        return {
            "routed_to": "staleness_detector",
            "error": str(exc),
            "sources": sources,
            "warnings": warnings,
        }

    stale_items = [_serialize(item) for item in stale[:max_items]]
    if stale_items:
        return {
            "routed_to": "staleness_detector",
            "candidate_count": len(candidates),
            "stale_count": len(stale),
            "returned_count": len(stale_items),
            "sources": sources,
            "warnings": warnings,
            "summary": detector.alert_summary(stale),
            "items": stale_items,
        }

    return {
        "routed_to": "staleness_detector",
        "candidate_count": len(candidates),
        "stale_count": 0,
        "returned_count": 0,
        "sources": sources,
        "warnings": warnings,
        "message": "Scanned items are all fresh according to configured staleness rules.",
        "summary": detector.alert_summary(stale),
    }


def _data_dictionary(question: str) -> dict:
    """Return browsable data dictionary from CQRS-backed table projections."""
    from runtime.cqrs import CommandBus
    from runtime.cqrs.queries.data_dictionary import QueryDataDictionary

    table_name = _extract_data_dictionary_table(question)
    return CommandBus(_subs).dispatch(
        QueryDataDictionary(
            table_name=table_name,
            include_relationships=True,
        )
    )


def _extract_data_dictionary_table(question: str) -> str | None:
    lowered = question.lower()
    patterns = [
        r"schema for ([a-z_][a-z0-9_]*)",
        r"schema of ([a-z_][a-z0-9_]*)",
        r"table ([a-z_][a-z0-9_]*)",
        r"columns? for ([a-z_][a-z0-9_]*)",
        r"fields? for ([a-z_][a-z0-9_]*)",
    ]
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            return match.group(1)
    return None


def _import_resolver(question: str) -> dict:
    """Resolve Python import paths from module_embeddings."""
    conn = _subs.get_pg_conn()

    # Extract symbol name — strip common question phrasing
    cleaned = question
    for phrase in ["import path for", "how to import", "where is", "defined in",
                   "from import", "import for", "import"]:
        cleaned = cleaned.replace(phrase, "")
    symbol = cleaned.strip().strip("'\"?")
    if not symbol:
        return {"routed_to": "import_resolver", "results": []}

    # Exact name match first
    rows = conn.execute(
        "SELECT name, kind, module_path FROM module_embeddings "
        "WHERE name = $1 ORDER BY kind", symbol,
    )
    if not rows:
        # Fuzzy fallback
        rows = conn.execute(
            "SELECT name, kind, module_path FROM module_embeddings "
            "WHERE name ILIKE $1 ORDER BY kind LIMIT 10", f"%{symbol}%",
        )
    if not rows:
        return {"routed_to": "import_resolver", "results": [],
                "message": f"No symbol '{symbol}' found in codebase index"}

    seen: set[str] = set()
    results = []
    for r in rows:
        mod_path = r["module_path"]
        # Convert file path → Python import path
        import_module = mod_path
        for prefix in ["Code&DBs/Workflow/", "Code and DBs/Workflow/",
                        "Code&DBs/Databases/", "Code and DBs/Databases/"]:
            if import_module.startswith(prefix):
                import_module = import_module[len(prefix):]
        import_module = import_module.replace("/", ".").replace(".py", "")

        if r["kind"] == "module":
            import_stmt = f"import {import_module}"
        else:
            import_stmt = f"from {import_module} import {r['name']}"

        # Dedup by import statement
        if import_stmt in seen:
            continue
        seen.add(import_stmt)
        results.append({
            "name": r["name"],
            "kind": r["kind"],
            "import": import_stmt,
            "file": mod_path,
        })

    return {"routed_to": "import_resolver", "results": results}


def _test_command_resolver(question: str) -> dict:
    """Resolve test commands for a given file path."""
    import glob as _glob
    from pathlib import Path as _Path

    # Extract file path from question
    cleaned = question
    for phrase in ["test command for", "how to test", "pytest for",
                   "verify command for"]:
        cleaned = cleaned.replace(phrase, "")
    file_path = cleaned.strip().strip("'\"")
    if not file_path:
        return {"routed_to": "test_commands", "error": "No file path found"}

    stem = _Path(file_path).stem
    workflow_root = str(REPO_ROOT / "Code&DBs" / "Workflow")

    # Find test files on disk by naming convention
    test_files = sorted(
        os.path.relpath(p, str(REPO_ROOT))
        for p in _glob.glob(f"{workflow_root}/**/test_{stem}*.py", recursive=True)
    )

    # Build pytest commands
    commands = [
        f"PYTHONPATH='Code&DBs/Workflow' python3 "
        f"-m pytest --noconftest -q {tf}"
        for tf in test_files
    ]

    result: dict = {
        "routed_to": "test_commands",
        "file": file_path,
        "test_files": test_files,
        "commands": commands,
    }
    if not test_files:
        result["hint"] = (
            f"No test_{stem}*.py found. "
            f"Syntax check: PYTHONPATH='Code&DBs/Workflow' python3 -m py_compile '{file_path}'"
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_query": (
        tool_praxis_query,
        {
            "description": (
                "Ask any question about the system in plain English. This is the best starting point "
                "when you're unsure which tool to use — it automatically routes your question to the "
                "right subsystem. Think of it as a router, not as the deep authority for every domain.\n\n"
                "USE WHEN: the user asks a question and you're not sure which specific tool handles it.\n\n"
                "EXAMPLES:\n"
                "  'what is the current pass rate?'         → workflow status\n"
                "  'what is failing right now?'             → recent failure evidence\n"
                "  'are there any open bugs?'               → bug tracker\n"
                "  'which agent performs best?'             → leaderboard\n"
                "  'what failed recently?'                  → failure analysis\n"
                "  'how much did we spend on tokens today?' → receipt analytics\n"
                "  'what does TaskAssembler do?'            → knowledge graph search\n"
                "  'find retry logic with exponential backoff' → code discovery\n"
                "  'data dictionary'                        → browsable table schema + valid values\n"
                "  'schema for workflow_runs'               → detailed table schema\n"
                "  'import path for SchemaProjector'        → exact import statement\n"
                "  'test command for runtime/compiler.py'   → pytest command + test files\n\n"
                "ROUTES TO: status, bugs, quality metrics, failure analysis, agent leaderboard, "
                "code discovery, receipt search, constraints, friction, artifacts, heartbeat, "
                "governance, health, data dictionary, import resolver, test commands, "
                "or knowledge graph (fallback).\n\n"
                "DO NOT USE: when you already know which specific tool to call, or when you need "
                "an exact static architecture scan (`workflow architecture scan`)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "Natural language question about the system."},
                },
                "required": ["question"],
            },
        },
    ),
}
