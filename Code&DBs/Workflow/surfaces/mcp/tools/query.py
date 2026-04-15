"""Tools: praxis_query — the natural-language router."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from ..subsystems import _subs, REPO_ROOT
from ..helpers import _serialize, _bug_to_dict, _matches


def tool_praxis_query(params: dict) -> dict:
    """Natural language query surface — routes to the right subsystem."""
    from runtime.receipt_store import list_receipts, receipt_stats

    question = (params.get("question") or "").strip().lower()
    if not question:
        return {"error": "question is required"}

    # Route based on keywords
    if _matches(question, ["status", "panel", "snapshot", "overview", "dashboard"]):
        panel = _subs.get_operator_panel()
        snap = panel.snapshot()
        result = {"routed_to": "operator_panel", "snapshot": _serialize(snap)}
        # Include in-flight workflows
        try:
            conn = _subs.get_pg_conn()
            import json as _json
            running = conn.execute(
                """SELECT run_id, requested_at, request_envelope
                FROM workflow_runs
                WHERE current_state = 'running'
                ORDER BY requested_at DESC LIMIT 5""",
            )
            if running:
                in_flight = []
                for r in running:
                    env = r["request_envelope"] if isinstance(r["request_envelope"], dict) else _json.loads(r["request_envelope"])
                    outbox_count = conn.execute(
                        "SELECT COUNT(*) as cnt FROM workflow_outbox WHERE run_id = $1 AND authority_table = 'receipts'",
                        r["run_id"],
                    )
                    completed = int(outbox_count[0]["cnt"]) if outbox_count else 0
                    in_flight.append({
                        "run_id": r["run_id"],
                        "workflow_name": env.get("name") or env.get("spec_name", ""),
                        "progress": f"{completed}/{env.get('total_jobs', '?')}",
                    })
                result["in_flight_workflows"] = in_flight
        except Exception:
            pass
        result["quick_lookups"] = {
            "data_dictionary": "praxis_query('data dictionary')",
            "import_resolver": "praxis_query('import path for <ClassName>')",
            "test_commands": "praxis_query('test command for <file.py>')",
        }
        return result

    if _matches(question, ["bug", "defect", "issue"]):
        bt = _subs.get_bug_tracker()
        bugs = bt.list_bugs(limit=20)
        return {
            "routed_to": "bug_tracker",
            "bugs": [_bug_to_dict(b) for b in bugs],
            "count": len(bugs),
        }

    if _matches(question, ["quality", "metric", "rollup", "pass rate"]):
        qmod = _subs.get_quality_views_mod()
        qm = _subs.get_quality_materializer()
        rollup = qm.latest_rollup(qmod.QualityWindow.DAILY)
        if rollup:
            return {"routed_to": "quality_views", "rollup": _serialize(rollup)}
        return {"routed_to": "quality_views", "rollup": None, "message": "no rollup data available"}

    if _matches(question, ["fail", "error", "crash", "broken"]):
        records = list_receipts(limit=5000, since_hours=24)
        counts: dict[str, int] = {}
        for record in records:
            if record.failure_code:
                counts[record.failure_code] = counts.get(record.failure_code, 0) + 1
        top_failure_codes = dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10])
        return {
            "routed_to": "failures",
            "top_failure_codes": top_failure_codes,
            "total_receipts_checked": len(records),
        }

    if _matches(question, ["agent", "leaderboard", "performance", "who", "how are"]):
        records = list_receipts(limit=10000, since_hours=72)
        by_agent: dict[str, dict[str, int]] = {}
        for record in records:
            bucket = by_agent.setdefault(record.agent, {"total": 0, "succeeded": 0})
            bucket["total"] += 1
            if record.status == "succeeded":
                bucket["succeeded"] += 1
        leaderboard = []
        for agent, counts in by_agent.items():
            total = counts["total"]
            succeeded = counts["succeeded"]
            pr = succeeded / total if total else 0.0
            leaderboard.append({"agent": agent, "dispatches": total, "pass_rate": round(pr, 4)})
        leaderboard.sort(key=lambda x: (-x["pass_rate"], -x["dispatches"]))
        return {"routed_to": "leaderboard", "agents": leaderboard}

    if _matches(question, ["discover", "infrastructure", "what exists", "what solves",
                             "similar", "equivalent", "synonym", "already built"]):
        from .discover import tool_praxis_discover
        # Strip routing keywords and pass the rest as query
        return tool_praxis_discover({"action": "search", "query": question, "limit": 10})

    if _matches(question, ["stale", "staleness", "inactive", "dormant"]):
        return _run_staleness_query(params)

    if _matches(question, ["import path", "how to import", "where is", "from import",
                            "import for", "defined in"]):
        return _import_resolver(question)

    if _matches(question, ["test command", "how to test", "pytest for",
                            "verify command"]):
        return _test_command_resolver(question)

    if _matches(question, ["receipt", "token burn", "cost breakdown"]):
        from .evidence import tool_praxis_receipts
        return tool_praxis_receipts({"action": "token_burn", "since_hours": 24})

    if _matches(question, ["constraint", "mined constraint", "learned constraint"]):
        from .evidence import tool_praxis_constraints
        return tool_praxis_constraints({"action": "list"})

    if _matches(question, ["friction", "guardrail", "bounce"]):
        from .evidence import tool_praxis_friction
        return tool_praxis_friction({"action": "stats"})

    if _matches(question, ["artifact", "sandbox artifact"]):
        from .artifacts import tool_praxis_artifacts
        return tool_praxis_artifacts({"action": "stats"})

    if _matches(question, ["heartbeat", "memory maintenance", "graph hygiene"]):
        from .session import tool_praxis_heartbeat
        return tool_praxis_heartbeat({"action": "status"})

    if _matches(question, ["governance", "secret", "scan"]):
        return {"routed_to": "governance", "message": "Use praxis_governance with action=scan_prompt or scan_scope."}

    if _matches(question, ["loop", "retry loop", "runaway"]):
        try:
            from runtime.loop_detector import LoopDetector
            detector = LoopDetector()
            return {"routed_to": "loop_detector", "message": "Loop detector ready. Use praxis_heal for specific failure diagnosis."}
        except Exception as e:
            return {"routed_to": "loop_detector", "error": str(e)}

    if _matches(question, ["classify", "failure type", "retryab"]):
        return {"routed_to": "failure_classifier",
                "message": "Use praxis_heal to classify a failure. Provide job_label plus failure_code and/or stderr."}

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
            return {"routed_to": "calibration", "params": {
                name: {"value": round(p.value, 4), "min": p.lower, "max": p.upper}
                for name, p in params.items()
            }}
        except Exception as e:
            return {"routed_to": "calibration", "error": str(e)}

    if _matches(question, ["route", "routing", "which model", "tier"]):
        try:
            import runtime.auto_router as auto_router_mod
            tiers = auto_router_mod.all_tiers()
            decisions = {}
            for tier in tiers:
                candidates = auto_router_mod.candidates_for_tier(tier)
                decisions[tier] = [{"provider": c.provider_slug, "model": c.model_slug,
                                    "healthy": c.healthy} for c in candidates] if candidates else []
            return {"routed_to": "auto_router", "tiers": decisions}
        except Exception as e:
            return {"routed_to": "auto_router", "error": str(e)}

    if _matches(question, ["timeout", "dynamic timeout", "complexity"]):
        return {"routed_to": "dynamic_timeout",
                "message": "Timeouts are auto-computed per complexity tier. Use praxis_query 'calibration' to see tuned parameters."}

    if _matches(question, ["operator status", "operator view", "cockpit"]):
        from .operator import tool_praxis_operator_view
        return tool_praxis_operator_view({"view": "status"})

    if _matches(question, ["scoreboard", "cutover"]):
        from .operator import tool_praxis_operator_view
        return tool_praxis_operator_view({"view": "scoreboard"})

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

    if _matches(question, ["decompose", "sprint", "breakdown"]):
        return {"routed_to": "decompose", "message": "Use praxis_decompose with an objective to break it into micro-sprints."}

    if _matches(question, ["health", "preflight", "probe"]):
        from .health import tool_praxis_health
        return tool_praxis_health({})

    if _matches(question, ["data dictionary", "what tables", "list tables",
                            "schema for", "table schema", "valid values",
                            "what columns", "what fields", "allowed values"]):
        return _data_dictionary(question)

    # Fallback: search knowledge graph (use same clean formatting as praxis_recall)
    from .knowledge import tool_praxis_recall
    recall_result = tool_praxis_recall({"query": question})
    recall_result["routed_to"] = "knowledge_graph"
    return recall_result


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
    """Return browsable data dictionary from schema-projected table entities."""
    import json as _json
    import re

    conn = _subs.get_pg_conn()

    # Live CHECK constraint lookup — always fresh, no heartbeat dependency
    check_values: dict[str, dict[str, list[str]]] = {}
    try:
        ck_rows = conn.execute(
            "SELECT conrelid::regclass::text AS table_name, "
            "pg_get_constraintdef(oid) AS check_def "
            "FROM pg_constraint "
            "WHERE contype = 'c' AND connamespace = 'public'::regnamespace"
        )
        for cr in ck_rows or []:
            defn = cr["check_def"] or ""
            array_match = re.search(r"ARRAY\[(.+?)\]", defn)
            if not array_match:
                continue
            col_match = re.search(r"\(+\s*\(?(\w+)\)?", defn)
            if not col_match:
                continue
            values = re.findall(r"'([^']+)'", array_match.group(1))
            if values:
                check_values.setdefault(cr["table_name"], {})[col_match.group(1)] = values
    except Exception:
        pass  # fall back to whatever metadata has

    rows = conn.execute(
        "SELECT name, content, metadata FROM memory_entities "
        "WHERE entity_type = 'table' AND NOT archived ORDER BY name"
    )
    if not rows:
        return {"routed_to": "data_dictionary", "tables": [], "count": 0}

    all_names = [r["name"] for r in rows]

    # Check if question targets a specific table
    target = None
    for name in all_names:
        if name in question:
            target = name
            break

    tables = []
    for r in rows:
        if target and r["name"] != target:
            continue
        meta = r["metadata"] if isinstance(r["metadata"], dict) else _json.loads(r["metadata"] or "{}")
        # Merge live CHECK values over metadata (live wins)
        vv = {**meta.get("valid_values", {}), **check_values.get(r["name"], {})}
        if target:
            # Detail mode — full schema for one table
            tables.append({
                "name": r["name"],
                "summary": r["content"],
                "columns": meta.get("columns", []),
                "valid_values": vv,
                "indexes": meta.get("indexes", []),
                "triggers": meta.get("triggers", []),
                "used_by": meta.get("used_by", {}),
                "approx_rows": meta.get("approx_rows", 0),
                "pg_notify_channels": meta.get("pg_notify_channels", []),
            })
        else:
            # Overview mode — only tables with constrained values
            if not vv:
                continue
            tables.append({
                "name": r["name"],
                "columns": len(meta.get("columns", [])),
                "rows": meta.get("approx_rows", 0),
                "valid_values": vv,
            })

    result: dict = {"routed_to": "data_dictionary", "tables": tables, "count": len(tables)}
    if not target:
        result["total_tables"] = len(all_names)
        result["hint"] = "Showing tables with constrained values. Use 'schema for <table_name>' for full detail on any table."
    return result


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
