"""Operational CLI command handlers."""

from __future__ import annotations

from typing import TextIO

from surfaces.cli.mcp_tools import print_json, render_health_payload, run_cli_tool


def _workflow_tool(params: dict[str, object]) -> dict[str, object]:
    from surfaces.mcp.tools.workflow import tool_praxis_workflow

    return tool_praxis_workflow(params)


def _circuits_command(*, stdout: TextIO) -> int:
    """Handle `workflow circuits` — print all circuit breaker states as JSON."""

    import json as _json

    from runtime.circuit_breaker import get_circuit_breakers

    registry = get_circuit_breakers()
    states = registry.all_states()
    stdout.write(_json.dumps(states, indent=2) + "\n")
    return 0


def _slots_command(*, stdout: TextIO) -> int:
    """Handle `workflow slots` -- show current global provider concurrency slot usage."""

    import json as _json

    from runtime.load_balancer import get_load_balancer

    balancer = get_load_balancer()
    status = balancer.slot_status()

    if not status:
        stdout.write(
            "provider concurrency control is not active "
            "(WORKFLOW_DATABASE_URL not set or DB unavailable)\n"
        )
        return 0

    rows = []
    for slug, limit in sorted(status.items()):
        rows.append(
            {
                "provider": slug,
                "max_concurrent": limit.max_concurrent,
                "active_slots": round(limit.current_active, 2),
                "available": round(limit.available, 2),
                "cost_weight_default": limit.cost_weight,
            }
        )

    stdout.write(_json.dumps(rows, indent=2) + "\n")
    return 0


def _params_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow params [adapt|set|reset]`."""

    import json as _json

    from runtime.adaptive_params import get_adaptive_params

    store = get_adaptive_params()

    if not args or args[0] in {"-h", "--help"}:
        if not args:
            stdout.write(_json.dumps(store.all_params_detail(), indent=2) + "\n")
            return 0
        stdout.write(
            "usage: workflow params            show all adaptive parameters\n"
            "       workflow params adapt       run one adaptation cycle from receipts\n"
            "       workflow params set <name> <value>   manual override\n"
            "       workflow params reset       reset all to initial defaults\n"
        )
        return 2

    sub = args[0]

    if sub == "adapt":
        result = store.adapt_from_receipts()
        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 0

    if sub == "set":
        if len(args) < 3:
            stdout.write("usage: workflow params set <name> <value>\n")
            return 2
        name = args[1]
        try:
            value = float(args[2])
        except ValueError:
            stdout.write(f"error: value must be numeric, got: {args[2]}\n")
            return 2
        try:
            clamped = store.set_param(name, value, reason="cli_manual")
        except KeyError as exc:
            stdout.write(f"error: {exc}\n")
            return 1
        stdout.write(_json.dumps({"name": name, "set_to": clamped}, indent=2) + "\n")
        return 0

    if sub == "reset":
        store.reset()
        stdout.write(_json.dumps(store.all_params(), indent=2) + "\n")
        return 0

    stdout.write(f"unknown params subcommand: {sub}\n")
    return 2


def _notifications_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow notifications [tail|drain]`."""

    import json as _json
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    show_tail = False
    drain_live = False
    tail_count = 10
    if args:
        if args[0] in {"-h", "--help"}:
            stdout.write(
                "usage: workflow notifications            show all persisted notifications\n"
                "       workflow notifications tail [N]   show last N persisted notifications (default: 10)\n"
                "       workflow notifications drain      drain pending live notifications\n"
            )
            return 2
        if args[0] == "tail":
            show_tail = True
            if len(args) > 1:
                try:
                    tail_count = int(args[1])
                except ValueError:
                    stdout.write(f"error: tail count must be numeric, got: {args[1]}\n")
                    return 2
        elif args[0] == "drain":
            drain_live = True

    if drain_live:
        payload = _workflow_tool({"action": "notifications"})
        if payload.get("error"):
            print_json(stdout, payload)
            return 1
        notifications = str(payload.get("notifications") or "").rstrip()
        stdout.write((notifications or "No pending workflow notifications.") + "\n")
        return 0

    conn = SyncPostgresConnection(get_workflow_pool())
    if show_tail:
        rows = conn.execute(
            """
            SELECT *
            FROM (
                SELECT
                    id,
                    run_id,
                    job_label,
                    spec_name,
                    agent_slug,
                    status,
                    failure_code,
                    duration_seconds,
                    created_at
                FROM workflow_notifications
                ORDER BY created_at DESC
                LIMIT $1
            ) AS recent
            ORDER BY created_at ASC, id ASC
            """,
            max(tail_count, 0),
        )
    else:
        rows = conn.execute(
            """
            SELECT
                id,
                run_id,
                job_label,
                spec_name,
                agent_slug,
                status,
                failure_code,
                duration_seconds,
                created_at
            FROM workflow_notifications
            ORDER BY created_at ASC, id ASC
            """
        )

    if not rows:
        stdout.write("no notifications found\n")
        return 0

    for notification in rows:
        stdout.write(_json.dumps(dict(notification), indent=2, default=str) + "\n")
        stdout.write("---\n")

    return 0


def _config_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle ``workflow config [set <key> <value>]``."""

    import json as _json

    from registry.config_registry import get_config

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow config             show all config entries\n"
            "       workflow config set <k> <v>  update one value\n"
        )
        return 2

    cfg = get_config()

    if args and args[0] == "seed":
        stdout.write(
            "config seed is no longer supported; platform_config authority must be present in Postgres\n"
        )
        return 1

    if args and args[0] == "set":
        if len(args) < 3:
            stdout.write("usage: workflow config set <key> <value>\n")
            return 2
        key, raw_value = args[1], args[2]
        value: float | int | str
        try:
            value = int(raw_value)
        except ValueError:
            try:
                value = float(raw_value)
            except ValueError:
                value = raw_value

        existing = cfg.all_entries().get(key)
        if existing:
            cat, desc = existing.category, existing.description
        else:
            cat, desc = "general", ""
        try:
            cfg.set(key, value, category=cat, description=desc)
            stdout.write(f"config: {key} = {value}\n")
            return 0
        except Exception as exc:
            stdout.write(f"config set failed: {exc}\n")
            return 1

    entries = cfg.all_entries()
    if not entries:
        stdout.write("no config entries found\n")
        return 0

    by_category: dict[str, list[dict[str, object]]] = {}
    for entry in sorted(entries.values(), key=lambda entry: (entry.category, entry.key)):
        by_category.setdefault(entry.category, []).append(
            {
                "key": entry.key,
                "value": entry.value,
                "description": entry.description,
            }
        )
    stdout.write(_json.dumps(by_category, indent=2) + "\n")
    return 0


def _dashboard_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow dashboard [--json]` — print consolidated dashboard."""

    from runtime.dashboard import build_dashboard, dashboard_as_json, format_dashboard

    if args and args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow dashboard [--json]\n")
        return 2

    data = build_dashboard()
    if "--json" in args:
        stdout.write(dashboard_as_json(data) + "\n")
    else:
        stdout.write(format_dashboard(data) + "\n")
    return 0


def _cache_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow cache stats` and `workflow cache clear`.

    Subcommands:
      stats — print cache statistics as JSON
      clear — clear all cache entries, or `--older-than HOURS` for selective
    """

    import json as _json

    from runtime.result_cache import get_result_cache

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow cache stats\n"
            "       workflow cache clear [--older-than HOURS]\n"
        )
        return 2

    subcommand = args[0]
    cache = get_result_cache()

    if subcommand == "stats":
        stats = cache.stats()
        stdout.write(_json.dumps(stats, indent=2) + "\n")
        return 0

    if subcommand == "clear":
        older_than_hours = None
        i = 1
        while i < len(args):
            if args[i] == "--older-than" and i + 1 < len(args):
                try:
                    older_than_hours = float(args[i + 1])
                except ValueError:
                    stdout.write(
                        f"error: --older-than value must be a number, got: {args[i + 1]}\n"
                    )
                    return 2
                i += 2
            else:
                stdout.write(f"unknown argument: {args[i]}\n")
                return 2

        deleted = cache.clear(older_than_hours=older_than_hours)
        result = {
            "status": "cleared",
            "entries_deleted": deleted,
        }
        if older_than_hours is not None:
            result["older_than_hours"] = older_than_hours
        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 0

    stdout.write(f"unknown cache subcommand: {subcommand}\n")
    return 2


def _capabilities_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle ``workflow capabilities [accuracy|reclassify] [--json]``.

    Subcommands
    -----------
    (default)
        Show the model x capability matrix: attempts, successes, avg quality.

    accuracy
        Show inference accuracy per capability: what % of inferences were
        confirmed by output quality signals.

    reclassify
        Show suggested reclassifications: runs where the inferred
        capability had low quality but another capability scored high.
    """

    import json as _json

    from runtime.capability_feedback import get_capability_tracker
    from runtime.capability_router import TaskCapability

    if args and args[0] in {"-h", "--help"}:
        caps = ", ".join(TaskCapability.all())
        stdout.write(
            "usage: workflow capabilities [accuracy|reclassify] [--json]\n"
            "\n"
            "  (default)    show model x capability quality matrix\n"
            "  accuracy     show per-capability inference accuracy\n"
            "  reclassify   show suggested capability reclassifications\n"
            "\n"
            f"  known capabilities: {caps}\n"
        )
        return 2

    subcommand = args[0] if args and args[0] not in {"--json"} else None
    as_json = "--json" in args

    tracker = get_capability_tracker()

    if subcommand == "accuracy":
        rows = [tracker.capability_accuracy(capability) for capability in TaskCapability.all()]
        if as_json:
            stdout.write(_json.dumps(rows, indent=2) + "\n")
            return 0

        header = (
            f"{'capability':<20} {'runs':>10} {'matched':>8} "
            f"{'accuracy':>9} {'avg_quality':>12}"
        )
        sep = "-" * len(header)
        stdout.write(sep + "\n")
        stdout.write(header + "\n")
        stdout.write(sep + "\n")
        for row in rows:
            stdout.write(
                f"{row['capability']:<20} {row['total_workflows']:>10} "
                f"{row['quality_matched']:>8} "
                f"{row['accuracy_rate'] * 100:>8.1f}% "
                f"{row['avg_quality']:>12.4f}\n"
            )
        stdout.write(sep + "\n")
        return 0

    if subcommand == "reclassify":
        suggestions = tracker.suggest_capability_reclassification()
        if as_json:
            stdout.write(_json.dumps(suggestions, indent=2) + "\n")
            return 0

        if not suggestions:
            stdout.write("no reclassification candidates found\n")
            return 0

        stdout.write(f"Capability reclassification candidates ({len(suggestions)} found):\n\n")
        for suggestion in suggestions:
            model = f"{suggestion['provider_slug']}/{suggestion['model_slug']}"
            inferred_q = ", ".join(
                f"{capability}={quality:.2f}"
                for capability, quality in suggestion["inferred_quality"].items()
            )
            suggested_q = ", ".join(
                f"{capability}={quality:.2f}"
                for capability, quality in suggestion["suggested_quality"].items()
            )
            stdout.write(
                f"  run_id: {suggestion['run_id']}\n"
                f"  model:  {model}\n"
                f"  inferred:  {', '.join(suggestion['inferred_capabilities'])} (quality: {inferred_q})\n"
                f"  suggested: {', '.join(suggestion['suggested_capabilities'])} (quality: {suggested_q})\n"
                f"  recorded:  {suggestion['recorded_at']}\n\n"
            )
        return 0

    matrix = tracker.model_capability_matrix()
    if as_json:
        stdout.write(_json.dumps(matrix, indent=2) + "\n")
        return 0

    if not matrix:
        stdout.write(
            "no capability outcome data found\n"
            "(outcomes are recorded automatically after each run)\n"
        )
        return 0

    col_model = 32
    header = (
        f"{'provider/model':<{col_model}} {'capability':<18} "
        f"{'attempts':>8} {'successes':>9} {'quality_ok':>10} {'avg_quality':>12}"
    )
    sep = "-" * len(header)
    stdout.write(sep + "\n")
    stdout.write(header + "\n")
    stdout.write(sep + "\n")
    for model_key in sorted(matrix):
        cap_data = matrix[model_key]
        for capability in sorted(cap_data):
            data = cap_data[capability]
            stdout.write(
                f"{model_key:<{col_model}} {capability:<18} "
                f"{data['attempts']:>8} {data['successes']:>9} "
                f"{data['quality_matched']:>10} {data['avg_quality']:>12.4f}\n"
            )
    stdout.write(sep + "\n")
    return 0


def _events_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow events` command for event log queries.

    Subcommands:
      (no subcommand)      - show recent 20 events
      --run <run_id>       - show all events for a specific run
      --type <event_type>  - filter by event type
      --limit <count>      - change limit (default 50)
    """

    import json as _json

    from runtime.event_log import read_since, read_all_since
    from storage.dev_postgres import get_sync_connection

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow events [--run <run_id>] [--type <type>] [--limit <count>]\n"
            "\n"
            "Show recent workflow events from the event log.\n"
            "\n"
            "Examples:\n"
            "  workflow events                           - recent 20 events\n"
            "  workflow events --run abc123              - all events for run abc123\n"
            "  workflow events --type workflow.failed    - filter by type\n"
            "  workflow events --limit 100               - get up to 100 recent events\n"
        )
        return 0

    run_id = None
    event_type = None
    limit = 50
    i = 0

    while i < len(args):
        if args[i] == "--run" and i + 1 < len(args):
            run_id = args[i + 1]
            i += 2
        elif args[i] == "--type" and i + 1 < len(args):
            event_type = args[i + 1]
            i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            try:
                limit = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: --limit must be a number, got: {args[i + 1]}\n")
                return 2
            i += 2
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    try:
        conn = get_sync_connection()
        if run_id:
            events = read_since(conn, channel="job_lifecycle", entity_id=run_id, limit=limit)
            result = {
                "kind": "event_timeline",
                "run_id": run_id,
                "event_count": len(events),
                "events": [e.to_dict() for e in events],
            }
        else:
            if event_type:
                events = read_since(conn, channel=event_type, limit=limit)
            else:
                events = read_all_since(conn, limit=limit)
            result = {
                "kind": "event_list",
                "event_type_filter": event_type,
                "limit": limit,
                "event_count": len(events),
                "events": [e.to_dict() for e in events],
            }

        stdout.write(_json.dumps(result, indent=2) + "\n")
        return 0
    except Exception as exc:
        stdout.write(f"error: failed to query events: {exc}\n")
        return 1


def _health_map_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow health-map [--json] [cycles|complexity]`.

    Analyzes module health across the codebase. Shows health scores based on
    complexity, interface width, circular imports, coupling, and file size.
    """

    import json as _json
    from pathlib import Path

    from runtime.health_map import HealthMapper, format_health_map, format_health_map_json

    workflow_root = str(Path(__file__).resolve().parents[3])

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow health-map [--json] [cycles|complexity] [--limit N]\n"
            "\n"
            "  Analyze module health across the codebase.\n"
            "  Subcommands:\n"
            "    (default)   - show top 20 unhealthiest modules\n"
            "    cycles      - show modules with circular imports\n"
            "    complexity  - show modules with complex functions\n"
            "  Options:\n"
            "    --json      - output as JSON\n"
            "    --limit N   - limit output to N modules (default: 20)\n"
        )
        return 0

    as_json = False
    filter_mode = None
    limit = 20
    i = 0

    while i < len(args):
        if args[i] == "--json":
            as_json = True
            i += 1
        elif args[i] == "--limit" and i + 1 < len(args):
            try:
                limit = int(args[i + 1])
            except ValueError:
                stdout.write(f"invalid limit: {args[i + 1]}\n")
                return 2
            i += 2
        elif args[i] in {"cycles", "complexity"}:
            filter_mode = args[i]
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    mapper = HealthMapper()
    modules = mapper.analyze_directory(workflow_root)

    cycles = mapper.detect_circular_imports(workflow_root)
    cycle_modules = set()
    for cycle in cycles:
        for module_name in cycle:
            cycle_modules.add(module_name)

    modules_with_cycles = [
        module.module_path for module in modules if Path(module.module_path).stem in cycle_modules
    ]
    modules = [
        (
            module
            if module.module_path not in modules_with_cycles
            else module.__class__(
                module_path=module.module_path,
                health_score=module.health_score + 15,
                function_count=module.function_count,
                line_count=module.line_count,
                complex_functions=module.complex_functions,
                very_complex_functions=module.very_complex_functions,
                wide_functions=module.wide_functions,
                import_count=module.import_count,
                has_circular_import=True,
            )
        )
        for module in modules
    ]

    if as_json:
        health_json = format_health_map_json(modules)

        if filter_mode == "cycles":
            health_json["modules"] = [
                module for module in health_json["modules"] if module["has_circular_import"]
            ]
        elif filter_mode == "complexity":
            health_json["modules"] = [
                module
                for module in health_json["modules"]
                if module["complex_functions"] > 0 or module["very_complex_functions"] > 0
            ]

        stdout.write(_json.dumps(health_json, indent=2) + "\n")
    else:
        filter_cycles = filter_mode == "cycles"
        filter_complex = filter_mode == "complexity"

        output = format_health_map(
            modules,
            limit=limit,
            filter_cycles=filter_cycles,
            filter_complex=filter_complex,
        )
        stdout.write(output + "\n")

        if cycles and filter_mode != "cycles":
            stdout.write("\n" + "=" * 120 + "\n")
            stdout.write(f"Circular Imports Detected ({len(cycles)} cycle(s)):\n")
            for index, cycle in enumerate(cycles, 1):
                stdout.write(f"  Cycle {index}: {' -> '.join(cycle)} -> {cycle[0]}\n")

    return 0


def _metrics_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow metrics [subcommand] [--json] [--days N]`."""

    import json as _json

    from runtime.observability import get_workflow_metrics_view

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            """usage: workflow metrics [subcommand] [--json] [--days N]

  (default)  show pass rate + cost + latency + observability summary
  heatmap    show failure code x provider matrix
  volume     show hourly workflow volume
  --json     output as JSON (works with any subcommand)
  --days N   look back N days (default: 7)
"""
        )
        return 2

    subcommand = None
    as_json = False
    days = 7

    i = 0
    while i < len(args):
        arg = args[i]
        if arg in {"--json"}:
            as_json = True
        elif arg == "--days":
            i += 1
            if i < len(args):
                try:
                    days = int(args[i])
                except ValueError:
                    stdout.write(f"error: --days must be an integer, got {args[i]!r}\n")
                    return 1
        elif arg in {"heatmap", "volume"}:
            subcommand = arg
        elif arg in {"-h", "--help"}:
            stdout.write("see above\n")
            return 0
        i += 1

    view = get_workflow_metrics_view()

    if subcommand is None:
        pass_rates = view.pass_rate_by_model(days=days)
        costs = view.cost_by_agent(days=days)
        latencies = view.latency_percentiles(days=days)
        efficiency = view.efficiency_summary(days=days)
        failure_breakdown = view.failure_category_breakdown(days=days)
        hourly_volume = view.hourly_workflow_volume(days=days)
        capability_distribution = view.capability_distribution(days=days)

        summary = {
            "pass_rate_by_model": pass_rates,
            "cost_by_agent": costs,
            "latency_percentiles": latencies,
            "efficiency_summary": efficiency,
            "failure_category_breakdown": failure_breakdown,
            "hourly_workflow_volume": hourly_volume,
            "capability_distribution": capability_distribution,
        }

        if as_json:
            stdout.write(_json.dumps(summary, indent=2) + "\n")
            return 0

        stdout.write(f"metrics_summary: window_days={days}\n")

        if pass_rates:
            stdout.write("pass_rate_by_model:\n")
            for row in pass_rates:
                stdout.write(
                    f"  provider={row['provider_slug']} model={str(row['model_slug'] or 'unknown')} "
                    f"total_workflows={row['total_workflows']} pass_rate_pct={row['pass_rate']:.1f}\n"
                )

        if costs:
            stdout.write("cost_by_agent:\n")
            for row in costs:
                stdout.write(
                    f"  provider={row['provider_slug']} total_cost_usd={row['total_cost_usd']:.4f} "
                    f"num_workflows={row['num_workflows']} avg_cost_per_workflow_usd={row['avg_cost_per_workflow']:.6f}\n"
                )

        stdout.write(
            "latency_percentiles: "
            f"p50_ms={latencies.get('p50', 0)} "
            f"p95_ms={latencies.get('p95', 0)} "
            f"p99_ms={latencies.get('p99', 0)}\n"
        )

        stdout.write(
            "observability_digest: "
            f"first_pass_success_rate_pct={efficiency.get('first_pass_success_rate', 0.0) * 100:.1f} "
            f"retry_success_rate_pct={efficiency.get('retry_success_rate', 0.0) * 100:.1f} "
            f"cost_per_success_usd={efficiency.get('cost_per_success_usd', 0.0):.6f} "
            f"tokens_per_success={efficiency.get('tokens_per_success', 0.0):.2f} "
            f"avg_latency_ms={efficiency.get('avg_latency_ms', 0.0):.2f} "
            f"avg_tool_uses={efficiency.get('avg_tool_uses', 0.0):.2f} "
            f"window_total_workflows={efficiency.get('total_workflows', 0)}\n"
        )
        stdout.write("failure_mix:")
        if failure_breakdown:
            parts = [
                f"{row.get('failure_category', 'unknown')}/{row.get('failure_zone', 'unknown')} "
                f"{row.get('count', 0)} ({row.get('pct', 0)}%)"
                for row in failure_breakdown[:3]
            ]
            stdout.write(" " + "; ".join(parts) + "\n")
        else:
            stdout.write("none\n")

        return 0

    if subcommand == "heatmap":
        heatmap = view.failure_heatmap(days=days)

        if as_json:
            stdout.write(_json.dumps(heatmap, indent=2) + "\n")
            return 0

        stdout.write(f"\nFailure Heatmap (last {days} days):\n")
        if not heatmap:
            stdout.write("  (no failures)\n\n")
            return 0

        stdout.write(f"  {'Failure Code':<25} {'Provider':<15} {'Count':>8}\n")
        stdout.write("  " + "-" * 50 + "\n")
        for row in heatmap:
            stdout.write(
                f"  {row['failure_code']:<25} {row['provider_slug']:<15} {row['count']:>8}\n"
            )
        stdout.write("\n")
        return 0

    if subcommand == "volume":
        volume = view.hourly_workflow_volume(days=days)

        if as_json:
            stdout.write(_json.dumps(volume, indent=2) + "\n")
            return 0

        stdout.write(f"\nHourly Workflow Volume (last {days} days):\n")
        if not volume:
            stdout.write("  (no data)\n\n")
            return 0

        stdout.write(f"  {'Hour':<30} {'Count':>8}\n")
        stdout.write("  " + "-" * 40 + "\n")
        for row in volume:
            hour_str = row["hour"] or "unknown"
            stdout.write(f"  {hour_str:<30} {row['count']:>8}\n")
        stdout.write("\n")
        return 0

    return 0


def _api_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow api [--host HOST] [--port PORT]`.

    Starts the DAG REST API server after reading the declared runtime
    dependency contract from ``requirements.runtime.txt``.

    Options:
      --host HOST   bind address (default: 0.0.0.0)
      --port PORT   TCP port (default: 8420)
    """

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow api [--host HOST] [--port PORT]\n"
            "\n"
            "Start the DAG REST API server.\n"
            "Reads the runtime dependency contract from requirements.runtime.txt\n"
            "\n"
            "  --host HOST   bind address (default: 0.0.0.0)\n"
            "  --port PORT   TCP port     (default: 8420)\n"
        )
        return 2

    host = "0.0.0.0"
    port = 8420

    i = 0
    while i < len(args):
        if args[i] == "--host" and i + 1 < len(args):
            host = args[i + 1]
            i += 2
        elif args[i] == "--port" and i + 1 < len(args):
            try:
                port = int(args[i + 1])
            except ValueError:
                stdout.write(f"error: --port must be an integer, got: {args[i + 1]}\n")
                return 2
            i += 2
        else:
            stdout.write(f"error: unknown argument: {args[i]}\n")
            return 2

    try:
        from surfaces.api.server import start_server
    except ImportError as exc:
        stdout.write(f"error: could not import API server: {exc}\n")
        return 1

    try:
        start_server(host=host, port=port)
    except RuntimeError as exc:
        stdout.write(f"error: {exc}\n")
        return 1
    except KeyboardInterrupt:
        pass

    return 0


def _supervisor_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle legacy `workflow supervisor {install|uninstall|status|logs|restart}`."""

    import subprocess
    from pathlib import Path

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow supervisor {install|uninstall|status|logs|restart}\n"
            "\n"
            "Legacy compatibility wrapper around ./scripts/praxis for service management.\n"
            "\n"
            "  supervisor install   - Install and load all services\n"
            "  supervisor uninstall - Unload and remove all services\n"
            "  supervisor status    - Show status of all services\n"
            "  supervisor logs      - Tail all service logs\n"
            "  supervisor restart   - Restart all services\n"
        )
        return 2

    subcommand = args[0]
    if subcommand not in {"install", "uninstall", "status", "logs", "restart"}:
        stdout.write(f"error: unknown supervisor subcommand: {subcommand}\n")
        return 2

    workflow_root = Path(__file__).resolve().parents[3]
    repo_root = workflow_root.parents[1]
    launcher_script = repo_root / "scripts" / "praxis"

    if not launcher_script.exists():
        stdout.write(f"error: praxis launcher not found at {launcher_script}\n")
        return 1

    try:
        result = subprocess.run(
            [str(launcher_script), subcommand],
            capture_output=False,
            text=True,
            check=False,
        )
        return result.returncode
    except Exception as exc:
        stdout.write(f"error: failed to run praxis launcher: {exc}\n")
        return 1


# ---------------------------------------------------------------------------
# workflow health — full system preflight check
# ---------------------------------------------------------------------------

def _health_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow health [--json]` — full system health check.

    Runs preflight probes (Postgres, disk, provider transport), operator
    panel snapshot, lane recommendation, dependency truth, and content health.
    """

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow health [--json]\n"
            "\n"
            "  Run full system health check: DB probes, provider transport,\n"
            "  disk space, operator snapshot, and lane recommendation.\n"
        )
        return 2

    as_json = "--json" in args if args else False
    exit_code, payload = run_cli_tool("praxis_health", {})
    if as_json:
        print_json(stdout, payload)
        return exit_code
    render_health_payload(payload, stdout=stdout)
    return exit_code
