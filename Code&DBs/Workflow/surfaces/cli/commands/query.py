"""Read/query-oriented CLI command handlers."""

from __future__ import annotations

from typing import TextIO

from surfaces.cli.mcp_tools import (
    print_json,
    render_artifacts_payload,
    render_bug_payload,
    render_discover_payload,
    render_recall_payload,
    run_cli_tool,
)


# ---------------------------------------------------------------------------
# Shared Postgres connection helper — lazy singleton for CLI commands that
# need direct DB access (bug tracker, knowledge graph, discover, artifacts).
# ---------------------------------------------------------------------------

_cli_pg_conn = None


def _get_conn():
    global _cli_pg_conn
    if _cli_pg_conn is None:
        from storage.postgres import ensure_postgres_available
        _cli_pg_conn = ensure_postgres_available()
    return _cli_pg_conn


def _receipts_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow receipts [receipt_id]`."""

    import json as _json

    from runtime.receipt_store import list_receipts, load_receipt

    if args and args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow receipts [<receipt_id>]\n")
        return 2

    if args:
        receipt_id = args[0]
        rec = load_receipt(receipt_id)
        if rec is None:
            stdout.write(f"receipt not found: {receipt_id}\n")
            return 1
        stdout.write(_json.dumps(rec.to_dict(), indent=2) + "\n")
        return 0

    try:
        records = list_receipts()
    except Exception as exc:
        stdout.write(f"failed to list receipts: {exc}\n")
        return 1
    stdout.write(
        _json.dumps(
            [
                {
                    "id": record.id,
                    "label": record.label,
                    "agent": record.agent,
                    "status": record.status,
                    "timestamp": record.timestamp.isoformat() if record.timestamp else None,
                    "run_id": record.run_id,
                }
                for record in records
            ],
            indent=2,
        )
        + "\n"
    )
    return 0


def _costs_command(*, stdout: TextIO) -> int:
    """Handle `workflow costs` — print cost summary as JSON."""

    import json as _json

    from runtime.cost_tracker import get_cost_tracker

    tracker = get_cost_tracker()
    stdout.write(_json.dumps(tracker.summary(), indent=2) + "\n")
    return 0


def _leaderboard_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow leaderboard [--json]`."""

    from runtime.leaderboard import (
        build_leaderboard,
        format_leaderboard,
        leaderboard_as_json,
    )

    if args and args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow leaderboard [--json]\n")
        return 2

    scores = build_leaderboard()
    if "--json" in args:
        stdout.write(leaderboard_as_json(scores) + "\n")
    else:
        stdout.write(format_leaderboard(scores) + "\n")
    return 0


def _trust_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow trust [--json] [--compute-from-receipts <dir>]`.

    Shows ELO-based trust scores for LLM providers and models.
    """

    import json as _json

    from runtime.trust_scoring import format_trust_scores, get_trust_scorer

    if args and args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow trust [--json] [--compute-from-receipts]\n")
        return 2

    scorer = get_trust_scorer()

    i = 0
    while i < len(args):
        if args[i] == "--compute-from-receipts":
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                i += 2
            else:
                i += 1
            try:
                scorer.compute_from_receipts()
            except Exception as exc:
                stdout.write(f"error: failed to compute from receipts: {exc}\n")
                return 1
        elif args[i] in {"--json"}:
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    scores = scorer.all_scores()
    if "--json" in args:
        scores_data = [
            {
                "provider_slug": score.provider_slug,
                "model_slug": score.model_slug,
                "elo_score": round(score.elo_score, 2),
                "total_runs": score.total_runs,
                "wins": score.wins,
                "losses": score.losses,
                "win_rate": round(score.win_rate, 4),
                "last_updated": score.last_updated.isoformat(),
            }
            for score in scores
        ]
        stdout.write(_json.dumps(scores_data, indent=2) + "\n")
    else:
        stdout.write(format_trust_scores(scores) + "\n")
    return 0


def _fitness_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow fitness [--capability <cap>] [--json]`.

    Shows per-capability model fitness scores computed from workflow receipt
    history. Fitness = success_rate*100 - avg_cost*10 + 1000/avg_latency.
    """

    import json as _json

    from runtime.capability_router import (
        TaskCapability,
        compute_model_fitness,
        format_fitness_table,
    )

    if args and args[0] in {"-h", "--help"}:
        caps = ", ".join(TaskCapability.all())
        stdout.write(
            "usage: workflow fitness [--capability <cap>] [--json]\n"
            "\n"
            f"  known capabilities: {caps}\n"
        )
        return 2

    cap_filter: str | None = None
    as_json = False
    i = 0
    while i < len(args):
        if args[i] == "--capability" and i + 1 < len(args):
            cap_filter = args[i + 1]
            i += 2
        elif args[i] == "--json":
            as_json = True
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    fitness_map = compute_model_fitness()

    if as_json:
        rows = []
        for model_fitness in sorted(
            fitness_map.values(),
            key=lambda model_fitness: (model_fitness.capability, -model_fitness.fitness_score),
        ):
            if cap_filter and model_fitness.capability != cap_filter:
                continue
            rows.append(
                {
                    "provider_slug": model_fitness.provider_slug,
                    "model_slug": model_fitness.model_slug,
                    "capability": model_fitness.capability,
                    "success_rate": model_fitness.success_rate,
                    "sample_count": model_fitness.sample_count,
                    "avg_latency_ms": model_fitness.avg_latency_ms,
                    "avg_cost_usd": model_fitness.avg_cost_usd,
                    "fitness_score": model_fitness.fitness_score,
                }
            )
        stdout.write(_json.dumps(rows, indent=2) + "\n")
    else:
        stdout.write(format_fitness_table(fitness_map, capability_filter=cap_filter) + "\n")

    return 0


def _trends_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow trends [--json]` — detect and display provider trends."""

    import json as _json

    from runtime.trend_detector import TrendDetector, format_trends

    if args and args[0] in {"-h", "--help"}:
        stdout.write("usage: workflow trends [--json]\n")
        return 2

    as_json = "--json" in args if args else False

    detector = TrendDetector()
    trends = detector.detect_from_receipts()

    if as_json:
        rows = []
        for trend in trends:
            rows.append(
                {
                    "metric_name": trend.metric_name,
                    "provider_slug": trend.provider_slug,
                    "direction": trend.direction.value,
                    "baseline_value": trend.baseline_value,
                    "current_value": trend.current_value,
                    "change_pct": trend.change_pct,
                    "sample_count": trend.sample_count,
                    "severity": trend.severity,
                }
            )
        stdout.write(_json.dumps(rows, indent=2) + "\n")
    else:
        stdout.write(format_trends(trends) + "\n")

    return 0


def _scope_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle ``workflow scope <file1> [file2 ...]``.

    Resolves the import-graph-derived read scope, blast radius, test files,
    and context sections for the given source files.

    Useful for scoping a workflow spec before writing it: supply the files you
    intend to write and see exactly what the model will need to read.

    Examples::

        workflow scope runtime/workflow/unified.py
        workflow scope runtime/workflow/unified.py runtime/prompt_renderer.py --json
        workflow scope runtime/workflow/unified.py --root /path/to/project
    """

    import json as _json

    from runtime.scope_resolver import resolve_scope

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow scope <file1> [file2 ...] [--root <dir>] [--json]\n"
            "\n"
            "  Show computed read scope, blast radius, tests, and context\n"
            "  sections for the given write-scope files.\n"
            "\n"
            "  --root <dir>   Project root (default: cwd)\n"
            "  --json         Output as JSON\n"
        )
        return 2

    files: list[str] = []
    root_dir = "."
    as_json = False

    i = 0
    while i < len(args):
        if args[i] == "--root" and i + 1 < len(args):
            root_dir = args[i + 1]
            i += 2
        elif args[i] == "--json":
            as_json = True
            i += 1
        elif args[i].startswith("-"):
            stdout.write(f"error: unknown argument: {args[i]}\n")
            return 2
        else:
            files.append(args[i])
            i += 1

    if not files:
        stdout.write("error: at least one file is required\n")
        return 2

    try:
        resolution = resolve_scope(files, root_dir=root_dir)
    except Exception as exc:
        stdout.write(f"error: scope resolution failed: {exc}\n")
        return 1

    if as_json:
        output = {
            "write_scope": resolution.write_scope,
            "computed_read_scope": resolution.computed_read_scope,
            "test_scope": resolution.test_scope,
            "blast_radius": resolution.blast_radius,
            "context_sections": [
                {"name": section["name"], "content_length": len(section["content"])}
                for section in resolution.context_sections
            ],
        }
        stdout.write(_json.dumps(output, indent=2) + "\n")
        return 0

    def _section(title: str, items: list[str]) -> None:
        stdout.write(f"\n{title} ({len(items)})\n")
        stdout.write("-" * (len(title) + 5) + "\n")
        if items:
            for item in items:
                stdout.write(f"  {item}\n")
        else:
            stdout.write("  (none)\n")

    stdout.write(f"Scope resolution for: {', '.join(resolution.write_scope)}\n")
    stdout.write("=" * 60 + "\n")

    _section("computed_read_scope", resolution.computed_read_scope)
    _section("test_scope", resolution.test_scope)
    _section("blast_radius", resolution.blast_radius)

    stdout.write(f"\ncontext_sections ({len(resolution.context_sections)})\n")
    stdout.write("-" * 25 + "\n")
    for section in resolution.context_sections:
        stdout.write(f"  {section['name']}  ({len(section['content'])} chars)\n")

    return 0


def _risk_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow risk` — show per-file risk scores.

    Analyzes workflow receipts to compute risk for each touched file,
    considering failure history, churn, complexity, failure diversity,
    staleness, and file size.
    """

    import json as _json
    from datetime import datetime, timezone

    from runtime.risk_scoring import RiskScorer, format_risk_table

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow risk [--json] [--limit N] [--file <path>]\n"
            "\n"
            "Show per-file risk scores computed from workflow history.\n"
            "Risk considers failure rate, churn, complexity, and staleness.\n"
            "\n"
            "  --json       Output as JSON array\n"
            "  --limit N    Limit to top N files (default 20)\n"
            "  --file PATH  Show risk for specific file only\n"
        )
        return 0

    limit = 20
    as_json = False
    target_file = None
    i = 0

    while i < len(args):
        if args[i] == "--limit" and i + 1 < len(args):
            try:
                limit = int(args[i + 1])
            except ValueError:
                stdout.write("error: --limit requires an integer\n")
                return 2
            i += 2
        elif args[i] == "--json":
            as_json = True
            i += 1
        elif args[i] == "--file" and i + 1 < len(args):
            target_file = args[i + 1]
            i += 2
        elif args[i] in {"-h", "--help"}:
            stdout.write("usage: workflow risk [--json] [--limit N] [--file <path>]\n")
            return 0
        else:
            stdout.write(f"error: unknown argument: {args[i]}\n")
            return 2

    try:
        scorer = RiskScorer()
        scores = scorer.compute_from_receipts()

        if target_file:
            matching = [score for score in scores if score.file_path == target_file]
            if not matching:
                stdout.write(f"error: no risk data for file: {target_file}\n")
                return 1
            scores = matching
        else:
            scores = scores[:limit]

        if as_json:
            result = {
                "kind": "risk_scores",
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "scores": [score.to_json() for score in scores],
                "summary": {
                    "total_files": len(scores),
                    "high_risk_count": len([score for score in scores if score.risk_score >= 70]),
                    "medium_risk_count": len(
                        [score for score in scores if 40 <= score.risk_score < 70]
                    ),
                    "low_risk_count": len([score for score in scores if score.risk_score < 40]),
                },
            }
            stdout.write(_json.dumps(result, indent=2) + "\n")
        else:
            stdout.write(format_risk_table(scores) + "\n")

        return 0

    except Exception as exc:
        stdout.write(f"error: failed to compute risk scores: {exc}\n")
        return 1


def _reviews_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle ``workflow reviews [--author <model>] [requirement <author> <task_type>]``.

    Subcommands / flags
    -------------------
    (no args)                   Show per-author bug density summary table.
    --author <model>            Show one author's review history and density stats.
    requirement <author> <tt>   Show the review requirement level for author + task_type.
    """

    import json as _json

    from runtime.review_tracker import get_review_tracker

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow reviews                       show author bug density summary\n"
            "       workflow reviews --author <model>      show one author's review history\n"
            "       workflow reviews requirement <author> <task_type>\n"
        )
        return 2

    tracker = get_review_tracker()

    if args and args[0] == "requirement":
        if len(args) < 3:
            stdout.write("usage: workflow reviews requirement <author_model> <task_type>\n")
            return 2
        author_model = args[1]
        task_type = args[2]
        level = tracker.review_requirement(author_model, task_type=task_type)
        density_stats = tracker.author_bug_density(author_model, task_type=task_type)
        stdout.write(
            _json.dumps(
                {
                    "author_model": author_model,
                    "task_type": task_type,
                    "review_requirement": level,
                    "stats": density_stats,
                },
                indent=2,
            )
            + "\n"
        )
        return 0

    if "--author" in args:
        idx = args.index("--author")
        if idx + 1 >= len(args):
            stdout.write("error: --author requires a model argument\n")
            return 2
        author_model = args[idx + 1]
        density = tracker.author_bug_density(author_model)
        history = tracker.author_review_history(author_model)
        requirement = tracker.review_requirement(author_model)
        stdout.write(
            _json.dumps(
                {
                    "author_model": author_model,
                    "review_requirement": requirement,
                    "density": density,
                    "history": history,
                },
                indent=2,
            )
            + "\n"
        )
        return 0

    summary = tracker.author_summary()
    if not summary:
        stdout.write("no review records found\n")
        return 0
    stdout.write(_json.dumps(summary, indent=2) + "\n")
    return 0


# ---------------------------------------------------------------------------
# workflow query <question> — natural language router (mirrors praxis_query)
# ---------------------------------------------------------------------------

def _query_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow query <question>` — route a natural language question."""

    as_json = False
    query_parts: list[str] = []
    for arg in args:
        if arg == "--json":
            as_json = True
        else:
            query_parts.append(arg)
    if not query_parts or query_parts[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow query <question> [--json]\n"
            "\n"
            "  Routes a natural language question to the right subsystem.\n"
            "  Examples:\n"
            "    workflow query 'what is the pass rate?'\n"
            "    workflow query 'are there any open bugs?'\n"
            "    workflow query 'schema for workflow_runs'\n"
            "    workflow query 'import path for BugTracker'\n"
            "    workflow query 'test command for runtime/compiler.py'\n"
        )
        return 2

    question = " ".join(query_parts)
    exit_code, payload = run_cli_tool("praxis_query", {"question": question})
    print_json(stdout, payload)
    return exit_code


# ---------------------------------------------------------------------------
# workflow bugs [list|search|stats] — bug tracker surface
# ---------------------------------------------------------------------------

def _bugs_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow bugs [list|search <query>|stats] [--status S] [--severity S] [--json]`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow bugs [list|search <query>|stats] [--status S] [--severity S] [--limit N] [--json]\n"
            "\n"
            "  list               List bugs (default: open only)\n"
            "  search <query>     Full-text search across bugs\n"
            "  stats              Bug counts by category/severity/status\n"
            "\n"
            "  --status S         Filter: OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED\n"
            "  --severity S       Filter: P0, P1, P2, P3\n"
            "  --limit N          Max results (default 25)\n"
            "  --all              Include resolved bugs (default: open only)\n"
        )
        return 2

    action = "list"
    search_query = ""
    status_filter = None
    severity_filter = None
    limit = 25
    open_only = True
    as_json = False
    i = 0

    if args and not args[0].startswith("-"):
        action = args[0]
        i = 1
        if action == "search" and i < len(args) and not args[i].startswith("-"):
            search_query = args[i]
            i += 1

    while i < len(args):
        if args[i] == "--status" and i + 1 < len(args):
            status_filter = args[i + 1].upper()
            i += 2
        elif args[i] == "--severity" and i + 1 < len(args):
            severity_filter = args[i + 1].upper()
            i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] == "--all":
            open_only = False
            i += 1
        elif args[i] == "--json":
            as_json = True
            i += 1
        else:
            i += 1

    params: dict[str, object] = {"action": action, "limit": limit}
    if action == "search":
        params["title"] = search_query
    if status_filter:
        params["status"] = status_filter
    if severity_filter:
        params["severity"] = severity_filter
    if open_only and action == "list" and not status_filter:
        params["open_only"] = True

    exit_code, payload = run_cli_tool("praxis_bugs", params)
    if as_json or action == "stats":
        print_json(stdout, payload)
        return exit_code
    render_bug_payload(payload, stdout=stdout)
    return exit_code


# ---------------------------------------------------------------------------
# workflow recall <query> — knowledge graph search
# ---------------------------------------------------------------------------

def _recall_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow recall <query> [--type T] [--json] [--limit N]`."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow recall <query> [--type T] [--limit N] [--json]\n"
            "\n"
            "  Search the knowledge graph by semantic similarity.\n"
            "  Types: task, fact, document, decision, constraint, topic,\n"
            "         table, code_unit, pattern, metric, module, person\n"
        )
        return 2

    query_parts: list[str] = []
    entity_type = None
    limit = 15
    as_json = False
    i = 0

    while i < len(args):
        if args[i] == "--type" and i + 1 < len(args):
            entity_type = args[i + 1]
            i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] == "--json":
            as_json = True
            i += 1
        else:
            query_parts.append(args[i])
            i += 1

    query = " ".join(query_parts)
    if not query:
        stdout.write("error: query is required\n")
        return 2

    params: dict[str, object] = {"query": query, "limit": limit}
    if entity_type:
        params["entity_type"] = entity_type
    exit_code, payload = run_cli_tool("praxis_recall", params)
    if as_json:
        print_json(stdout, payload)
        return exit_code
    render_recall_payload(payload, stdout=stdout)
    return exit_code


# ---------------------------------------------------------------------------
# workflow discover <query> — code similarity search
# ---------------------------------------------------------------------------

def _discover_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow discover <query> [--kind K] [--limit N] [--json]`."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow discover <query> [--kind K] [--limit N] [--json]\n"
            "\n"
            "  Find functionally similar code using vector similarity over AST fingerprints.\n"
            "  Kinds: module, class, function (default: all)\n"
            "\n"
            "  workflow discover 'retry with exponential backoff'\n"
            "  workflow discover 'parse JSON from stdin' --kind function\n"
            "  workflow discover reindex   (re-index the codebase)\n"
            "  workflow discover stats     (index statistics)\n"
        )
        return 2

    # Special actions
    if args[0] == "reindex":
        exit_code, payload = run_cli_tool("praxis_discover", {"action": "reindex"})
        print_json(stdout, payload)
        return exit_code
    if args[0] == "stats":
        exit_code, payload = run_cli_tool("praxis_discover", {"action": "stats"})
        print_json(stdout, payload)
        return exit_code

    query_parts: list[str] = []
    kind = None
    limit = 10
    as_json = False
    i = 0

    while i < len(args):
        if args[i] == "--kind" and i + 1 < len(args):
            kind = args[i + 1]
            i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] == "--json":
            as_json = True
            i += 1
        else:
            query_parts.append(args[i])
            i += 1

    query = " ".join(query_parts)
    if not query:
        stdout.write("error: query is required\n")
        return 2

    params: dict[str, object] = {"action": "search", "query": query, "limit": limit}
    if kind:
        params["kind"] = kind
    exit_code, payload = run_cli_tool("praxis_discover", params)
    if as_json:
        print_json(stdout, payload)
        return exit_code
    render_discover_payload(payload, stdout=stdout)
    return exit_code


# ---------------------------------------------------------------------------
# workflow artifacts [stats|search <q>|list <sandbox_id>]
# ---------------------------------------------------------------------------

def _artifacts_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow artifacts [stats|search <query>|list <sandbox_id>]`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow artifacts [stats|search <query>|list <sandbox_id>] [--json]\n"
            "\n"
            "  stats                 Index statistics (default)\n"
            "  search <query>        Search artifact file paths\n"
            "  list <sandbox_id>     List artifacts for a sandbox run\n"
        )
        return 2

    action = args[0] if args else "stats"
    as_json = "--json" in args
    params: dict[str, object] = {"action": action}
    if action == "search":
        query = " ".join(arg for arg in args[1:] if arg != "--json")
        if not query:
            stdout.write("error: search query required\n")
            return 2
        params["query"] = query
    elif action == "list":
        sandbox_id = next((arg for arg in args[1:] if arg != "--json"), "")
        if not sandbox_id:
            stdout.write("error: sandbox_id required\n")
            return 2
        params["sandbox_id"] = sandbox_id
    elif action not in {"stats", "search", "list"}:
        stdout.write(f"unknown action: {action}\n")
        return 2

    exit_code, payload = run_cli_tool("praxis_artifacts", params)
    if as_json or action == "stats":
        print_json(stdout, payload)
        return exit_code
    render_artifacts_payload(payload, stdout=stdout)
    return exit_code
