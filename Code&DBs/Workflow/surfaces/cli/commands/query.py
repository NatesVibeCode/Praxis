"""Read/query-oriented CLI command handlers."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, TextIO

from runtime.primitive_contracts import bug_query_default_open_only_backlog
from runtime.workspace_paths import repo_root as workspace_repo_root
from surfaces.cli.mcp_tools import (
    get_definition,
    print_json,
    require_confirmation,
    render_artifacts_payload,
    render_bug_payload,
    render_discover_payload,
    render_discover_stale_payload,
    render_recall_payload,
    run_cli_tool,
)


# ---------------------------------------------------------------------------
# Shared Postgres connection helper — lazy singleton for CLI commands that
# need direct DB access (bug tracker, knowledge graph, discover, artifacts).
# ---------------------------------------------------------------------------

_cli_pg_conn = None
WORKFLOW_ROOT = workspace_repo_root()
_SQL_KEYWORD_RE = re.compile(
    r"\b(SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM|WITH|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE)\b",
    re.IGNORECASE,
)
_SQL_CONTEXT_RE = re.compile(
    r"\b(FROM|WHERE|VALUES|SET|JOIN|RETURNING|LIMIT|GROUP\s+BY|ORDER\s+BY)\b",
    re.IGNORECASE,
)


def _get_conn():
    global _cli_pg_conn
    if _cli_pg_conn is None:
        from surfaces.cli._db import cli_sync_conn

        _cli_pg_conn = cli_sync_conn()
    return _cli_pg_conn


def _compact_excerpt(text: str, *, limit: int = 140) -> str:
    collapsed = " ".join(text.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3] + "..."


def _looks_like_sql_literal(value: str) -> bool:
    if len(value.strip()) < 20:
        return False
    if not _SQL_KEYWORD_RE.search(value):
        return False
    return "$1" in value or bool(_SQL_CONTEXT_RE.search(value))


def _scan_sql_literals(path: Path) -> list[dict[str, object]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return []

    violations: list[dict[str, object]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and _looks_like_sql_literal(node.value):
            violations.append(
                {
                    "path": str(path.relative_to(WORKFLOW_ROOT)),
                    "line": int(getattr(node, "lineno", 1)),
                    "excerpt": _compact_excerpt(node.value),
                }
            )
    return violations


def _scan_boundary_imports(path: Path, *, frontdoor: str) -> list[dict[str, object]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return []

    violations: list[dict[str, object]] = []

    def _record(module_name: str, *, line: int) -> None:
        if module_name == "runtime" or module_name.startswith("runtime."):
            violations.append(
                {
                    "rule": f"{frontdoor}_imports_runtime",
                    "path": str(path.relative_to(WORKFLOW_ROOT)),
                    "line": line,
                    "import": module_name,
                }
            )
        if module_name == "storage.postgres" or module_name.startswith("storage.postgres."):
            violations.append(
                {
                    "rule": f"{frontdoor}_imports_storage_postgres",
                    "path": str(path.relative_to(WORKFLOW_ROOT)),
                    "line": line,
                    "import": module_name,
                }
            )

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _record(alias.name, line=int(getattr(node, "lineno", 1)))
        elif isinstance(node, ast.ImportFrom) and node.module:
            _record(node.module, line=int(getattr(node, "lineno", 1)))

    return violations


def _iter_python_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def _scan_architecture(scope: str) -> dict[str, Any]:
    roots: list[tuple[str, Path]] = []
    if scope in {"all", "surfaces"}:
        roots.append(("surfaces", WORKFLOW_ROOT / "surfaces"))
    if scope in {"all", "scripts"}:
        roots.append(("scripts", WORKFLOW_ROOT / "scripts"))

    sql_literals: list[dict[str, object]] = []
    boundary_imports: list[dict[str, object]] = []
    scanned_files = 0

    for frontdoor, root in roots:
        if not root.exists():
            continue
        for path in _iter_python_files(root):
            scanned_files += 1
            sql_literals.extend(_scan_sql_literals(path))
            boundary_imports.extend(_scan_boundary_imports(path, frontdoor=frontdoor))

    summary = {
        "scanned_files": scanned_files,
        "sql_literals_outside_storage": len(sql_literals),
        "frontdoor_runtime_imports": sum(
            1 for violation in boundary_imports if str(violation.get("rule", "")).endswith("_imports_runtime")
        ),
        "frontdoor_storage_postgres_imports": sum(
            1
            for violation in boundary_imports
            if str(violation.get("rule", "")).endswith("_imports_storage_postgres")
        ),
    }
    summary["total_violations"] = (
        summary["sql_literals_outside_storage"]
        + summary["frontdoor_runtime_imports"]
        + summary["frontdoor_storage_postgres_imports"]
    )

    return {
        "scope": scope,
        "rule_set": {
            "sql_literals_outside_storage": (
                "Raw SQL string literals should stay in storage or repository authority modules, "
                "not in CLI/API/scripts front doors."
            ),
            "frontdoor_imports_runtime": (
                "Front-door modules importing runtime.* are probably reaching past a stable service boundary."
            ),
            "frontdoor_imports_storage_postgres": (
                "Front-door modules importing storage.postgres.* are coupling directly to SQL authority."
            ),
        },
        "summary": summary,
        "violations": {
            "sql_literals_outside_storage": sql_literals,
            "frontdoor_imports": boundary_imports,
        },
    }


def _render_architecture_payload(payload: dict[str, Any], *, stdout: TextIO, limit: int) -> None:
    summary = payload.get("summary", {})
    stdout.write(f"Architecture scan ({payload.get('scope', 'all')})\n")
    stdout.write(
        "  files scanned: {files} | total violations: {violations}\n".format(
            files=summary.get("scanned_files", 0),
            violations=summary.get("total_violations", 0),
        )
    )
    stdout.write(
        "  raw SQL outside storage: {sql} | front-door runtime imports: {runtime} | "
        "front-door storage.postgres imports: {storage}\n".format(
            sql=summary.get("sql_literals_outside_storage", 0),
            runtime=summary.get("frontdoor_runtime_imports", 0),
            storage=summary.get("frontdoor_storage_postgres_imports", 0),
        )
    )

    sql_literals = list(payload.get("violations", {}).get("sql_literals_outside_storage", []))
    if sql_literals:
        stdout.write("\nRaw SQL literals outside storage:\n")
        for violation in sql_literals[:limit]:
            stdout.write(
                f"  {violation['path']}:{violation['line']}  {violation['excerpt']}\n"
            )

    boundary_imports = list(payload.get("violations", {}).get("frontdoor_imports", []))
    if boundary_imports:
        stdout.write("\nFront-door imports reaching inward:\n")
        for violation in boundary_imports[:limit]:
            stdout.write(
                f"  {violation['path']}:{violation['line']}  {violation['rule']} -> {violation['import']}\n"
            )

    if not sql_literals and not boundary_imports:
        stdout.write("\nNo violations found.\n")


def _architecture_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow architecture [scan] [--scope S] [--limit N] [--json]`."""

    if args and (
        args[0] in {"-h", "--help"}
        or (args[0] == "scan" and len(args) > 1 and args[1] in {"-h", "--help"})
    ):
        stdout.write(
            "usage: workflow architecture [scan] [--scope all|surfaces|scripts] [--limit N] [--json]\n"
            "\n"
            "  Exact static scan for front-door architecture drift.\n"
            "  Reports raw SQL literals in `surfaces/` or `scripts/`, plus front-door imports\n"
            "  of `runtime.*` and `storage.postgres.*`.\n"
            "\n"
            "  Examples:\n"
            "    workflow architecture scan\n"
            "    workflow architecture scan --scope surfaces\n"
            "    workflow architecture scan --scope surfaces --json\n"
            "    workflow architecture --scope scripts --limit 10\n"
        )
        return 2

    action = "scan"
    scope = "all"
    limit = 20
    as_json = False
    i = 0

    if args and args[0] == "scan":
        i = 1

    while i < len(args):
        if args[i] == "--scope" and i + 1 < len(args):
            scope = args[i + 1].strip()
            i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] == "--json":
            as_json = True
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    if action != "scan":
        stdout.write(f"unknown architecture action: {action}\n")
        return 2
    if scope not in {"all", "surfaces", "scripts"}:
        stdout.write("error: --scope must be one of all, surfaces, scripts\n")
        return 2

    payload = _scan_architecture(scope)
    if as_json:
        print_json(stdout, payload)
        return 0
    _render_architecture_payload(payload, stdout=stdout, limit=limit)
    return 0


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
        elif args[i] in {"--json"}:
            i += 1
        else:
            stdout.write(f"unknown argument: {args[i]}\n")
            return 2

    try:
        scorer.compute_from_receipts()
    except Exception as exc:
        stdout.write(f"error: failed to compute from receipts: {exc}\n")
        return 1

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
            "  Natural-language router for the platform. Best first stop when you do not yet know\n"
            "  which exact specialist command to use.\n"
            "  Examples:\n"
            "    workflow query 'what is failing right now?'\n"
            "    workflow query 'what is the pass rate?'\n"
            "    workflow query 'are there any open bugs?'\n"
            "    workflow query 'issue backlog'\n"
            "    workflow query 'schema for workflow_runs'\n"
            "    workflow query 'import path for BugTracker'\n"
            "    workflow query 'test command for runtime/compiler.py'\n"
            "    workflow query 'how much did we spend on tokens today?'\n"
        )
        return 2

    question = " ".join(query_parts)
    exit_code, payload = run_cli_tool("praxis_query", {"question": question})
    print_json(stdout, payload)
    return exit_code


# ---------------------------------------------------------------------------
# workflow bugs [list|search|duplicate_check|stats] — bug tracker surface
# ---------------------------------------------------------------------------

def _bugs_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow bugs [list|search <query>|duplicate_check <query>|stats] ...`."""

    def _write_bugs_usage() -> None:
        stdout.write(
            "usage: workflow bugs "
            "[list|search <query>|duplicate_check <query>|stats|file|history|packet|replay|backfill_replay|attach_evidence|patch_resume|resolve] "
            "[--status S] [--severity S] [--limit N] [--json]\n"
            "\n"
            "  list               List bugs (default: open only)\n"
            "  search <query>     Hybrid bug search (Postgres FTS plus vector ranking when enabled)\n"
            "  duplicate_check <query>\n"
            "                     Fast title-like duplicate check without replay or cluster enrichment\n"
            "  stats              Bug counts by category/severity/status\n"
            "  file               File a new bug (use --dry-run to validate without inserting)\n"
            "  history            Show bug history and linked evidence\n"
            "  packet             Show a replay packet for one bug\n"
            "  replay             Replay a bug from canonical evidence\n"
            "  backfill_replay    Backfill replay provenance for bugs\n"
            "  attach_evidence    Attach canonical evidence to a bug\n"
            "  patch_resume       Update a bug's resume context\n"
            "  resolve            Mark an existing bug fixed, deferred, or won't-fix; FIXED may run verifier proof\n"
            "\n"
            "  --status S         Filter: OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED\n"
            "  --severity S       Filter: P0, P1, P2, P3\n"
            "  --category S       Filing category: SCOPE, VERIFY, IMPORT, WIRING, ARCHITECTURE, RUNTIME, TEST, OTHER\n"
            "  --limit N          Max results (default 25)\n"
            "  --all              Include resolved bugs (default: open only)\n"
            "  --open-only        Limit list output to open bugs; accepted for agent-friendly parity\n"
            "  --body TEXT        Alias for --description on filing and duplicate checks\n"
            "  --dry-run, --preview\n"
            "                     With file: validate and show preview; do not insert a bug\n"
            "\n"
            "  Examples:\n"
            "    workflow bugs list --severity P1\n"
            "    workflow bugs list --open-only --json\n"
            "    workflow bugs history BUG-1234\n"
            "    workflow bugs duplicate_check --title 'routing timeout' --body 'worker hangs during dispatch'\n"
            "    workflow bugs duplicate_check 'routing timeout'\n"
            "    workflow bugs search routing\n"
            "    workflow bugs search timeout --status OPEN --limit 5\n"
            "    workflow bugs stats\n"
            "    workflow bugs resolve --bug-id BUG-1234 --status FIXED --verifier-ref verifier.job.python.pytest_file --inputs-json '{\"path\":\"Code&DBs/Workflow/tests/unit/test_bug.py\"}'\n"
        )

    if args and args[0] in {"-h", "--help", "help"}:
        _write_bugs_usage()
        return 0

    action = "list"
    search_query = ""
    status_filter = None
    severity_filter = None
    limit = 25
    open_only = bug_query_default_open_only_backlog()
    as_json = False
    params: dict[str, object] = {}
    i = 0

    if args and not args[0].startswith("-"):
        action = args[0].replace("-", "_")
        i = 1
        if action in {"search", "duplicate_check"} and i < len(args) and not args[i].startswith("-"):
            search_query = args[i]
            i += 1
        elif action in {
            "history",
            "packet",
            "replay",
            "attach_evidence",
            "patch_resume",
            "resolve",
        } and i < len(args) and not args[i].startswith("-"):
            params["bug_id"] = args[i]
            i += 1

    def _require_value(flag: str) -> str | None:
        nonlocal i
        if i + 1 >= len(args):
            stdout.write(f"error: {flag} requires a value\n")
            return None
        value = args[i + 1]
        i += 2
        return value

    while i < len(args):
        token = args[i]
        if token == "--status":
            value = _require_value(token)
            if value is None:
                return 2
            status_filter = value.upper()
            continue
        if token == "--severity":
            value = _require_value(token)
            if value is None:
                return 2
            severity_filter = value.upper()
            continue
        if token == "--limit":
            value = _require_value(token)
            if value is None:
                return 2
            limit = int(value)
            continue
        if token == "--all":
            open_only = False
            i += 1
            continue
        if token == "--open-only":
            open_only = True
            i += 1
            continue
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token in {"--dry-run", "--preview"}:
            params["dry_run"] = True
            i += 1
            continue
        if token == "--yes":
            i += 1
            continue
        if token == "--bug-id":
            value = _require_value(token)
            if value is None:
                return 2
            params["bug_id"] = value
            continue
        if token == "--title":
            value = _require_value(token)
            if value is None:
                return 2
            params["title"] = value
            continue
        if token == "--body":
            value = _require_value(token)
            if value is None:
                return 2
            params["description"] = value
            continue
        if token == "--description":
            value = _require_value(token)
            if value is None:
                return 2
            params["description"] = value
            continue
        if token == "--category":
            value = _require_value(token)
            if value is None:
                return 2
            params["category"] = value
            continue
        if token == "--filed-by":
            value = _require_value(token)
            if value is None:
                return 2
            params["filed_by"] = value
            continue
        if token == "--source-kind":
            value = _require_value(token)
            if value is None:
                return 2
            params["source_kind"] = value
            continue
        if token == "--decision-ref":
            value = _require_value(token)
            if value is None:
                return 2
            params["decision_ref"] = value
            continue
        if token == "--discovered-in-run-id":
            value = _require_value(token)
            if value is None:
                return 2
            params["discovered_in_run_id"] = value
            continue
        if token == "--discovered-in-receipt-id":
            value = _require_value(token)
            if value is None:
                return 2
            params["discovered_in_receipt_id"] = value
            continue
        if token == "--owner-ref":
            value = _require_value(token)
            if value is None:
                return 2
            params["owner_ref"] = value
            continue
        if token == "--evidence-kind":
            value = _require_value(token)
            if value is None:
                return 2
            params["evidence_kind"] = value
            continue
        if token == "--evidence-ref":
            value = _require_value(token)
            if value is None:
                return 2
            params["evidence_ref"] = value
            continue
        if token == "--evidence-role":
            value = _require_value(token)
            if value is None:
                return 2
            params["evidence_role"] = value
            continue
        if token == "--created-by":
            value = _require_value(token)
            if value is None:
                return 2
            params["created_by"] = value
            continue
        if token == "--notes":
            value = _require_value(token)
            if value is None:
                return 2
            params["notes"] = value
            continue
        if token == "--verifier-ref":
            value = _require_value(token)
            if value is None:
                return 2
            params["verifier_ref"] = value
            continue
        if token == "--inputs-json":
            value = _require_value(token)
            if value is None:
                return 2
            params["inputs"] = json.loads(value)
            continue
        if token == "--target-kind":
            value = _require_value(token)
            if value is None:
                return 2
            params["target_kind"] = value
            continue
        if token == "--target-ref":
            value = _require_value(token)
            if value is None:
                return 2
            params["target_ref"] = value
            continue
        if token == "--resume-context-json":
            value = _require_value(token)
            if value is None:
                return 2
            params["resume_context"] = json.loads(value)
            continue
        if token == "--resume-patch-json":
            value = _require_value(token)
            if value is None:
                return 2
            params["resume_patch"] = json.loads(value)
            continue
        if token == "--patch-json":
            value = _require_value(token)
            if value is None:
                return 2
            params["patch"] = json.loads(value)
            continue
        if token == "--tags":
            value = _require_value(token)
            if value is None:
                return 2
            params["tags"] = value
            continue
        if token == "--exclude-tags":
            value = _require_value(token)
            if value is None:
                return 2
            params["exclude_tags"] = value
            continue
        if token == "--receipt-limit":
            value = _require_value(token)
            if value is None:
                return 2
            params["receipt_limit"] = int(value)
            continue
        if token in {"-h", "--help", "help"}:
            _write_bugs_usage()
            return 0
        if token == "--include-replay-state":
            params["include_replay_state"] = True
            i += 1
            continue
        if token == "--replay-ready-only":
            params["replay_ready_only"] = True
            i += 1
            continue
        stdout.write(f"unknown argument: {token}\n")
        return 2

    params["action"] = action
    params["limit"] = limit
    if action == "search":
        params["title"] = search_query
    if action == "duplicate_check":
        params["title_like"] = search_query or str(params.get("title") or "")
    if status_filter:
        params["status"] = status_filter
    if severity_filter:
        params["severity"] = severity_filter
    if open_only and action == "list" and not status_filter:
        params["open_only"] = True

    exit_code, payload = run_cli_tool("praxis_bugs", params)
    if as_json or action == "stats" or action not in {"list", "search", "duplicate_check"}:
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
            "  Search the knowledge graph plus durable operator decisions by ranked text match,\n"
            "  graph traversal, vector similarity, and authority-backed decision scans.\n"
            "  Types: task, fact, document, decision, constraint, topic,\n"
            "         table, code_unit, pattern, metric, module, person\n"
            "\n"
            "  Examples:\n"
            "    workflow recall 'provider routing' --type decision\n"
            "    workflow recall 'dispatch run completion trigger retirement'\n"
            "    workflow recall 'workflow_runs' --type table --limit 5\n"
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
            "  Find related code with hybrid retrieval: AST fingerprint vectors + Postgres full-text search,\n"
            "  fused into one ranked result set.\n"
            "  Kinds: module, class, function (default: all)\n"
            "\n"
            "  workflow discover 'retry with exponential backoff'\n"
            "  workflow discover 'parse JSON from stdin' --kind function\n"
            "  workflow discover 'rate limit backoff' --limit 5\n"
            "  workflow discover 'Postgres connection pooling' --kind module\n"
            "  workflow discover reindex --yes   (re-index the codebase)\n"
            "  workflow discover stats     (index statistics)\n"
            "  workflow discover stale-check [--json]   (count files whose source drifted from the index)\n"
        )
        return 2

    # Special actions
    if args[0] == "reindex":
        confirmed = "--yes" in args[1:]
        definition = get_definition("praxis_discover")
        if definition is None:
            stdout.write("tool definition not found: praxis_discover\n")
            return 2
        confirmation_result = require_confirmation(
            definition,
            {"action": "reindex"},
            confirmed=confirmed,
            stdout=stdout,
        )
        if confirmation_result is not None:
            return confirmation_result
        exit_code, payload = run_cli_tool("praxis_discover", {"action": "reindex"})
        print_json(stdout, payload)
        return exit_code
    if args[0] == "stats":
        exit_code, payload = run_cli_tool("praxis_discover", {"action": "stats"})
        print_json(stdout, payload)
        return exit_code
    if args[0] == "stale-check":
        as_json = "--json" in args[1:]
        exit_code, payload = run_cli_tool("praxis_discover", {"action": "stale-check"})
        if as_json:
            print_json(stdout, payload)
            return exit_code
        render_discover_stale_payload(payload, stdout=stdout)
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
# workflow artifacts [stats|search <q>|list [sandbox_id]]
# ---------------------------------------------------------------------------

def _artifacts_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow artifacts [stats|search <query>|list [sandbox_id]]`."""

    if args and args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow artifacts [stats|search <query>|list [sandbox_id]] [--json]\n"
            "\n"
            "  stats                 Index statistics (default)\n"
            "  search <query>        Search artifact file paths\n"
            "  list [sandbox_id]     List artifacts for a sandbox run (defaults to latest)\n"
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
        if sandbox_id:
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


# ---------------------------------------------------------------------------
# workflow research [list|<topic>] — parallel research workflow launcher
# ---------------------------------------------------------------------------

def _research_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow research [list|<topic>] [--workers N] [--agent SLUG] [--threshold N] [--json]`."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow research [list|<topic>] [--workers N] [--agent SLUG] [--threshold N] [--json]\n"
            "\n"
            "  Launch or inspect the parallel research workflow frontdoor.\n"
            "  list               Show recent research workflow runs\n"
            "  <topic>            Build and launch a multi-angle research workflow\n"
            "\n"
            "  Examples:\n"
            "    workflow research 'API auth drift'\n"
            "    workflow research 'provider routing tradeoffs' --workers 20 --agent auto/research\n"
            "    workflow research list --json\n"
        )
        return 2

    action = "run"
    topic_parts: list[str] = []
    workers = 40
    agent = "auto/research"
    threshold = None
    as_json = False
    i = 0

    if args[0] == "list":
        action = "list"
        i = 1

    while i < len(args):
        token = args[i]
        if token == "--workers" and i + 1 < len(args):
            workers = int(args[i + 1])
            i += 2
            continue
        if token == "--agent" and i + 1 < len(args):
            agent = args[i + 1]
            i += 2
            continue
        if token == "--threshold" and i + 1 < len(args):
            threshold = int(args[i + 1])
            i += 2
            continue
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            i += 1
            continue
        topic_parts.append(token)
        i += 1

    params: dict[str, object] = {"action": action}
    if action == "run":
        topic = " ".join(topic_parts).strip()
        if not topic:
            stdout.write("error: topic is required\n")
            return 2
        params.update({
            "topic": topic,
            "workers": workers,
            "agent": agent,
        })
        if threshold is not None:
            params["threshold"] = threshold

    exit_code, payload = run_cli_tool("praxis_research_workflow", params)
    if as_json or action == "list":
        print_json(stdout, payload)
        return exit_code
    print_json(stdout, payload)
    return exit_code


# ---------------------------------------------------------------------------
# workflow decompose <objective...> [--scope-files a,b,c] [--json]
# ---------------------------------------------------------------------------

def _render_decompose_payload(payload: dict[str, Any], *, stdout: TextIO) -> None:
    if payload.get("error"):
        print_json(stdout, payload)
        return

    stdout.write(
        "DECOMPOSE\n"
        f"  total_sprints: {payload.get('total_sprints', 0)}\n"
        f"  total_estimate_minutes: {payload.get('total_estimate_minutes', 0)}\n"
    )
    critical_path = [str(step) for step in payload.get("critical_path", []) if str(step).strip()]
    if critical_path:
        stdout.write(f"  critical_path: {' -> '.join(critical_path)}\n")
    sprints = payload.get("sprints", [])
    if isinstance(sprints, list) and sprints:
        stdout.write("  sprints:\n")
        for sprint in sprints:
            if not isinstance(sprint, dict):
                continue
            label = str(sprint.get("label") or "").strip() or "<unnamed>"
            complexity = str(sprint.get("complexity") or "").strip() or "unknown"
            estimate = sprint.get("estimate_minutes")
            stdout.write(f"    - {label} ({complexity}, {estimate}m)\n")


def _decompose_command(args: list[str], *, stdout: TextIO) -> int:
    """Handle `workflow decompose <objective...>` via the canonical sprint decomposer."""

    if not args or args[0] in {"-h", "--help"}:
        stdout.write(
            "usage: workflow decompose <objective...> [--scope-files a,b,c] [--json]\n"
            "\n"
            "  Decompose one objective into micro-sprints using the canonical sprint decomposer.\n"
            "  --scope-files   Optional comma-separated file list to bias the decomposition\n"
            "  --json          Emit machine-readable JSON output\n"
            "\n"
            "  Examples:\n"
            "    workflow decompose 'build real-time notifications'\n"
            "    workflow decompose 'refactor auth flow' --scope-files src/auth.py,src/session.py\n"
        )
        return 2

    objective_parts: list[str] = []
    scope_files: list[str] = []
    as_json = False
    i = 0

    while i < len(args):
        token = args[i]
        if token == "--scope-files" and i + 1 < len(args):
            scope_files = [part.strip() for part in args[i + 1].split(",") if part.strip()]
            i += 2
            continue
        if token == "--json":
            as_json = True
            i += 1
            continue
        objective_parts.append(token)
        i += 1

    objective = " ".join(objective_parts).strip()
    if not objective:
        stdout.write("error: objective is required\n")
        return 2

    params: dict[str, object] = {"objective": objective}
    if scope_files:
        params["scope_files"] = scope_files

    exit_code, payload = run_cli_tool("praxis_decompose", params)
    if as_json:
        print_json(stdout, payload)
        return exit_code
    _render_decompose_payload(payload, stdout=stdout)
    return exit_code
