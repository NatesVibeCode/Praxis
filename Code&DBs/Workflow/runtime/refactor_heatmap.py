"""Refactor heatmap read model.

This module turns architecture-bug authority plus local source topology into a
ranked, deterministic refactor map. It is intentionally read-only: bug rows
remain the durable issue authority, while this read model explains where spread
and coupling make future work expensive.
"""

from __future__ import annotations

import ast
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from runtime.bug_tracker import BugCategory, bug_resolved_status_values_with_legacy
from surfaces.api.handlers._shared import _bug_to_dict


SOURCE_SUFFIXES = frozenset({".py", ".sql", ".ts", ".tsx", ".js", ".json", ".md", ".sh"})
EXCLUDED_PARTS = frozenset({
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "artifacts",
    "dist",
    "node_modules",
})


@dataclass(frozen=True, slots=True)
class RefactorDomain:
    slug: str
    title: str
    terms: tuple[str, ...]
    authority_model: str
    failure_modes: tuple[str, ...]
    recommended_change: str


DOMAINS: tuple[RefactorDomain, ...] = (
    RefactorDomain(
        slug="workflow_execution_lifecycle",
        title="Workflow execution lifecycle",
        terms=("workflow run", "workflow_runs", "run_status", "execution_status", "claim", "lease", "worker", "receipt", "admission"),
        authority_model="One lifecycle state-machine authority emits state transitions, leases, receipts, and status projections.",
        failure_modes=("state transitions hidden in frontdoors", "receipts disagree with status", "agents cannot identify the owner of run health"),
        recommended_change="Collapse admission, claim, lease, worker, receipt, and status decisions behind one workflow lifecycle read/write boundary.",
    ),
    RefactorDomain(
        slug="service_lifecycle",
        title="Service lifecycle",
        terms=("service", "launch", "readiness", "health", "runtime_target", "substrate", "docker", "compose", "daemon"),
        authority_model="Runtime-target records own service lifecycle; scripts become thin launch adapters.",
        failure_modes=("target-specific launch paths drift", "setup implies mutation without durable state", "health checks inspect different authorities"),
        recommended_change="Promote service lifecycle to runtime-target-neutral DB authority and demote shell paths to adapters.",
    ),
    RefactorDomain(
        slug="registry_catalog_tool_authority",
        title="Registry/catalog/tool authority",
        terms=("operation_catalog_registry", "authority_object_registry", "data_dictionary_objects", "tools list", "tools describe", "tool catalog", "registry", "catalog"),
        authority_model="The operation catalog and data dictionary are the single registry authority; surfaces consume projections.",
        failure_modes=("catalog rows point at stale handlers", "MCP/API/CLI metadata diverge", "tool contracts cannot be trusted by future agents"),
        recommended_change="Register operations atomically through the catalog wizard and make tool/frontdoor metadata projections, not parallel registries.",
    ),
    RefactorDomain(
        slug="database_environment_authority",
        title="Database environment authority",
        terms=("workflow_database_url", "database_url", "psql", "postgres", "localhost", "_workflow_database"),
        authority_model="One resolver identifies the network Postgres authority and exposes a redacted fingerprint.",
        failure_modes=("shell and runtime resolve different databases", "localhost fallback revives retired state", "tests pass against the wrong authority"),
        recommended_change="Centralize DB resolution and make every surface expose the same redacted authority fingerprint.",
    ),
    RefactorDomain(
        slug="discovery_recall_query_memory",
        title="Discovery/recall/query/memory retrieval",
        terms=("praxis_search", "praxis_discover", "praxis_recall", "praxis_query", "memory", "module_indexer", "retrieval"),
        authority_model="One retrieval authority binds freshness, source selection, and evidence pointers across code, memory, bugs, receipts, and DB.",
        failure_modes=("query routes to empty projections", "agents get different answers by frontdoor", "freshness and source ranking are implicit"),
        recommended_change="Move retrieval semantics into one DB-backed/search-backed read model and keep query/discover/recall as explicit views.",
    ),
    RefactorDomain(
        slug="provider_routing_admission",
        title="Provider routing/admission",
        terms=("provider", "model", "route", "transport", "access_control", "task_type_router", "onboarding"),
        authority_model="A recorded route decision is the only dispatchable provider/model/transport truth.",
        failure_modes=("disabled routes remain mechanically callable", "API/CLI lane choice bypasses operator policy", "adapters recompute admission"),
        recommended_change="Move candidate loading and admission filters to one route authority; adapters consume recorded decisions only.",
    ),
    RefactorDomain(
        slug="credential_secret_authority",
        title="Credential/secret authority",
        terms=("credential", "secret", "keychain", "oauth", "dotenv", "env forwarding", "api key", "auth"),
        authority_model="Keychain-backed credential resolver owns secrets; sandboxes receive only scoped hydrated material.",
        failure_modes=("host env leaks into sandboxes", "provider auth bypasses catalog state", "LLMs are asked to handle secrets"),
        recommended_change="Make per-sandbox credential manifests explicit, allowlisted, and receipt-backed.",
    ),
    RefactorDomain(
        slug="event_receipt_projection",
        title="Event/receipt projection",
        terms=("authority_operation_receipts", "authority_events", "receipt", "event", "projection", "evidence"),
        authority_model="Gateway receipts and events are atomic proof, not response decoration.",
        failure_modes=("proof cannot be replayed", "events and read models drift", "bugs lack evidence links"),
        recommended_change="Make receipt/event writes inseparable from operation execution and project all operator views from that proof.",
    ),
    RefactorDomain(
        slug="workspace_scope_boundary",
        title="Workspace/scope boundary",
        terms=("sandbox", "workspace", "write_scope", "read_scope", "blast_radius", "source_refs", "scope"),
        authority_model="Workflow tokens and envelopes define read/write/test/blast-radius scope; broad tools fail closed.",
        failure_modes=("writes cross workspace boundaries", "scope lives in prompts instead of enforcement", "future agents over-read or over-write"),
        recommended_change="Clamp tool, repo materialization, and write-set reconciliation to the compiled workflow scope.",
    ),
    RefactorDomain(
        slug="observability_quality_metrics",
        title="Observability/quality metrics",
        terms=("metrics", "observability", "topology", "dashboard", "quality", "telemetry", "quality_rollups"),
        authority_model="Metrics are populated read models with receipts/freshness, not empty dashboard affordances.",
        failure_modes=("quality queries return empty rollups", "architecture debt is prose-only", "operator cannot rank cleanup by evidence"),
        recommended_change="Materialize operator heatmaps and quality rollups as queryable, receipt-backed read models.",
    ),
)


def _repo_root(subsystems: Any) -> Path:
    candidate = getattr(subsystems, "_repo_root", None) or getattr(subsystems, "repo_root", None)
    if candidate is not None:
        return Path(candidate)
    from runtime.workspace_paths import repo_root

    return repo_root()


def _normalized_terms(domain: RefactorDomain) -> tuple[str, ...]:
    return tuple(term.lower() for term in domain.terms)


def _source_roots(root: Path) -> tuple[Path, ...]:
    candidates = (root / "Code&DBs" / "Workflow", root / "scripts")
    return tuple(path for path in candidates if path.exists())


def _should_scan(path: Path) -> bool:
    return path.is_file() and path.suffix in SOURCE_SUFFIXES and not any(
        part in EXCLUDED_PARTS for part in path.parts
    ) and path.name != "refactor_heatmap.py"


def _coarse_module(rel: str) -> str:
    parts = rel.split("/")
    return "/".join(parts[:3]) if len(parts) >= 3 else rel


def _read_source_files(root: Path, *, include_tests: bool) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for source_root in _source_roots(root):
        for path in source_root.rglob("*"):
            if not _should_scan(path):
                continue
            rel = path.relative_to(root).as_posix()
            if not include_tests and "/tests/" in f"/{rel}/":
                continue
            try:
                rows.append((rel, path.read_text(errors="ignore")))
            except OSError:
                continue
    return rows


def _contains_any(haystack: str, terms: tuple[str, ...]) -> bool:
    return any(term in haystack for term in terms)


def _term_hit_count(haystack: str, terms: tuple[str, ...]) -> int:
    return sum(1 for term in terms if term in haystack)


def _specific_term_hit(haystack: str, terms: tuple[str, ...]) -> bool:
    return any(
        term in haystack and ("_" in term or "." in term or " " in term)
        for term in terms
    )


def _source_domain_hit(haystack: str, terms: tuple[str, ...]) -> bool:
    return _term_hit_count(haystack, terms) >= 2 or _specific_term_hit(haystack, terms)


def _surface_coupling(rel: str, text: str) -> bool:
    if not rel.startswith("Code&DBs/Workflow/surfaces/"):
        return False
    return any(
        marker in text
        for marker in (
            "from runtime.",
            "import runtime.",
            "from storage.",
            "import storage.",
            "from registry.",
            "import registry.",
            "from adapters.",
            "import adapters.",
        )
    )


def _long_symbols(
    *,
    root: Path,
    source_files: list[tuple[str, str]],
    threshold: int,
    domains: tuple[RefactorDomain, ...],
) -> dict[str, list[dict[str, Any]]]:
    by_domain: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rel, text in source_files:
        if not rel.endswith(".py"):
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        lower = text.lower()
        matching_domains = [
            domain for domain in domains if _source_domain_hit(lower, _normalized_terms(domain))
        ]
        if not matching_domains:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            end_lineno = getattr(node, "end_lineno", None)
            if end_lineno is None:
                continue
            length = int(end_lineno) - int(node.lineno) + 1
            if length < threshold:
                continue
            symbol = {
                "path": rel,
                "line": int(node.lineno),
                "kind": type(node).__name__,
                "name": str(node.name),
                "lines": length,
            }
            for domain in matching_domains:
                by_domain[domain.slug].append(symbol)
    for slug, symbols in list(by_domain.items()):
        by_domain[slug] = sorted(symbols, key=lambda item: (-int(item["lines"]), item["path"]))[:8]
    return by_domain


def _bug_text(bug: dict[str, Any]) -> str:
    parts = [
        bug.get("title"),
        bug.get("description"),
        bug.get("category"),
        bug.get("severity"),
        bug.get("status"),
        " ".join(str(tag) for tag in bug.get("tags") or ()),
        json.dumps(bug.get("resume_context") or {}, sort_keys=True, default=str),
    ]
    return " ".join(str(part or "") for part in parts).lower()


def _load_architecture_bugs(subsystems: Any, *, bug_limit: int, open_only: bool) -> list[dict[str, Any]]:
    try:
        tracker = subsystems.get_bug_tracker()
        bugs = tracker.list_bugs(
            category=BugCategory.ARCHITECTURE,
            open_only=open_only,
            limit=bug_limit,
        )
    except Exception as exc:
        return [{
            "bug_id": None,
            "title": "Architecture bug authority unavailable",
            "status": "UNKNOWN",
            "severity": "UNKNOWN",
            "category": "ARCHITECTURE",
            "description": f"{type(exc).__name__}: {exc}",
            "_heatmap_error": "bug_authority_unavailable",
        }]
    return [_bug_to_dict(bug) for bug in bugs]


def _bug_domain_matches(
    bugs: list[dict[str, Any]],
    domains: tuple[RefactorDomain, ...],
) -> dict[str, list[dict[str, Any]]]:
    matches: dict[str, list[dict[str, Any]]] = {domain.slug: [] for domain in domains}
    for bug in bugs:
        text = _bug_text(bug)
        for domain in domains:
            if _term_hit_count(text, _normalized_terms(domain)) >= 2:
                matches[domain.slug].append(bug)
    return matches


def _scan_topology(
    *,
    root: Path,
    source_files: list[tuple[str, str]],
    domains: tuple[RefactorDomain, ...],
) -> dict[str, dict[str, Any]]:
    topology: dict[str, dict[str, Any]] = {}
    for domain in domains:
        terms = _normalized_terms(domain)
        hit_files: list[tuple[int, str]] = []
        coupled_files: list[tuple[int, str]] = []
        loc = 0
        modules: set[str] = set()
        for rel, text in source_files:
            lower = text.lower()
            hit_count = _term_hit_count(lower, terms)
            if hit_count < 2 and not _specific_term_hit(lower, terms):
                continue
            hit_files.append((hit_count, rel))
            loc += text.count("\n") + 1
            modules.add(_coarse_module(rel))
            if _surface_coupling(rel, text):
                coupled_files.append((hit_count, rel))
        ranked_hit_files = [rel for _count, rel in sorted(hit_files, key=lambda item: (-item[0], item[1]))]
        ranked_coupled_files = [
            rel for _count, rel in sorted(coupled_files, key=lambda item: (-item[0], item[1]))
        ]
        topology[domain.slug] = {
            "spread_files": len(hit_files),
            "coarse_modules": len(modules),
            "loc_in_hit_files": loc,
            "surface_coupling_files": len(coupled_files),
            "representative_paths": ranked_hit_files[:10],
            "surface_coupling_examples": ranked_coupled_files[:8],
        }
    return topology


def _priority(score: float, p1_count: int) -> str:
    if p1_count or score >= 90:
        return "P1"
    if score >= 45:
        return "P2"
    return "P3"


def build_refactor_heatmap(
    subsystems: Any,
    *,
    limit: int = 15,
    include_tests: bool = False,
    include_domains: list[str] | None = None,
    bug_limit: int = 250,
    long_symbol_threshold: int = 120,
    open_only: bool = True,
) -> dict[str, Any]:
    """Build the deterministic refactor heatmap read model."""

    root = _repo_root(subsystems)
    selected = DOMAINS
    if include_domains:
        allowed = {str(slug).strip() for slug in include_domains if str(slug).strip()}
        selected = tuple(domain for domain in DOMAINS if domain.slug in allowed)

    source_files = _read_source_files(root, include_tests=include_tests)
    topology = _scan_topology(root=root, source_files=source_files, domains=selected)
    long_symbols = _long_symbols(
        root=root,
        source_files=source_files,
        threshold=max(40, int(long_symbol_threshold)),
        domains=selected,
    )
    bugs = _load_architecture_bugs(subsystems, bug_limit=bug_limit, open_only=open_only)
    bug_matches = _bug_domain_matches(bugs, selected)

    rows: list[dict[str, Any]] = []
    for domain in selected:
        topo = topology[domain.slug]
        domain_bugs = bug_matches[domain.slug]
        severity_counts = Counter(str(bug.get("severity") or "UNKNOWN") for bug in domain_bugs)
        status_counts = Counter(str(bug.get("status") or "UNKNOWN") for bug in domain_bugs)
        bug_count = len(domain_bugs)
        p1_count = severity_counts.get("P1", 0)
        p2_count = severity_counts.get("P2", 0)
        score = round(
            (p1_count * 25)
            + (p2_count * 10)
            + (bug_count * 6)
            + (float(topo["spread_files"]) / 25.0)
            + (float(topo["coarse_modules"]) * 3.0)
            + (float(topo["surface_coupling_files"]) * 4.0)
            + (len(long_symbols.get(domain.slug, [])) * 2.0),
            2,
        )
        rows.append({
            "domain": domain.slug,
            "title": domain.title,
            "score": score,
            "priority": _priority(score, p1_count),
            "authority_model": domain.authority_model,
            "failure_modes": list(domain.failure_modes),
            "recommended_change": domain.recommended_change,
            "metrics": {
                "open_architecture_bugs": bug_count,
                "p1_bugs": p1_count,
                "p2_bugs": p2_count,
                "spread_files": topo["spread_files"],
                "coarse_modules": topo["coarse_modules"],
                "loc_in_hit_files": topo["loc_in_hit_files"],
                "surface_coupling_files": topo["surface_coupling_files"],
                "large_symbols": len(long_symbols.get(domain.slug, [])),
            },
            "evidence": {
                "bug_ids": [bug.get("bug_id") for bug in domain_bugs if bug.get("bug_id")][:12],
                "bug_titles": [bug.get("title") for bug in domain_bugs if bug.get("title")][:8],
                "severity_counts": dict(severity_counts),
                "status_counts": dict(status_counts),
                "representative_paths": topo["representative_paths"],
                "surface_coupling_examples": topo["surface_coupling_examples"],
                "large_symbols": long_symbols.get(domain.slug, []),
            },
        })

    rows.sort(key=lambda row: (-float(row["score"]), row["domain"]))
    capped = rows[: max(1, int(limit))]
    priority_counts = Counter(row["priority"] for row in rows)
    return {
        "ok": True,
        "view": "refactor_heatmap",
        "authority": "runtime.refactor_heatmap.build_refactor_heatmap",
        "requires": {
            "bug_authority": "runtime.bug_tracker",
            "source_topology": "workspace_filesystem_read",
            "operation_dispatch": "operation_catalog_gateway",
        },
        "inputs": {
            "limit": limit,
            "include_tests": include_tests,
            "include_domains": include_domains or [],
            "bug_limit": bug_limit,
            "long_symbol_threshold": long_symbol_threshold,
            "open_only": open_only,
        },
        "summary": {
            "domain_count": len(rows),
            "returned_count": len(capped),
            "source_files_scanned": len(source_files),
            "architecture_bugs_considered": len([bug for bug in bugs if not bug.get("_heatmap_error")]),
            "priority_counts": dict(priority_counts),
            "top_domain": capped[0]["domain"] if capped else None,
            "resolved_statuses_excluded": sorted(bug_resolved_status_values_with_legacy()) if open_only else [],
        },
        "heatmap": capped,
    }


__all__ = ["DOMAINS", "build_refactor_heatmap"]
