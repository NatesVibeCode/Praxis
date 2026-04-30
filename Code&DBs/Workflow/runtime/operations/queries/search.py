"""CQRS query handlers for ``praxis_search`` and per-source operations.

Each handler is gateway-dispatched via ``operation_catalog_gateway``:

- ``search.federated`` — orchestrator that fans out to each declared
  source, ranks the union, and returns the canonical envelope.
- ``search.code`` / ``search.knowledge`` / ``search.bugs`` /
  ``search.receipts`` / ``search.git_history`` / ``search.files`` /
  ``search.db`` — single-source variants for callers that want one
  receipt per source.

Per the no-shims standing order, all of these go through the
``operation_catalog_registry`` so each call records a read receipt in
``authority_operation_receipts``. The MCP tool ``praxis_search`` calls
the federated handler via ``execute_operation_from_subsystems``.

Registered by migration 278.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from runtime.operations.queries.search_clustering import build_clusters
from runtime.sources.authority_receipts_source import (
    search_authority_receipts as _impl_authority_receipts,
)
from runtime.sources.bugs_source import search_bugs as _impl_bugs
from runtime.sources.code_source import (
    maybe_refresh_index as _impl_maybe_refresh,
    search_code as _impl_code,
)
from runtime.sources.compliance_receipts_source import (
    search_compliance_receipts as _impl_compliance_receipts,
)
from runtime.sources.db_read_source import search_db as _impl_db
from runtime.sources.files_source import search_files as _impl_files
from runtime.sources.git_source import search_git as _impl_git
from runtime.sources.knowledge_source import search_knowledge as _impl_knowledge
from runtime.sources.receipts_source import search_receipts as _impl_receipts
from surfaces.mcp.tools._search_envelope import (
    SOURCE_AUTHORITY_RECEIPTS,
    SOURCE_BUGS,
    SOURCE_CODE,
    SOURCE_COMPLIANCE_RECEIPTS,
    SOURCE_DB,
    SOURCE_DECISIONS,
    SOURCE_FILES,
    SOURCE_GIT,
    SOURCE_KNOWLEDGE,
    SOURCE_RECEIPTS,
    SOURCE_RESEARCH,
    SearchEnvelope,
    SearchEnvelopeError,
    build_response,
    parse_envelope,
)


_DEFAULT_FEDERATED_SOURCES = (SOURCE_CODE,)


class SearchEnvelopeQuery(BaseModel):
    """Common envelope shape for search query operations."""

    model_config = ConfigDict(extra="allow")

    query: str = Field(...)
    mode: str = "auto"
    sources: list[str] | None = None
    scope: dict[str, Any] | None = None
    shape: str = "context"
    context_lines: int = 5
    limit: int = 20
    cursor: str | None = None
    explain: bool = False
    auto_reindex_if_stale: bool = True
    stale_threshold: int = 5

    @field_validator("query", mode="before")
    @classmethod
    def _validate_query(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("query is required")
        return value.strip()

    def to_envelope(self) -> SearchEnvelope:
        payload = self.model_dump(exclude_none=True)
        try:
            return parse_envelope(payload)
        except SearchEnvelopeError as exc:
            raise ValueError(str(exc)) from exc


class FederatedSearchQuery(SearchEnvelopeQuery):
    pass


class CodeSearchQuery(SearchEnvelopeQuery):
    pass


class KnowledgeSearchQuery(SearchEnvelopeQuery):
    pass


class DecisionsSearchQuery(SearchEnvelopeQuery):
    pass


class ResearchSearchQuery(SearchEnvelopeQuery):
    pass


class BugsSearchQuery(SearchEnvelopeQuery):
    pass


class ReceiptsSearchQuery(SearchEnvelopeQuery):
    pass


class AuthorityReceiptsSearchQuery(SearchEnvelopeQuery):
    pass


class ComplianceReceiptsSearchQuery(SearchEnvelopeQuery):
    pass


class GitSearchQuery(SearchEnvelopeQuery):
    pass


class FilesSearchQuery(SearchEnvelopeQuery):
    pass


class DbReadSearchQuery(SearchEnvelopeQuery):
    pass


def _repo_root(subsystems: Any) -> Path:
    repo = getattr(subsystems, "_repo_root", None)
    if repo is None:
        raise RuntimeError("subsystems missing _repo_root")
    return Path(repo)


def _run_code(
    envelope: SearchEnvelope,
    subsystems: Any,
    *,
    auto_reindex: bool,
    stale_threshold: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    indexer = subsystems.get_module_indexer()
    refresh_report: dict[str, Any] | None = None
    if auto_reindex:
        refresh_report = _impl_maybe_refresh(indexer, stale_threshold=stale_threshold)
    try:
        results, freshness = _impl_code(
            envelope=envelope,
            indexer=indexer,
            repo_root=_repo_root(subsystems),
        )
    except Exception as exc:
        snapshot: dict[str, Any] = {"status": "error", "error": str(exc)}
        if refresh_report is not None:
            snapshot["auto_reindex"] = refresh_report
        return [], snapshot, "error"
    if refresh_report is not None:
        freshness["auto_reindex"] = refresh_report
    return results, freshness, "ok"


def _run_knowledge(
    envelope: SearchEnvelope,
    subsystems: Any,
    *,
    source_label: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_knowledge(
        envelope=envelope, subsystems=subsystems, source_label=source_label
    )
    return results, freshness, freshness.get("status") or "ok"


def _run_bugs(
    envelope: SearchEnvelope, subsystems: Any
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_bugs(envelope=envelope, subsystems=subsystems)
    return results, freshness, freshness.get("status") or "ok"


def _run_receipts(
    envelope: SearchEnvelope, subsystems: Any
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_receipts(envelope=envelope, subsystems=subsystems)
    return results, freshness, freshness.get("status") or "ok"


def _run_authority_receipts(
    envelope: SearchEnvelope, subsystems: Any
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_authority_receipts(envelope=envelope, subsystems=subsystems)
    return results, freshness, freshness.get("status") or "ok"


def _run_compliance_receipts(
    envelope: SearchEnvelope, subsystems: Any
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_compliance_receipts(envelope=envelope, subsystems=subsystems)
    return results, freshness, freshness.get("status") or "ok"


def _run_git(
    envelope: SearchEnvelope, subsystems: Any
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_git(envelope=envelope, repo_root=_repo_root(subsystems))
    return results, freshness, freshness.get("status") or "ok"


def _run_files(
    envelope: SearchEnvelope, subsystems: Any
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_files(envelope=envelope, repo_root=_repo_root(subsystems))
    return results, freshness, freshness.get("status") or "ok"


def _run_db(
    envelope: SearchEnvelope, subsystems: Any
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    results, freshness = _impl_db(envelope=envelope, subsystems=subsystems)
    return results, freshness, freshness.get("status") or "ok"


_SOURCE_DISPATCH = {
    SOURCE_CODE: lambda env, subs, **kw: _run_code(env, subs, **kw),
    SOURCE_KNOWLEDGE: lambda env, subs, **kw: _run_knowledge(
        env, subs, source_label=SOURCE_KNOWLEDGE
    ),
    SOURCE_DECISIONS: lambda env, subs, **kw: _run_knowledge(
        env, subs, source_label=SOURCE_DECISIONS
    ),
    SOURCE_RESEARCH: lambda env, subs, **kw: _run_knowledge(
        env, subs, source_label=SOURCE_RESEARCH
    ),
    SOURCE_BUGS: lambda env, subs, **kw: _run_bugs(env, subs),
    SOURCE_RECEIPTS: lambda env, subs, **kw: _run_receipts(env, subs),
    SOURCE_AUTHORITY_RECEIPTS: lambda env, subs, **kw: _run_authority_receipts(env, subs),
    SOURCE_COMPLIANCE_RECEIPTS: lambda env, subs, **kw: _run_compliance_receipts(env, subs),
    SOURCE_GIT: lambda env, subs, **kw: _run_git(env, subs),
    SOURCE_FILES: lambda env, subs, **kw: _run_files(env, subs),
    SOURCE_DB: lambda env, subs, **kw: _run_db(env, subs),
}


def _run_envelope(
    envelope: SearchEnvelope,
    subsystems: Any,
    *,
    auto_reindex: bool,
    stale_threshold: int,
    cluster: bool = True,
) -> dict[str, Any]:
    all_results: list[dict[str, Any]] = []
    freshness: dict[str, dict[str, Any]] = {}
    sources_status: dict[str, str] = {}
    for source in envelope.sources:
        runner = _SOURCE_DISPATCH.get(source)
        if runner is None:
            sources_status[source] = "unknown_source"
            freshness[source] = {"status": "unknown_source"}
            continue
        results, snapshot, status = runner(
            envelope, subsystems, auto_reindex=auto_reindex, stale_threshold=stale_threshold
        )
        sources_status[source] = status
        freshness[source] = snapshot
        all_results.extend(results)

    all_results.sort(key=lambda row: -float(row.get("score") or 0.0))
    flat_results = list(all_results)
    if len(flat_results) > envelope.limit:
        flat_results = flat_results[: envelope.limit]
    if not envelope.explain:
        for row in flat_results:
            row.pop("_explain", None)

    response = build_response(
        envelope=envelope,
        results=flat_results,
        sources_status=sources_status,
        freshness=freshness,
    )

    if cluster:
        cluster_block = build_clusters(
            envelope=envelope,
            raw_hits=all_results,
            sources_status=sources_status,
            subsystems=subsystems,
        )
        # Surface clusters as the primary shape; keep flat 'results' for
        # backward-compat with callers that still iterate it.
        response["clusters"] = cluster_block.get("clusters", [])
        response["anchor_count"] = cluster_block.get("anchor_count", 0)
        if "also" in cluster_block:
            response["also"] = cluster_block["also"]
        if "empty_state" in cluster_block:
            response["empty_state"] = cluster_block["empty_state"]
        if "source_empty_states" in cluster_block:
            response["source_empty_states"] = cluster_block["source_empty_states"]

    return response


def _run_single_source(
    query: SearchEnvelopeQuery,
    subsystems: Any,
    *,
    source_label: str,
) -> dict[str, Any]:
    envelope = query.to_envelope()
    envelope = SearchEnvelope(
        query=envelope.query,
        mode=envelope.mode,
        sources=(source_label,),
        scope=envelope.scope,
        shape=envelope.shape,
        context_lines=envelope.context_lines,
        limit=envelope.limit,
        cursor=envelope.cursor,
        explain=envelope.explain,
    )
    # Single-source ops skip clustering — caller asked for one source by name.
    return _run_envelope(
        envelope,
        subsystems,
        auto_reindex=query.auto_reindex_if_stale,
        stale_threshold=query.stale_threshold,
        cluster=False,
    )


def handle_federated_search(
    query: FederatedSearchQuery, subsystems: Any
) -> dict[str, Any]:
    envelope = query.to_envelope()
    if not envelope.sources:
        envelope = SearchEnvelope(
            query=envelope.query,
            mode=envelope.mode,
            sources=_DEFAULT_FEDERATED_SOURCES,
            scope=envelope.scope,
            shape=envelope.shape,
            context_lines=envelope.context_lines,
            limit=envelope.limit,
            cursor=envelope.cursor,
            explain=envelope.explain,
        )
    return _run_envelope(
        envelope,
        subsystems,
        auto_reindex=query.auto_reindex_if_stale,
        stale_threshold=query.stale_threshold,
    )


def handle_code_search(query: CodeSearchQuery, subsystems: Any) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_CODE)


def handle_knowledge_search(
    query: KnowledgeSearchQuery, subsystems: Any
) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_KNOWLEDGE)


def handle_decisions_search(
    query: DecisionsSearchQuery, subsystems: Any
) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_DECISIONS)


def handle_research_search(
    query: ResearchSearchQuery, subsystems: Any
) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_RESEARCH)


def handle_bugs_search(query: BugsSearchQuery, subsystems: Any) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_BUGS)


def handle_receipts_search(
    query: ReceiptsSearchQuery, subsystems: Any
) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_RECEIPTS)


def handle_authority_receipts_search(
    query: AuthorityReceiptsSearchQuery, subsystems: Any
) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_AUTHORITY_RECEIPTS)


def handle_compliance_receipts_search(
    query: ComplianceReceiptsSearchQuery, subsystems: Any
) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_COMPLIANCE_RECEIPTS)


def handle_git_search(query: GitSearchQuery, subsystems: Any) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_GIT)


def handle_files_search(query: FilesSearchQuery, subsystems: Any) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_FILES)


def handle_db_read_search(
    query: DbReadSearchQuery, subsystems: Any
) -> dict[str, Any]:
    return _run_single_source(query, subsystems, source_label=SOURCE_DB)


__all__ = [
    "BugsSearchQuery",
    "CodeSearchQuery",
    "DbReadSearchQuery",
    "DecisionsSearchQuery",
    "FederatedSearchQuery",
    "FilesSearchQuery",
    "GitSearchQuery",
    "KnowledgeSearchQuery",
    "ReceiptsSearchQuery",
    "ResearchSearchQuery",
    "SearchEnvelopeQuery",
    "handle_bugs_search",
    "handle_code_search",
    "handle_db_read_search",
    "handle_decisions_search",
    "handle_federated_search",
    "handle_files_search",
    "handle_git_search",
    "handle_knowledge_search",
    "handle_receipts_search",
    "handle_research_search",
]
