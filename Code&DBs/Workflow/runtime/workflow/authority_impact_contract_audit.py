"""Audit helper for authority-bearing path coverage by impact contracts.

The compose → submit → preflight → review → materialize chain enforces the
impact contract for any change that flows through `code_change_candidate`.
This audit closes the orthogonal gap: changes that bypass the candidate
flow entirely (direct git commits, scripted edits, hot-fixes) leave
authority-bearing files modified with no backing impact contract row.

The audit takes a list of paths (typically `git diff --name-only` output
from a recent window) and classifies each one:

* `not_authority_bearing` — outside the authority-bearing path patterns
  defined in `runtime.workflow.authority_overlap`. Safe to ignore.
* `covered` — the path appears in `intended_files` of at least one
  candidate row in `code_change_candidate_payloads`. Coverage is best-
  effort: presence of a candidate is the contract anchor; this audit
  does not re-validate the contract content (preflight + materialize
  verifier do that on the candidate path).
* `uncovered` — authority-bearing but not present in any candidate's
  intended_files. Drift signal — the change landed without going through
  the gated pipeline.

Returns coverage findings + summary counts. Caller decides what to do
(file a bug, surface in Moon, gate a CI step, etc.). The audit itself is
purely descriptive — no writes, no side effects.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from runtime.workflow.authority_overlap import classify_path, is_authority_bearing


@dataclass(frozen=True, slots=True)
class PathCoverageFinding:
    path: str
    authority_bearing: bool
    classified_unit_kind: str | None
    coverage: str  # 'not_authority_bearing' | 'covered' | 'uncovered'
    candidate_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AuthorityImpactContractAuditResult:
    findings: list[PathCoverageFinding] = field(default_factory=list)
    not_authority_bearing_count: int = 0
    covered_count: int = 0
    uncovered_count: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "findings": [
                {
                    "path": f.path,
                    "authority_bearing": f.authority_bearing,
                    "classified_unit_kind": f.classified_unit_kind,
                    "coverage": f.coverage,
                    "candidate_ids": list(f.candidate_ids),
                }
                for f in self.findings
            ],
            "summary": {
                "not_authority_bearing_count": self.not_authority_bearing_count,
                "covered_count": self.covered_count,
                "uncovered_count": self.uncovered_count,
                "total_paths": len(self.findings),
            },
            "notes": list(self.notes),
        }


def _normalize_paths(raw_paths: Sequence[str] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in raw_paths or ():
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _candidate_ids_for_paths(
    conn: Any,
    *,
    paths: Sequence[str],
) -> dict[str, list[str]]:
    if not paths:
        return {}
    rows = conn.fetch(
        """
        SELECT candidate_id::text AS candidate_id,
               intended_files
          FROM code_change_candidate_payloads
         WHERE intended_files && $1::text[]
         ORDER BY created_at DESC
        """,
        list(paths),
    )
    coverage: dict[str, list[str]] = {path: [] for path in paths}
    target_set = set(paths)
    for row in rows or ():
        record = dict(row)
        candidate_id = str(record.get("candidate_id") or "").strip()
        intended = record.get("intended_files") or []
        if not candidate_id or not isinstance(intended, list):
            continue
        for path_value in intended:
            text = str(path_value or "").strip()
            if text in target_set and candidate_id not in coverage[text]:
                coverage[text].append(candidate_id)
    return coverage


def audit_authority_impact_contract_coverage(
    conn: Any,
    *,
    paths: Sequence[str],
) -> AuthorityImpactContractAuditResult:
    """Audit a list of paths for impact contract coverage.

    Pure read; no side effects. Caller is responsible for supplying the
    path list (typically from `git diff --name-only` over the audit window).
    """

    result = AuthorityImpactContractAuditResult()
    normalized_paths = _normalize_paths(paths)
    if not normalized_paths:
        result.notes.append("no_paths_supplied")
        return result

    authority_paths = [path for path in normalized_paths if is_authority_bearing([path])]
    coverage_map = _candidate_ids_for_paths(conn, paths=authority_paths)

    for path in normalized_paths:
        unit_kind = classify_path(path)
        if unit_kind is None:
            result.findings.append(
                PathCoverageFinding(
                    path=path,
                    authority_bearing=False,
                    classified_unit_kind=None,
                    coverage="not_authority_bearing",
                )
            )
            result.not_authority_bearing_count += 1
            continue
        candidate_ids = list(coverage_map.get(path, []))
        if candidate_ids:
            result.findings.append(
                PathCoverageFinding(
                    path=path,
                    authority_bearing=True,
                    classified_unit_kind=unit_kind,
                    coverage="covered",
                    candidate_ids=candidate_ids,
                )
            )
            result.covered_count += 1
        else:
            result.findings.append(
                PathCoverageFinding(
                    path=path,
                    authority_bearing=True,
                    classified_unit_kind=unit_kind,
                    coverage="uncovered",
                )
            )
            result.uncovered_count += 1

    if result.uncovered_count:
        result.notes.append(
            f"{result.uncovered_count}_authority_bearing_paths_lack_impact_contract_coverage"
        )

    return result


__all__ = [
    "AuthorityImpactContractAuditResult",
    "PathCoverageFinding",
    "audit_authority_impact_contract_coverage",
]
