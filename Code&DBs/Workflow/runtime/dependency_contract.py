"""Canonical runtime dependency contract for the workflow package.

The same requirements manifest drives setup, packaging, and runtime
verification. Scope metadata lives in the manifest comments so the file itself
stays the single source of truth; runtime projects declared scopes from that
same manifest instead of maintaining a second dependency list.
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

_WORKFLOW_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST_PATH = _WORKFLOW_ROOT / "requirements.runtime.txt"

_IMPORT_NAME_OVERRIDES: dict[str, str] = {
    "google-genai": "google.genai",
    "psycopg2-binary": "psycopg2",
    "sentence-transformers": "sentence_transformers",
}

_SCOPE_HEADER_RE = re.compile(r"^#\s*scopes?\s*[:=]\s*(?P<scopes>.+?)\s*$", re.IGNORECASE)
_SCOPE_INLINE_RE = re.compile(r"\bscopes?\s*[:=]\s*(?P<scopes>[^#]+)", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class _ManifestEntry:
    """One requirement line and the scopes it declares."""

    requirement: str
    distribution: str
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DependencyStatus:
    """One declared dependency and its importability status."""

    requirement: str
    distribution: str
    import_name: str
    available: bool
    installed_version: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            "requirement": self.requirement,
            "distribution": self.distribution,
            "import_name": self.import_name,
            "available": self.available,
            "installed_version": self.installed_version,
        }


def _normalize_scope_name(scope: str) -> str:
    return scope.strip().lower().replace("-", "_")


def _parse_scope_names(raw_scopes: str) -> tuple[str, ...]:
    names: list[str] = []
    for token in re.split(r"[,\s]+", raw_scopes):
        normalized = _normalize_scope_name(token)
        if normalized:
            names.append(normalized)
    if not names:
        raise ValueError("dependency manifest scope metadata is empty")
    return tuple(dict.fromkeys(names))


def _scopes_from_comment(comment: str) -> tuple[str, ...]:
    match = _SCOPE_INLINE_RE.search(comment)
    if match is None:
        return ()
    return _parse_scope_names(match.group("scopes"))


_DISTRIBUTION_RE = re.compile(r"^\s*(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)")


def _distribution_name(requirement: str) -> str:
    match = _DISTRIBUTION_RE.match(requirement)
    if match is None:
        raise ValueError(f"invalid requirement line: {requirement!r}")
    return match.group("name")


def _read_manifest(manifest_path: Path) -> tuple[_ManifestEntry, ...]:
    if not manifest_path.is_file():
        raise FileNotFoundError(f"dependency manifest not found: {manifest_path}")

    entries: list[_ManifestEntry] = []
    current_scopes: tuple[str, ...] = ()
    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            scope_header = _SCOPE_HEADER_RE.match(stripped)
            if scope_header:
                current_scopes = _parse_scope_names(scope_header.group("scopes"))
            continue

        requirement_text, _, comment_text = stripped.partition("#")
        requirement = requirement_text.strip()
        if not requirement:
            continue
        if requirement.startswith("-r "):
            raise ValueError(
                f"nested requirement includes are not allowed in {manifest_path}: {raw_line!r}"
            )

        scoped_names = _scopes_from_comment(comment_text) or current_scopes
        entries.append(
            _ManifestEntry(
                requirement=requirement,
                distribution=_distribution_name(requirement),
                scopes=scoped_names,
            )
        )
    return tuple(entries)


def _import_name_for(distribution: str) -> str:
    return _IMPORT_NAME_OVERRIDES.get(distribution, distribution.replace("-", "_"))


def _installed_version(distribution: str) -> str | None:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def _scope_entries(
    scope: str,
    declared: tuple[_ManifestEntry, ...],
    *,
    manifest_path: Path,
) -> tuple[_ManifestEntry, ...]:
    normalized = _normalize_scope_name(scope or "all") or "all"
    if normalized == "all":
        return declared

    scoped = tuple(
        entry
        for entry in declared
        if normalized in entry.scopes
    )
    if scoped:
        return scoped

    declared_scopes = tuple(
        dict.fromkeys(scope_name for entry in declared for scope_name in entry.scopes)
    )
    if declared_scopes:
        raise ValueError(
            f"dependency manifest {manifest_path} does not declare scope {scope!r}; "
            f"declared scopes: {', '.join(declared_scopes)}"
        )
    raise ValueError(
        f"dependency manifest {manifest_path} does not declare any scoped dependencies; "
        f"requested scope {scope!r}"
    )


def requirements_for_scope(
    *,
    scope: str = "all",
    manifest_path: Path | None = None,
) -> tuple[str, ...]:
    """Return the exact requirement lines declared for one scope."""

    manifest = DEFAULT_MANIFEST_PATH if manifest_path is None else manifest_path
    declared = _read_manifest(manifest)
    scoped = _scope_entries(scope, declared, manifest_path=manifest)
    return tuple(entry.requirement for entry in scoped)


def _status_for_entry(entry: _ManifestEntry) -> DependencyStatus:
    import_name = _import_name_for(entry.distribution)
    available = importlib.util.find_spec(import_name) is not None
    version = _installed_version(entry.distribution) if available else None
    return DependencyStatus(
        requirement=entry.requirement,
        distribution=entry.distribution,
        import_name=import_name,
        available=available,
        installed_version=version,
    )


def dependency_statuses(
    *,
    scope: str = "all",
    manifest_path: Path | None = None,
) -> tuple[DependencyStatus, ...]:
    """Return the declared dependency statuses for one scope."""

    manifest = DEFAULT_MANIFEST_PATH if manifest_path is None else manifest_path
    declared = _read_manifest(manifest)
    scoped = _scope_entries(scope, declared, manifest_path=manifest)
    return tuple(_status_for_entry(entry) for entry in scoped)


def dependency_truth_report(
    *,
    scope: str = "all",
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Return a JSON-safe report for one dependency scope.

    The function never raises on missing packages. It returns ``ok: False`` and
    attaches the parse/import error instead. Callers that need fail-closed
    startup should use :func:`require_runtime_dependencies`.
    """

    manifest = DEFAULT_MANIFEST_PATH if manifest_path is None else manifest_path
    try:
        declared = _read_manifest(manifest)
        scoped = _scope_entries(scope, declared, manifest_path=manifest)
        statuses = tuple(_status_for_entry(entry) for entry in scoped)
    except Exception as exc:
        return {
            "ok": False,
            "scope": scope,
            "manifest_path": str(manifest),
            "error": str(exc),
            "required_count": 0,
            "available_count": 0,
            "missing_count": 0,
            "packages": [],
            "missing": [],
        }

    packages = [status.to_json() for status in statuses]
    missing = [pkg for pkg in packages if not pkg["available"]]
    return {
        "ok": not missing,
        "scope": scope,
        "manifest_path": str(manifest),
        "required_count": len(packages),
        "available_count": len(packages) - len(missing),
        "missing_count": len(missing),
        "packages": packages,
        "missing": missing,
    }


def format_dependency_truth_report(report: Mapping[str, Any]) -> str:
    """Render a compact, human-readable dependency report."""

    lines = [
        f"scope: {report.get('scope', 'unknown')}",
        f"manifest: {report.get('manifest_path', 'unknown')}",
    ]

    if report.get("error"):
        lines.append(f"error: {report['error']}")
        return "\n".join(lines)

    lines.append(
        f"satisfied: {report.get('available_count', 0)}/{report.get('required_count', 0)}"
    )
    missing = report.get("missing", [])
    if missing:
        lines.append("missing:")
        for entry in missing:
            lines.append(
                f"  - {entry.get('distribution')} (import {entry.get('import_name')})"
            )
    return "\n".join(lines)


def require_runtime_dependencies(
    *,
    scope: str = "api_server",
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Fail closed when a dependency scope is not satisfied."""

    report = dependency_truth_report(scope=scope, manifest_path=manifest_path)
    if report.get("ok"):
        return report
    raise RuntimeError(format_dependency_truth_report(report))


__all__ = [
    "DEFAULT_MANIFEST_PATH",
    "DependencyStatus",
    "dependency_statuses",
    "dependency_truth_report",
    "format_dependency_truth_report",
    "require_runtime_dependencies",
]
