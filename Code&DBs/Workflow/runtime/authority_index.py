"""Loader for ``config/authority_index.yaml``.

The authority index maps each cross-cutting concept to its authoring module,
the storage tables that back it, the operator surfaces (CLI / MCP) that drive
it, and the proving tests that pin its contract. Used by
``praxis workflow authority-index`` and the contract test that keeps the
manifest honest.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

import yaml

from runtime.workspace_paths import _repo_root


_INDEX_FILENAME = "authority_index.yaml"


class AuthorityIndexError(RuntimeError):
    """Raised when the authority index file is missing or malformed."""


@dataclass(frozen=True, slots=True)
class AuthorityEntry:
    concept: str
    authority_module: str
    storage_tables: tuple[str, ...] = ()
    cli: str = ""
    api: str = ""
    mcp: str = ""
    proving_tests: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "concept": self.concept,
            "authority_module": self.authority_module,
            "storage_tables": list(self.storage_tables),
            "cli": self.cli,
            "api": self.api,
            "mcp": self.mcp,
            "proving_tests": list(self.proving_tests),
        }


def authority_index_path(repo_root: Path | None = None) -> Path:
    return (repo_root or _repo_root()) / "config" / _INDEX_FILENAME


@lru_cache(maxsize=1)
def load_authority_index() -> tuple[AuthorityEntry, ...]:
    path = authority_index_path()
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or []
    if not isinstance(raw, list):
        raise AuthorityIndexError(
            f"{_INDEX_FILENAME} must be a list of entries"
        )
    entries: list[AuthorityEntry] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise AuthorityIndexError(
                f"{_INDEX_FILENAME} entries must be mappings; got {type(item).__name__}"
            )
        try:
            entries.append(
                AuthorityEntry(
                    concept=str(item["concept"]),
                    authority_module=str(item["authority_module"]),
                    storage_tables=tuple(item.get("storage_tables") or ()),
                    cli=str(item.get("cli") or ""),
                    api=str(item.get("api") or ""),
                    mcp=str(item.get("mcp") or ""),
                    proving_tests=tuple(item.get("proving_tests") or ()),
                )
            )
        except KeyError as exc:
            raise AuthorityIndexError(
                f"{_INDEX_FILENAME} entry missing required field {exc.args[0]!r}"
            ) from exc
    return tuple(entries)


def validate_authority_index(
    repo_root: Path | None = None,
) -> tuple[str, ...]:
    """Return a tuple of human-readable validation errors. Empty = healthy."""

    root = repo_root or _repo_root()
    errors: list[str] = []
    for entry in load_authority_index():
        module_path = root / entry.authority_module
        if not module_path.is_file():
            errors.append(
                f"{entry.concept}: missing authority_module {entry.authority_module}"
            )
        for test_ref in entry.proving_tests:
            test_path = root / test_ref
            if not test_path.is_file():
                errors.append(
                    f"{entry.concept}: missing proving_test {test_ref}"
                )
    return tuple(errors)
