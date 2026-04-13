"""Shared Helm block catalog authority."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def block_catalog_path() -> Path:
    return _repo_root() / "Code&DBs/Workflow/surfaces/app/src/blocks/catalog.v1.json"


@lru_cache(maxsize=1)
def load_block_catalog() -> list[dict[str, Any]]:
    payload = json.loads(block_catalog_path().read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("block catalog must be a JSON array")
    return [dict(item) for item in payload if isinstance(item, dict)]


def block_catalog_map() -> dict[str, dict[str, Any]]:
    return {
        str(entry.get("id")): entry
        for entry in load_block_catalog()
        if isinstance(entry.get("id"), str) and entry.get("id")
    }


def block_ids() -> tuple[str, ...]:
    return tuple(block_catalog_map().keys())


def format_block_catalog_for_prompt() -> str:
    rows: list[str] = []
    for entry in load_block_catalog():
        rows.append(
            f"- {entry.get('id')} ({entry.get('type')}, span={entry.get('defaultSpan')}): "
            f"{entry.get('description') or entry.get('name')}"
        )
    return "\n".join(rows)


__all__ = ["block_catalog_map", "block_catalog_path", "block_ids", "format_block_catalog_for_prompt", "load_block_catalog"]
