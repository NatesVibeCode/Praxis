"""Tag normalization, extraction, and payload fingerprinting for the bug tracker."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


_TAG_VALUE_PATTERN = re.compile(r"[^a-z0-9._:/-]+")


def normalize_tag_value(value: object) -> str:
    text = _TAG_VALUE_PATTERN.sub("-", str(value or "").strip().lower())
    return text.strip("-") or "none"


def extract_tag_value(tags: tuple[str, ...], prefix: str) -> str | None:
    normalized_prefix = f"{prefix.lower()}:"
    for raw_tag in tags:
        tag = str(raw_tag or "").strip()
        if tag.lower().startswith(normalized_prefix):
            return tag.split(":", 1)[1].strip() or None
    return None


def stable_fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:20]


def ordered_unique(values: list[Any]) -> tuple[Any, ...]:
    deduped: list[Any] = []
    seen: set[str] = set()
    for value in values:
        if value in (None, "", [], (), {}):
            continue
        try:
            key = json.dumps(value, sort_keys=True, default=str)
        except TypeError:
            key = str(value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return tuple(deduped)


def payload_keys(payload: Any) -> tuple[str, ...]:
    if isinstance(payload, dict):
        return tuple(sorted(str(key) for key in payload.keys()))
    return ()
