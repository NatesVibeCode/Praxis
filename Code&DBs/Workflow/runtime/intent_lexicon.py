"""Shared synonym expansion for compile-time intent detection."""

from __future__ import annotations

import re
from functools import lru_cache

_SPACE_RE = re.compile(r"\s+")

_CONCEPTS: tuple[dict[str, tuple[str, ...]], ...] = (
    {
        "matches": (
            "app",
            "apps",
            "application",
            "applications",
            "app name",
            "app domain",
            "domain",
            "company domain",
            "service",
            "product",
        ),
        "expands_to": ("app", "application", "domain", "service", "product"),
    },
    {
        "matches": (
            "connector",
            "integration",
            "adapter",
            "bridge",
            "client",
            "plugin",
            "hook up",
            "wire up",
            "onboard",
            "onboarding",
        ),
        "expands_to": ("connector", "integration", "adapter", "bridge"),
    },
    {
        "matches": (
            "docs",
            "documentation",
            "api docs",
            "api documentation",
            "developer docs",
            "developer portal",
            "api reference",
            "reference docs",
            "official api",
            "official docs",
            "manual",
            "spec",
            "openapi",
            "swagger",
        ),
        "expands_to": ("api", "docs", "documentation", "reference"),
    },
    {
        "matches": (
            "research",
            "investigate",
            "analyze",
            "look up",
            "lookup",
            "find",
            "discover",
            "search",
        ),
        "expands_to": ("research", "investigate", "search", "discover"),
    },
    {
        "matches": (
            "web",
            "internet",
            "online",
            "browser",
            "browse",
            "fan out",
            "fan-out",
            "scan",
            "sweep",
            "broad sweep",
            "multiple sources",
            "cross check",
            "cross-check",
            "brave",
            "google",
            "search the web",
        ),
        "expands_to": ("web", "internet", "online", "browse", "fan out"),
    },
    {
        "matches": (
            "build",
            "create",
            "make",
            "implement",
            "develop",
            "ship",
            "craft",
            "assemble",
        ),
        "expands_to": ("build", "create", "implement", "develop"),
    },
    {
        "matches": (
            "plan",
            "planning",
            "design",
            "scope",
            "strategy",
            "outline",
            "blueprint",
            "map",
            "mapping",
        ),
        "expands_to": ("plan", "design", "scope", "strategy"),
    },
    {
        "matches": (
            "test",
            "testing",
            "validate",
            "validation",
            "verify",
            "verification",
            "qa",
            "check",
            "smoke",
        ),
        "expands_to": ("test", "validate", "verify", "check"),
    },
    {
        "matches": (
            "auth",
            "authentication",
            "oauth",
            "credentials",
            "credential",
            "api key",
            "bearer",
            "token",
            "login",
        ),
        "expands_to": ("auth", "authentication", "oauth", "token", "credential"),
    },
    {
        "matches": (
            "workflow",
            "pipeline",
            "flow",
            "stage",
            "staged",
            "step",
            "step by step",
            "handoff",
            "orchestrate",
        ),
        "expands_to": ("workflow", "pipeline", "stage", "step"),
    },
    {
        "matches": (
            "persist",
            "store",
            "save",
            "record",
            "capture",
            "collect",
            "gather",
            "stash",
        ),
        "expands_to": ("persist", "store", "record", "capture"),
    },
    {
        "matches": (
            "common objects",
            "object mapping",
            "object mappings",
            "field mapping",
            "field mappings",
            "target mapping",
            "target mappings",
            "schema mapping",
        ),
        "expands_to": ("common objects", "mapping", "schema"),
    },
)


def normalize_match_text(text: str) -> str:
    lowered = (text or "").lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered)
    return _SPACE_RE.sub(" ", cleaned).strip()


def expand_query_terms(text: str) -> tuple[str, ...]:
    normalized = normalize_match_text(text)
    if not normalized:
        return ()

    padded = f" {normalized} "
    expanded = {word for word in normalized.split() if len(word) > 2}
    for concept in _CONCEPTS:
        if any(_contains_phrase(padded, phrase) for phrase in concept["matches"]):
            for phrase in concept["expands_to"]:
                expanded.update(word for word in phrase.split() if len(word) > 2)
    return tuple(sorted(expanded))


def text_has_any(text: str, *terms: str) -> bool:
    normalized = normalize_match_text(text)
    if not normalized:
        return False
    padded = f" {normalized} "
    for term in terms:
        for candidate in expand_equivalent_phrases(term):
            if _contains_phrase(padded, candidate):
                return True
    return False


@lru_cache(maxsize=512)
def expand_equivalent_phrases(term: str) -> tuple[str, ...]:
    normalized = normalize_match_text(term)
    if not normalized:
        return ()

    candidates = {normalized}
    for concept in _CONCEPTS:
        if normalized in concept["matches"] or normalized in concept["expands_to"]:
            candidates.update(concept["matches"])
            candidates.update(concept["expands_to"])
    return tuple(sorted(candidates, key=lambda value: (value != normalized, len(value), value)))


def _contains_phrase(padded_text: str, phrase: str) -> bool:
    normalized_phrase = normalize_match_text(phrase)
    if not normalized_phrase:
        return False
    if f" {normalized_phrase} " in padded_text:
        return True
    if " " in normalized_phrase:
        return False
    return bool(re.search(rf" {re.escape(normalized_phrase)}(?:s|ed|ing)? ", padded_text))
