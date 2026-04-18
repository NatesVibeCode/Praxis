"""Contract test: every entry in config/authority_index.yaml must point at
real modules and real tests. Fails CI if any path goes stale.
"""

from __future__ import annotations

import pytest

from runtime.authority_index import load_authority_index, validate_authority_index


def test_authority_index_loads_with_at_least_ten_entries() -> None:
    entries = load_authority_index()
    assert len(entries) >= 10, (
        f"authority_index.yaml has {len(entries)} entries; expected at least 10"
    )


def test_authority_index_module_and_test_paths_exist() -> None:
    errors = validate_authority_index()
    assert not errors, "stale authority_index entries:\n" + "\n".join(errors)


def test_authority_index_concepts_are_unique() -> None:
    entries = load_authority_index()
    concepts = [entry.concept for entry in entries]
    assert len(concepts) == len(set(concepts)), (
        f"duplicate concepts in authority_index.yaml: {concepts}"
    )
