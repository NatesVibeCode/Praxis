"""Regression tests: pill binder must skip filename and path patterns.

Before this fix, prose like ``edit catalog.py`` produced a candidate
ref ``object_kind=catalog, field_path=py``. The default fail-closed
mode then rejected the entire compose for "unbound pills" — a guard
that was guarding the wrong thing. File mentions in prose are not
data-pill references.

Two filters: known file extensions on the right side, and
path-separator context (``/foo.py`` or ``foo.py/bar``) regardless
of extension.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.intent_binding import _extract_candidate_refs


def _refs(intent: str) -> set[tuple[str, str]]:
    return {(obj, fld) for _span, obj, fld in _extract_candidate_refs(intent)}


def test_python_filename_is_not_a_pill() -> None:
    intent = "Read the catalog.py module to understand the registration shape."
    assert ("catalog", "py") not in _refs(intent)


def test_typescript_filename_is_not_a_pill() -> None:
    intent = "Edit MoonActionDock.tsx to fix the silent catch."
    # Note: matched as 'moonactiondock.tsx' (case-folded). Either way,
    # the .tsx extension means filename, not data ref.
    assert ("moonactiondock", "tsx") not in _refs(intent)


def test_full_path_is_not_a_pill() -> None:
    intent = "The bug lives in Code&DBs/Workflow/runtime/system_events.py."
    assert ("system_events", "py") not in _refs(intent)


def test_json_artifact_filename_is_not_a_pill() -> None:
    intent = "Write a summary to artifacts/workflow/atlas_defer.json."
    assert ("atlas_defer", "json") not in _refs(intent)


def test_praxis_db_literal_is_not_a_pill() -> None:
    intent = "Compare latency in Praxis.db before you propose a routing change."
    assert ("praxis", "db") not in _refs(intent)


def test_praxis_db_status_is_treated_as_a_pill_candidate() -> None:
    intent = "Compare latency in Praxis.db.status before you propose a routing change."
    assert ("praxis.db", "status") in _refs(intent)


def test_real_data_pill_still_binds() -> None:
    intent = "Backfill the users.first_name column from existing rows."
    assert ("users", "first_name") in _refs(intent)


def test_nested_data_pill_still_binds() -> None:
    intent = "Project workflow_runs.metadata.cost into the dashboard."
    assert ("workflow_runs", "metadata.cost") in _refs(intent)


def test_path_separator_blocks_match_even_without_extension() -> None:
    # Even if the right side isn't a known extension, a leading slash
    # signals path context — not a column reference.
    intent = "look in /etc/some.weirdthing for the value"
    refs = _refs(intent)
    # 'some.weirdthing' should be rejected by path-separator context
    # ('weirdthing' is not in the extension blocklist).
    assert ("some", "weirdthing") not in refs


def test_dockerfile_blocked_as_extension() -> None:
    intent = "edit ops/build.dockerfile"
    assert ("build", "dockerfile") not in _refs(intent)


def test_shell_script_filename_not_a_pill() -> None:
    intent = "Run scripts/bootstrap.sh to set up the env."
    assert ("bootstrap", "sh") not in _refs(intent)


def test_multipart_filename_blocked_via_last_segment() -> None:
    # 'archive.tar.gz' — last segment 'gz' is a known archive extension.
    intent = "Upload archive.tar.gz to the artifact store."
    refs = _refs(intent)
    assert ("archive", "tar.gz") not in refs


def test_mix_of_filenames_and_real_pills() -> None:
    intent = (
        "Add users.first_name. Update profile_view.tsx to render it. "
        "Migration lives at migrations/0042_add_first_name.sql."
    )
    refs = _refs(intent)
    # Real pill binds.
    assert ("users", "first_name") in refs
    # Filenames don't.
    assert ("profile_view", "tsx") not in refs
    assert ("0042_add_first_name", "sql") not in refs


def test_md_documentation_reference_not_a_pill() -> None:
    intent = "Per the README.md, ensure the bootstrap step runs idempotently."
    assert ("readme", "md") not in _refs(intent)
