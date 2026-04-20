"""Regression pin for BUG-05D46110.

Two code paths were flagged for bypassing registry-backed authority:

1. ``surfaces/app/src/moon/MoonRunPanel.tsx`` hardcoded REST URL strings
   (``/api/runs/recent``, ``/api/workflow-runs/{runId}/stream``,
   ``/api/runs/{runId}/jobs/{id}``) inline instead of citing a single
   path authority. That locked the UI to string literals that could
   drift from the real REST surface whenever an endpoint moved.

2. ``registry/integration_registry_sync.py`` used to carry inline
   integration seed rows (``praxis-dispatch``, ``notifications``,
   ``webhook`` etc.) alongside the sync executor itself. That mixed two
   concerns — "what integrations exist" (authority) and "how do we
   upsert them" (mechanism) — inside one module.

The fix collapses each concern onto one authority:

* Run-API paths live in ``surfaces/app/src/dashboard/runApi.ts`` as
  exported helper functions. ``MoonRunPanel.tsx`` only uses those
  helpers; it does not reconstruct REST paths inline. The helpers file
  is the single authority and any future endpoint rename happens in
  exactly one place.
* Integration rows come from three registered sources:
  - ``runtime.integrations.platform.projected_platform_integrations`` —
    built-in platform integrations
  - ``runtime.integration_manifest`` — third-party TOML manifests
  - ``surfaces.mcp.catalog.projected_mcp_integrations`` — MCP tool catalog
  ``integration_registry_sync.py`` only orchestrates the merge and
  upsert — it does not declare integration identity.

Pins:

1. MoonRunPanel.tsx contains no inline ``/api/runs`` or
   ``/api/workflow-runs`` literals — all path construction routes
   through ``runApi.ts``.
2. runApi.ts exports the three path helpers MoonRunPanel depends on
   (``runsRecentPath``, ``workflowRunStreamPath``, ``runJobsPath``).
3. integration_registry_sync.py does not inline integration ids —
   it delegates to the three authority sources.
4. integration_registry_sync.py imports from exactly those three
   delegate modules (platform, manifest, mcp catalog) — so the
   delegation surface is explicit.
"""
from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(rel: str) -> str:
    return (_REPO_ROOT / rel).read_text(encoding="utf-8")


# -- 1. MoonRunPanel.tsx has no inline run-API path literals ------------


def test_moon_run_panel_has_no_inline_run_api_path_literals() -> None:
    """The core BUG-05D46110 UI pin.

    Before the fix, ``MoonRunPanel.tsx`` lines 77–163 embedded raw REST
    paths for the workflow-runs surface. Post-fix, those paths live in
    ``runApi.ts`` and MoonRunPanel calls the helpers. This test greps
    for the offending-shape literal inside the panel and fails if it
    returns — the only acceptable run-API call in the panel file is via
    the helper functions.
    """
    text = _read("surfaces/app/src/moon/MoonRunPanel.tsx")
    forbidden = ["/api/runs/", "/api/workflow-runs/"]
    for needle in forbidden:
        assert needle not in text, (
            f"MoonRunPanel.tsx reintroduced inline REST literal {needle!r}. "
            f"Route it through surfaces/app/src/dashboard/runApi.ts helpers "
            f"(BUG-05D46110)."
        )


# -- 2. runApi.ts exports the three helpers MoonRunPanel depends on ----


@pytest.mark.parametrize(
    "helper_name",
    [
        "runsRecentPath",
        "workflowRunStreamPath",
        "runJobsPath",
    ],
)
def test_run_api_exports_helper(helper_name: str) -> None:
    """The run-API authority module must export the helpers that
    MoonRunPanel (and any future run-panel view) calls. If this fails,
    the single-source-of-truth contract for run-API paths is broken."""
    text = _read("surfaces/app/src/dashboard/runApi.ts")
    assert f"export function {helper_name}(" in text, (
        f"runApi.ts is missing helper {helper_name!r} — the run-API path "
        f"authority contract is incomplete (BUG-05D46110)."
    )


# -- 3. integration_registry_sync.py has no inline integration ids ----


def test_integration_registry_sync_inlines_no_integration_ids() -> None:
    """The sync executor must not re-declare integration identity.

    Before the fix, ``integration_registry_sync.py`` carried inline
    rows (``praxis-dispatch``, ``notifications``, ``webhook``) alongside
    the upsert. Post-fix, those rows live in
    ``runtime.integrations.platform`` and the sync module only executes
    the merge. This test pins the mechanism/authority split: if an id
    that belongs to the platform integrations registry appears as a
    string literal in the sync file, delegation has broken.
    """
    text = _read("registry/integration_registry_sync.py")
    forbidden_ids = (
        '"praxis-dispatch"',
        "'praxis-dispatch'",
        '"notifications"',
        '"webhook"',
    )
    offenders = [needle for needle in forbidden_ids if needle in text]
    assert not offenders, (
        f"integration_registry_sync.py re-inlined integration ids: "
        f"{offenders}. Delegate to runtime.integrations.platform instead "
        f"(BUG-05D46110)."
    )


# -- 4. sync imports exactly the three delegate sources ---------------


def test_integration_registry_sync_imports_three_delegate_sources() -> None:
    """The integration sync is an orchestrator over three named
    authority sources: platform-owned integrations, TOML manifests,
    and the MCP tool catalog. Pinning each import documents the
    delegation contract so that a future refactor cannot silently drop
    one source and re-introduce inline hardcoding of the missing set.
    """
    text = _read("registry/integration_registry_sync.py")
    required_imports = (
        "from runtime.integrations.platform import projected_platform_integrations",
        "import runtime.integration_manifest",
        "from surfaces.mcp.catalog import projected_mcp_integrations",
    )
    for fragment in required_imports:
        assert fragment in text, (
            f"integration_registry_sync.py is missing delegate import "
            f"{fragment!r}. Authority delegation is broken (BUG-05D46110)."
        )
