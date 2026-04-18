"""Import-boundary lint for leaf runtime modules.

``runtime/sandbox_runtime.py`` and ``runtime/block_catalog.py`` are leaf
modules: they implement narrow low-level concerns (sandbox lifecycle,
static block definitions) and must not drag in high-level orchestration.
When an edit accidentally imports from orchestration or surface layers,
the CQRS/authority separation breaks and circular imports can sneak in.

This test pins the allowed import prefixes for those two modules. Every
`from X import …` / `import X` statement must resolve to either the
Python standard library (no dotted package prefix) or a module on the
allowlist. Add to the allowlist only when the new dependency is
deliberately narrow (no upward dependency sprawl).
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]

_SANDBOX_RUNTIME = _WORKFLOW_ROOT / "runtime" / "sandbox_runtime.py"
_BLOCK_CATALOG = _WORKFLOW_ROOT / "runtime" / "block_catalog.py"

_SANDBOX_RUNTIME_ALLOWED_PREFIXES: frozenset[str] = frozenset({
    "runtime.docker_image_authority",
    "runtime.workflow.execution_policy",
})

_BLOCK_CATALOG_ALLOWED_PREFIXES: frozenset[str] = frozenset({
    "runtime.workspace_paths",
})


def _module_imports(path: Path, *, package: str) -> list[str]:
    """Return the fully-qualified module names this file imports at module top level.

    ``import foo`` → ``['foo']``.
    ``from foo import X`` → ``['foo']`` (X-level resolution is lint-irrelevant).
    ``from .sibling import X`` → ``['runtime.sibling']`` (resolved against package).

    Only top-level imports are checked. Late-bound imports inside functions
    are a legitimate pattern for breaking circular dependencies and do not
    define the module's compile-time dependency shape.
    """

    tree = ast.parse(path.read_text())
    out: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            out.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            if node.level > 0:
                parts = package.split(".")
                base = ".".join(parts[: len(parts) - node.level + 1])
                out.append(f"{base}.{node.module}" if base else node.module)
            else:
                out.append(node.module)
    return out


def _is_stdlib(module: str) -> bool:
    head = module.split(".", 1)[0]
    return head in sys.stdlib_module_names


def _assert_boundaries(
    path: Path,
    *,
    package: str,
    allowed_prefixes: frozenset[str],
) -> None:
    imports = _module_imports(path, package=package)
    violations: list[str] = []
    for module in imports:
        if _is_stdlib(module):
            continue
        if any(module == prefix or module.startswith(f"{prefix}.") for prefix in allowed_prefixes):
            continue
        violations.append(module)
    assert not violations, (
        f"{path.name} imports outside its leaf boundary: {sorted(violations)}. "
        f"Allowed non-stdlib prefixes: {sorted(allowed_prefixes)}. "
        f"If the new dependency is deliberately narrow, add it to the allowlist; "
        f"otherwise move the call through an existing allowed module."
    )


def test_sandbox_runtime_import_boundary_holds() -> None:
    _assert_boundaries(
        _SANDBOX_RUNTIME,
        package="runtime",
        allowed_prefixes=_SANDBOX_RUNTIME_ALLOWED_PREFIXES,
    )


def test_block_catalog_import_boundary_holds() -> None:
    _assert_boundaries(
        _BLOCK_CATALOG,
        package="runtime",
        allowed_prefixes=_BLOCK_CATALOG_ALLOWED_PREFIXES,
    )
