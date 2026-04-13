"""Generate docs/MCP.md from the catalog-backed MCP metadata."""

from __future__ import annotations

import sys
from pathlib import Path


WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = WORKFLOW_ROOT.parents[1]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from surfaces.mcp.docs import render_mcp_markdown


def main() -> int:
    output_path = REPO_ROOT / "docs" / "MCP.md"
    output_path.write_text(render_mcp_markdown(), encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
