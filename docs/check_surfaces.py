
import sys
from unittest.mock import MagicMock
sys.modules['runtime'] = MagicMock()
sys.modules['runtime.primitive_contracts'] = MagicMock()
from surfaces.mcp.catalog import get_tool_catalog
from pathlib import Path
import re

docs = Path('docs/CLI.md').read_text()
# Find all tools and their surfaces in docs
# In Full Catalog Entrypoints, we have ### Surface\n\n| ... | Tool | ...
sections = re.split(r'### ', docs)
documented_surfaces = {}
for section in sections[1:]:
    lines = section.split('\n')
    surface = lines[0].strip().lower()
    for line in lines[1:]:
        match = re.search(r'\| `[^`]+` \| `([^`]+)` \|', line)
        if match:
            documented_surfaces[match.group(1)] = surface

catalog = get_tool_catalog()
for name, definition in catalog.items():
    actual_surface = definition.cli_surface.lower()
    if name in documented_surfaces:
        if documented_surfaces[name] != actual_surface:
            print(f"Surface mismatch for {name}: docs={documented_surfaces[name]}, catalog={actual_surface}")
    else:
        # Check if it's in Stable Aliases
        pass

print("Surface check complete")
