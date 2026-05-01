
import sys
from unittest.mock import MagicMock
sys.modules['runtime'] = MagicMock()
sys.modules['runtime.primitive_contracts'] = MagicMock()
from surfaces.mcp.catalog import get_tool_catalog
from pathlib import Path
import re

docs = Path('docs/CLI.md').read_text()
# Extract Stable Aliases table
match = re.search(r'## Stable Aliases\n\n(.*?)\n\n##', docs, re.S)
if not match:
    print("Could not find Stable Aliases section")
    sys.exit(1)
stable_aliases_table = match.group(1)
documented_tools = set(re.findall(r'\| `[^`]+` \| `([^`]+)` \|', stable_aliases_table))

catalog = get_tool_catalog()
tools_with_alias = {name for name, definition in catalog.items() if definition.cli_recommended_alias}

print(f"Tools with alias in catalog: {len(tools_with_alias)}")
print(f"Tools in Stable Aliases table: {len(documented_tools)}")
print(f"Missing from docs table: {sorted(tools_with_alias - documented_tools)}")
print(f"Extra in docs table: {sorted(documented_tools - tools_with_alias)}")
