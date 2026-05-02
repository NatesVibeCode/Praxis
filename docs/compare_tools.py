
import sys
from unittest.mock import MagicMock
sys.modules['runtime'] = MagicMock()
sys.modules['runtime.primitive_contracts'] = MagicMock()
from surfaces.mcp.catalog import get_tool_catalog
from pathlib import Path
import re

docs = Path('docs/MCP.md').read_text()
doc_tools = set(re.findall(r'^\| `([^`]+)`', docs, re.M))
catalog_tools = set(get_tool_catalog().keys())

print(f"Doc tools: {len(doc_tools)}")
print(f"Catalog tools: {len(catalog_tools)}")
print(f"Missing from catalog: {sorted(doc_tools - catalog_tools)}")
print(f"Missing from docs: {sorted(catalog_tools - doc_tools)}")
