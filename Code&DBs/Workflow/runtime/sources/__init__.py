"""Search source plugins consumed by praxis_search.

Each plugin maps a SearchEnvelope to a uniform list of result rows.
Plugins live here (runtime layer) so they can hold the heavy lifting
(filesystem walks, regex compilation, embedding queries) without
the MCP surface importing runtime internals tool-by-tool.
"""
