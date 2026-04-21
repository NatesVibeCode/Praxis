# cli

Owns:

- command parsing
- command routing into application services
- human-readable rendering for operators
- inspect/replay commands that render derived observability views
- catalog-backed tool discovery via `workflow tools`
- discoverable tool browsing via `praxis workflow tools list`, which shows each tool's
  recommended alias and direct entrypoint, plus filtered `workflow tools search`
  with relevance-ranked matches that lift exact alias and entrypoint hits first,
  and `--exact` when you already know the alias, name, or entrypoint; single-hit
  searches and top exact/prefix matches also print the direct describe and
  entrypoint commands, while empty searches now print broadening hints instead
  of ending on a bare zero; `workflow tools help <list|search|describe|call>`
  prints targeted subcommand usage, and `workflow tools help <tool|alias>`
  prints the matching tool description
- HTTP route discovery via `praxis workflow api routes` and flat alias `praxis workflow routes`, including `--search`,
  `--method`, `--tag`, and `--path-prefix` filters, plus route-facet summaries
  plus `--visibility` for public vs internal slices, and `workflow api help`
  or `workflow help routes` as help-topic aliases for the same discovery surface
- legacy discovery command aliases `workflow_cli.py routes` and `workflow_cli.py tools`,
  plus `workflow_cli.py diagnose`, which forward to the modern route, tool, and
  run-diagnosis frontdoors for older scripts
- legacy discovery help aliases `workflow_cli.py help api` and `workflow_cli.py help mcp`,
  which point operators at the modern HTTP route and tool discovery frontdoors

Does not own:

- domain rules
- lifecycle truth

The CLI is a frontdoor, not the product brain. It should expose the direct
entrypoint for a tool when that path exists, and fall back to
`workflow tools call <tool|alias>` when it does not.

For catalog-backed tools, the `workflow tools describe` and `workflow tools call`
commands accept either the canonical tool id or the recommended alias shown by
`praxis workflow tools list`.
