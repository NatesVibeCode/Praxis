# cli

Owns:

- command parsing
- command routing into application services
- human-readable rendering for operators
- inspect/replay commands that render derived observability views
- catalog-backed tool discovery via `workflow tools`
- discoverable tool browsing via `workflow tools list`, which shows each tool's
  recommended alias and direct entrypoint, plus filtered `workflow tools search`
  with relevance-ranked matches that lift exact alias and entrypoint hits first,
  and `--exact` when you already know the alias, name, or entrypoint; single-hit
  searches also print the direct describe and entrypoint commands
- HTTP route discovery via `workflow api routes`, including `--search`,
  `--method`, `--tag`, and `--path-prefix` filters

Does not own:

- domain rules
- lifecycle truth

The CLI is a frontdoor, not the product brain. It should expose the direct
entrypoint for a tool when that path exists, and fall back to
`workflow tools call <tool|alias>` when it does not.

For catalog-backed tools, the `workflow tools describe` and `workflow tools call`
commands accept either the canonical tool id or the recommended alias shown by
`workflow tools list`.
