# cli

Owns:

- command parsing
- command routing into application services
- human-readable rendering for operators
- inspect/replay commands that render derived observability views
- catalog-backed tool discovery via `workflow tools`
- discoverable tool browsing via `workflow tools list` and filtered `workflow tools search`
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
