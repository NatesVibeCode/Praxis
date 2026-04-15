# cli

Owns:

- command parsing
- command routing into application services
- human-readable rendering for operators
- inspect/replay commands that render derived observability views

Does not own:

- domain rules
- lifecycle truth

The CLI is a frontdoor, not the product brain.
