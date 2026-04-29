.PHONY: bootstrap stack refresh worker stack-down stack-rebuild antipattern-sweep antipattern-clear

# Initial setup — run once on a fresh clone (or after large schema changes).
bootstrap:
	./scripts/bootstrap

# Recreate api-server + workflow-worker + scheduler with fresh Keychain-
# hydrated provider auth. Use this after `claude /login`, host reboot,
# Docker Desktop / OrbStack restart, or any time the worker is "Up" but
# failing auth probes. NEVER `docker compose up -d --force-recreate
# workflow-worker` directly — that path skips the keychain re-export and
# leaves the worker with empty CLAUDE_CODE_OAUTH_TOKEN.
# Standing-order ref: architecture-policy::auth::via-docker-creds-not-shell
stack: refresh

refresh:
	./scripts/praxis-up

# Recreate just the workflow-worker (cheaper than the full stack — useful
# when only the worker auth went stale). Same hydration guarantees as
# `make refresh`.
worker:
	./scripts/praxis-up workflow-worker

# Stop the stack (keeps containers + their volume mounts so a `make refresh`
# brings everything back without rebuilding images).
stack-down:
	docker compose --profile worker stop api-server workflow-worker scheduler

# Force a full rebuild + recreate. Use sparingly — most issues are fixed by
# `make refresh`, not by rebuilding images.
stack-rebuild:
	docker compose --profile worker build api-server workflow-worker scheduler
	./scripts/praxis-up

# Run the build-anti-pattern detection sweep inside the worker. Currently
# fires the zero_token_silent_failure detector (migration 334); future
# detectors register in the same registry and will be picked up.
antipattern-sweep:
	./scripts/praxis-antipattern-sweep

# Manually clear all open anti-pattern hits (operator override). Normally
# happens automatically on `make refresh` after a successful auth probe.
antipattern-clear:
	./scripts/praxis-antipattern-sweep --clear
