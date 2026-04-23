# praxis-codex — thin per-agent sandbox image for OpenAI Codex jobs.
#
# Built manually:
#   cd Code&DBs/Workflow && docker build -f docker/praxis-codex.Dockerfile -t praxis-codex:latest .
#
# Built automatically by docker_image_authority when dispatched to an openai/
# codex agent and the image is missing on the host.
#
# Scope: codex CLI only. No claude, no gemini, no cursor-agent. No Python
# runtime. No Praxis repo. Agents communicate with Praxis via the MCP bridge
# at http://host.docker.internal:8420/mcp and submit sealed results through
# praxis_submit_* tools — filesystem writes are not the authority.

FROM node:22-bookworm-slim

ARG PRAXIS_CONTAINER_WORKSPACE_ROOT
ARG PRAXIS_CONTAINER_HOME

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV PRAXIS_CONTAINER_WORKSPACE_ROOT=${PRAXIS_CONTAINER_WORKSPACE_ROOT} \
    PRAXIS_CONTAINER_HOME=${PRAXIS_CONTAINER_HOME}

RUN test -n "$PRAXIS_CONTAINER_WORKSPACE_ROOT" && test -n "$PRAXIS_CONTAINER_HOME"

# Minimal system — bash for the command launcher, curl for health probes,
# ca-certificates for TLS to MCP / upstream APIs, python3-minimal for the
# `praxis` shell shim (uses only stdlib urllib, no pip). Nothing else.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        python3-minimal \
    && rm -rf /var/lib/apt/lists/*

# Just codex. One CLI, one family.
RUN npm install -g @openai/codex@latest \
    && npm cache clean --force

# ── uniform sandbox tool surface ────────────────────────────────────
# `praxis` is the single shell-callable binary that replaces per-provider
# MCP client config. The agent invokes tools via its native Bash tool
# (e.g. `praxis discover "..."` or `praxis submit_code_change ...`). No
# claude/codex/gemini-specific MCP wiring needed. See
# architecture-policy::sandbox::uniform-shell-tool-surface.
COPY bin/praxis_sandbox_client.py /usr/local/bin/praxis
RUN chmod 0755 /usr/local/bin/praxis

WORKDIR ${PRAXIS_CONTAINER_WORKSPACE_ROOT}

# Non-root agent user matches the uid=1100 the worker uses for auth-file
# mounts targeting the configured container home (see praxis-worker.Dockerfile for
# rationale).
RUN useradd -m -d "${PRAXIS_CONTAINER_HOME}" -u 1100 -s /bin/bash praxis-agent \
    && mkdir -p "${PRAXIS_CONTAINER_HOME}/.codex" \
    && chown -R 1100:1100 "${PRAXIS_CONTAINER_HOME}"

# Smoke test — verify the CLI and the praxis shim are reachable.
RUN bash -lc "node --version && which codex && which praxis && id praxis-agent"
