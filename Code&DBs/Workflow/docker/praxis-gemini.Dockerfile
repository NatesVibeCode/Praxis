# praxis-gemini — thin per-agent sandbox image for Google Gemini jobs.
#
# Built manually:
#   cd Code&DBs/Workflow && docker build -f docker/praxis-gemini.Dockerfile -t praxis-gemini:latest .
#
# Built automatically by docker_image_authority when dispatched to a google/
# gemini agent and the image is missing on the host.
#
# Scope: gemini CLI + Python 3.14 runtime so the agent can run repo scripts,
# pytest, migrations, and verify edits against the same interpreter the
# Praxis API server uses. No codex, no claude, no cursor-agent. Agents
# communicate with Praxis via the MCP bridge at
# http://host.docker.internal:8420/mcp and submit sealed results through
# praxis_submit_* tools — filesystem writes are not the authority.

FROM python:3.14-slim

ARG PRAXIS_CONTAINER_WORKSPACE_ROOT
ARG PRAXIS_CONTAINER_HOME

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV PRAXIS_CONTAINER_WORKSPACE_ROOT=${PRAXIS_CONTAINER_WORKSPACE_ROOT} \
    PRAXIS_CONTAINER_HOME=${PRAXIS_CONTAINER_HOME}

RUN test -n "$PRAXIS_CONTAINER_WORKSPACE_ROOT" && test -n "$PRAXIS_CONTAINER_HOME"

# Base image already ships Python 3.14. Add Node 22 (gemini CLI runs on it),
# bash, ca-certificates, and curl.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        gnupg \
        ripgrep \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @google/gemini-cli@latest \
    && npm cache clean --force

# ── uniform sandbox tool surface ────────────────────────────────────
# See architecture-policy::sandbox::uniform-shell-tool-surface.
COPY bin/praxis_sandbox_client.py /usr/local/bin/praxis
RUN chmod 0755 /usr/local/bin/praxis

WORKDIR ${PRAXIS_CONTAINER_WORKSPACE_ROOT}

# Non-root agent user matches the uid=1100 the worker uses for auth-file
# mounts targeting the configured container home (see praxis-worker.Dockerfile for
# rationale).
RUN useradd -m -d "${PRAXIS_CONTAINER_HOME}" -u 1100 -s /bin/bash praxis-agent \
    && mkdir -p "${PRAXIS_CONTAINER_HOME}/.gemini" \
    && chown -R 1100:1100 "${PRAXIS_CONTAINER_HOME}"

# Smoke test — verify Python 3.14, Node, gemini CLI, and the praxis shim
# are all reachable.
RUN bash -lc "python3 --version && python3 -c 'import json' && rg --version >/dev/null && node --version && which gemini && which praxis && id praxis-agent"
