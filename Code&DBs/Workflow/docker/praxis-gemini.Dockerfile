# praxis-gemini — thin per-agent sandbox image for Google Gemini jobs.
#
# Built manually:
#   cd Code&DBs/Workflow && docker build -f docker/praxis-gemini.Dockerfile -t praxis-gemini:latest .
#
# Built automatically by docker_image_authority when dispatched to a google/
# gemini agent and the image is missing on the host.
#
# Scope: gemini CLI only. No codex, no claude, no cursor-agent. No Python
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

RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        python3-minimal \
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

RUN bash -lc "node --version && which gemini && which praxis && id praxis-agent"
