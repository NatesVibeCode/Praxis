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

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Minimal system — bash for the command launcher, curl for health probes,
# ca-certificates for TLS to MCP / upstream APIs. Nothing else.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Just codex. One CLI, one family.
RUN npm install -g @openai/codex@latest \
    && npm cache clean --force

WORKDIR /workspace

# Non-root agent user matches the uid=1100 the worker uses for auth-file
# mounts targeting /home/praxis-agent (see praxis-worker.Dockerfile for
# rationale).
RUN useradd -m -u 1100 -s /bin/bash praxis-agent \
    && mkdir -p /home/praxis-agent/.codex \
    && chown -R 1100:1100 /home/praxis-agent

# Smoke test — verify the one CLI is reachable from login shell.
RUN bash -lc "node --version && which codex && id praxis-agent"
