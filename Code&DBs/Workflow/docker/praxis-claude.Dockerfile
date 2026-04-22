# praxis-claude — thin per-agent sandbox image for Anthropic Claude jobs.
#
# Built manually:
#   cd Code&DBs/Workflow && docker build -f docker/praxis-claude.Dockerfile -t praxis-claude:latest .
#
# Built automatically by docker_image_authority when dispatched to an anthropic/
# claude agent and the image is missing on the host.
#
# Scope: claude CLI only. No codex, no gemini, no cursor-agent. No Python
# runtime. No Praxis repo. Agents communicate with Praxis via the MCP bridge
# at http://host.docker.internal:8420/mcp and submit sealed results through
# praxis_submit_* tools — filesystem writes are not the authority.

FROM node:22-bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code@latest \
    && npm cache clean --force

WORKDIR /workspace

# Claude CLI's --permission-mode bypassPermissions refuses to run as root.
# Matches the uid=1100 the worker uses for auth-file mounts targeting
# /home/praxis-agent (see praxis-worker.Dockerfile for rationale).
RUN useradd -m -u 1100 -s /bin/bash praxis-agent \
    && mkdir -p /home/praxis-agent/.claude \
    && chown -R 1100:1100 /home/praxis-agent

RUN bash -lc "node --version && which claude && id praxis-agent"
