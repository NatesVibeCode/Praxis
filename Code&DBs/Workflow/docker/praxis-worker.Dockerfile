# praxis-worker — sandboxed execution environment for Praxis Engine workflow jobs.
#
# Built automatically by docker_image_authority.py when the image is missing.
# Manual build:
#   cd Code&DBs/Workflow && docker build -f docker/praxis-worker.Dockerfile -t praxis-worker:latest .

FROM node:22-bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

# ── system packages ──────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        jq \
        python3 \
        python3-pip \
        python3-venv \
        ripgrep \
    && rm -rf /var/lib/apt/lists/*

# ── python symlink ──────────────────────────────────────────────────
RUN ln -sf /usr/bin/python3 /usr/local/bin/python

# ── AI CLI tools (agent execution) ──────────────────────────────────
RUN npm install -g \
        @anthropic-ai/claude-code@latest \
        @openai/codex@latest \
        @google/gemini-cli@latest \
    && npm cache clean --force

# ── workspace mount point ───────────────────────────────────────────
WORKDIR /workspace

# Smoke test — verify tools are reachable from login shell
RUN bash -lc "node --version && python3 --version && git --version && which claude && which codex && which gemini"
