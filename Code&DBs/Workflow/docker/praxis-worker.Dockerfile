# praxis-worker — sandboxed execution environment for Praxis Engine workflow jobs.
#
# Built automatically by docker_image_authority.py when the image is missing.
# Manual build:
#   cd Code&DBs/Workflow && docker build -f docker/praxis-worker.Dockerfile -t praxis-worker:latest .

FROM node:22-bookworm-slim

ARG PRAXIS_DEPENDENCY_SCOPE=workflow_worker
ARG PRAXIS_CONTAINER_WORKSPACE_ROOT
ARG PRAXIS_CONTAINER_HOME

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV PRAXIS_CONTAINER_WORKSPACE_ROOT=${PRAXIS_CONTAINER_WORKSPACE_ROOT} \
    PRAXIS_CONTAINER_HOME=${PRAXIS_CONTAINER_HOME}

RUN test -n "$PRAXIS_CONTAINER_WORKSPACE_ROOT" && test -n "$PRAXIS_CONTAINER_HOME"

# ── system packages ──────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        docker.io \
        git \
        jq \
        python3 \
        python3-pip \
        python3-venv \
        ripgrep \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

# ── python symlink ──────────────────────────────────────────────────
RUN ln -sf /usr/bin/python3 /usr/local/bin/python

# ── AI CLI tools (agent execution) ──────────────────────────────────
RUN npm install -g \
        @anthropic-ai/claude-code@latest \
        @openai/codex@latest \
        @google/gemini-cli@latest \
    && npm cache clean --force

RUN curl https://cursor.com/install -fsS | bash
RUN ln -sf /root/.local/bin/agent /usr/local/bin/agent \
    && ln -sf /root/.local/bin/cursor-agent /usr/local/bin/cursor-agent

# Trust the configured container workspace regardless of inode ownership — on Windows/SMB-mounted
# bind mounts the file owner often differs from the container's uid, which
# makes git refuse ls-files with "dubious ownership" and forces the
# sandbox fingerprinter to fall back to os.walk (which trips on broken
# symlinks in .venv/). Applying safe.directory at build time keeps this
# out of the runtime path.
RUN git config --system --add safe.directory "${PRAXIS_CONTAINER_WORKSPACE_ROOT}" \
    && git config --system --add safe.directory '*'

RUN mkdir -p /opt/praxis/workflow/runtime /opt/praxis/workflow/scripts
COPY requirements.runtime.txt /opt/praxis/workflow/requirements.runtime.txt
COPY runtime/__init__.py /opt/praxis/workflow/runtime/__init__.py
COPY runtime/dependency_contract.py /opt/praxis/workflow/runtime/dependency_contract.py
COPY scripts/export_dependency_scope.py /opt/praxis/workflow/scripts/export_dependency_scope.py

# ── uniform sandbox tool surface ────────────────────────────────────
# `praxis` is the single shell-callable binary that replaces per-provider
# MCP client configuration. Every CLI (claude/codex/gemini) can invoke
# `praxis workflow tools ...` via its native Bash tool. See
# architecture-policy::sandbox::uniform-shell-tool-surface.
COPY bin/praxis_sandbox_client.py /usr/local/bin/praxis
RUN chmod 0755 /usr/local/bin/praxis

ENV PYTHONPATH=/opt/praxis/workflow

RUN set -eux; \
    tmp_requirements="$(mktemp)"; \
    trap 'rm -f "$tmp_requirements"' EXIT; \
    python3 /opt/praxis/workflow/scripts/export_dependency_scope.py \
        --manifest /opt/praxis/workflow/requirements.runtime.txt \
        --scope "${PRAXIS_DEPENDENCY_SCOPE}" \
        --output "$tmp_requirements"; \
    python3 -m pip install --break-system-packages --no-cache-dir -r "$tmp_requirements"

# ── workspace mount point ───────────────────────────────────────────
WORKDIR ${PRAXIS_CONTAINER_WORKSPACE_ROOT}

# ── non-root agent user ─────────────────────────────────────────────
# Claude's --permission-mode bypassPermissions (and --dangerously-skip-permissions)
# refuse to run as root for security. Agent subprocesses spawned by the cli_llm
# adapter run as this user inside the ephemeral CLI container.
RUN useradd -m -d "${PRAXIS_CONTAINER_HOME}" -u 1100 -s /bin/bash praxis-agent \
    && mkdir -p "${PRAXIS_CONTAINER_HOME}/.claude" \
    && chown -R 1100:1100 "${PRAXIS_CONTAINER_HOME}"

# Smoke test — verify tools are reachable from login shell
RUN bash -lc "node --version && python3 --version && git --version && which claude && which codex && which gemini && which cursor-agent && which praxis && id praxis-agent"
