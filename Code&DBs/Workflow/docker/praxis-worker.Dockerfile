# praxis-worker — sandboxed execution environment for Praxis Engine workflow jobs.
#
# Built automatically by docker_image_authority.py when the image is missing.
# Manual build:
#   cd Code&DBs/Workflow && docker build -f docker/praxis-worker.Dockerfile -t praxis-worker:latest .

FROM node:22-bookworm-slim

ARG PRAXIS_DEPENDENCY_SCOPE=workflow_worker

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

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

RUN mkdir -p /opt/praxis/workflow/runtime /opt/praxis/workflow/scripts
COPY requirements.runtime.txt /opt/praxis/workflow/requirements.runtime.txt
COPY runtime/__init__.py /opt/praxis/workflow/runtime/__init__.py
COPY runtime/dependency_contract.py /opt/praxis/workflow/runtime/dependency_contract.py
COPY scripts/export_dependency_scope.py /opt/praxis/workflow/scripts/export_dependency_scope.py

ENV PYTHONPATH=/opt/praxis/workflow

RUN python3 /opt/praxis/workflow/scripts/export_dependency_scope.py \
        --manifest /opt/praxis/workflow/requirements.runtime.txt \
        --scope "${PRAXIS_DEPENDENCY_SCOPE}" \
        --output /tmp/praxis-scope-requirements.txt \
    && python3 -m pip install --break-system-packages --no-cache-dir -r /tmp/praxis-scope-requirements.txt \
    && rm -f /tmp/praxis-scope-requirements.txt

# ── workspace mount point ───────────────────────────────────────────
WORKDIR /workspace

# Smoke test — verify tools are reachable from login shell
RUN bash -lc "node --version && python3 --version && git --version && which claude && which codex && which gemini && which cursor-agent"
