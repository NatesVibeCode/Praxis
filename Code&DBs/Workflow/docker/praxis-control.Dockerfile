FROM python:3.13-slim

ARG PRAXIS_DEPENDENCY_SCOPE=api_server
ARG PRAXIS_CONTAINER_WORKSPACE_ROOT
ARG PRAXIS_CONTAINER_HOME

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PRAXIS_CONTAINER_WORKSPACE_ROOT=${PRAXIS_CONTAINER_WORKSPACE_ROOT} \
    PRAXIS_CONTAINER_HOME=${PRAXIS_CONTAINER_HOME}

RUN test -n "$PRAXIS_CONTAINER_WORKSPACE_ROOT" && test -n "$PRAXIS_CONTAINER_HOME"

RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        jq \
        nodejs \
        npm \
        ripgrep \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code@latest @openai/codex@latest \
    && npm cache clean --force

RUN mkdir -p /opt/praxis/workflow/runtime /opt/praxis/workflow/scripts
COPY requirements.runtime.txt /opt/praxis/workflow/requirements.runtime.txt
COPY runtime/__init__.py /opt/praxis/workflow/runtime/__init__.py
COPY runtime/dependency_contract.py /opt/praxis/workflow/runtime/dependency_contract.py
COPY scripts/export_dependency_scope.py /opt/praxis/workflow/scripts/export_dependency_scope.py

ENV PYTHONPATH=/opt/praxis/workflow

RUN set -eux; \
    tmp_requirements="$(mktemp)"; \
    trap 'rm -f "$tmp_requirements"' EXIT; \
    python3 /opt/praxis/workflow/scripts/export_dependency_scope.py \
        --manifest /opt/praxis/workflow/requirements.runtime.txt \
        --scope "${PRAXIS_DEPENDENCY_SCOPE}" \
        --output "$tmp_requirements"; \
    python3 -m pip install --no-cache-dir -r "$tmp_requirements"

WORKDIR ${PRAXIS_CONTAINER_WORKSPACE_ROOT}
