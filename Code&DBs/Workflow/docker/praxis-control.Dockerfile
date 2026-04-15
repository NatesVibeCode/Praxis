FROM python:3.13-slim

ARG PRAXIS_DEPENDENCY_SCOPE=api_server

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        jq \
        ripgrep \
    && rm -rf /var/lib/apt/lists/*

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
    && python3 -m pip install --no-cache-dir -r /tmp/praxis-scope-requirements.txt \
    && rm -f /tmp/praxis-scope-requirements.txt

WORKDIR /workspace
