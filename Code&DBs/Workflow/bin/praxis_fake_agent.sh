#!/bin/bash
# Fake-agent shim for plumbing smoke tests. The real CLIs (codex/claude/gemini)
# make LLM calls that cost money and take 5-10 minutes per job. This shim
# does the one thing we want to test: call the `praxis` shell tool to seal
# a submission. It completes in under 1 second.
#
# Usage (from inside a sandbox spawn, matching the CLI command shape):
#   /usr/local/bin/praxis-fake-agent <submit_verb> [extra args...]
#
# All args after the verb are passed through to `praxis <verb>` — the shim
# only supplies the mandatory fields with stub values so the submission
# row lands in workflow_job_submissions. Use this to verify:
#
#   1. The sandbox image has /usr/local/bin/praxis mounted / baked
#   2. PRAXIS_WORKFLOW_MCP_URL + PRAXIS_WORKFLOW_MCP_TOKEN are injected
#   3. The MCP bridge accepts the signed token
#   4. A workflow_job_submissions row actually lands
#
# A successful smoke run prints the bridge's JSON response and exits 0. A
# failing run prints the bridge error and exits non-zero.

set -eu

verb="${1:-submit_code_change}"
shift || true

if [ -z "${PRAXIS_WORKFLOW_MCP_TOKEN:-}" ]; then
    echo "FAKE_AGENT_ERROR: PRAXIS_WORKFLOW_MCP_TOKEN not set — env injection broken" >&2
    exit 2
fi
if [ -z "${PRAXIS_WORKFLOW_MCP_URL:-}" ]; then
    echo "FAKE_AGENT_ERROR: PRAXIS_WORKFLOW_MCP_URL not set — env injection broken" >&2
    exit 2
fi
if ! command -v praxis >/dev/null 2>&1; then
    echo "FAKE_AGENT_ERROR: praxis binary not on PATH — bind-mount or bake broken" >&2
    exit 2
fi

case "$verb" in
  submit_code_change)
    exec praxis submit_code_change \
      --summary "fake-agent plumbing smoke" \
      --primary-paths '[]' \
      --result-kind code_change \
      --notes "issued by praxis_fake_agent.sh — proves env+binary+bridge" \
      "$@"
    ;;
  submit_artifact_bundle)
    exec praxis submit_artifact_bundle \
      --summary "fake-agent plumbing smoke" \
      --primary-paths '[]' \
      --result-kind artifact_bundle \
      --notes "issued by praxis_fake_agent.sh — proves env+binary+bridge" \
      "$@"
    ;;
  health)
    exec praxis health "$@"
    ;;
  *)
    exec praxis "$verb" "$@"
    ;;
esac
