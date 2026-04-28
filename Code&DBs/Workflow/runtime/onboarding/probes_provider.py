"""Provider credential gate probes.

One probe per named provider. Each checks that the credential env var is
resolvable via the standard secret-resolution chain (explicit env → macOS
Keychain → process env → .env). Anthropic is CLI-only for Nate's private
operator profile per standing order
``decision.2026-04-20.anthropic-cli-only-restored`` — its probe verifies the
claude binary is authenticated, not an API key. PUBLIC_RELEASE_REMOVE: public
builds must replace this with registry/profile-driven Anthropic API admission.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

from adapters.credential_capture import secure_entry_action
from adapters.keychain import resolve_secret

from .graph import (
    GateProbe,
    GateResult,
    ONBOARDING_GRAPH,
    gate_result,
)


# (provider_slug, env_var_name, human_name)
_API_KEY_PROVIDERS: tuple[tuple[str, str, str], ...] = (
    ("openai", "OPENAI_API_KEY", "OpenAI"),
    ("google", "GEMINI_API_KEY", "Google (Gemini)"),
    ("openrouter", "OPENROUTER_API_KEY", "OpenRouter"),
    ("deepseek", "DEEPSEEK_API_KEY", "DeepSeek"),
)


_ANTHROPIC_SLUG = "anthropic"


def _make_api_key_probe(provider_slug: str, env_var: str, human_name: str) -> GateProbe:
    return GateProbe(
        gate_ref=f"provider.{provider_slug}",
        domain="provider",
        title=f"{human_name} credential resolvable",
        purpose=(
            f"Workflow jobs routed to {human_name} require {env_var} to be "
            "resolvable via the Keychain → env → .env chain before admission."
        ),
        ok_cache_ttl_s=900,
    )


_ANTHROPIC_PROBE = GateProbe(
    gate_ref=f"provider.{_ANTHROPIC_SLUG}",
    domain="provider",
    title="Anthropic CLI (claude) authenticated",
    purpose=(
        "Anthropic access is CLI-only for this private operator profile (standing order "
        "decision.2026-04-20.anthropic-cli-only-restored). Direct API calls are "
        "forbidden here; public builds must admit ANTHROPIC_API_KEY through "
        "registry/profile authority for users who have one."
    ),
    ok_cache_ttl_s=900,
)


def _probe_api_key(
    env: Mapping[str, str], repo_root: Path, *, provider_slug: str, env_var: str, human_name: str
) -> GateResult:
    probe = _make_api_key_probe(provider_slug, env_var, human_name)
    # Ignore the repo .env for credential resolution in the probe itself, since
    # we want to report the resolved authority, not accidentally hide bad state.
    value = resolve_secret(env_var, env=dict(env))
    if value:
        return gate_result(
            probe,
            status="ok",
            observed_state={"env_var": env_var, "resolved_from": "keychain_env_or_dotenv"},
        )
    if sys.platform == "darwin":
        remediation = (
            f"Open the Praxis secure API-key entry window for {human_name}; "
            f"it stores {env_var} in macOS Keychain without exposing the key to chat."
        )
    else:
        remediation = (
            f"Export {env_var} in your shell rc: "
            f"echo 'export {env_var}=\"<your-key>\"' >> ~/.bashrc && source ~/.bashrc"
        )
    return gate_result(
        probe,
        status="missing",
        observed_state={
            "env_var": env_var,
            "resolved": False,
            "credential_capture": secure_entry_action(env_var, human_name),
        },
        remediation_hint=remediation,
    )


def probe_openai(env: Mapping[str, str], repo_root: Path) -> GateResult:
    return _probe_api_key(
        env, repo_root, provider_slug="openai", env_var="OPENAI_API_KEY", human_name="OpenAI"
    )


def probe_google(env: Mapping[str, str], repo_root: Path) -> GateResult:
    return _probe_api_key(
        env, repo_root, provider_slug="google", env_var="GEMINI_API_KEY", human_name="Google (Gemini)"
    )


def probe_openrouter(env: Mapping[str, str], repo_root: Path) -> GateResult:
    return _probe_api_key(
        env,
        repo_root,
        provider_slug="openrouter",
        env_var="OPENROUTER_API_KEY",
        human_name="OpenRouter",
    )


def probe_deepseek(env: Mapping[str, str], repo_root: Path) -> GateResult:
    return _probe_api_key(
        env, repo_root, provider_slug="deepseek", env_var="DEEPSEEK_API_KEY", human_name="DeepSeek"
    )


def probe_anthropic_cli(env: Mapping[str, str], repo_root: Path) -> GateResult:
    claude_path = shutil.which("claude")
    if claude_path is None:
        return gate_result(
            _ANTHROPIC_PROBE,
            status="missing",
            observed_state={"claude_on_path": False},
            remediation_hint=(
                "Install Claude Code CLI and authenticate: "
                "https://docs.claude.com/en/docs/claude-code/setup. "
                "After install, run 'claude' once interactively to complete OAuth."
            ),
        )
    try:
        completed = subprocess.run(
            [claude_path, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return gate_result(
            _ANTHROPIC_PROBE,
            status="blocked",
            observed_state={"claude_path": claude_path, "error": str(exc)},
            remediation_hint="claude binary is on PATH but does not execute; reinstall Claude Code CLI",
        )
    if completed.returncode != 0:
        return gate_result(
            _ANTHROPIC_PROBE,
            status="blocked",
            observed_state={
                "claude_path": claude_path,
                "returncode": completed.returncode,
                "stderr": (completed.stderr or "").strip()[:512],
            },
            remediation_hint=(
                "claude --version failed. Reinstall Claude Code CLI and complete "
                "OAuth authentication."
            ),
        )
    auth_home = Path(env.get("CLAUDE_HOME", str(Path.home() / ".claude")))
    oauth_token_set = bool((env.get("CLAUDE_CODE_OAUTH_TOKEN") or "").strip())
    if not auth_home.exists() and not oauth_token_set:
        return gate_result(
            _ANTHROPIC_PROBE,
            status="missing",
            observed_state={
                "claude_path": claude_path,
                "version_output": (completed.stdout or "").strip()[:256],
                "oauth_token_set": False,
                "claude_home_exists": False,
            },
            remediation_hint=(
                "Claude Code CLI is installed but not authenticated. Run 'claude' "
                "once interactively to complete OAuth, or set CLAUDE_CODE_OAUTH_TOKEN."
            ),
        )
    return gate_result(
        _ANTHROPIC_PROBE,
        status="ok",
        observed_state={
            "claude_path": claude_path,
            "version_output": (completed.stdout or "").strip()[:256],
            "oauth_token_set": oauth_token_set,
            "claude_home_exists": auth_home.exists(),
        },
    )


_API_KEY_PROBE_FUNCTIONS = {
    "openai": (_make_api_key_probe("openai", "OPENAI_API_KEY", "OpenAI"), probe_openai),
    "google": (_make_api_key_probe("google", "GEMINI_API_KEY", "Google (Gemini)"), probe_google),
    "openrouter": (
        _make_api_key_probe("openrouter", "OPENROUTER_API_KEY", "OpenRouter"),
        probe_openrouter,
    ),
    "deepseek": (_make_api_key_probe("deepseek", "DEEPSEEK_API_KEY", "DeepSeek"), probe_deepseek),
}


def register(graph=ONBOARDING_GRAPH) -> None:
    for slug, (probe, fn) in _API_KEY_PROBE_FUNCTIONS.items():
        graph.register(probe, fn)
    graph.register(_ANTHROPIC_PROBE, probe_anthropic_cli)
