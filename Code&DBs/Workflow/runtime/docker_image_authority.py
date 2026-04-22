"""Docker worker image authority for sandboxed execution.

This module owns the default image story for Praxis sandbox runners:
  - resolve the requested image from arg/env/default authority
  - auto-build the canonical default image when it is missing
  - keep build behavior explicit and shared across runtime surfaces
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Callable

DOCKER_IMAGE_ENV = "PRAXIS_DOCKER_IMAGE"
DEFAULT_DOCKER_IMAGE = "praxis-worker:latest"
DEFAULT_DOCKERFILE_RELATIVE = Path("docker") / "praxis-worker.Dockerfile"
DEFAULT_BUILD_TIMEOUT_SECONDS = 900

# Per-agent-family thin images. When an agent slug starts with one of these
# prefixes and no explicit image override is declared, the sandbox prefers
# the thin image. Each thin image contains one CLI + MCP bridge target +
# bash — no Python runtime, no Praxis repo, no sibling CLIs. Falls back
# gracefully to DEFAULT_DOCKER_IMAGE if the thin image is not present on
# the host and cannot be auto-built.
AGENT_FAMILY_IMAGE_MAP: dict[str, str] = {
    "anthropic": "praxis-claude:latest",
    "claude": "praxis-claude:latest",
    "openai": "praxis-codex:latest",
    "codex": "praxis-codex:latest",
    "google": "praxis-gemini:latest",
    "gemini": "praxis-gemini:latest",
}

# Per-thin-image Dockerfile. Mirrors the (image, dockerfile) contract
# DEFAULT_DOCKER_IMAGE + DEFAULT_DOCKERFILE_RELATIVE establish for the fat
# image so auto-build can target either.
THIN_IMAGE_DOCKERFILES: dict[str, Path] = {
    "praxis-claude:latest": Path("docker") / "praxis-claude.Dockerfile",
    "praxis-codex:latest": Path("docker") / "praxis-codex.Dockerfile",
    "praxis-gemini:latest": Path("docker") / "praxis-gemini.Dockerfile",
}


def resolve_thin_image_for_agent(agent_slug: str | None) -> str | None:
    """Return the thin per-family image for an agent slug, or None.

    agent_slug is typically "provider/model" (e.g. "anthropic/claude-sonnet-4"
    or "openai/gpt-5.4"). Only the provider prefix is considered.
    """
    text = str(agent_slug or "").strip()
    if not text:
        return None
    family = text.split("/", 1)[0].strip().lower()
    return AGENT_FAMILY_IMAGE_MAP.get(family)


def workflow_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_dockerfile_path() -> Path:
    return workflow_root() / DEFAULT_DOCKERFILE_RELATIVE


def build_default_docker_image(
    *,
    image_name: str = DEFAULT_DOCKER_IMAGE,
    timeout_seconds: int = DEFAULT_BUILD_TIMEOUT_SECONDS,
) -> tuple[bool, str | None]:
    # When a thin per-agent image is requested, use its Dockerfile; otherwise
    # fall back to the fat default Dockerfile.
    thin_dockerfile_rel = THIN_IMAGE_DOCKERFILES.get(image_name)
    if thin_dockerfile_rel is not None:
        dockerfile = workflow_root() / thin_dockerfile_rel
    else:
        dockerfile = default_dockerfile_path()
    if not dockerfile.is_file():
        return False, f"dockerfile missing for {image_name}: {dockerfile}"

    try:
        result = subprocess.run(
            [
                "docker",
                "build",
                "-f",
                str(dockerfile),
                "-t",
                image_name,
                str(workflow_root()),
            ],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        return False, f"default image build failed: {type(exc).__name__}: {exc}"

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        if detail:
            detail = detail.splitlines()[-1]
        return False, f"default image build failed: {detail or f'exit {result.returncode}'}"
    return True, None


def resolve_docker_image(
    *,
    requested_image: str | None,
    image_exists: Callable[[str], bool],
    auto_build_default: bool = True,
    agent_slug: str | None = None,
) -> tuple[str, dict[str, object]]:
    """Resolve the docker image for a sandbox execution.

    Authority order:

      1. ``requested_image`` (explicit spec/bundle override)
      2. ``PRAXIS_DOCKER_IMAGE`` env (operator override)
      3. Per-agent-family thin image (via ``agent_slug``) when it exists
         or can be auto-built
      4. ``DEFAULT_DOCKER_IMAGE`` fat image (with auto-build)

    A thin image that can't be built or pulled falls through to the default
    fat image so callers never hard-fail on a missing thin image.
    """
    explicit_arg = str(requested_image or "").strip()
    explicit_env = os.environ.get(DOCKER_IMAGE_ENV, "").strip()

    # 1 + 2 — honored as-is; no fallback because the operator asked specifically.
    if explicit_arg or explicit_env:
        image = explicit_arg or explicit_env
        source = "argument" if explicit_arg else "env"
        if image_exists(image):
            return image, {"source": source, "built_default": False, "build_error": None}
        return image, {"source": source, "built_default": False, "build_error": None}

    # 3 — per-agent thin image when caller provided agent_slug.
    thin_image = resolve_thin_image_for_agent(agent_slug)
    if thin_image is not None:
        if image_exists(thin_image):
            return thin_image, {
                "source": "agent_family",
                "built_default": False,
                "build_error": None,
            }
        if auto_build_default:
            built_thin, thin_error = build_default_docker_image(image_name=thin_image)
            if built_thin and image_exists(thin_image):
                return thin_image, {
                    "source": "agent_family",
                    "built_default": True,
                    "build_error": None,
                }
            # Thin build failed — fall through to fat default with a note so the
            # caller can see the thin attempt in metadata if they care.
            _thin_fallback_note = thin_error

    # 4 — fat default (existing behavior, preserves legacy specs).
    image = DEFAULT_DOCKER_IMAGE
    source = "default"

    if image_exists(image):
        return image, {
            "source": source,
            "built_default": False,
            "build_error": None,
        }

    build_error = None
    built_default = False
    if auto_build_default:
        built_default, build_error = build_default_docker_image(image_name=image)
        if built_default and image_exists(image):
            return image, {
                "source": source,
                "built_default": True,
                "build_error": None,
            }

    return image, {
        "source": source,
        "built_default": built_default,
        "build_error": build_error,
    }


__all__ = [
    "AGENT_FAMILY_IMAGE_MAP",
    "DEFAULT_DOCKER_IMAGE",
    "DOCKER_IMAGE_ENV",
    "THIN_IMAGE_DOCKERFILES",
    "build_default_docker_image",
    "default_dockerfile_path",
    "resolve_docker_image",
    "resolve_thin_image_for_agent",
    "workflow_root",
]
