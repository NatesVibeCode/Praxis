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


def workflow_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_dockerfile_path() -> Path:
    return workflow_root() / DEFAULT_DOCKERFILE_RELATIVE


def build_default_docker_image(
    *,
    image_name: str = DEFAULT_DOCKER_IMAGE,
    timeout_seconds: int = DEFAULT_BUILD_TIMEOUT_SECONDS,
) -> tuple[bool, str | None]:
    dockerfile = default_dockerfile_path()
    if not dockerfile.is_file():
        return False, f"default dockerfile missing: {dockerfile}"

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
) -> tuple[str, dict[str, object]]:
    explicit_arg = str(requested_image or "").strip()
    explicit_env = os.environ.get(DOCKER_IMAGE_ENV, "").strip()
    image = explicit_arg or explicit_env or DEFAULT_DOCKER_IMAGE
    source = "argument" if explicit_arg else ("env" if explicit_env else "default")

    if image_exists(image):
        return image, {
            "source": source,
            "built_default": False,
            "build_error": None,
        }

    build_error = None
    built_default = False
    if source == "default" and auto_build_default:
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
    "DEFAULT_DOCKER_IMAGE",
    "DOCKER_IMAGE_ENV",
    "build_default_docker_image",
    "default_dockerfile_path",
    "resolve_docker_image",
    "workflow_root",
]
