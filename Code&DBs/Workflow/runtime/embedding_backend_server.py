"""Internal semantic backend server.

Hosts the local embedding runtime behind a small HTTP contract so control-plane
surfaces can keep semantic capability without bundling local inference by
default.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from pydantic import BaseModel, Field

from runtime.dependency_contract import require_runtime_dependencies
from runtime.embedding_service import (
    EmbeddingService,
    resolve_embedding_runtime_authority,
)


def _env_flag(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    return normalized not in {"", "0", "false", "no", "off"}


class EmbedRequest(BaseModel):
    texts: list[str] = Field(default_factory=list)
    model_name: str | None = None


class PrewarmRequest(BaseModel):
    model_name: str | None = None


def _embedding_service() -> EmbeddingService:
    return EmbeddingService(authority=resolve_embedding_runtime_authority())


def _client_http_base_url(*, host: str, port: int) -> str:
    display_host = host.strip()
    if display_host in {"0.0.0.0", "::", "[::]"}:
        display_host = "localhost"
    return f"http://{display_host}:{port}"


def _materialize_app():
    from fastapi import FastAPI

    app = FastAPI(title="Praxis Semantic Backend", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        authority = resolve_embedding_runtime_authority()
        return {
            "status": "healthy",
            "backend": "local",
            "model_name": authority.model_name,
            "dimensions": authority.dimensions,
            "cached": EmbeddingService._is_model_cached(authority.model_name),
        }

    @app.post("/prewarm")
    def prewarm(body: PrewarmRequest) -> dict[str, Any]:
        service = _embedding_service()
        model_name = body.model_name or service.model_name
        EmbeddingService.prewarm_model(model_name)
        return {
            "status": "ok",
            "model_name": model_name,
            "dimensions": service.dimensions,
            "cached": EmbeddingService._is_model_cached(model_name),
        }

    @app.post("/embed")
    def embed(body: EmbedRequest) -> dict[str, Any]:
        service = _embedding_service()
        if body.model_name and body.model_name.strip():
            service.authority.validate_embedder_model(body.model_name)
        vectors = service.embed(list(body.texts))
        return {
            "model_name": service.model_name,
            "dimensions": service.dimensions,
            "vectors": vectors,
        }

    return app


app = _materialize_app()


def start_server(host: str = "0.0.0.0", port: int = 8421) -> None:
    report = require_runtime_dependencies(scope="semantic_backend")
    if _env_flag("WORKFLOW_EMBEDDING_PREWARM_ON_STARTUP", True):
        EmbeddingService.start_background_prewarm(
            resolve_embedding_runtime_authority().model_name
        )
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - dependency contract should prevent this
        raise RuntimeError("uvicorn is required: pip install uvicorn") from exc

    client_base_url = _client_http_base_url(host=host, port=port)
    print(f"Starting Praxis semantic backend on {client_base_url}")
    print(f"  Bind: http://{host}:{port}")
    print(f"Dependency contract: {report['manifest_path']}")
    print(f"  Health: {client_base_url}/health")
    print("Press Ctrl+C to stop.\n")
    uvicorn.run(app, host=host, port=port, log_level="info")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Praxis semantic backend server")
    parser.add_argument(
        "--host",
        default=os.environ.get("PRAXIS_SEMANTIC_HOST", "0.0.0.0"),
        help="Bind address (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PRAXIS_SEMANTIC_PORT", "8421")),
        help="TCP port (default: 8421)",
    )
    args = parser.parse_args(argv)
    start_server(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
