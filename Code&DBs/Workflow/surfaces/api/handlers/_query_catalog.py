"""Catalog route family for the workflow query surface."""

from __future__ import annotations

from typing import Any


def _handle_catalog_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_catalog_get(request, path)


def _handle_catalog_review_decisions_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_catalog_review_decisions_get(request, path)


def _handle_catalog_review_decisions_post(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_catalog_review_decisions_post(request, path)


def _handle_integrations_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_integrations_get(request, path)


def _handle_models_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_models_get(request, path)


def _handle_market_models_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_market_models_get(request, path)


def _handle_references_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_references_get(request, path)


def _handle_source_options_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_source_options_get(request, path)


def _handle_templates_get(request: Any, path: str) -> None:
    from . import workflow_query as _legacy

    _legacy._handle_templates_get(request, path)


__all__ = [
    "_handle_catalog_get",
    "_handle_catalog_review_decisions_get",
    "_handle_catalog_review_decisions_post",
    "_handle_integrations_get",
    "_handle_market_models_get",
    "_handle_models_get",
    "_handle_references_get",
    "_handle_source_options_get",
    "_handle_templates_get",
]
