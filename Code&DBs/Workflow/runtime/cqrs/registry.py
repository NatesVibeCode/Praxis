from dataclasses import dataclass
from typing import Any, Callable, Type
from pydantic import BaseModel


def _callable_ref(value: Callable[..., Any] | None) -> str | None:
    if value is None:
        return None
    return f"{value.__module__}.{value.__qualname__}"


def _model_ref(model_class: Type[BaseModel]) -> str:
    return f"{model_class.__module__}.{model_class.__qualname__}"


@dataclass
class CapabilityRoute:
    path: str
    method: str
    command_class: Type[BaseModel]
    description: str
    operation_name: str | None = None
    operation_kind: str | None = None
    source_kind: str | None = None
    authority_ref: str | None = None
    projection_ref: str | None = None
    posture: str | None = None
    idempotency_policy: str | None = None


class CapabilityRegistry:
    """
    A transport-agnostic registry defining the system's capabilities.
    This single source of truth can be used to dynamically generate
    HTTP APIs, CLI commands, and MCP tools without duplicating code.
    """
    def __init__(self) -> None:
        self.routes: list[CapabilityRoute] = []
        self.handlers: dict[Type[BaseModel], Callable[..., Any]] = {}

    def register(
        self, 
        path: str, 
        method: str, 
        command_class: Type[BaseModel], 
        handler: Callable[..., Any], 
        description: str = "",
        *,
        operation_name: str | None = None,
        operation_kind: str | None = None,
        source_kind: str | None = None,
        authority_ref: str | None = None,
        projection_ref: str | None = None,
        posture: str | None = None,
        idempotency_policy: str | None = None,
    ) -> None:
        self.routes.append(
            CapabilityRoute(
                path=path, 
                method=method, 
                command_class=command_class, 
                description=description,
                operation_name=operation_name,
                operation_kind=operation_kind,
                source_kind=source_kind,
                authority_ref=authority_ref,
                projection_ref=projection_ref,
                posture=posture,
                idempotency_policy=idempotency_policy,
            )
        )
        self.handlers[command_class] = handler

    def get_handler(self, command_class: Type[BaseModel]) -> Callable[..., Any] | None:
        return self.handlers.get(command_class)

    def list_operation_bindings(self) -> list[dict[str, str | None]]:
        bindings: list[dict[str, str | None]] = []
        for route in self.routes:
            handler = self.get_handler(route.command_class)
            bindings.append(
                {
                    "operation_name": route.operation_name,
                    "operation_kind": route.operation_kind,
                    "source_kind": route.source_kind,
                    "http_method": route.method,
                    "http_path": route.path,
                    "input_model_ref": _model_ref(route.command_class),
                    "handler_ref": _callable_ref(handler),
                    "authority_ref": route.authority_ref,
                    "projection_ref": route.projection_ref,
                    "posture": route.posture,
                    "idempotency_policy": route.idempotency_policy,
                }
            )
        return bindings


# The global singleton registry
registry = CapabilityRegistry()
