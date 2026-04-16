from dataclasses import dataclass
from typing import Any, Callable, Type
from pydantic import BaseModel

@dataclass
class CapabilityRoute:
    path: str
    method: str
    command_class: Type[BaseModel]
    description: str

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
        description: str = ""
    ) -> None:
        self.routes.append(
            CapabilityRoute(
                path=path, 
                method=method, 
                command_class=command_class, 
                description=description
            )
        )
        self.handlers[command_class] = handler

    def get_handler(self, command_class: Type[BaseModel]) -> Callable[..., Any] | None:
        return self.handlers.get(command_class)

# The global singleton registry
registry = CapabilityRegistry()
