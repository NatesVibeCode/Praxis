from typing import Any, TypeVar
import logging
from pydantic import BaseModel

from .registry import registry

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

class CommandBus:
    """
    The execution boundary for all system operations.
    Handles command validation, centralized telemetry/auditing, and handler routing.
    """
    def __init__(self, subsystems: Any) -> None:
        self.subsystems = subsystems

    def dispatch(self, command: T) -> Any:
        command_type = type(command)
        handler = registry.get_handler(command_type)
        
        if not handler:
            raise ValueError(f"No handler registered for command: {command_type.__name__}")
        
        logger.info(f"Dispatching command: {command_type.__name__}")
        
        # In a full implementation, we could emit a system event here
        # _emit_system_event(self.subsystems.get_pg_conn(), f"command.{command_type.__name__}.started", command)
        
        try:
            result = handler(command, self.subsystems)
            return result
        except Exception as exc:
            logger.error(f"Command {command_type.__name__} failed: {exc}", exc_info=True)
            raise
