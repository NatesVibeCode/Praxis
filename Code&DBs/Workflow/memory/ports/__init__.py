"""Port interfaces for the memory package.

These protocols define the infrastructure boundaries that memory modules
depend on. Memory code imports from here; concrete adapters live in
memory.adapters.
"""
from __future__ import annotations

from .embedding import EmbeddingPort
from .vector import VectorStorePort, VectorFilter
from .scheduling import SchedulingPort, SchedulingResult

__all__ = [
    "EmbeddingPort",
    "SchedulingPort",
    "SchedulingResult",
    "VectorFilter",
    "VectorStorePort",
]
