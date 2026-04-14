"""Compatibility shim for split operator maintenance work.

The live MCP catalog authority remains `operator.py`. This module exists so
imports can move incrementally without defining duplicate tool ids.
"""

from __future__ import annotations

from .operator import tool_praxis_maintenance

__all__ = ["tool_praxis_maintenance"]
