from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Callable

from .types import AnalysisMode


ToolHandler = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    handler: ToolHandler
    live_allowed: bool = True
    backtest_allowed: bool = False
    point_in_time_aware: bool = False


class ToolPolicyError(RuntimeError):
    pass


class ApprovedToolRegistry:
    def __init__(self, tools: list[ToolDefinition], *, max_calls: int = 6):
        self.tools = {tool.name: tool for tool in tools}
        self.max_calls = max(0, int(max_calls))
        self.calls = 0
        self.cache: dict[str, dict[str, Any]] = {}

    def schemas(self, mode: AnalysisMode) -> list[dict[str, Any]]:
        allowed = [tool for tool in self.tools.values() if tool.live_allowed] if mode is AnalysisMode.LIVE else [
            tool for tool in self.tools.values() if tool.backtest_allowed and tool.point_in_time_aware
        ]
        return [
            {
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "ticker": {"type": "string"},
                        "as_of_timestamp": {"type": "string"},
                    },
                    "required": ["ticker", "as_of_timestamp"],
                },
                "strict": True,
            }
            for tool in allowed
        ]

    def call(self, name: str, arguments: dict[str, Any], *, mode: AnalysisMode, as_of: str) -> dict[str, Any]:
        tool = self.tools.get(name)
        if tool is None:
            raise ToolPolicyError(f"tool is not approved: {name}")
        if mode is AnalysisMode.LIVE and not tool.live_allowed:
            raise ToolPolicyError(f"tool is disabled in live mode: {name}")
        if mode is AnalysisMode.BACKTEST and (not tool.backtest_allowed or not tool.point_in_time_aware):
            raise ToolPolicyError(f"live-only or non-point-in-time tool blocked during backtest: {name}")
        if arguments.get("as_of_timestamp") != as_of:
            raise ToolPolicyError("tool as_of_timestamp must match analysis timestamp")
        key = sha256(f"{name}:{json.dumps(arguments, sort_keys=True)}".encode()).hexdigest()
        if key in self.cache:
            return self.cache[key]
        if self.calls >= self.max_calls:
            raise ToolPolicyError("MAX_TOOL_CALLS budget exceeded")
        self.calls += 1
        result = tool.handler(arguments)
        if not isinstance(result, dict):
            raise ToolPolicyError("tool result must be a structured object")
        serialized = json.dumps(result, default=str)
        if len(serialized) > 8000:
            raise ToolPolicyError("tool result exceeded structured result limit")
        self.cache[key] = result
        return result
