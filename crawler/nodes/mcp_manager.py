"""MCP lifecycle manager for the Playwright MCP server.

Manages a single long-lived `npx @playwright/mcp stdio` subprocess so Chromium
stays warm across multiple ReAct investigator invocations without the cost of
booting a new browser per request.

Usage (inside react_investigator):
    async with McpToolManager() as mcp:
        tools = await mcp.get_tools()  # list of langchain-compatible Tool objects
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

_manager_instance: "McpToolManager | None" = None


class McpToolManager:
    """Async context manager that keeps a playwright-mcp stdio process alive."""

    def __init__(self) -> None:
        self._client: Any = None
        self._tools: list[Any] = []

    async def __aenter__(self) -> "McpToolManager":
        try:
            from langchain_mcp_adapters.client import MultiServerMCPClient

            self._client = MultiServerMCPClient(
                {
                    "playwright": {
                        "command": "npx",
                        "args": ["@playwright/mcp@latest", "--headless"],
                        "transport": "stdio",
                    }
                }
            )
            await self._client.__aenter__()
            self._tools = await self._client.get_tools()
            print(
                f"[McpToolManager] Playwright MCP ready — "
                f"{len(self._tools)} browser tools available."
            )
        except Exception as exc:
            print(f"[McpToolManager] Failed to start Playwright MCP server: {exc}")
            print("[McpToolManager] Browser-control tools will be unavailable this run.")
            self._tools = []
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client is not None:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception:
                pass
            self._client = None
        print("[McpToolManager] Playwright MCP server shut down.")

    def get_tools(self) -> list[Any]:
        """Return LangChain-compatible Playwright tool objects."""
        return list(self._tools)

    @property
    def available(self) -> bool:
        return len(self._tools) > 0
