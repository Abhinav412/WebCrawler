"""Agent package exports.

This module uses lazy imports to avoid circular imports when graph/node modules
import a specific agent submodule during startup.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "Orchestrator",
    "AgentToAgentPipeline",
    "A2AResult",
    "AgentMessage",
    "CrawlerAgent",
    "ValidatorAgent",
    "StructuringAgent",
    "StructuredTable",
    "StructuredRow",
    "MissingFieldsReport",
    "RankingAgent",
    "RankedTable",
    "RankedRow",
    "URLRelevanceAgent",
    "URLRelevanceDecision",
]

_EXPORT_MAP = {
    "Orchestrator": ("crawler.agents.orchestrator", "Orchestrator"),
    "AgentToAgentPipeline": ("crawler.agents.orchestrator", "AgentToAgentPipeline"),
    "A2AResult": ("crawler.agents.orchestrator", "A2AResult"),
    "AgentMessage": ("crawler.agents.orchestrator", "AgentMessage"),
    "CrawlerAgent": ("crawler.agents.orchestrator", "CrawlerAgent"),
    "ValidatorAgent": ("crawler.agents.orchestrator", "ValidatorAgent"),
    "StructuringAgent": ("crawler.agents.structuring_agent", "StructuringAgent"),
    "StructuredTable": ("crawler.agents.structuring_agent", "StructuredTable"),
    "StructuredRow": ("crawler.agents.structuring_agent", "StructuredRow"),
    "MissingFieldsReport": ("crawler.agents.structuring_agent", "MissingFieldsReport"),
    "RankingAgent": ("crawler.agents.ranking_agent", "RankingAgent"),
    "RankedTable": ("crawler.agents.ranking_agent", "RankedTable"),
    "RankedRow": ("crawler.agents.ranking_agent", "RankedRow"),
    "URLRelevanceAgent": ("crawler.agents.url_relevance_agent", "URLRelevanceAgent"),
    "URLRelevanceDecision": (
        "crawler.agents.url_relevance_agent",
        "URLRelevanceDecision",
    ),
}


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
