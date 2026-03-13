"""StructureRankPipeline — orchestrates StructuringAgent ↔ RankingAgent.

Pipeline flow:
  1. run_structure()  — fetch ChromaDB entities → StructuredTable
  2. apply_patch()    — merge recrawled entities to fill gaps (loop)
  3. run_ranking()    — LLM scores → RankedTable

Integration:
  Input : session_id (from Crawler Agent), user_query
  Output: StructureRankResult with .ranked_table.to_dict()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from crawler.agents.structuring_agent import (
    MissingFieldsReport,
    StructuredTable,
    StructuringAgent,
)
from crawler.agents.ranking_agent import RankedTable, RankingAgent


@dataclass
class StructureRankResult:
    session_id: str
    user_query: str
    structured_table: StructuredTable
    ranked_table: RankedTable
    rounds_used: int
    final_missing_cells: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "user_query": self.user_query,
            "rounds_used": self.rounds_used,
            "final_missing_cells": self.final_missing_cells,
            "structured_table": self.structured_table.to_dict(),
            "ranked_table": self.ranked_table.to_dict(),
        }


class StructureRankPipeline:
    """
    Coordinates StructuringAgent ↔ RankingAgent with gap-fill retry loop.

    Typical orchestrator usage:

        pipeline = StructureRankPipeline(session_id="...", user_query="...")

        table = pipeline.run_structure()

        for _ in range(max_patch_rounds):
            if table.missing_report.is_complete():
                break
            patch = await crawler_agent.crawl(
                base_query=user_query,
                missing_metrics=table.missing_report.missing_columns,
                session_id=session_id,
            )
            table = pipeline.apply_patch(patch.entities)

        result = pipeline.run_ranking()
    """

    def __init__(
        self,
        *,
        session_id: str,
        user_query: str,
        chroma_persist_dir: str = "./chroma_db",
        chroma_entity_collection: str = "crawler_entities",
        chroma_embedding_dim: int = 384,
        model: str = "meta/meta-llama-3-70b-instruct",
        max_patch_rounds: int = 3,
    ) -> None:
        self.session_id = session_id
        self.user_query = user_query
        self.max_patch_rounds = max_patch_rounds

        self.structuring_agent = StructuringAgent(
            chroma_persist_dir=chroma_persist_dir,
            chroma_entity_collection=chroma_entity_collection,
            chroma_embedding_dim=chroma_embedding_dim,
            model=model,
        )
        self.ranking_agent = RankingAgent(model=model)
        self._current_table: StructuredTable | None = None
        self._rounds_used: int = 0

    def run_structure(self) -> StructuredTable:
        """Fetch ChromaDB entities → initial StructuredTable."""
        self._rounds_used = 1
        self._current_table = self.structuring_agent.structure(
            session_id=self.session_id,
            user_query=self.user_query,
            round_number=self._rounds_used,
        )
        return self._current_table

    def apply_patch(self, patch_entities: list[dict[str, Any]]) -> StructuredTable:
        """Merge recrawled entities to fill missing cells."""
        if self._current_table is None:
            raise RuntimeError("Call run_structure() before apply_patch().")
        self._rounds_used += 1
        self._current_table = self.structuring_agent.patch(
            table=self._current_table,
            patch_entities=patch_entities,
        )
        return self._current_table

    def run_ranking(self) -> StructureRankResult:
        """Compute rankings on the current table. Returns final StructureRankResult."""
        if self._current_table is None:
            raise RuntimeError("Call run_structure() before run_ranking().")

        ranked = self.ranking_agent.rank(self._current_table)

        missing_cells = (
            self._current_table.missing_report.total_missing_cells
            if self._current_table.missing_report
            else 0
        )

        return StructureRankResult(
            session_id=self.session_id,
            user_query=self.user_query,
            structured_table=self._current_table,
            ranked_table=ranked,
            rounds_used=self._rounds_used,
            final_missing_cells=missing_cells,
        )

    @property
    def missing_report(self) -> MissingFieldsReport | None:
        if self._current_table and self._current_table.missing_report:
            return self._current_table.missing_report
        return None
