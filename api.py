"""FastAPI server for the LangGraph Crawler Pipeline.

Endpoints:
  POST /crawl/rank        — Full ranking pipeline (SSE streaming progress)
  GET  /crawl/rank/{id}   — Poll for ranking result
  POST /crawl/a2a         — Agent-to-agent pipeline
  GET  /health            — Health check
  GET  /cost-summary      — Latest cost report

Run:
    uvicorn api:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

load_dotenv()

from crawler.graph import graph  # noqa: E402
from crawler.cost_tracker import tracker  # noqa: E402
from crawler.agents import AgentToAgentPipeline  # noqa: E402
from crawler.agents.structure_rank_pipeline import StructureRankPipeline  # noqa: E402

app = FastAPI(
    title="WebCrawler Ranking Pipeline",
    description="Multi-agent research pipeline with Neo4j knowledge graph and LLM ranking.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory job store ──────────────────────────────────────
_jobs: dict[str, dict[str, Any]] = {}


# ── SSE helper ───────────────────────────────────────────────
def _sse(event: str, data: Any) -> str:
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


# ── Request / Response models ────────────────────────────────
class RankRequest(BaseModel):
    query: str = Field(..., description="The ranking question to research.")
    max_retries: int = Field(default=2, ge=0, le=5)
    min_credibility: float = Field(default=0.65, ge=0.0, le=1.0)


class A2ACrawlRequest(BaseModel):
    query: str = Field(..., description="The research query to process.")
    required_metrics: list[str] = Field(..., min_length=1)
    max_rounds: int = Field(default=3, ge=1, le=10)


class A2ACrawlResponse(BaseModel):
    status: str
    message: str
    session_id: str
    query: str
    required_metrics: list[str]
    available_metrics: list[str]
    missing_metrics: list[str]
    entities: list[dict[str, Any]]
    communication_log: list[dict[str, Any]]
    rounds_used: int
    cost_summary: dict[str, Any]


# ── Core pipeline runner with progress events ────────────────
async def _run_rank_pipeline(
    job_id: str,
    query: str,
    config: dict,
) -> None:
    """Run the full ranking pipeline and push SSE progress to job store."""
    job = _jobs[job_id]
    events: list[dict] = job["events"]

    def push(event_type: str, payload: dict) -> None:
        entry = {"type": event_type, "timestamp": datetime.now(timezone.utc).isoformat(), **payload}
        events.append(entry)

    try:
        # ── Phase 1: LangGraph pipeline (crawl → extract → Neo4j) ──
        push("phase_start", {"phase": "crawl", "label": "Starting web crawl pipeline"})

        push("node_start", {"node": "intent_parser", "label": "Parsing intent & generating search queries"})
        push("node_start", {"node": "url_discovery", "label": "Discovering URLs via Tavily search"})
        push("node_start", {"node": "web_crawler", "label": "Crawling pages (crawl4ai + httpx)"})
        push("node_start", {"node": "source_verifier", "label": "Verifying source credibility"})
        push("node_start", {"node": "mongo_logger", "label": "Persisting verified sources"})

        result = await graph.ainvoke(
            {"user_query": query},
            config={"configurable": config},
        )

        session_id = result.get("session_id", "")
        structured_results = result.get("structured_results", [])
        extracted_entities = result.get("extracted_entities", [])

        push("node_complete", {"node": "entity_extractor", "label": "Entities extracted as triples", "count": len(extracted_entities)})
        push("node_complete", {"node": "neo4j_ingester", "label": "Knowledge graph populated in Neo4j"})
        push("node_complete", {"node": "graph_structurer", "label": "Structured results built from graph", "count": len(structured_results)})
        push("node_complete", {"node": "metrics_evaluator", "label": "Metrics evaluated for completeness"})

        missing = result.get("missing_data_targets", [])
        if missing:
            push("agent_message", {
                "from": "metrics_evaluator",
                "to": "intent_parser",
                "content": f"Missing data for {len(missing)} targets. Triggering retry loop.",
                "missing": missing,
            })

        push("phase_complete", {"phase": "crawl", "label": "Crawl pipeline complete", "entities": len(extracted_entities)})

        # ── Phase 2: Agent loop (StructuringAgent + RankingAgent) ──
        push("phase_start", {"phase": "agents", "label": "Starting agent ranking loop"})

        if not session_id:
            push("warning", {"message": "No session ID — skipping structure/rank phase"})
            job["status"] = "completed"
            job["ranked_table"] = {}
            job["cost_summary"] = result.get("cost_summary", {})
            return

        push("agent_message", {
            "from": "orchestrator",
            "to": "structuring_agent",
            "content": f"Structure entities from ChromaDB for session {session_id}",
        })

        pipeline = StructureRankPipeline(
            session_id=session_id,
            user_query=query,
        )

        table = pipeline.run_structure()
        push("agent_message", {
            "from": "structuring_agent",
            "to": "validator",
            "content": f"Structured {len(table.rows)} entities across {len(table.columns)} columns",
            "columns": table.columns,
            "missing_cells": table.missing_report.total_missing_cells if table.missing_report else 0,
        })

        # Gap-fill loop
        max_patch_rounds = 2
        for patch_round in range(max_patch_rounds):
            if not table.missing_report or table.missing_report.is_complete():
                push("agent_message", {
                    "from": "validator",
                    "to": "orchestrator",
                    "content": "All metrics satisfied. Proceeding to ranking.",
                })
                break

            missing_cols = table.missing_report.missing_columns
            push("agent_message", {
                "from": "validator",
                "to": "crawler_agent",
                "content": f"Round {patch_round + 1}: Missing {len(missing_cols)} columns — requesting targeted recrawl",
                "missing_columns": missing_cols,
            })

            # Targeted recrawl via A2A crawler agent
            a2a = AgentToAgentPipeline(max_rounds=1)
            recrawl = await a2a.crawler_agent.crawl(
                base_query=query,
                missing_metrics=missing_cols,
                session_id=session_id,
            )

            push("agent_message", {
                "from": "crawler_agent",
                "to": "structuring_agent",
                "content": f"Recrawl complete: {len(recrawl.entities)} new entities found",
            })

            table = pipeline.apply_patch(recrawl.entities)

            push("agent_message", {
                "from": "structuring_agent",
                "to": "validator",
                "content": f"After patch: {table.missing_report.total_missing_cells if table.missing_report else 0} missing cells remain",
            })

        push("agent_message", {
            "from": "orchestrator",
            "to": "ranking_agent",
            "content": f"Ranking {len(table.rows)} entities using LLM-determined criteria",
        })

        final = pipeline.run_ranking()

        push("agent_message", {
            "from": "ranking_agent",
            "to": "orchestrator",
            "content": f"Ranking complete. Top entity: {final.ranked_table.rows[0].entity_name if final.ranked_table.rows else 'none'}",
            "criteria": [c.to_dict() for c in final.ranked_table.criteria],
        })

        push("phase_complete", {"phase": "agents", "label": "Agent loop complete"})

        # ── Phase 3: Final result ──
        push("phase_start", {"phase": "result", "label": "Building final ranking table"})

        ranked_dict = final.ranked_table.to_dict()
        push("phase_complete", {"phase": "result", "label": "Ranking table ready", "row_count": len(ranked_dict.get("rows", []))})

        job["status"] = "completed"
        job["session_id"] = session_id
        job["ranked_table"] = ranked_dict
        job["structured_table"] = final.structured_table.to_dict()
        job["cost_summary"] = result.get("cost_summary", tracker.get_summary())
        job["completed_at"] = datetime.now(timezone.utc).isoformat()

        push("done", {"status": "completed", "job_id": job_id})

    except Exception as exc:
        push("error", {"message": str(exc)})
        job["status"] = "failed"
        job["error"] = str(exc)
        job["completed_at"] = datetime.now(timezone.utc).isoformat()


# ── Endpoints ────────────────────────────────────────────────

@app.post("/crawl/rank")
async def start_rank(request: RankRequest):
    """Start ranking pipeline. Returns job_id immediately."""
    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {
        "job_id": job_id,
        "status": "running",
        "query": request.query,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "events": [],
        "ranked_table": None,
        "structured_table": None,
        "cost_summary": {},
        "session_id": "",
        "error": None,
    }
    config = {
        "max_retries": request.max_retries,
        "min_credibility": request.min_credibility,
    }
    asyncio.create_task(_run_rank_pipeline(job_id, request.query, config))
    return {"job_id": job_id, "status": "running"}


@app.get("/crawl/rank/{job_id}/stream")
async def stream_rank_events(job_id: str):
    """SSE stream of pipeline progress events."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    async def event_generator() -> AsyncGenerator[str, None]:
        job = _jobs[job_id]
        sent_idx = 0
        while True:
            events = job["events"]
            while sent_idx < len(events):
                yield _sse(events[sent_idx]["type"], events[sent_idx])
                sent_idx += 1
            if job["status"] in ("completed", "failed"):
                yield _sse("status", {"status": job["status"], "job_id": job_id})
                break
            await asyncio.sleep(0.3)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/crawl/rank/{job_id}")
async def get_rank_result(job_id: str):
    """Poll for ranking result."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return _jobs[job_id]


@app.post("/crawl/a2a", response_model=A2ACrawlResponse)
async def crawl_agent_to_agent(request: A2ACrawlRequest):
    """Run strict agent-to-agent crawl/validation orchestration."""
    pipeline = AgentToAgentPipeline(max_rounds=request.max_rounds)
    result = await pipeline.run(
        query=request.query,
        required_metrics=request.required_metrics,
    )
    return A2ACrawlResponse(**result.to_dict())


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "active_jobs": sum(1 for j in _jobs.values() if j["status"] == "running"),
    }


@app.get("/cost-summary")
async def cost_summary():
    return tracker.get_summary()
