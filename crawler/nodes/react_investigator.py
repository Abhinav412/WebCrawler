"""ReAct Agent Investigator node — targeted gap-filling for missing metrics.

Uses LangGraph's prebuilt ReAct agent with specific tools to:
1. Search the web for specific missing metrics.
2. Scrape promising URLs.
3. Extract and save findings directly to Neo4j.
"""

from __future__ import annotations

import contextvars
import os
import re
from typing import Any, Optional

import aiohttp
from bs4 import BeautifulSoup
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from crawler.config import Configuration
from crawler.state import State
from crawler.utils import clean_text


# ── Per-invocation context for tools ────────────────────────
# ContextVars isolate each concurrent pipeline run so that tools
# always operate on the correct session/database/config,
# even when multiple ainvoke() calls are in flight simultaneously.
_active_session_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_active_session_id", default=""
)
_active_db_name: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_active_db_name", default="neo4j"
)
_active_config: contextvars.ContextVar[Optional[Configuration]] = contextvars.ContextVar(
    "_active_config", default=None
)


def _make_skip_finding(reason: str) -> dict[str, Any]:
    """Emit a structured skip record so API/UI can surface investigator behavior."""
    import time

    return {
        "status": "skipped",
        "reason": reason,
        "timestamp": time.time(),
    }


@tool
async def search_web(query: str) -> str:
    """Search the web via SearXNG for specific information.
    Use targeted queries like 'UPSC pass rate 2025' rather than broad ones.
    Returns the top 3 results with title, URL and preview text.
    Prefer openclaw_search when OpenClaw is available — it returns richer content.
    """
    base_url = os.getenv("SEARXNG_BASE_URL", "http://localhost:8080")
    url = f"{base_url.rstrip('/')}/search"
    params = {"q": query, "format": "json"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    results = data.get("results", [])[:3]
                    output = []
                    for i, res in enumerate(results, 1):
                        output.append(
                            f"Result {i}:\n"
                            f"Title: {res.get('title', '')}\n"
                            f"URL: {res.get('url', '')}\n"
                            f"Preview: {res.get('content', '')}\n"
                        )
                    return "\n".join(output) if output else "No results found."
                return f"Search error (HTTP {response.status})"
    except Exception as exc:
        return f"Search failed: {str(exc)}"


@tool
async def openclaw_search(query: str) -> str:
    """Search for specific information via the OpenClaw backend.
    Returns richer pre-fetched content than search_web — prefer this tool
    when investigating missing metrics. Falls back to search_web if unavailable.

    Use targeted queries like 'IIMB NSRCEL portfolio companies 2024'.
    Returns up to 5 results with URL, title, snippet and full content.
    """
    cfg = _active_config.get()
    if cfg is None:
        return "OpenClaw unavailable: configuration not set."

    from crawler.openclaw_client import search_documents

    try:
        docs = await search_documents(cfg, query, limit=5)
    except Exception as exc:
        return f"OpenClaw search failed: {exc}"

    if not docs:
        return "No OpenClaw results found for this query."

    output = []
    for i, doc in enumerate(docs, 1):
        content_preview = doc.content[:600].strip() if doc.content else doc.snippet
        output.append(
            f"Result {i}:\n"
            f"Title: {doc.title}\n"
            f"URL: {doc.url}\n"
            f"Content: {content_preview}\n"
        )
    return "\n".join(output)


@tool
async def scrape_page(url: str) -> str:
    """Fetch a webpage and return its main text content.
    Call this after search_web returns a promising URL to find the exact metric.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=15) as response:
                if response.status != 200:
                    return f"Failed to fetch page (HTTP {response.status})"
                
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                
                # Remove junk
                for element in soup(["script", "style", "nav", "footer", "header", "aside"]):
                    element.decompose()
                
                text = clean_text(soup.get_text(separator=" "))
                
                # Limit to 8000 chars to save context window
                return text[:8000] + ("..." if len(text) > 8000 else "")
    except Exception as exc:
        return f"Scrape failed: {str(exc)}"


_SAFE_REL_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")

@tool
async def save_finding(entity_name: str, metric_name: str, value: str, source_url: str) -> str:
    """Save a discovered data point to the knowledge graph.
    Call this when you have successfully found a missing metric.
    
    Args:
        entity_name: The target entity (e.g., "UPSC")
        metric_name: The metric you were asked to find (e.g., "pass_rate")
        value: The extracted value (e.g., "0.2%")
        source_url: The URL where you found this data
    """
    from crawler.neo4j_client import get_driver, check_neo4j_available
    
    available = await check_neo4j_available()
    if not available:
        return "Error: Neo4j database is unreachable. Cannot save."

    driver = get_driver()
    
    entity_norm = entity_name.lower().strip()
    attr_name = str(value).strip()
    attr_norm = attr_name.lower()
    metric = metric_name.strip()

    # Convert metric name to Neo4j relationship type
    predicate = metric.upper().replace(" ", "_")
    if not predicate.startswith("HAS_"):
        predicate = f"HAS_{predicate}"
    if not _SAFE_REL_RE.match(predicate) or len(predicate) > 40:
        predicate = "HAS_PROPERTY"

    merge_attr = (
        "MERGE (a:Attribute {normalized_name: $attr_norm}) "
        "ON CREATE SET a.name = $attr_name"
    )
    rel_query = f"""
        MATCH (e:Entity)
        WHERE coalesce(e.normalized_name, e.norm_name) = $norm_name
        MATCH (a:Attribute)
        WHERE coalesce(a.normalized_name, a.norm_name) = $attr_norm
        MERGE (e)-[r:{predicate}]->(a)
        ON CREATE SET r.original_pred = $original_pred,
                      r.source = $source_url,
                      r.confidence = 0.8,
                      r.evidence = "Found by ReAct Investigator"
        RETURN id(r)
    """

    try:
        async with driver.session(database=_active_db_name.get()) as session:
            # Note: We don't MERGE the Entity here because it should already exist
            # from the main pipeline. We just link to it.
            await session.run(merge_attr, {"attr_norm": attr_norm, "attr_name": attr_name})
            result = await session.run(
                rel_query, 
                {
                    "norm_name": entity_norm,
                    "attr_norm": attr_norm,
                    "original_pred": metric_name,
                    "source_url": source_url
                }
            )
            records = [r.data() async for r in result]
            if not records:
                return f"Warning: Entity '{entity_name}' not found in database. Could not save."
            return f"Successfully saved {metric_name} = {value} for {entity_name}."
    except Exception as exc:
        return f"Database error: {str(exc)}"


@tool
async def save_recovery_script(domain: str, python_code: str) -> str:
    """Save an auto-generated Python Playwright recovery script for a domain.

    Call this AFTER you have successfully used Playwright MCP tools to scrape
    a website that crawl4ai could not handle. Translate the exact sequence of
    browser actions you took into a self-contained Python async function and
    pass it here. The script will be saved so future runs skip the LLM entirely.

    Args:
        domain: Clean domain name without protocol, e.g. 't-hub.co'
        python_code: A complete, self-contained async Python function called
                     `async def scrape(url: str) -> str` using `playwright.async_api`.
                     It must return the extracted text or an empty string on failure.
    """
    import os
    import re

    # Sanitize domain to a valid Python filename
    safe_name = re.sub(r"[^a-z0-9]", "_", domain.lower()).strip("_")
    if not safe_name:
        return "Error: could not derive a valid filename from domain."

    scripts_dir = os.path.join(
        os.path.dirname(__file__), "..", "recovery_scripts"
    )
    os.makedirs(scripts_dir, exist_ok=True)
    script_path = os.path.join(scripts_dir, f"{safe_name}.py")

    header = (
        f'"""Auto-generated recovery script for: {domain}\n'
        f'Generated by ReAct Investigator. DO NOT manually edit header.\n'
        f'"""\nfrom playwright.async_api import async_playwright\n\n'
    )

    try:
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(header + python_code.strip() + "\n")
        print(f"[RecoveryScript] Saved recovery script -> {script_path}")
        return f"Recovery script saved to {script_path}. Future crawls will use it automatically."
    except Exception as exc:
        return f"Failed to save recovery script: {exc}"


# ── Main Node ────────────────────────────────────────────────


_SYSTEM_PROMPT = """You are a precise data investigator agent.
Your mission is to find missing metrics for entities in a knowledge graph.

Missing data to find:
{missing_data}

Available tools and escalation hierarchy:

  TIER 1 — Fast (try first):
  - `openclaw_search` (preferred when available): pre-fetched rich content.
  - `search_web`: SearXNG — use if openclaw_search is unavailable or returns nothing.

  TIER 2 — Deep (if Tier 1 URLs don't contain the value):
  - `scrape_page`: fetches full text of a specific URL. Use after a promising URL is found.

  TIER 3 — Manual Browser Control (ONLY if Tier 2 returns an error, 403, or empty text):
  - `browser_navigate`: Navigate to a URL inside a real Chromium browser.
  - `browser_click`: Click a button, cookie banner, or dropdown.
  - `browser_fill`: Fill a form field.
  - `browser_evaluate`: Execute JavaScript in the page to extract hidden text or expand sections.
  Use these browser_* tools to manually operate the website just like a human would.

  COMMIT TOOLS (call when done):
  - `save_finding`: write a confirmed metric to the knowledge graph.
  - `save_recovery_script`: MANDATORY after any Tier 3 success — translate your
    exact browser_* tool sequence into a Python `async def scrape(url: str) -> str`
    function using `playwright.async_api`, then call this tool to save it.
    Future runs will reuse the script instead of launching a new LLM session.

For each missing item:
1. Search with a targeted query using `openclaw_search` or `search_web`.
2. If the preview shows a promising URL but lacks the exact value, use `scrape_page`.
3. If `scrape_page` returns a 403 / empty page / bot block, ESCALATE to browser_* tools.
4. Once you have confirmed the metric value, call `save_finding` immediately.
5. If you used ANY browser_* tool to obtain this data, you MUST call `save_recovery_script`
   before moving on to the next missing item.

CRITICAL RULES:
- Do NOT hallucinate data. If you cannot find a metric after all tiers, skip it.
- Keep tool calls focused — one gap at a time.
- After saving a finding, move to the next missing item immediately.
- Return a final summary of what you found and what you could not find.
"""


async def run_react_investigator(state: State, config: Optional[RunnableConfig] = None) -> dict[str, Any]:
    """LangGraph node that runs the ReAct gap-filling agent."""
    configuration = Configuration.from_runnable_config(config)
    
    if not configuration.enable_react_investigator:
        print("[ReActInvestigator] Agent disabled in config. Skipping.")
        return {
            "retry_count": 1,  # delta — LangGraph adds this to the current count
            "investigator_findings": [_make_skip_finding("disabled_in_config")],
        }
        
    gaps = state.missing_data_targets
    if not gaps:
        print("[ReActInvestigator] No missing_data_targets. Skipping.")
        return {
            "retry_count": 1,  # delta
            "investigator_findings": [_make_skip_finding("no_missing_targets")],
        }

    # Set per-coroutine context for the tools (safe for concurrent runs).
    _active_session_id.set(state.session_id)
    _active_db_name.set(configuration.neo4j_database)
    _active_config.set(configuration)

    budget = max(3, min(15, len(gaps) * 3))
    print(f"\n[ReActInvestigator] Triggered to fix {len(gaps)} gaps (budget: {budget} tool steps)")
    for gap in gaps[:5]:
        print(f"  - {gap}")
    
    import time
    
    api_key = os.getenv("REPLICATE_API_TOKEN")
    if not api_key:
        print("[ReActInvestigator] Error: REPLICATE_API_TOKEN not found.")
        return {
            "retry_count": 1,  # delta
            "investigator_findings": [_make_skip_finding("missing_replicate_api_token")],
        }

    # Initialize the LLM (using LangChain's OpenAI wrapper pointing to Replicate's compatibility endpoint)
    llm = ChatOpenAI(
        base_url="https://api.replicate.com/v1",
        api_key=api_key,
        model=configuration.react_investigator_model,
        temperature=0.1,
        max_tokens=1024
    )

    # Build base tool list
    tools = [scrape_page, save_finding, save_recovery_script]
    if configuration.enable_openclaw:
        tools = [openclaw_search, search_web] + tools
        print("[ReActInvestigator] OpenClaw tool enabled — agent will prefer openclaw_search.")
    else:
        tools = [search_web] + tools

    # Boot (or reuse) the Playwright MCP subprocess and inject browser tools
    from crawler.nodes.mcp_manager import McpToolManager

    async with McpToolManager() as mcp:
        playwright_tools = mcp.get_tools()
        if playwright_tools:
            # Rename MCP tools to browser_* so the system prompt matches exactly
            for pt in playwright_tools:
                pt.name = f"browser_{pt.name}" if not pt.name.startswith("browser_") else pt.name
            tools = tools + playwright_tools
            print(f"[ReActInvestigator] {len(playwright_tools)} Playwright MCP tools injected.")
        else:
            print("[ReActInvestigator] Playwright MCP unavailable — Tier 3 escalation disabled.")

        try:
            # Build system prompt
            formatted_gaps = "\n".join(f"- {gap}" for gap in gaps)
            sys_msg = SystemMessage(content=_SYSTEM_PROMPT.format(missing_data=formatted_gaps))

            agent = create_react_agent(
                llm,
                tools=tools,
                state_modifier=sys_msg,
            )

            inputs = {"messages": [HumanMessage(content="Start investigating the missing data targets.")]}
            result = await agent.ainvoke(
                inputs,
                config={"recursion_limit": budget + 2}
            )
        
            messages = result.get("messages", [])
            if messages:
                final_msg = messages[-1].content
                print(f"[ReActInvestigator] Agent finished: {final_msg[:200]}...")
                findings = [
                    {
                        "status": "completed",
                        "reason": "ran",
                        "agent_summary": final_msg,
                        "timestamp": time.time(),
                        "playwright_mcp_used": mcp.available,
                    }
                ]
                return {
                    "investigator_findings": findings,
                    "retry_count": 1,
                }

        except Exception as exc:
            print(f"[ReActInvestigator] Agent execution failed: {exc}")
            return {
                "retry_count": 1,
                "investigator_findings": [
                    {
                        "status": "failed",
                        "reason": "agent_execution_failed",
                        "error": str(exc),
                        "timestamp": time.time(),
                    }
                ],
            }

    return {
        "retry_count": 1,
        "investigator_findings": [_make_skip_finding("no_agent_messages")],
    }
