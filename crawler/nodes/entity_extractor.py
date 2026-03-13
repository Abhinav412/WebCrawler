"""Entity Extractor node — extracts triples for Neo4j knowledge graph."""
from __future__ import annotations
import json, os, re, time
from typing import Any, Optional
import replicate
from langchain_core.runnables import RunnableConfig
from motor.motor_asyncio import AsyncIOMotorClient
from crawler.config import Configuration
from crawler.cost_tracker import tracker
from crawler.models import GraphEntity, Triple
from crawler.state import State

_client: AsyncIOMotorClient | None = None

def _get_client():
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    return _client

def _clean_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()

_PROMPT = """\
You are an expert knowledge graph engineer. Given a user's search query and webpage text,
extract all relevant ENTITIES as (subject, predicate, object) triples.
Use UPPERCASE_SNAKE_CASE for predicates like LOCATED_IN, HAS_FUNDING, FOUNDED_IN, FOUNDED_BY,
SUPPORTS_INDUSTRY, HAS_FEATURE, HAS_PRICING, COMPETES_WITH, INTEGRATES_WITH, IS_TYPE_OF.

Return a JSON array. Each object MUST have:
- "name": String
- "entity_type": String (Framework|Company|Technology|Platform|Tool|Person|Organization|Concept|Entity)
- "description": String (1-2 sentences)
- "triples": Array of {subject, predicate, object, evidence_snippet, confidence}
- "priority_score": Float 0.0-1.0

RULES:
1. Only include triples with ACTUAL data. Skip unknown/N/A values entirely.
2. Subject in each triple = entity name.

User query: {query}
Document Content (truncated to 4000 chars):
{content}

Return ONLY the JSON array, no markdown, no explanation. Return [] if no relevant entities.
"""

_PLACEHOLDER_VALUES = {"not specified","not mentioned","unknown","n/a","not disclosed","not available","not publicly disclosed","none mentioned"}

async def extract_entities(state: State, config: Optional[RunnableConfig] = None) -> dict[str, Any]:
    configuration = Configuration.from_runnable_config(config)
    entity_aggregator: dict[str, GraphEntity] = {}

    for src in state.verified_sources:
        prompt = _PROMPT.format(query=state.user_query, content=_clean_text(src.content)[:4000])
        t0 = time.time()
        try:
            output = replicate.run(configuration.model, input={"prompt": prompt, "max_tokens": 2048, "temperature": 0.1})
            raw_text = "".join(str(c) for c in output)
            tracker.record(node="entity_extractor", model=configuration.model, input_tokens=len(prompt)//4, output_tokens=len(raw_text)//4, latency_s=time.time()-t0)

            cleaned = raw_text.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0]
            entities_data = json.loads(cleaned)
            if not isinstance(entities_data, list):
                entities_data = [entities_data] if isinstance(entities_data, dict) and "name" in entities_data else []

            for data in entities_data:
                name = data.get("name", "Unknown Entity")
                norm_name = name.lower().strip()
                if not norm_name or norm_name == "unknown entity":
                    continue
                triples = []
                for t in data.get("triples", []):
                    obj_val = str(t.get("object", "")).strip()
                    if not obj_val or obj_val.lower() in _PLACEHOLDER_VALUES:
                        continue
                    triples.append(Triple(subject=t.get("subject", name), predicate=t.get("predicate", "HAS_PROPERTY"), object=obj_val, evidence_snippet=t.get("evidence_snippet", ""), source_url=src.url, confidence=float(t.get("confidence", 0.8))))

                priority = float(data.get("priority_score", 0.5))
                desc = data.get("description", "")
                if norm_name in entity_aggregator:
                    ex = entity_aggregator[norm_name]
                    if priority > ex.priority_score: ex.priority_score = priority
                    if len(desc) > len(ex.description): ex.description = desc
                    if src.url not in ex.source_url: ex.source_url += f", {src.url}"
                    existing_keys = {(t.predicate, t.object.lower()) for t in ex.triples}
                    for triple in triples:
                        if (triple.predicate, triple.object.lower()) not in existing_keys:
                            ex.triples.append(triple); existing_keys.add((triple.predicate, triple.object.lower()))
                else:
                    entity_aggregator[norm_name] = GraphEntity(name=name, entity_type=data.get("entity_type", "Entity"), description=desc, triples=triples, source_url=src.url, priority_score=priority)
        except Exception as exc:
            print(f"[EntityExtractor] Failed for {src.url}: {exc}")

    graph_entities = list(entity_aggregator.values())
    try:
        if graph_entities:
            from datetime import datetime, timezone
            client = _get_client()
            col = client[configuration.mongo_db_name]["graph_entities"]
            now = datetime.now(timezone.utc)
            await col.insert_many([{**ge.model_dump(), "session_id": state.session_id, "created_at": now} for ge in graph_entities])
    except Exception as exc:
        print(f"[EntityExtractor] MongoDB write failed: {exc}")

    print(f"[EntityExtractor] {len(graph_entities)} entities, {sum(len(ge.triples) for ge in graph_entities)} triples")
    return {"graph_entities": graph_entities}
