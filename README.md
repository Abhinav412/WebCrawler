# WebCrawler Intelligence — Multi-Agent Ranking Pipeline

## Architecture

```
User Query
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  LANGGRAPH PIPELINE (graph.py)                              │
│                                                             │
│  intent_parser ──► url_discovery ──► web_crawler           │
│       ▲                                   │                 │
│       │ (retry if gaps)         source_verifier             │
│       │                               │                     │
│       │                         mongo_logger                │
│       │                         + preprocessor  ──► ChromaDB│
│       │                               │                     │
│       │                       entity_extractor (triples)    │
│       │                               │                     │
│       │                        neo4j_ingester ──► Neo4j     │
│       │                               │                     │
│       │                       graph_structurer               │
│       │                               │                     │
│       └──────── metrics_evaluator ◄───┘                     │
│                  (missing targets?)                         │
└─────────────────────────────────────────────────────────────┘
    │
    │  session_id, extracted_entities, structured_results
    ▼
┌─────────────────────────────────────────────────────────────┐
│  AGENT LOOP (api.py → StructureRankPipeline)               │
│                                                             │
│  StructuringAgent ──► StructuredTable                      │
│       │                    │                               │
│       │              (missing cells?)                      │
│       │                    │ yes                           │
│  Validator ◄──────────────►│                               │
│       │                    │                               │
│  CrawlerAgent (targeted recrawl) ──► apply_patch()         │
│       │                    │                               │
│  [repeat up to 2 rounds]   │                               │
│                            ▼                               │
│  RankingAgent ──► LLM criteria ──► RankedTable             │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
  Frontend (React) — SSE-streamed progress + ranking table
```

## Where Each Part Lives

| Concern | File(s) |
|---|---|
| LangGraph pipeline wiring | `crawler/graph.py` |
| Intent parsing + target metrics | `crawler/nodes/intent_parser.py` |
| Web search (Tavily) | `crawler/nodes/url_discovery.py` |
| Crawling (crawl4ai + httpx) | `crawler/nodes/web_crawler.py` |
| Source credibility scoring | `crawler/nodes/source_verifier.py` |
| MongoDB + ChromaDB write | `crawler/nodes/mongo_logger.py` + `preprocessor.py` |
| Triple extraction (KG) | `crawler/nodes/entity_extractor.py` |
| Neo4j ingestion | `crawler/nodes/neo4j_ingester.py` |
| Graph query → StructuredResult | `crawler/nodes/graph_structurer.py` |
| Gap detection + retry routing | `crawler/nodes/metrics_evaluator.py` |
| Agent loop orchestration | `crawler/agents/structure_rank_pipeline.py` |
| Structuring agent (ChromaDB→table) | `crawler/agents/structuring_agent.py` |
| Ranking agent (LLM scoring) | `crawler/agents/ranking_agent.py` |
| A2A pipeline (crawl + validate) | `crawler/agents/a2a_pipeline.py` |
| FastAPI server + SSE streaming | `api.py` |
| React frontend | `frontend/src/App.jsx` |

## Setup

```bash
# 1. Copy env vars
cp .env.template .env
# edit .env with your API keys

# 2. Install Python deps
pip install -r requirements.txt

# 3. Start Neo4j (Desktop or Aura)
# 4. Start MongoDB (local or Atlas)

# 5. Run backend
uvicorn api:app --reload --port 8000

# 6. Run frontend (from frontend/)
npm install && npm run dev
```

## Ranking Flow

1. User submits a ranking question in the frontend
2. Backend starts SSE stream — frontend shows live node progress
3. LangGraph runs all 9 nodes; Neo4j gets populated with entity triples
4. Agent loop: StructuringAgent builds table → Validator detects gaps
5. CrawlerAgent does targeted recrawls → gaps filled
6. RankingAgent uses LLM to pick weighted criteria → scores every entity
7. Frontend renders animated ranking table
