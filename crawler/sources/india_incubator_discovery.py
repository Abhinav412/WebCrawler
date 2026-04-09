"""
India Incubator Discovery - Multi-source entity collection.

Sources prioritized by reliability:
1. Government directories (Startup India, DST, MeitY, AIM)
2. Institutional websites (IITs, IIMs, IISc, NITs)
3. Industry associations (ISBA)
4. Commercial databases (YourStory, Inc42)
5. Academic papers and reports
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import httpx
import os
import json
from crawl4ai import AsyncWebCrawler


@dataclass
class IncubatorSeed:
    """Minimal data for initial discovery."""
    name: str
    source_url: str
    source_type: str  # gov_list, institution, commercial, secondary
    confidence: float = 0.5
    discovered_at: str = field(default_factory=lambda: __import__('datetime').datetime.now().isoformat())


@dataclass
class IncubatorEntity:
    """Full incubator entity with all fields."""
    id: str = ""  # UUID after deduplication
    name: str = ""
    official_name: str = ""
    short_name: str = ""
    
    # Contact
    website: str = ""
    email: str = ""
    phone: str = ""
    
    # Location
    city: str = ""
    state: str = ""
    address: str = ""
    pincode: str = ""
    
    # Classification
    type: str = ""  # government, private, academic, corporate, social
    backing: str = ""  # dst, meity, aim, self-funded, corporate
    
    # Financial
    funding_type: str = ""  # grant, equity, debt, hybrid
    investment_range: str = ""
    equity_taken: str = ""
    
    # Programs
    focus_sectors: list[str] = field(default_factory=list)
    programs: list[str] = field(default_factory=list)
    duration_months: int = 0
    virtual_available: bool = False
    
    # Stats
    established_year: int = 0
    alumni_count: int = 0
    active_startups: int = 0
    total_investment_made: str = ""
    
    # Team
    team_size: int = 0
    mentor_count: int = 0
    
    # Metadata
    data_completeness: float = 0.0  # 0-1
    sources: list[str] = field(default_factory=list)
    last_updated: str = ""
    
    # Missing fields tracking
    missing_fields: list[str] = field(default_factory=list)


class IndiaIncubatorDiscovery:
    """
    Multi-source discovery for Indian incubators.
    
    Expected entities: ~1100-1200
    - Government-backed: ~400 (DST, MeitY, AIM, DBT)
    - Academic: ~300 (IITs, IIMs, IISc, NITs, Central Univs)
    - Private: ~400-500 (Corporate, Independent)
    """
    
    # Government source URLs
    GOV_SOURCES = {
        "startup_india_pdf": "https://www.startupindia.gov.in/content/dam/invest-india/Tenders/Incubator-List.pdf",
        "startup_india_portal": "https://startupindia.gov.in/content/sih/en/startup-scheme/recognized-incubators.html",
        "dst_nidhi": "https://dst.gov.in/nidhi-scheme",
        "meity_tide": "https://meity.gov.in/content/technology-incubation-and-development-entrepreneurs",
        "meity_startup_hub": "https://www.meitystartuphub.in",
        "dbt_bionest": "https://www.birac.nic.in/desc_biotechnology_incubation.php",
        "aim_atl": "https://aim.gov.in/atal-tinkering-labs.php",
        "aim_aic": "https://aim.gov.in/atal-incubation-centres.php",
        "aim_acic": "https://aim.gov.in/atal-community-innovation-centre.php",
        "isba_members": "https://www.isba.in/members.php",
    }
    
    # Major institution patterns
    INSTITUTION_PATTERNS = {
        "iits": [
            "https://www.iitb.ac.in/sine",  # Society for Innovation and Entrepreneurship
            "https://www.iitd.ac.in/incubation",
            "https://www.iitm.ac.in/research/research-centres/rural-technology-and-business-incubator",
            "https://www.iitkgp.ac.in/research/technology-incubation",
            # Add more IITs
        ],
        "iims": [
            "https://www.iimahmedabad.ac.in/entrepreneurship",
            "https://www.iimb.ac.in/entrepreneurship",
            "https://www.iimcal.ac.in/centres/entrepreneurship-centre",
            # Add more IIMs
        ],
        "iisc": [
            "https://www.sociis.io/",  # Society for Innovation and Development
            "https://www.iisc.ac.in/centers/ced/",
        ],
        "state_startups": {
            "karnataka": "https://startup.karnataka.gov.in/",
            "maharashtra": "https://msins.in/",
            "telangana": "https://www.t-hub.co/incubators",
            "tamil_nadu": "https://startup.tn.gov.in/",
        }
    }
    
    def __init__(self, output_dir: str = "./datasets"):
        self.seeds: list[IncubatorSeed] = []
        self.entities: dict[str, IncubatorEntity] = {}  # key = normalized name
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._live_file = os.path.join(output_dir, "discovery_live.json")
        self._lock = asyncio.Lock()
        
    async def _save_seed_incremental(self, seed: IncubatorSeed):
        """Append one newly discovered incubator seed to the live JSON file immediately."""
        async with self._lock:
            existing = []
            if os.path.exists(self._live_file):
                try:
                    with open(self._live_file, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except Exception:
                    existing = []
            
            # Avoid direct duplicates in the live file
            if not any(e.get("name", "").lower() == seed.name.lower() for e in existing):
                existing.append({
                    "name": seed.name,
                    "source_url": seed.source_url,
                    "source_type": seed.source_type,
                    "confidence": seed.confidence,
                    "discovered_at": seed.discovered_at
                })
                
                with open(self._live_file, "w", encoding="utf-8") as f:
                    json.dump(existing, f, indent=2, ensure_ascii=False)
    async def discover_all(self, max_concurrent: int = 5) -> list[IncubatorEntity]:
        """
        Main entry: Discover all incubators from all sources.
        
        Returns list of unique IncubatorEntity objects.
        """
        print(f"[IncubatorDiscovery] Starting discovery across {len(self.GOV_SOURCES)} primary sources...")
        
        # Phase 1: Government lists (highest confidence)
        await self._crawl_government_sources()
        
        # Phase 2: Institution pages
        await self._crawl_institutional_sources(max_concurrent)
        
        # Phase 3: Commercial sources
        await self._crawl_commercial_sources()
        
        # Phase 4: Deduplicate
        entities = await self._deduplicate_and_merge()
        
        print(f"[IncubatorDiscovery] Total unique incubators found: {len(entities)}")
        return entities
    
    async def _search_searxng(self, query: str, limit: int = 15) -> list[str]:
        """Search SearXNG with pagination and return discovered URLs."""
        base_url = os.getenv("SEARXNG_BASE_URL", "http://localhost:8080")
        endpoint = f"{base_url.rstrip('/')}/search"
        DISCOVERY_PAGES = int(os.getenv("DISCOVERY_SEARXNG_PAGES", "3"))

        all_urls: list[str] = []
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                for page in range(1, DISCOVERY_PAGES + 1):
                    resp = await client.get(
                        endpoint,
                        params={"q": query, "format": "json", "pageno": page},
                    )
                    if resp.status_code != 200:
                        break
                    results = resp.json().get("results", [])
                    if not results:
                        break
                    for r in results:
                        u = r.get("url")
                        if u and u not in all_urls:
                            all_urls.append(u)
                    if len(all_urls) >= limit:
                        break
        except Exception as e:
            print(f"[IncubatorDiscovery] SearXNG search failed for '{query}': {e}")
        return all_urls[:limit]

    async def _crawl_and_extract(self, urls: list[str], source_type: str, semaphore: asyncio.Semaphore):
        """Crawl a list of URLs and extract entities via LLM."""
        from crawler.llm import replicate
        model = os.getenv("LLM_MODEL", "meta/meta-llama-3-70b-instruct")
        
        async def _process(url: str):
            async with semaphore:
                try:
                    async with AsyncWebCrawler() as crawler:
                        result = await crawler.arun(url=url)
                        text = result.markdown
                        if not text or len(text) < 200:
                            return
                        
                        prompt = f"""Extract all startup incubators, accelerators, or innovation hubs mentioned in this text.
Return ONLY a valid JSON array of objects with a 'name' key. No other text or markdown.
Example: [{{"name": "NSRCEL"}}]
Text: {text[:8000]}"""
                        
                        output = replicate.run(
                            model,
                            input={"prompt": prompt, "max_tokens": 1024, "temperature": 0.1},
                        )
                        raw = "".join(str(chunk) for chunk in output)
                        
                        cleaned = raw.strip()
                        if cleaned.startswith("```"):
                            cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0].strip()
                        try:
                            items = json.loads(cleaned)
                            if isinstance(items, dict) and "entities" in items:
                                items = items["entities"]
                            if isinstance(items, list):
                                extracted_count = 0
                                for item in items:
                                    if isinstance(item, dict) and item.get("name"):
                                        name = str(item.get("name")).strip()
                                        if len(name) > 3:
                                            seed = IncubatorSeed(
                                                name=name,
                                                source_url=url,
                                                source_type=source_type,
                                                confidence=0.8
                                            )
                                            self.seeds.append(seed)
                                            # Save immediately to prevent data loss
                                            await self._save_seed_incremental(seed)
                                            extracted_count += 1
                                print(f"[IncubatorDiscovery] Extracted {extracted_count} from {url}")
                        except Exception as e:
                            print(f"[IncubatorDiscovery] Failed to parse JSON from {url}")
                            
                except Exception as e:
                    print(f"[IncubatorDiscovery] Failed to crawl {url}: {e}")
                    
        await asyncio.gather(*[_process(u) for u in urls])

    async def _crawl_government_sources(self):
        """Deep-search government incubator directories across all schemes."""
        print("[IncubatorDiscovery] Phase 1: Government sources (Deep Search)...")

        queries = [
            # National directories
            "Startup India recognized incubators complete list",
            "Startup India incubators list statewise directory",
            "Startup India incubator map all states",
            # AIM / Niti Aayog
            "Atal Incubation Centres AIC complete list all India",
            "Atal Community Innovation Centre ACIC list",
            "Atal Tinkering Labs ATL host institutions list",
            # DST / NIDHI
            "NIDHI TBI Technology Business Incubator recognized list",
            "NIDHI SSS Seed Support System centres list",
            "NIDHI EIR Entrepreneur in Residence centres",
            "DST supported incubators complete directory",
            # MeitY
            "MeitY startup hub incubators India list",
            "TIDE Technology Incubation Development Entrepreneurs centres list",
            "MeitY Software Technology Parks incubation centres",
            # DBT / BIRAC
            "DBT BioNEST bioincubators India list",
            "BIRAC biotech incubators supported list India",
            # MSME
            "MSME incubation centres India directory",
            "MSME technology centres incubation list",
            # State government portals
            "NASSCOM 10000 Startups incubation warehouse list",
            "ISBA India incubation members directory complete list",
            "ISBA member incubators registered list",
        ]

        # State-specific government startup portal queries
        states = [
            "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
            "Chhattisgarh", "Goa", "Gujarat", "Haryana", "Himachal Pradesh",
            "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh",
            "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
            "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
            "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
            "West Bengal", "Delhi", "Chandigarh", "Puducherry",
            "Jammu and Kashmir", "Ladakh",
        ]
        for state in states:
            queries.append(f"{state} startup incubators list government")
            queries.append(f"{state} state government incubation centres startup policy")

        all_urls: set[str] = set()
        for q in queries:
            print(f"[IncubatorDiscovery] Searching: {q}")
            urls = await self._search_searxng(q, limit=15)
            all_urls.update(urls)

        print(f"[IncubatorDiscovery] Phase 1: {len(all_urls)} unique government source URLs found.")
        semaphore = asyncio.Semaphore(10)
        await self._crawl_and_extract(list(all_urls), "gov_list", semaphore)

    async def _crawl_institutional_sources(self, max_concurrent: int = 5):
        """Deep-search academic and institutional incubation centres."""
        print("[IncubatorDiscovery] Phase 2: Institutional sources (Deep Search)...")

        queries = [
            # IITs
            "IIT incubators complete list all IITs India",
            "IIT Bombay SINE incubator startups",
            "IIT Delhi Foundation for Innovation Technology Transfer",
            "IIT Madras incubation cell RTBI startups",
            "IIT Kanpur SIIC incubation startups",
            "IIT Kharagpur technology incubation",
            "IIT Hyderabad incubation centre",
            "IIT Roorkee TIDES incubation centre",
            "IIT BHU incubation centre startups",
            "IIT Guwahati incubation centre",
            "IIT Indore incubation startups",
            "IIT Mandi catalyst incubation",
            "IIT Patna incubation",
            "IIT Ropar incubation startups",
            "IIT Jodhpur incubation centre",
            "IIT Tirupati incubation centre",
            "IIT Palakkad incubation centre",
            "IIT Dharwad incubation centre",
            "IIT Bhilai incubation centre",
            "IIT Goa incubation centre",
            "IIT Jammu incubation centre",
            # IIMs
            "IIM incubators accelerators complete list all IIMs",
            "IIM Ahmedabad CIIE incubation",
            "IIM Bangalore NSRCEL incubation",
            "IIM Calcutta innovation park incubation",
            "IIM Lucknow incubation centre",
            "IIM Kozhikode incubation",
            "IIM Indore incubation centre",
            # NITs
            "NIT incubation centres complete list India",
            "NIT Trichy incubation centre",
            "NIT Warangal incubation centre",
            "NIT Surathkal incubation centre",
            "NIT Calicut incubation centre",
            # Central Universities
            "central university incubation centres India list",
            "university innovation incubation centres India complete list",
            "IISc Bangalore incubation SID",
            "IIIT incubation centres India list",
            "BITS Pilani incubation startups",
            # State Universities
            "state university incubation centres India",
            "engineering college incubation centres India list",
            "medical college incubation biotech India",
        ]

        all_urls: set[str] = set()

        # Also include the hardcoded institutional URLs
        for category, urls in self.INSTITUTION_PATTERNS.items():
            if isinstance(urls, list):
                all_urls.update(urls)
            elif isinstance(urls, dict):
                for u in urls.values():
                    if str(u).startswith("http"):
                        all_urls.add(str(u))

        for q in queries:
            print(f"[IncubatorDiscovery] Searching: {q}")
            urls = await self._search_searxng(q, limit=15)
            all_urls.update(urls)

        print(f"[IncubatorDiscovery] Phase 2: {len(all_urls)} unique institutional source URLs found.")
        semaphore = asyncio.Semaphore(max_concurrent)
        await self._crawl_and_extract(list(all_urls), "institution", semaphore)

    async def _crawl_commercial_sources(self):
        """Deep-search commercial lists, media rankings, and directories."""
        print("[IncubatorDiscovery] Phase 3: Commercial sources (Deep Search)...")

        # Tier 1 cities
        tier1_cities = [
            "Bangalore", "Hyderabad", "Delhi", "Mumbai", "Pune", "Chennai",
            "Kolkata", "Ahmedabad", "Noida", "Gurgaon",
        ]
        # Tier 2/3 cities with known startup ecosystems
        tier2_cities = [
            "Jaipur", "Kochi", "Coimbatore", "Chandigarh", "Lucknow",
            "Bhopal", "Indore", "Nagpur", "Visakhapatnam", "Bhubaneswar",
            "Thiruvananthapuram", "Mangalore", "Mysore", "Guwahati",
            "Patna", "Ranchi", "Raipur", "Dehradun", "Surat",
            "Vadodara", "Jodhpur", "Udaipur", "Agra", "Varanasi",
            "Kanpur", "Nashik", "Aurangabad", "Madurai", "Tiruchirappalli",
            "Salem", "Vijayawada", "Warangal", "Hubballi", "Belgaum",
            "Shillong", "Imphal", "Aizawl",
        ]

        queries = []
        for city in tier1_cities:
            queries.append(f"Top startup incubators accelerators in {city} India list")
            queries.append(f"Best incubation centres in {city} 2024 2025")
        for city in tier2_cities:
            queries.append(f"Startup incubators in {city} India")

        # Sector-specific
        sectors = [
            "fintech", "healthtech", "agritech", "edtech", "cleantech",
            "biotech", "deeptech", "AI", "SaaS", "social enterprise",
            "women entrepreneurs", "rural innovation", "defence",
        ]
        for sector in sectors:
            queries.append(f"{sector} startup incubators India list")

        # Media / aggregator lists
        media_queries = [
            "best private corporate startup incubators accelerators India list",
            "top 100 incubators India ranking",
            "YourStory best incubators India",
            "Inc42 top incubators India 2024",
            "top startup accelerators India complete list",
            "angel investor networks incubation India list",
            "social enterprise incubators India list",
            "women focused startup incubators India list",
            "rural incubation centres India government list",
            "biotech incubators India list",
            "cleantech incubators India list",
            "space tech defence incubators India list",
            "corporate innovation labs incubation India",
            "Google Launchpad Microsoft India incubation",
            "AWS startup loft India incubation",
            "T-Hub incubators accelerators Hyderabad",
            "NSRCEL IIM Bangalore incubation",
            "Villgro social innovation incubation India",
        ]
        queries.extend(media_queries)

        all_urls: set[str] = set()
        for q in queries:
            print(f"[IncubatorDiscovery] Searching: {q}")
            urls = await self._search_searxng(q, limit=15)
            all_urls.update(urls)

        print(f"[IncubatorDiscovery] Phase 3: {len(all_urls)} unique commercial source URLs found.")
        semaphore = asyncio.Semaphore(10)
        await self._crawl_and_extract(list(all_urls), "commercial", semaphore)
    
    async def _deduplicate_and_merge(self) -> list[IncubatorEntity]:
        """
        Deduplicate seeds and create initial entities.
        
        Key matching strategies:
        1. Exact name match (after normalization)
        2. Website domain match
        3. Location + name similarity
        """
        print(f"[IncubatorDiscovery] Phase 4: Deduplicating {len(self.seeds)} seeds...")
        
        entities = {}
        
        for seed in self.seeds:
            normalized_name = self._normalize_name(seed.name)
            
            if normalized_name in entities:
                # Merge sources
                entities[normalized_name].sources.append(seed.source_url)
            else:
                entity = IncubatorEntity(
                    name=seed.name,
                    sources=[seed.source_url],
                )
                entities[normalized_name] = entity
        
        return list(entities.values())
    
    def _normalize_name(self, name: str) -> str:
        """Normalize incubator name for deduplication."""
        # Remove common suffixes
        name = re.sub(r'\s+(incubator|centre|center|hub|foundation|society)\s*$', '', name, flags=re.IGNORECASE)
        # Remove IIT/IIM/institution prefix temporarily
        name = re.sub(r'^(IIT\s+\w+|IIM\s+\w+)\s*[-,]?\s*', '', name, flags=re.IGNORECASE)
        # Normalize whitespace and case
        name = ' '.join(name.lower().split())
        return name
    
    def _extract_from_html(self, html: str, base_url: str) -> list[dict]:
        """Extract incubator data from HTML content."""
        # Use BeautifulSoup or similar to extract structured data
        # Look for: name, contact, location, programs
        pass


# ─────────────────────────────────────────────────────────────────────────────
# ENRICHMENT PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

class IncubatorEnricher:
    """
    Iterative enrichment pipeline for incubator entities.
    
    For each entity, attempts to fill missing fields by:
    1. Crawling the official website
    2. Searching for specific missing data
    3. Using LLM extraction from web content
    """
    
    # Priority order for field discovery
    FIELD_PRIORITY = [
        "official_name", "website", "email", "phone",
        "city", "state", "type", "backing",
        "funding_type", "focus_sectors", "programs",
        "established_year", "alumni_count", "team_size"
    ]
    
    def __init__(self, output_dir: str = "./datasets"):
        from crawler.vector.chroma_kb import ChromaKnowledgeBase
        self.kb = ChromaKnowledgeBase(
            persist_dir="./chroma_db",
            collection_name="crawler_raw_sources",
            embedding_dimensions=384
        )
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._live_file = os.path.join(output_dir, "enrichment_live.json")
        self._progress_file = os.path.join(output_dir, "enrichment_progress.json")
        self._completed = 0
        self._total = 0
        self._lock = asyncio.Lock()

    def _save_progress(self, stage: str, detail: str = ""):
        """Write a lightweight progress file that the dashboard reads."""
        import time
        progress = {
            "stage": stage,
            "completed": self._completed,
            "total": self._total,
            "pct": round(self._completed / max(self._total, 1) * 100, 1),
            "detail": detail,
            "updated_at": time.time(),
        }
        with open(self._progress_file, "w", encoding="utf-8") as f:
            json.dump(progress, f)

    async def _save_entity_incremental(self, entity: IncubatorEntity):
        """Append one enriched entity to the live JSON file immediately."""
        async with self._lock:
            # Load existing
            existing = []
            if os.path.exists(self._live_file):
                try:
                    with open(self._live_file, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except Exception:
                    existing = []

            existing.append({
                "name": entity.name,
                "official_name": entity.official_name,
                "website": entity.website,
                "email": entity.email,
                "city": entity.city,
                "state": entity.state,
                "type": entity.type,
                "backing": entity.backing,
                "funding_type": entity.funding_type,
                "focus_sectors": entity.focus_sectors,
                "programs": entity.programs,
                "established_year": entity.established_year,
                "data_completeness": entity.data_completeness,
                "sources": entity.sources,
            })

            with open(self._live_file, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)

            self._completed += 1
            self._save_progress("synthesis", f"Enriched: {entity.name}")

    async def batch_enrich(self, entities: list[IncubatorEntity]):
        """Run Stage 1 and Stage 2 for a list of entities with incremental saves."""
        self._total = len(entities)
        self._completed = 0

        print(f"[Enricher] Starting Stage 1: Vector Harvesting for {len(entities)} entities...")
        self._save_progress("harvesting", "Starting vector harvesting...")
        semaphore = asyncio.Semaphore(15)

        # Stage 1: Dump vectors
        tasks_1 = [self._harvest_vectors_for_entity(e, semaphore) for e in entities]
        await asyncio.gather(*tasks_1)

        print(f"[Enricher] Stage 1 complete. Starting Stage 2: RAG Synthesis...")
        self._save_progress("synthesis", "Starting RAG synthesis...")

        # Stage 2: LLM JSON inference — one at a time so we can save incrementally
        for i, entity in enumerate(entities, 1):
            await self._synthesize_json_from_vectors(entity, semaphore)
            await self._save_entity_incremental(entity)
            if i % 10 == 0:
                print(f"[Enricher] Progress: {i}/{len(entities)} entities synthesized.")

        self._save_progress("done", "Enrichment complete.")

    async def _search_searxng_paginated(
        self,
        query: str,
        results_per_page: int = 10,
        max_pages: int = 10,
    ) -> list[str]:
        """Paginate through SearXNG and return all unique URLs found."""
        base_url = os.getenv("SEARXNG_BASE_URL", "http://localhost:8080")
        endpoint = f"{base_url.rstrip('/')}/search"
        all_urls: list[str] = []

        async with httpx.AsyncClient(timeout=15) as client:
            for page in range(1, max_pages + 1):
                try:
                    resp = await client.get(
                        endpoint,
                        params={
                            "q": query,
                            "format": "json",
                            "pageno": page,
                        },
                    )
                    if resp.status_code != 200:
                        break
                    results = resp.json().get("results", [])
                    if not results:
                        break  # no more pages
                    for r in results[:results_per_page]:
                        u = r.get("url")
                        if u:
                            all_urls.append(u)
                except Exception:
                    break

        return all_urls

    # Keep old signature for discovery-phase calls that pass limit=
    async def _search_searxng(self, query: str, limit: int = 10) -> list[str]:
        urls = await self._search_searxng_paginated(query, results_per_page=limit, max_pages=1)
        return urls[:limit]

    async def _harvest_vectors_for_entity(
        self, entity: IncubatorEntity, semaphore: asyncio.Semaphore
    ):
        """Deep-harvest the full digital footprint of an incubator into ChromaDB.

        Runs 8 targeted queries per entity, each paginated up to 10 SearXNG
        pages (≈100 URLs per query), fully deduplicates, then concurrently
        crawls every URL and upserts raw-text chunks into ChromaDB.
        No LLM extraction happens here — this is purely a vector dump.
        """
        async with semaphore:
            MAX_PAGES = int(os.getenv("ENRICH_SEARXNG_PAGES", "10"))
            RESULTS_PER_PAGE = 10
            CRAWL_CONCURRENCY = int(os.getenv("ENRICH_CRAWL_CONCURRENCY", "10"))

            # 8 intent-specific query templates to cover all available web data
            query_templates = [
                "{name} incubator official website India",
                "{name} startup programs funding equity India",
                "{name} portfolio companies alumni startups",
                "{name} incubator news 2023 2024 2025",
                "{name} annual report founders success stories",
                "{name} application process eligibility criteria",
                "{name} mentors team contact location address",
                "{name} incubator government NIDHI DST AIM recognition",
            ]

            # Collect and deduplicate all URLs across all queries
            seen_urls: set[str] = set()
            all_urls: list[str] = []

            for template in query_templates:
                q = template.format(name=entity.name)
                page_urls = await self._search_searxng_paginated(
                    q,
                    results_per_page=RESULTS_PER_PAGE,
                    max_pages=MAX_PAGES,
                )
                for u in page_urls:
                    if u not in seen_urls:
                        seen_urls.add(u)
                        all_urls.append(u)

            print(
                f"[Enricher] {entity.name} -> {len(all_urls)} unique URLs to crawl"
                f" ({len(query_templates)} queries × up to {MAX_PAGES} pages)"
            )

            if not all_urls:
                return

            async def _upsert_text(text: str, url: str) -> None:
                if not text or len(text) < 100:
                    return
                chunks = [text[i : i + 4000] for i in range(0, len(text), 4000)]
                records = [
                    {
                        "id": f"{hash(entity.name)}_{hash(url)}_{idx}",
                        "document": chunk,
                        "metadata": {
                            "entity": entity.name,
                            "url": url,
                            "session_id": "enrichment_run",
                        },
                    }
                    for idx, chunk in enumerate(chunks)
                ]
                await asyncio.to_thread(self.kb._upsert_records, records)

            async def _crawl_one(url: str, crawl_sem: asyncio.Semaphore) -> None:
                async with crawl_sem:
                    try:
                        # ── Try recovery script first (free, no LLM cost) ──
                        domain = url.split("//")[-1].split("/")[0]
                        safe_name = re.sub(r"[^a-z0-9]", "_", domain.lower()).strip("_")
                        script_path = os.path.join(
                            os.path.dirname(__file__),
                            "..",
                            "recovery_scripts",
                            f"{safe_name}.py",
                        )
                        if os.path.exists(script_path):
                            import importlib.util

                            spec = importlib.util.spec_from_file_location(safe_name, script_path)
                            mod = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(mod)
                            if hasattr(mod, "scrape"):
                                text = await mod.scrape(url)
                                if text and len(text) > 100:
                                    await _upsert_text(text, url)
                                    return  # success — skip crawl4ai

                        # ── Fall back to crawl4ai ──
                        async with AsyncWebCrawler() as crawler:
                            res = await crawler.arun(url=url)
                            await _upsert_text(res.markdown, url)
                    except Exception:
                        pass

            crawl_sem = asyncio.Semaphore(CRAWL_CONCURRENCY)
            await asyncio.gather(*[_crawl_one(u, crawl_sem) for u in all_urls])
            print(f"[Enricher] {entity.name} -> Harvest complete, vectors dumped to ChromaDB.")

    async def _synthesize_json_from_vectors(self, entity: IncubatorEntity, semaphore: asyncio.Semaphore):
        """Query ChromaDB and force Replicate to spit out structured JSON mapping."""
        from crawler.llm import replicate
        model = os.getenv("LLM_MODEL", "meta/meta-llama-3-70b-instruct")
        
        async with semaphore:
            query = f"{entity.name} incubator funding location programs equity sectors website"
            # Retrieve semantically related chunks
            results = await asyncio.to_thread(
                self.kb.query,
                query_text=query,
                top_k=4,
                session_id="enrichment_run"
            )
            
            context_blocks = "\n---\n".join([r["document"] for r in results])
            if not context_blocks.strip():
                return
            
            prompt = f"""You are an analyst enriching a database of Indian startup incubators.
Extract details for the incubator "{entity.name}" based ONLY on the provided context chunks.
Return ONLY valid JSON with no markdown brackets or prefixes. If data is unknown, use null.

Expected JSON schema:
{{
  "official_name": "string",
  "website": "string",
  "email": "string",
  "city": "string",
  "state": "string",
  "funding_type": "string (e.g., Grant, Equity, Debt)",
  "focus_sectors": ["string", "string"],
  "programs": ["string", "string"],
  "established_year": "string"
}}

Context:
{context_blocks}"""

            try:
                output = replicate.run(
                    model,
                    input={"prompt": prompt, "max_tokens": 1024, "temperature": 0.1},
                )
                raw = "".join(str(chunk) for chunk in output)
                
                cleaned = raw.strip()
                if cleaned.startswith("```"):
                    cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0].strip()
                
                try:
                    data = json.loads(cleaned)
                    entity.official_name = data.get("official_name")
                    entity.website = data.get("website")
                    entity.email = data.get("email")
                    entity.city = data.get("city")
                    entity.state = data.get("state")
                    entity.funding_type = data.get("funding_type", "")
                    
                    sectors = data.get("focus_sectors")
                    entity.focus_sectors = [str(x) for x in sectors] if isinstance(sectors, list) else []
                    
                    programs = data.get("programs")
                    entity.programs = [str(x) for x in programs] if isinstance(programs, list) else []
                    
                    yr = data.get("established_year")
                    entity.established_year = str(yr) if yr else ""
                    
                    filled = sum(1 for f in self.FIELD_PRIORITY if getattr(entity, f))
                    entity.data_completeness = filled / len(self.FIELD_PRIORITY)
                    print(f"[Enricher-Synthesize] JSON enriched for {entity.name}")
                except json.JSONDecodeError:
                    print(f"[Enricher-Synthesize] JSON parse failed for {entity.name}")
            except Exception as e:
                print(f"[Enricher-Synthesize] LLM failed for {entity.name}")


# ─────────────────────────────────────────────────────────────────────────────
# DATASET MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class IncubatorDatasetManager:
    """
    Manages the comprehensive incubator dataset.
    
    Features:
    - Incremental updates
    - Data versioning
    - Export to CSV/JSON/Excel
    - Completeness reporting
    """
    
    def __init__(self, output_dir: str = "./datasets"):
        self.output_dir = output_dir
        self.entities: list[IncubatorEntity] = []
        
    async def build_dataset(self, target_count: int = 1170) -> dict:
        """
        Build the complete dataset.
        
        Returns statistics about the dataset.
        """
        # 1. Discovery
        discovery = IndiaIncubatorDiscovery()
        entities = await discovery.discover_all()
        
        # 2. Enrichment
        enricher = IncubatorEnricher()
        await enricher.batch_enrich(entities)
        
        # 3. Save
        self.entities = entities
        await self._save_dataset()
        
        return {
            "total_entities": len(entities),
            "target_count": target_count,
            "coverage": len(entities) / target_count,
            "avg_completeness": sum(e.data_completeness for e in entities) / len(entities),
            "by_type": self._group_by_type(entities),
            "by_state": self._group_by_state(entities),
        }
    
    def _group_by_type(self, entities: list[IncubatorEntity]) -> dict:
        """Group entities by incubator type."""
        groups = {}
        for e in entities:
            groups[e.type] = groups.get(e.type, 0) + 1
        return groups
    
    def _group_by_state(self, entities: list[IncubatorEntity]) -> dict:
        """Group entities by state."""
        groups = {}
        for e in entities:
            if e.state:
                groups[e.state] = groups.get(e.state, 0) + 1
        return groups
    
    async def _save_dataset(self, format: str = "csv"):
        """Save dataset to disk."""
        import csv
        import os
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Save as CSV
        output_path = f"{self.output_dir}/indian_incubators.csv"
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow([
                'name', 'official_name', 'website', 'email', 'phone',
                'city', 'state', 'type', 'backing', 'funding_type',
                'focus_sectors', 'programs', 'established_year',
                'alumni_count', 'data_completeness', 'sources'
            ])
            # Write data
            for e in self.entities:
                writer.writerow([
                    e.name, e.official_name, e.website, e.email, e.phone,
                    e.city, e.state, e.type, e.backing, e.funding_type,
                    '|'.join(e.focus_sectors), '|'.join(e.programs), e.established_year,
                    e.alumni_count, e.data_completeness, '|'.join(e.sources)
                ])
        
        print(f"[DatasetManager] Saved {len(self.entities)} entities to {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# USAGE EXAMPLE
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    """Build the complete Indian incubators dataset."""
    manager = IncubatorDatasetManager()
    
    stats = await manager.build_dataset(target_count=1170)
    
    print("\n" + "="*60)
    print("INDIAN INCUBATORS DATASET - BUILD COMPLETE")
    print("="*60)
    print(f"Total entities discovered: {stats['total_entities']}")
    print(f"Target: {stats['target_count']}")
    print(f"Coverage: {stats['coverage']:.1%}")
    print(f"Average data completeness: {stats['avg_completeness']:.1%}")
    print(f"\nBy type:")
    for t, count in stats['by_type'].items():
        print(f"  {t}: {count}")
    print(f"\nTop states:")
    sorted_states = sorted(stats['by_state'].items(), key=lambda x: x[1], reverse=True)[:10]
    for state, count in sorted_states:
        print(f"  {state}: {count}")


if __name__ == "__main__":
    asyncio.run(main())
