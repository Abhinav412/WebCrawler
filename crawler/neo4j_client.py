"""Async Neo4j driver wrapper for the crawler pipeline."""

from __future__ import annotations
import os
from typing import Any
from neo4j import AsyncGraphDatabase, AsyncDriver
from dotenv import load_dotenv

load_dotenv()

_driver: AsyncDriver | None = None


def get_driver() -> AsyncDriver:
    global _driver
    if _driver is None:
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        _driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
    return _driver


async def run_query(cypher: str, parameters: dict[str, Any] | None = None, database: str = "neo4j") -> list[dict[str, Any]]:
    driver = get_driver()
    async with driver.session(database=database) as session:
        result = await session.run(cypher, parameters or {})
        return [record.data() async for record in result]


async def run_write(cypher: str, parameters: dict[str, Any] | None = None, database: str = "neo4j") -> None:
    driver = get_driver()
    async with driver.session(database=database) as session:
        await session.execute_write(lambda tx: tx.run(cypher, parameters or {}))


async def close() -> None:
    global _driver
    if _driver is not None:
        await _driver.close()
        _driver = None
