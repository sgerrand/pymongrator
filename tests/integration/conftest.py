"""Shared fixtures for integration tests.

Tests in this package require Docker. They are skipped automatically when
Docker or MongoDB is unavailable, so `uv run pytest` always passes in CI
without Docker. Run integration tests explicitly:

    uv run pytest tests/integration/
"""

import uuid
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from pymongo import AsyncMongoClient, MongoClient

from mongrator.config import MigratorConfig
from mongrator.runner import AsyncRunner, SyncRunner

# ---------------------------------------------------------------------------
# Container (session-scoped — started once for the whole test run)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mongo_url() -> Generator[str]:
    try:
        from testcontainers.mongodb import MongoDbContainer

        container = MongoDbContainer()
        container.start()
    except Exception as e:
        pytest.skip(f"Docker/MongoDB unavailable: {e}")

    yield container.get_connection_url()
    container.stop()


# ---------------------------------------------------------------------------
# Per-test config and runners
# ---------------------------------------------------------------------------


@pytest.fixture
def db_name() -> str:
    """Unique database name per test — prevents state bleed between tests."""
    return f"test_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def sync_runner(mongo_url: str, db_name: str, tmp_path: Path) -> Generator[SyncRunner]:
    client = MongoClient(mongo_url)
    config = MigratorConfig(uri=mongo_url, database=db_name, migrations_dir=tmp_path)
    runner = SyncRunner(client, config)
    yield runner
    client.drop_database(db_name)
    client.close()


@pytest.fixture
async def async_runner(mongo_url: str, db_name: str, tmp_path: Path) -> AsyncGenerator[AsyncRunner]:
    async_client = AsyncMongoClient(mongo_url)
    config = MigratorConfig(uri=mongo_url, database=db_name, migrations_dir=tmp_path)
    runner = AsyncRunner(async_client, config)
    yield runner
    cleanup = MongoClient(mongo_url)
    cleanup.drop_database(db_name)
    cleanup.close()
    await async_client.close()


# ---------------------------------------------------------------------------
# Migration file helper
# ---------------------------------------------------------------------------


def write_migration(directory: Path, name: str, content: str) -> Path:
    path = directory / name
    path.write_text(content)
    return path
