"""Integration tests for AsyncRunner against a real MongoDB instance."""

from collections.abc import Generator
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from mongrator.exceptions import NoDownMethodError
from mongrator.runner import AsyncRunner

from .conftest import write_migration

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runner(async_runner: AsyncRunner) -> AsyncRunner:
    return async_runner


@pytest.fixture
def migrations_dir(runner: AsyncRunner) -> Path:
    return runner._config.migrations_dir


@pytest.fixture
def db(runner: AsyncRunner, mongo_url: str) -> Generator[Database]:
    client = MongoClient(mongo_url)
    yield client[runner._config.database]
    client.close()


# ---------------------------------------------------------------------------
# up()
# ---------------------------------------------------------------------------


async def test_up_empty_dir(runner: AsyncRunner) -> None:
    assert await runner.up() == []


async def test_up_applies_migration(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    applied = await runner.up()
    assert applied == ["001_a"]


async def test_up_returns_applied_ids_in_order(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\n")
    applied = await runner.up()
    assert applied == ["001_a", "002_b"]


async def test_up_idempotent(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    await runner.up()
    second = await runner.up()
    assert second == []


async def test_up_with_target(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\n")
    write_migration(migrations_dir, "003_c.py", "def up(db): pass\n")
    applied = await runner.up(target="002_b")
    assert applied == ["001_a", "002_b"]
    # Third migration still pending
    remaining = await runner.up()
    assert remaining == ["003_c"]


async def test_up_records_in_tracking_collection(runner: AsyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    await runner.up()
    record = db[runner._config.collection].find_one({"_id": "001_a"})  # type: ignore[index]
    assert record is not None
    assert record["direction"] == "up"
    assert record["checksum"]
    assert record["duration_ms"] >= 0


async def test_up_plain_pymongo_migration_executes(runner: AsyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_insert.py",
        'def up(db):\n    db["markers"].insert_one({"_id": "up_ran"})\n',
    )
    await runner.up()
    assert db["markers"].find_one({"_id": "up_ran"}) is not None  # type: ignore[index]


async def test_up_ops_migration_creates_index(runner: AsyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_index.py",
        "from mongrator import ops\ndef up(db):\n    return [ops.create_index('col', {'email': 1})]\n",
    )
    await runner.up()
    index_names = [idx["name"] for idx in db["col"].list_indexes()]  # type: ignore[index]
    assert "email_1" in index_names


# ---------------------------------------------------------------------------
# down()
# ---------------------------------------------------------------------------


async def test_down_rolls_back_last(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\ndef down(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\ndef down(db): pass\n")
    await runner.up()
    rolled_back = await runner.down()
    assert rolled_back == ["002_b"]
    # 001_a still applied
    statuses = {s.id: s for s in await runner.status()}
    assert statuses["001_a"].applied
    assert not statuses["002_b"].applied


async def test_down_multiple_steps(runner: AsyncRunner, migrations_dir: Path) -> None:
    for name in ["001_a.py", "002_b.py", "003_c.py"]:
        write_migration(migrations_dir, name, "def up(db): pass\ndef down(db): pass\n")
    await runner.up()
    rolled_back = await runner.down(steps=2)
    assert rolled_back == ["003_c", "002_b"]


async def test_down_records_direction_in_tracking(runner: AsyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\ndef down(db): pass\n")
    await runner.up()
    await runner.down()
    record = db[runner._config.collection].find_one({"_id": "001_a"})  # type: ignore[index]
    assert record is not None
    assert record["direction"] == "down"


async def test_down_plain_pymongo_executes_down(runner: AsyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_insert.py",
        'def up(db):\n    db["markers"].insert_one({"_id": "present"})\n'
        'def down(db):\n    db["markers"].delete_one({"_id": "present"})\n',
    )
    await runner.up()
    assert db["markers"].find_one({"_id": "present"}) is not None  # type: ignore[index]
    await runner.down()
    assert db["markers"].find_one({"_id": "present"}) is None  # type: ignore[index]


async def test_down_ops_auto_rollback_drops_index(runner: AsyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_index.py",
        "from mongrator import ops\ndef up(db):\n    return [ops.create_index('col', {'email': 1})]\n",
    )
    await runner.up()
    await runner.down()
    index_names = [idx["name"] for idx in db["col"].list_indexes()]  # type: ignore[index]
    assert "email_1" not in index_names


async def test_down_no_down_method_raises(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    await runner.up()
    with pytest.raises(NoDownMethodError):
        await runner.down()


# ---------------------------------------------------------------------------
# status()
# ---------------------------------------------------------------------------


async def test_status_empty(runner: AsyncRunner) -> None:
    assert await runner.status() == []


async def test_status_shows_pending(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    statuses = await runner.status()
    assert len(statuses) == 1
    assert not statuses[0].applied


async def test_status_shows_applied(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    await runner.up()
    statuses = await runner.status()
    assert statuses[0].applied
    assert statuses[0].applied_at is not None


async def test_status_reflects_partial_apply(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\n")
    await runner.up(target="001_a")
    statuses = {s.id: s for s in await runner.status()}
    assert statuses["001_a"].applied
    assert not statuses["002_b"].applied


# ---------------------------------------------------------------------------
# validate()
# ---------------------------------------------------------------------------


async def test_validate_clean(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    await runner.up()
    assert await runner.validate() == []


async def test_validate_detects_modified_file(runner: AsyncRunner, migrations_dir: Path) -> None:
    path = write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    await runner.up()
    path.write_text("def up(db): pass  # modified\n")
    errors = await runner.validate()
    assert len(errors) == 1
    assert errors[0].migration_id == "001_a"


async def test_validate_skips_unapplied(runner: AsyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    # No up() call — migration is pending
    assert await runner.validate() == []
