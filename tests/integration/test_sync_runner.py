"""Integration tests for SyncRunner against a real MongoDB instance."""

from collections.abc import Generator
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from mongrator.exceptions import NoDownMethodError
from mongrator.runner import SyncRunner

from .conftest import write_migration

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runner(sync_runner: SyncRunner) -> SyncRunner:
    return sync_runner


@pytest.fixture
def migrations_dir(runner: SyncRunner) -> Path:
    return runner._config.migrations_dir


@pytest.fixture
def db(runner: SyncRunner, mongo_url: str) -> Generator[Database]:
    client = MongoClient(mongo_url)
    yield client[runner._config.database]
    client.close()


# ---------------------------------------------------------------------------
# up()
# ---------------------------------------------------------------------------


def test_up_empty_dir(runner: SyncRunner) -> None:
    assert runner.up() == []


def test_up_applies_migration(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    applied = runner.up()
    assert applied == ["001_a"]


def test_up_returns_applied_ids_in_order(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\n")
    applied = runner.up()
    assert applied == ["001_a", "002_b"]


def test_up_idempotent(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    runner.up()
    second = runner.up()
    assert second == []


def test_up_with_target(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\n")
    write_migration(migrations_dir, "003_c.py", "def up(db): pass\n")
    applied = runner.up(target="002_b")
    assert applied == ["001_a", "002_b"]
    # Third migration still pending
    remaining = runner.up()
    assert remaining == ["003_c"]


def test_up_records_in_tracking_collection(runner: SyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    runner.up()
    record = db[runner._config.collection].find_one({"_id": "001_a"})  # type: ignore[index]
    assert record is not None
    assert record["direction"] == "up"
    assert record["checksum"]
    assert record["duration_ms"] >= 0


def test_up_plain_pymongo_migration_executes(runner: SyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_insert.py",
        'def up(db):\n    db["markers"].insert_one({"_id": "up_ran"})\n',
    )
    runner.up()
    assert db["markers"].find_one({"_id": "up_ran"}) is not None  # type: ignore[index]


def test_up_ops_migration_creates_index(runner: SyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_index.py",
        "from mongrator import ops\ndef up(db):\n    return [ops.create_index('col', {'email': 1})]\n",
    )
    runner.up()
    index_names = [idx["name"] for idx in db["col"].list_indexes()]  # type: ignore[index]
    assert "email_1" in index_names


# ---------------------------------------------------------------------------
# down()
# ---------------------------------------------------------------------------


def test_down_rolls_back_last(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\ndef down(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\ndef down(db): pass\n")
    runner.up()
    rolled_back = runner.down()
    assert rolled_back == ["002_b"]
    # 001_a still applied
    statuses = {s.id: s for s in runner.status()}
    assert statuses["001_a"].applied
    assert not statuses["002_b"].applied


def test_down_multiple_steps(runner: SyncRunner, migrations_dir: Path) -> None:
    for name in ["001_a.py", "002_b.py", "003_c.py"]:
        write_migration(migrations_dir, name, "def up(db): pass\ndef down(db): pass\n")
    runner.up()
    rolled_back = runner.down(steps=2)
    assert rolled_back == ["003_c", "002_b"]


def test_down_records_direction_in_tracking(runner: SyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\ndef down(db): pass\n")
    runner.up()
    runner.down()
    record = db[runner._config.collection].find_one({"_id": "001_a"})  # type: ignore[index]
    assert record is not None
    assert record["direction"] == "down"


def test_down_plain_pymongo_executes_down(runner: SyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_insert.py",
        'def up(db):\n    db["markers"].insert_one({"_id": "present"})\n'
        'def down(db):\n    db["markers"].delete_one({"_id": "present"})\n',
    )
    runner.up()
    assert db["markers"].find_one({"_id": "present"}) is not None  # type: ignore[index]
    runner.down()
    assert db["markers"].find_one({"_id": "present"}) is None  # type: ignore[index]


def test_down_ops_auto_rollback_drops_index(runner: SyncRunner, migrations_dir: Path, db: Database) -> None:
    write_migration(
        migrations_dir,
        "001_index.py",
        "from mongrator import ops\ndef up(db):\n    return [ops.create_index('col', {'email': 1})]\n",
    )
    runner.up()
    runner.down()
    index_names = [idx["name"] for idx in db["col"].list_indexes()]  # type: ignore[index]
    assert "email_1" not in index_names


def test_down_no_down_method_raises(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    runner.up()
    with pytest.raises(NoDownMethodError):
        runner.down()


# ---------------------------------------------------------------------------
# status()
# ---------------------------------------------------------------------------


def test_status_empty(runner: SyncRunner) -> None:
    assert runner.status() == []


def test_status_shows_pending(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    statuses = runner.status()
    assert len(statuses) == 1
    assert not statuses[0].applied


def test_status_shows_applied(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    runner.up()
    statuses = runner.status()
    assert statuses[0].applied
    assert statuses[0].applied_at is not None


def test_status_reflects_partial_apply(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    write_migration(migrations_dir, "002_b.py", "def up(db): pass\n")
    runner.up(target="001_a")
    statuses = {s.id: s for s in runner.status()}
    assert statuses["001_a"].applied
    assert not statuses["002_b"].applied


# ---------------------------------------------------------------------------
# validate()
# ---------------------------------------------------------------------------


def test_validate_clean(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    runner.up()
    assert runner.validate() == []


def test_validate_detects_modified_file(runner: SyncRunner, migrations_dir: Path) -> None:
    path = write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    runner.up()
    path.write_text("def up(db): pass  # modified\n")
    errors = runner.validate()
    assert len(errors) == 1
    assert errors[0].migration_id == "001_a"


def test_validate_skips_unapplied(runner: SyncRunner, migrations_dir: Path) -> None:
    write_migration(migrations_dir, "001_a.py", "def up(db): pass\n")
    # No up() call — migration is pending
    assert runner.validate() == []
