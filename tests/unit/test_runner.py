"""Unit tests for mongrator.runner — SyncRunner and AsyncRunner with mocked I/O."""

import types
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mongrator.config import MigratorConfig
from mongrator.exceptions import MigrationLockError, NoDownMethodError
from mongrator.migration import MigrationFile
from mongrator.ops import create_index, drop_index
from mongrator.runner import AsyncRunner, SyncRunner
from mongrator.state import make_record

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(tmp_path: Path) -> MigratorConfig:
    return MigratorConfig(uri="mongodb://localhost:27017", database="testdb", migrations_dir=tmp_path)


def _migration(
    migration_id: str,
    *,
    has_down: bool = False,
    ops_based: bool = False,
    ops_fn: Any = None,
) -> MigrationFile:
    mod = types.ModuleType(f"_test_{migration_id}")
    if ops_fn is not None:
        setattr(mod, "up", ops_fn)
    elif ops_based:
        index_op = create_index("col", {"field": 1})
        setattr(mod, "up", lambda db: [index_op])
    else:
        setattr(mod, "up", lambda db: None)
    if has_down:
        setattr(mod, "down", lambda db: None)
    return MigrationFile(id=migration_id, path=Path(f"{migration_id}.py"), checksum="abc", module=mod)


def _sync_runner(tmp_path: Path) -> tuple[SyncRunner, MagicMock, MagicMock]:
    """Return (runner, mock_db, mock_store)."""
    mock_db = MagicMock()
    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    mock_store = MagicMock()
    config = _config(tmp_path)
    runner = SyncRunner(mock_client, config)
    runner._store = mock_store
    runner._lock = MagicMock()
    return runner, mock_db, mock_store


def _async_runner(tmp_path: Path) -> tuple[AsyncRunner, MagicMock, AsyncMock]:
    """Return (runner, mock_db, mock_store)."""
    mock_db = MagicMock()
    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    mock_store = AsyncMock()
    config = _config(tmp_path)
    runner = AsyncRunner(mock_client, config)
    runner._store = mock_store
    runner._lock = AsyncMock()
    return runner, mock_db, mock_store


# ---------------------------------------------------------------------------
# SyncRunner.up
# ---------------------------------------------------------------------------


def test_sync_up_applies_pending(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = runner.up()

    assert applied == ["001_a", "002_b"]
    assert store.record_applied.call_count == 2


def test_sync_up_skips_applied(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = runner.up()

    assert applied == ["002_b"]


def test_sync_up_nothing_to_apply(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a")]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = runner.up()

    assert applied == []
    store.record_applied.assert_not_called()


def test_sync_up_with_target(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b"), _migration("003_c")]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = runner.up(target="002_b")

    assert applied == ["001_a", "002_b"]


def test_sync_up_warns_irreversible_ops(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    runner, db, store = _sync_runner(tmp_path)

    def up_fn(db: Any) -> list:
        return [drop_index("col", "email_1")]

    migrations = [_migration("001_a", ops_fn=up_fn)]
    store.get_applied.return_value = set()
    db["col"].index_information.return_value = {}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        runner.up()

    captured = capsys.readouterr()
    assert "warning:" in captured.err
    assert "non-auto-reversible" in captured.err
    assert "drop_index" in captured.err


def test_sync_up_no_warning_when_down_defined(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    runner, db, store = _sync_runner(tmp_path)

    def up_fn(db: Any) -> list:
        return [drop_index("col", "email_1")]

    migrations = [_migration("001_a", ops_fn=up_fn, has_down=True)]
    store.get_applied.return_value = set()
    db["col"].index_information.return_value = {}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        runner.up()

    captured = capsys.readouterr()
    assert "warning:" not in captured.err


def test_sync_up_no_warning_for_reversible_ops(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    runner, db, store = _sync_runner(tmp_path)

    def up_fn(db: Any) -> list:
        return [drop_index("col", "email_1", keys=[("email", 1)])]

    migrations = [_migration("001_a", ops_fn=up_fn)]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        runner.up()

    captured = capsys.readouterr()
    assert "warning:" not in captured.err


def test_sync_up_records_direction_up(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a")]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        runner.up()

    record = store.record_applied.call_args[0][0]
    assert record["direction"] == "up"
    assert record["_id"] == "001_a"


# ---------------------------------------------------------------------------
# SyncRunner.down
# ---------------------------------------------------------------------------


def test_sync_down_rolls_back_last(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a", has_down=True), _migration("002_b", has_down=True)]
    store.get_applied.return_value = {"001_a", "002_b"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        rolled_back = runner.down(steps=1)

    assert rolled_back == ["002_b"]


def test_sync_down_multiple_steps(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [
        _migration("001_a", has_down=True),
        _migration("002_b", has_down=True),
        _migration("003_c", has_down=True),
    ]
    store.get_applied.return_value = {"001_a", "002_b", "003_c"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        rolled_back = runner.down(steps=2)

    assert rolled_back == ["003_c", "002_b"]


def test_sync_down_no_down_method_raises(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a", has_down=False)]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        with pytest.raises(NoDownMethodError):
            runner.down()


def test_sync_down_ops_based_auto_rollback(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a", ops_based=True)]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        rolled_back = runner.down()

    assert rolled_back == ["001_a"]


def test_sync_down_drop_index_ops_auto_rollback(tmp_path: Path) -> None:
    """SyncRunner.down() auto-reverts drop_index on fresh Operation instances."""
    runner, db, store = _sync_runner(tmp_path)

    def up_fn(db: Any) -> list:
        return [drop_index("col", "email_1", keys=[("email", 1)], unique=True)]

    migrations = [_migration("001_a", ops_fn=up_fn)]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        rolled_back = runner.down()

    assert rolled_back == ["001_a"]
    db["col"].create_index.assert_called_once_with(
        [("email", 1)],
        name="email_1",
        unique=True,
    )


def test_sync_down_records_direction_down(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a", has_down=True)]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        runner.down()

    record = store.record_applied.call_args[0][0]
    assert record["direction"] == "down"


# ---------------------------------------------------------------------------
# SyncRunner.status
# ---------------------------------------------------------------------------


def test_sync_status_pending(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a")]
    store.get_applied.return_value = set()
    store.get_record.return_value = None

    with patch("mongrator.runner.loader.load", return_value=migrations):
        statuses = runner.status()

    assert len(statuses) == 1
    assert statuses[0].id == "001_a"
    assert not statuses[0].applied


def test_sync_status_applied(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a")]
    store.get_applied.return_value = {"001_a"}
    store.get_record.return_value = make_record("001_a", "abc", "up", 5)

    with patch("mongrator.runner.loader.load", return_value=migrations):
        statuses = runner.status()

    assert statuses[0].applied


def test_sync_status_checksum_mismatch(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    m = _migration("001_a")
    store.get_applied.return_value = {"001_a"}
    store.get_record.return_value = make_record("001_a", "different_checksum", "up", 5)

    with patch("mongrator.runner.loader.load", return_value=[m]):
        statuses = runner.status()

    assert not statuses[0].checksum_ok


# ---------------------------------------------------------------------------
# SyncRunner.validate
# ---------------------------------------------------------------------------


def test_sync_validate_clean(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    m = _migration("001_a")
    store.get_applied.return_value = {"001_a"}
    store.get_record.return_value = make_record("001_a", m.checksum, "up", 5)

    with patch("mongrator.runner.loader.load", return_value=[m]):
        errors = runner.validate()

    assert errors == []


def test_sync_validate_detects_mismatch(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    m = _migration("001_a")
    store.get_applied.return_value = {"001_a"}
    store.get_record.return_value = make_record("001_a", "stale_checksum", "up", 5)

    with patch("mongrator.runner.loader.load", return_value=[m]):
        errors = runner.validate()

    assert len(errors) == 1
    assert errors[0].migration_id == "001_a"


def test_sync_validate_skips_unapplied(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    m = _migration("001_a")
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=[m]):
        errors = runner.validate()

    assert errors == []


# ---------------------------------------------------------------------------
# AsyncRunner
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_up_applies_pending(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = await runner.up()

    assert applied == ["001_a", "002_b"]


@pytest.mark.asyncio
async def test_async_down_rolls_back(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    migrations = [_migration("001_a", has_down=True), _migration("002_b", has_down=True)]
    store.get_applied.return_value = {"001_a", "002_b"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        rolled_back = await runner.down(steps=1)

    assert rolled_back == ["002_b"]


@pytest.mark.asyncio
async def test_async_down_drop_index_ops_auto_rollback(tmp_path: Path) -> None:
    """AsyncRunner.down() auto-reverts drop_index on fresh Operation instances."""
    runner, db, store = _async_runner(tmp_path)
    # AsyncRunner._db is set from a real MongoClient in __init__; override with mock.
    runner._db = db

    def up_fn(db: Any) -> list:
        return [drop_index("col", "email_1", keys=[("email", 1)], unique=True)]

    migrations = [_migration("001_a", ops_fn=up_fn)]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        rolled_back = await runner.down()

    assert rolled_back == ["001_a"]
    db["col"].create_index.assert_called_once_with(
        [("email", 1)],
        name="email_1",
        unique=True,
    )


@pytest.mark.asyncio
async def test_async_validate_clean(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    m = _migration("001_a")
    store.get_applied.return_value = {"001_a"}
    store.get_record.return_value = make_record("001_a", m.checksum, "up", 5)

    with patch("mongrator.runner.loader.load", return_value=[m]):
        errors = await runner.validate()

    assert errors == []


@pytest.mark.asyncio
async def test_async_validate_detects_mismatch(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    m = _migration("001_a")
    store.get_applied.return_value = {"001_a"}
    store.get_record.return_value = make_record("001_a", "stale", "up", 5)

    with patch("mongrator.runner.loader.load", return_value=[m]):
        errors = await runner.validate()

    assert len(errors) == 1


# ---------------------------------------------------------------------------
# SyncRunner.plan_up / plan_down
# ---------------------------------------------------------------------------


def test_sync_plan_up_returns_pending(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        plan = runner.plan_up()

    assert [m.id for m in plan.to_apply] == ["002_b"]
    assert [m.id for m in plan.to_skip] == ["001_a"]
    store.record_applied.assert_not_called()


def test_sync_plan_up_with_target(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b"), _migration("003_c")]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        plan = runner.plan_up(target="002_b")

    assert [m.id for m in plan.to_apply] == ["001_a", "002_b"]


def test_sync_plan_down_returns_rollback(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = {"001_a", "002_b"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        plan = runner.plan_down(steps=1)

    assert [m.id for m in plan.to_apply] == ["002_b"]
    store.record_applied.assert_not_called()


# ---------------------------------------------------------------------------
# AsyncRunner.plan_up / plan_down
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_plan_up_returns_pending(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        plan = await runner.plan_up()

    assert [m.id for m in plan.to_apply] == ["002_b"]
    assert [m.id for m in plan.to_skip] == ["001_a"]
    store.record_applied.assert_not_called()


# ---------------------------------------------------------------------------
# SyncRunner lock usage
# ---------------------------------------------------------------------------


def test_sync_up_acquires_and_releases_lock(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=[_migration("001_a")]):
        runner.up()

    runner._lock.__enter__.assert_called_once()  # ty: ignore[unresolved-attribute]
    runner._lock.__exit__.assert_called_once()  # ty: ignore[unresolved-attribute]


def test_sync_down_acquires_and_releases_lock(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=[_migration("001_a", has_down=True)]):
        runner.down()

    runner._lock.__enter__.assert_called_once()  # ty: ignore[unresolved-attribute]
    runner._lock.__exit__.assert_called_once()  # ty: ignore[unresolved-attribute]


def test_sync_up_propagates_lock_error(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    runner._lock.__enter__.side_effect = MigrationLockError()  # ty: ignore[unresolved-attribute]

    with pytest.raises(MigrationLockError):
        runner.up()

    store.record_applied.assert_not_called()


def test_sync_down_propagates_lock_error(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    runner._lock.__enter__.side_effect = MigrationLockError()  # ty: ignore[unresolved-attribute]

    with pytest.raises(MigrationLockError):
        runner.down()

    store.record_applied.assert_not_called()


# ---------------------------------------------------------------------------
# AsyncRunner lock usage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_up_acquires_and_releases_lock(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=[_migration("001_a")]):
        await runner.up()

    runner._lock.__aenter__.assert_called_once()  # ty: ignore[unresolved-attribute]
    runner._lock.__aexit__.assert_called_once()  # ty: ignore[unresolved-attribute]


@pytest.mark.asyncio
async def test_async_down_acquires_and_releases_lock(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=[_migration("001_a", has_down=True)]):
        await runner.down()

    runner._lock.__aenter__.assert_called_once()  # ty: ignore[unresolved-attribute]
    runner._lock.__aexit__.assert_called_once()  # ty: ignore[unresolved-attribute]


@pytest.mark.asyncio
async def test_async_up_propagates_lock_error(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    runner._lock.__aenter__.side_effect = MigrationLockError()  # ty: ignore[unresolved-attribute]

    with pytest.raises(MigrationLockError):
        await runner.up()

    store.record_applied.assert_not_called()


@pytest.mark.asyncio
async def test_async_down_propagates_lock_error(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    runner._lock.__aenter__.side_effect = MigrationLockError()  # ty: ignore[unresolved-attribute]

    with pytest.raises(MigrationLockError):
        await runner.down()

    store.record_applied.assert_not_called()


@pytest.mark.asyncio
async def test_async_plan_down_returns_rollback(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = {"001_a", "002_b"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        plan = await runner.plan_down(steps=1)

    assert [m.id for m in plan.to_apply] == ["002_b"]
    store.record_applied.assert_not_called()
