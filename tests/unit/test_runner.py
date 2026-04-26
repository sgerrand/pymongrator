"""Unit tests for mongrator.runner — SyncRunner and AsyncRunner with mocked I/O."""

import types
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mongrator.config import MigratorConfig
from mongrator.exceptions import MigrationLockError, NoDownMethodError, TransactionNotSupportedError
from mongrator.migration import MigrationFile
from mongrator.ops import create_index, drop_index
from mongrator.runner import AsyncRunner, SyncRunner, _check_transaction_support
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
    """Return (runner, mock_sync_db, mock_store).

    The runner's sync _db is set to mock_sync_db.  The async _async_db is a
    separate MagicMock accessible via ``runner._async_db``.
    """
    mock_sync_db = MagicMock()
    mock_async_db = MagicMock()
    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_async_db)
    mock_store = AsyncMock()
    config = _config(tmp_path)
    runner = AsyncRunner(mock_client, config)
    runner._db = mock_sync_db
    runner._store = mock_store
    runner._lock = AsyncMock()
    return runner, mock_sync_db, mock_store


def _async_migration(
    migration_id: str,
    *,
    has_async_down: bool = False,
) -> MigrationFile:
    """Create a migration with async up() and optionally async down()."""
    mod = types.ModuleType(f"_test_async_{migration_id}")

    async def async_up(db: Any) -> None:
        # Record calls on the db mock for assertions.
        db.async_up_called(migration_id)

    setattr(mod, "up", async_up)

    if has_async_down:

        async def async_down(db: Any) -> None:
            db.async_down_called(migration_id)

        setattr(mod, "down", async_down)

    return MigrationFile(id=migration_id, path=Path(f"{migration_id}.py"), checksum="abc", module=mod)


# ---------------------------------------------------------------------------
# _check_transaction_support
# ---------------------------------------------------------------------------


def test_check_transaction_support_raises_for_standalone() -> None:
    client = MagicMock()
    client.admin.command.return_value = {"isWritablePrimary": True}
    with pytest.raises(TransactionNotSupportedError):
        _check_transaction_support(client)


def test_check_transaction_support_passes_for_replica_set() -> None:
    client = MagicMock()
    client.admin.command.return_value = {"isWritablePrimary": True, "setName": "rs0"}
    _check_transaction_support(client)  # should not raise


def test_check_transaction_support_passes_for_sharded_cluster() -> None:
    client = MagicMock()
    client.admin.command.return_value = {"msg": "isdbgrid"}
    _check_transaction_support(client)  # should not raise


def test_check_transaction_support_propagates_connection_error() -> None:
    client = MagicMock()
    client.admin.command.side_effect = ConnectionError("unreachable")
    with pytest.raises(ConnectionError):
        _check_transaction_support(client)


# ---------------------------------------------------------------------------
# SyncRunner transactional — session injection
# ---------------------------------------------------------------------------


def _transactional_runner(tmp_path: Path) -> tuple[SyncRunner, MagicMock, MagicMock, MagicMock]:
    """Return (runner, mock_db, mock_store, mock_session).

    Configures the client to report a replica set and exposes the session
    that will be bound inside ``with client.start_session() as session:``.
    """
    runner, db, store = _sync_runner(tmp_path)
    runner._client.admin.command.return_value = {"setName": "rs0"}  # ty: ignore[unresolved-attribute]
    mock_session = runner._client.start_session.return_value.__enter__.return_value  # ty: ignore[unresolved-attribute]
    return runner, db, store, mock_session


def test_sync_up_transactional_injects_session_into_migration(tmp_path: Path) -> None:
    runner, db, store, mock_session = _transactional_runner(tmp_path)

    def up_fn(received_db: Any) -> None:
        received_db["col"].insert_one({"x": 1})

    mod = types.ModuleType("_test_txn_up")
    setattr(mod, "up", up_fn)
    migration = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc", module=mod)
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=[migration]):
        runner.up(transactional=True)

    db["col"].insert_one.assert_called_once_with({"x": 1}, session=mock_session)


def test_sync_up_transactional_injects_session_into_ops(tmp_path: Path) -> None:
    runner, db, store, mock_session = _transactional_runner(tmp_path)
    migrations = [_migration("001_a", ops_based=True)]  # create_index op
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        runner.up(transactional=True)

    _, call_kwargs = db["col"].create_index.call_args
    assert call_kwargs.get("session") is mock_session


def test_sync_down_transactional_injects_session_into_migration(tmp_path: Path) -> None:
    runner, db, store, mock_session = _transactional_runner(tmp_path)

    def down_fn(received_db: Any) -> None:
        received_db["col"].delete_many({})

    mod = types.ModuleType("_test_txn_down")
    setattr(mod, "up", lambda d: None)
    setattr(mod, "down", down_fn)
    migration = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc", module=mod)
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=[migration]):
        runner.down(transactional=True)

    db["col"].delete_many.assert_called_once_with({}, session=mock_session)


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


def test_sync_status_detects_orphaned(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a")]
    store.get_applied.return_value = {"001_a", "002_deleted"}
    store.get_record.side_effect = lambda mid: make_record(mid, "abc", "up", 5)

    with patch("mongrator.runner.loader.load", return_value=migrations):
        statuses = runner.status()

    assert len(statuses) == 2
    orphaned = [s for s in statuses if s.orphaned]
    assert len(orphaned) == 1
    assert orphaned[0].id == "002_deleted"
    assert orphaned[0].applied is True


def test_sync_status_no_orphans_when_all_files_present(tmp_path: Path) -> None:
    runner, db, store = _sync_runner(tmp_path)
    migrations = [_migration("001_a"), _migration("002_b")]
    store.get_applied.return_value = {"001_a"}
    store.get_record.side_effect = lambda mid: make_record(mid, "abc", "up", 5) if mid == "001_a" else None

    with patch("mongrator.runner.loader.load", return_value=migrations):
        statuses = runner.status()

    assert len(statuses) == 2
    assert all(not s.orphaned for s in statuses)


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
async def test_async_status_detects_orphaned(tmp_path: Path) -> None:
    runner, db, store = _async_runner(tmp_path)
    migrations = [_migration("001_a")]
    store.get_applied.return_value = {"001_a", "002_deleted"}
    store.get_record.side_effect = lambda mid: make_record(mid, "abc", "up", 5)

    with patch("mongrator.runner.loader.load", return_value=migrations):
        statuses = await runner.status()

    assert len(statuses) == 2
    orphaned = [s for s in statuses if s.orphaned]
    assert len(orphaned) == 1
    assert orphaned[0].id == "002_deleted"
    assert orphaned[0].applied is True


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


# ---------------------------------------------------------------------------
# AsyncRunner — async migration functions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_up_coroutine_receives_async_db(tmp_path: Path) -> None:
    """Coroutine up() functions should receive the async database and be awaited."""
    runner, sync_db, store = _async_runner(tmp_path)
    migrations = [_async_migration("001_a")]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = await runner.up()

    assert applied == ["001_a"]
    # The async up() calls db.async_up_called — verify it was called on the async db.
    runner._async_db.async_up_called.assert_called_once_with("001_a")
    # Sync db should NOT have been called.
    sync_db.async_up_called.assert_not_called()


@pytest.mark.asyncio
async def test_async_down_coroutine_receives_async_db(tmp_path: Path) -> None:
    """Coroutine down() functions should receive the async database and be awaited."""
    runner, sync_db, store = _async_runner(tmp_path)
    migrations = [_async_migration("001_a", has_async_down=True)]
    store.get_applied.return_value = {"001_a"}

    with patch("mongrator.runner.loader.load", return_value=migrations):
        rolled_back = await runner.down()

    assert rolled_back == ["001_a"]
    runner._async_db.async_down_called.assert_called_once_with("001_a")
    sync_db.async_down_called.assert_not_called()


@pytest.mark.asyncio
async def test_async_up_sync_function_receives_sync_db(tmp_path: Path) -> None:
    """Regular (non-coroutine) up() functions should still receive the sync database."""
    runner, sync_db, store = _async_runner(tmp_path)
    up_fn = MagicMock(return_value=None)
    migrations = [_migration("001_a", ops_fn=up_fn)]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = await runner.up()

    assert applied == ["001_a"]
    up_fn.assert_called_once_with(sync_db)
    assert up_fn.call_args[0][0] is not runner._async_db


@pytest.mark.asyncio
async def test_async_up_coroutine_returning_ops_applies_them(tmp_path: Path) -> None:
    """Coroutine up() that returns list[Operation] should apply ops via the sync database."""
    runner, sync_db, store = _async_runner(tmp_path)

    async def async_up_with_ops(db: Any) -> list:
        return [create_index("col", {"field": 1})]

    mod = types.ModuleType("_test_async_ops_001")
    setattr(mod, "up", async_up_with_ops)
    migration = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc", module=mod)

    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=[migration]):
        applied = await runner.up()

    assert applied == ["001_a"]
    sync_db["col"].create_index.assert_called_once()


@pytest.mark.asyncio
async def test_async_up_ops_based_uses_sync_db(tmp_path: Path) -> None:
    """Ops-based migrations should use the sync database for apply()."""
    runner, sync_db, store = _async_runner(tmp_path)
    migrations = [_migration("001_a", ops_based=True)]
    store.get_applied.return_value = set()

    with patch("mongrator.runner.loader.load", return_value=migrations):
        applied = await runner.up()

    assert applied == ["001_a"]
    # Ops apply via sync db — create_index should be called on the sync mock.
    sync_db["col"].create_index.assert_called_once()
