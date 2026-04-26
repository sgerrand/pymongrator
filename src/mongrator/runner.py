"""Migration runners: Protocol definitions and sync/async implementations.

SyncRunner wraps a pymongo MongoClient.
AsyncRunner wraps a pymongo AsyncMongoClient.

Both share the same non-IO logic via loader and planner.
"""

import inspect
import sys
import time
from typing import Any, Protocol, cast, runtime_checkable

from pymongo import AsyncMongoClient, MongoClient

from . import loader, planner
from .config import MigratorConfig
from .exceptions import ChecksumMismatchError, NoDownMethodError, TransactionNotSupportedError
from .migration import MigrationFile, MigrationId, MigrationStatus
from .ops import Operation
from .planner import MigrationPlan
from .state import AsyncMigrationLock, AsyncMongoStateStore, SyncMigrationLock, SyncStateStore, make_record


@runtime_checkable
class MigrationRunner(Protocol):
    def plan_up(self, target: MigrationId | None = None) -> MigrationPlan: ...
    def plan_down(self, steps: int = 1) -> MigrationPlan: ...
    def up(self, target: MigrationId | None = None, *, transactional: bool = False) -> list[MigrationId]: ...
    def down(self, steps: int = 1, *, transactional: bool = False) -> list[MigrationId]: ...
    def status(self) -> list[MigrationStatus]: ...
    def validate(self) -> list[ChecksumMismatchError]: ...


@runtime_checkable
class AsyncMigrationRunner(Protocol):
    async def plan_up(self, target: MigrationId | None = None) -> MigrationPlan: ...
    async def plan_down(self, steps: int = 1) -> MigrationPlan: ...
    async def up(self, target: MigrationId | None = None, *, transactional: bool = False) -> list[MigrationId]: ...
    async def down(self, steps: int = 1, *, transactional: bool = False) -> list[MigrationId]: ...
    async def status(self) -> list[MigrationStatus]: ...
    async def validate(self) -> list[ChecksumMismatchError]: ...


def _run_up_migration(migration: MigrationFile, db: Any) -> None:
    """Execute the up() callable, calling Operation.apply() for each op if needed."""
    up_fn = migration.up
    if up_fn is None:
        return
    result = up_fn(db)
    if isinstance(result, list) and all(isinstance(op, Operation) for op in result):
        # ops-based migration: up() returns ops, runner applies them.
        # Raw pymongo migrations return None.
        ops = cast(list[Operation], result)
        if not migration.has_down():
            irreversible = [op.description for op in ops if not op.is_reversible]
            if irreversible:
                op_list = ", ".join(irreversible)
                print(
                    f"warning: migration {migration.id} ({migration.path}) has "
                    f"{len(irreversible)} non-auto-reversible operation(s): {op_list}; "
                    "rollback will fail without a down() function.",
                    file=sys.stderr,
                )
        for op in ops:
            op.apply(db)  # type: ignore[arg-type]


def _run_down_migration(migration: MigrationFile, db: Any) -> None:
    """Execute the down() callable, or auto-revert ops returned by up()."""
    down_fn = migration.down
    if down_fn is not None:
        down_fn(db)
        return
    # Try auto-rollback via ops returned from up()
    up_fn = migration.up
    if up_fn is None:
        raise NoDownMethodError(migration.id)
    result = up_fn(db)
    if isinstance(result, list) and all(isinstance(op, Operation) for op in result):
        for op in reversed(cast(list[Operation], result)):
            op.revert(db)  # type: ignore[arg-type]
        return
    raise NoDownMethodError(migration.id)


async def _async_run_up_migration(migration: MigrationFile, async_db: Any, sync_db: Any) -> None:
    """Execute the up() callable, dispatching to async or sync path as needed.

    If up() is a coroutine function, it receives the async database and is awaited.
    If up() returns a list of Operations, those are applied using the sync database
    (ops helpers are synchronous). Otherwise, the sync database is passed directly
    for backwards compatibility.
    """
    up_fn = migration.up
    if up_fn is None:
        return
    if inspect.iscoroutinefunction(up_fn):
        result = await up_fn(async_db)
        if isinstance(result, list) and all(isinstance(op, Operation) for op in result):
            for op in cast(list[Operation], result):
                op.apply(sync_db)
        return
    result = up_fn(sync_db)
    if isinstance(result, list) and all(isinstance(op, Operation) for op in result):
        for op in cast(list[Operation], result):
            op.apply(sync_db)


async def _async_run_down_migration(migration: MigrationFile, async_db: Any, sync_db: Any) -> None:
    """Execute the down() callable, dispatching to async or sync path as needed.

    If down() is a coroutine function, it receives the async database and is awaited.
    If no down() exists, auto-rollback via ops is attempted using the sync database.
    Otherwise, the sync database is passed for backwards compatibility.
    """
    down_fn = migration.down
    if down_fn is not None:
        if inspect.iscoroutinefunction(down_fn):
            await down_fn(async_db)
            return
        down_fn(sync_db)
        return
    # Try auto-rollback via ops returned from up()
    up_fn = migration.up
    if up_fn is None:
        raise NoDownMethodError(migration.id)
    result = up_fn(sync_db)
    if isinstance(result, list) and all(isinstance(op, Operation) for op in result):
        for op in reversed(cast(list[Operation], result)):
            op.revert(sync_db)
        return
    raise NoDownMethodError(migration.id)


class _SessionBoundCollection:
    """Wraps a pymongo Collection, injecting ``session=`` into every call."""

    def __init__(self, collection: Any, session: Any) -> None:
        self._collection = collection
        self._session = session

    def __getitem__(self, name: str) -> "_SessionBoundCollection":
        return _SessionBoundCollection(self._collection[name], self._session)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._collection, name)
        if callable(attr):
            session = self._session

            def _bound(*args: Any, **kwargs: Any) -> Any:
                kwargs.setdefault("session", session)
                return attr(*args, **kwargs)

            return _bound
        return attr


class _SessionBoundDatabase:
    """Wraps a pymongo Database, injecting ``session=`` into every collection/db call.

    Passed to migration functions and ops helpers when running under a
    transaction so that every pymongo operation is part of the session.
    """

    def __init__(self, db: Any, session: Any) -> None:
        self._db = db
        self._session = session

    def __getitem__(self, name: str) -> _SessionBoundCollection:
        return _SessionBoundCollection(self._db[name], self._session)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._db, name)
        # Collection accessed via attribute (e.g. db.users)
        if hasattr(attr, "insert_one"):
            return _SessionBoundCollection(attr, self._session)
        if callable(attr):
            session = self._session

            def _bound(*args: Any, **kwargs: Any) -> Any:
                kwargs.setdefault("session", session)
                return attr(*args, **kwargs)

            return _bound
        return attr


def _check_transaction_support(client: Any) -> None:
    """Verify the server supports transactions (replica set or sharded cluster).

    Issues a ``hello`` command to inspect the topology. Connection and
    authentication errors propagate as-is. Only raises
    TransactionNotSupportedError when the server is confirmed to be a
    standalone (neither a replica set member nor a mongos router).
    """
    result = client.admin.command("hello")
    is_replica_set = bool(result.get("setName"))
    is_sharded = result.get("msg") == "isdbgrid"
    if not (is_replica_set or is_sharded):
        raise TransactionNotSupportedError


class SyncRunner:
    """Synchronous migration runner backed by pymongo."""

    def __init__(self, client: "MongoClient", config: MigratorConfig) -> None:  # type: ignore[type-arg]
        self._client = client
        self._db = client[config.database]
        self._store = SyncStateStore(self._db[config.collection])
        self._lock = SyncMigrationLock(self._db[config.collection])
        self._config = config

    def plan_up(self, target: MigrationId | None = None) -> MigrationPlan:
        """Return the plan for applying pending migrations without executing."""
        files = loader.load(self._config)
        applied = self._store.get_applied()
        return planner.plan_up(files, applied, target)

    def plan_down(self, steps: int = 1) -> MigrationPlan:
        """Return the plan for rolling back migrations without executing."""
        files = loader.load(self._config)
        applied = self._store.get_applied()
        return planner.plan_down(files, applied, steps)

    def up(self, target: MigrationId | None = None, *, transactional: bool = False) -> list[MigrationId]:
        """Apply pending migrations, optionally up to `target`.

        Args:
            target: Stop after applying this migration ID.
            transactional: When True, wrap each migration in a MongoDB
                transaction. Requires a replica set; raises
                ``TransactionNotSupportedError`` otherwise.
        """
        if transactional:
            _check_transaction_support(self._client)
        with self._lock:
            files = loader.load(self._config)
            applied = self._store.get_applied()
            plan = planner.plan_up(files, applied, target)
            applied_ids: list[MigrationId] = []
            for migration in plan.to_apply:
                start = time.monotonic()
                if transactional:
                    with self._client.start_session() as session:
                        with session.start_transaction():
                            _run_up_migration(migration, _SessionBoundDatabase(self._db, session))
                else:
                    _run_up_migration(migration, self._db)
                duration_ms = int((time.monotonic() - start) * 1000)
                self._store.record_applied(make_record(migration.id, migration.checksum, "up", duration_ms))
                applied_ids.append(migration.id)
            return applied_ids

    def down(self, steps: int = 1, *, transactional: bool = False) -> list[MigrationId]:
        """Roll back the most recently applied migrations.

        Args:
            steps: Number of migrations to roll back.
            transactional: When True, wrap each migration in a MongoDB
                transaction. Requires a replica set; raises
                ``TransactionNotSupportedError`` otherwise.
        """
        if transactional:
            _check_transaction_support(self._client)
        with self._lock:
            files = loader.load(self._config)
            applied = self._store.get_applied()
            plan = planner.plan_down(files, applied, steps)
            rolled_back: list[MigrationId] = []
            for migration in plan.to_apply:
                start = time.monotonic()
                if transactional:
                    with self._client.start_session() as session:
                        with session.start_transaction():
                            _run_down_migration(migration, _SessionBoundDatabase(self._db, session))
                else:
                    _run_down_migration(migration, self._db)
                duration_ms = int((time.monotonic() - start) * 1000)
                self._store.record_applied(make_record(migration.id, migration.checksum, "down", duration_ms))
                rolled_back.append(migration.id)
            return rolled_back

    def status(self) -> list[MigrationStatus]:
        """Return the status of every known migration, including orphans.

        This is a read-only operation and intentionally does not acquire the
        advisory lock so it can run safely while migrations are in progress.
        """
        files = loader.load(self._config)
        applied = self._store.get_applied()
        file_ids = {f.id for f in files}
        statuses: list[MigrationStatus] = []
        for f in files:
            if f.id in applied:
                record = self._store.get_record(f.id)
                checksum_ok = record is None or record["checksum"] == f.checksum
                applied_at = record["applied_at"] if record else None
            else:
                checksum_ok = True
                applied_at = None
            statuses.append(
                MigrationStatus(
                    id=f.id,
                    applied=f.id in applied,
                    applied_at=applied_at,
                    checksum_ok=checksum_ok,
                )
            )
        for orphan_id in sorted(applied - file_ids):
            record = self._store.get_record(orphan_id)
            statuses.append(
                MigrationStatus(
                    id=orphan_id,
                    applied=True,
                    applied_at=record["applied_at"] if record else None,
                    orphaned=True,
                )
            )
        return statuses

    def validate(self) -> list[ChecksumMismatchError]:
        """Check that applied migration files match their recorded checksums.

        This is a read-only operation and intentionally does not acquire the
        advisory lock so it can run safely while migrations are in progress.
        """
        files = loader.load(self._config)
        applied = self._store.get_applied()
        errors: list[ChecksumMismatchError] = []
        for f in files:
            if f.id not in applied:
                continue
            record = self._store.get_record(f.id)
            if record and record["checksum"] != f.checksum:
                errors.append(ChecksumMismatchError(f.id, record["checksum"], f.checksum))
        return errors


class AsyncRunner:
    """Asynchronous migration runner backed by pymongo AsyncMongoClient.

    Migration functions are dispatched based on their type:

    - **Coroutine functions** (``async def up(db)``) receive the async database
      and are awaited. This allows migrations to use ``await`` for non-blocking
      I/O when running with ``--async``.
    - **Regular functions** that return a ``list[Operation]`` (ops-based migrations)
      continue to receive the sync database, since ops helpers are synchronous.
    - **Regular functions** that perform raw pymongo calls also receive the sync
      database, preserving backwards compatibility.

    State tracking always uses the async client for non-blocking operation.
    """

    def __init__(  # type: ignore[type-arg]
        self,
        client: "AsyncMongoClient",
        config: MigratorConfig,
        *,
        sync_client: "MongoClient | None" = None,
    ) -> None:
        # Sync DB passed to sync migration functions and ops helpers.
        if sync_client is None:
            sync_client = MongoClient(config.uri)
        self._sync_client = sync_client
        self._db = sync_client[config.database]
        # Async DB passed to coroutine migration functions.
        self._async_db = client[config.database]
        # Async store for non-blocking state tracking.
        self._store = AsyncMongoStateStore(self._async_db[config.collection])
        self._lock = AsyncMigrationLock(self._async_db[config.collection])
        self._config = config

    async def plan_up(self, target: MigrationId | None = None) -> MigrationPlan:
        """Return the plan for applying pending migrations without executing."""
        files = loader.load(self._config)
        applied = await self._store.get_applied()
        return planner.plan_up(files, applied, target)

    async def plan_down(self, steps: int = 1) -> MigrationPlan:
        """Return the plan for rolling back migrations without executing."""
        files = loader.load(self._config)
        applied = await self._store.get_applied()
        return planner.plan_down(files, applied, steps)

    async def up(self, target: MigrationId | None = None, *, transactional: bool = False) -> list[MigrationId]:
        """Apply pending migrations, optionally up to `target`.

        Coroutine migration functions receive the async database and are awaited;
        sync functions and ops-based migrations use the sync database.

        Args:
            target: Stop after applying this migration ID.
            transactional: When True, wrap each migration in a MongoDB
                transaction. Requires a replica set; raises
                ``TransactionNotSupportedError`` otherwise.
        """
        if transactional:
            _check_transaction_support(self._sync_client)
        async with self._lock:
            files = loader.load(self._config)
            applied = await self._store.get_applied()
            plan = planner.plan_up(files, applied, target)
            applied_ids: list[MigrationId] = []
            for migration in plan.to_apply:
                start = time.monotonic()
                if transactional:
                    with self._sync_client.start_session() as session:
                        with session.start_transaction():
                            await _async_run_up_migration(
                                migration, self._async_db, _SessionBoundDatabase(self._db, session)
                            )
                else:
                    await _async_run_up_migration(migration, self._async_db, self._db)
                duration_ms = int((time.monotonic() - start) * 1000)
                await self._store.record_applied(make_record(migration.id, migration.checksum, "up", duration_ms))
                applied_ids.append(migration.id)
            return applied_ids

    async def down(self, steps: int = 1, *, transactional: bool = False) -> list[MigrationId]:
        """Roll back the most recently applied migrations.

        Coroutine migration functions receive the async database and are awaited;
        sync functions and ops-based migrations use the sync database.

        Args:
            steps: Number of migrations to roll back.
            transactional: When True, wrap each migration in a MongoDB
                transaction. Requires a replica set; raises
                ``TransactionNotSupportedError`` otherwise.
        """
        if transactional:
            _check_transaction_support(self._sync_client)
        async with self._lock:
            files = loader.load(self._config)
            applied = await self._store.get_applied()
            plan = planner.plan_down(files, applied, steps)
            rolled_back: list[MigrationId] = []
            for migration in plan.to_apply:
                start = time.monotonic()
                if transactional:
                    with self._sync_client.start_session() as session:
                        with session.start_transaction():
                            await _async_run_down_migration(
                                migration, self._async_db, _SessionBoundDatabase(self._db, session)
                            )
                else:
                    await _async_run_down_migration(migration, self._async_db, self._db)
                duration_ms = int((time.monotonic() - start) * 1000)
                await self._store.record_applied(make_record(migration.id, migration.checksum, "down", duration_ms))
                rolled_back.append(migration.id)
            return rolled_back

    async def status(self) -> list[MigrationStatus]:
        """Return the status of every known migration, including orphans.

        This is a read-only operation and intentionally does not acquire the
        advisory lock so it can run safely while migrations are in progress.
        """
        files = loader.load(self._config)
        applied = await self._store.get_applied()
        file_ids = {f.id for f in files}
        statuses: list[MigrationStatus] = []
        for f in files:
            if f.id in applied:
                record = await self._store.get_record(f.id)
                checksum_ok = record is None or record["checksum"] == f.checksum
                applied_at = record["applied_at"] if record else None
            else:
                checksum_ok = True
                applied_at = None
            statuses.append(
                MigrationStatus(
                    id=f.id,
                    applied=f.id in applied,
                    applied_at=applied_at,
                    checksum_ok=checksum_ok,
                )
            )
        for orphan_id in sorted(applied - file_ids):
            record = await self._store.get_record(orphan_id)
            statuses.append(
                MigrationStatus(
                    id=orphan_id,
                    applied=True,
                    applied_at=record["applied_at"] if record else None,
                    orphaned=True,
                )
            )
        return statuses

    async def validate(self) -> list[ChecksumMismatchError]:
        """Check that applied migration files match their recorded checksums.

        This is a read-only operation and intentionally does not acquire the
        advisory lock so it can run safely while migrations are in progress.
        """
        files = loader.load(self._config)
        applied = await self._store.get_applied()
        errors: list[ChecksumMismatchError] = []
        for f in files:
            if f.id not in applied:
                continue
            record = await self._store.get_record(f.id)
            if record and record["checksum"] != f.checksum:
                errors.append(ChecksumMismatchError(f.id, record["checksum"], f.checksum))
        return errors
