"""Migration runners: Protocol definitions and sync/async implementations.

SyncRunner wraps a pymongo MongoClient.
AsyncRunner wraps a pymongo AsyncMongoClient.

Both share the same non-IO logic via loader and planner.
"""

import time
from typing import Any, Protocol, cast, runtime_checkable

from pymongo import AsyncMongoClient, MongoClient

from . import loader, planner
from .config import MigratorConfig
from .exceptions import ChecksumMismatchError, NoDownMethodError
from .migration import MigrationFile, MigrationId, MigrationStatus
from .ops import Operation
from .state import AsyncMongoStateStore, SyncStateStore, make_record


@runtime_checkable
class MigrationRunner(Protocol):
    def up(self, target: MigrationId | None = None) -> list[MigrationId]: ...
    def down(self, steps: int = 1) -> list[MigrationId]: ...
    def status(self) -> list[MigrationStatus]: ...
    def validate(self) -> list[ChecksumMismatchError]: ...


@runtime_checkable
class AsyncMigrationRunner(Protocol):
    async def up(self, target: MigrationId | None = None) -> list[MigrationId]: ...
    async def down(self, steps: int = 1) -> list[MigrationId]: ...
    async def status(self) -> list[MigrationStatus]: ...
    async def validate(self) -> list[ChecksumMismatchError]: ...


def _resolve_rollback(migration: MigrationFile) -> None:
    """Raise NoDownMethodError if the migration cannot be rolled back."""
    if migration.has_down():
        return
    # Check if up() returns a list of Operations with revert support
    # (We cannot call up() here; this is checked at rollback time instead.)
    raise NoDownMethodError(migration.id)


def _run_up_migration(migration: MigrationFile, db: Any) -> None:
    """Execute the up() callable, calling Operation.apply() for each op if needed."""
    up_fn = migration.up
    if up_fn is None:
        return
    result = up_fn(db)
    if isinstance(result, list) and all(isinstance(op, Operation) for op in result):
        # ops-based migration: up() returns ops, runner applies them.
        # Raw pymongo migrations return None.
        for op in cast(list[Operation], result):
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


class SyncRunner:
    """Synchronous migration runner backed by pymongo."""

    def __init__(self, client: "MongoClient", config: MigratorConfig) -> None:  # type: ignore[type-arg]
        self._db = client[config.database]
        self._store = SyncStateStore(self._db[config.collection])
        self._config = config

    def up(self, target: MigrationId | None = None) -> list[MigrationId]:
        """Apply pending migrations, optionally up to `target`."""
        files = loader.load(self._config)
        applied = self._store.get_applied()
        plan = planner.plan_up(files, applied, target)
        applied_ids: list[MigrationId] = []
        for migration in plan.to_apply:
            start = time.monotonic()
            _run_up_migration(migration, self._db)
            duration_ms = int((time.monotonic() - start) * 1000)
            self._store.record_applied(make_record(migration.id, migration.checksum, "up", duration_ms))
            applied_ids.append(migration.id)
        return applied_ids

    def down(self, steps: int = 1) -> list[MigrationId]:
        """Roll back the most recently applied migrations."""
        files = loader.load(self._config)
        applied = self._store.get_applied()
        plan = planner.plan_down(files, applied, steps)
        rolled_back: list[MigrationId] = []
        for migration in plan.to_apply:
            start = time.monotonic()
            _run_down_migration(migration, self._db)
            duration_ms = int((time.monotonic() - start) * 1000)
            self._store.record_applied(make_record(migration.id, migration.checksum, "down", duration_ms))
            rolled_back.append(migration.id)
        return rolled_back

    def status(self) -> list[MigrationStatus]:
        """Return the status of every known migration."""
        files = loader.load(self._config)
        applied = self._store.get_applied()
        statuses: list[MigrationStatus] = []
        for f in files:
            record = self._store.get_record(f.id)
            checksum_ok = record is None or record["checksum"] == f.checksum
            statuses.append(
                MigrationStatus(
                    id=f.id,
                    applied=f.id in applied,
                    applied_at=record["applied_at"] if record else None,
                    checksum_ok=checksum_ok,
                )
            )
        return statuses

    def validate(self) -> list[ChecksumMismatchError]:
        """Check that applied migration files match their recorded checksums."""
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
    """Asynchronous migration runner backed by pymongo AsyncMongoClient."""

    def __init__(self, client: "AsyncMongoClient", config: MigratorConfig) -> None:  # type: ignore[type-arg]
        self._db = client[config.database]
        self._store = AsyncMongoStateStore(self._db[config.collection])
        self._config = config

    async def up(self, target: MigrationId | None = None) -> list[MigrationId]:
        """Apply pending migrations, optionally up to `target`."""
        files = loader.load(self._config)
        applied = await self._store.get_applied()
        plan = planner.plan_up(files, applied, target)
        applied_ids: list[MigrationId] = []
        for migration in plan.to_apply:
            start = time.monotonic()
            _run_up_migration(migration, self._db)
            duration_ms = int((time.monotonic() - start) * 1000)
            await self._store.record_applied(make_record(migration.id, migration.checksum, "up", duration_ms))
            applied_ids.append(migration.id)
        return applied_ids

    async def down(self, steps: int = 1) -> list[MigrationId]:
        """Roll back the most recently applied migrations."""
        files = loader.load(self._config)
        applied = await self._store.get_applied()
        plan = planner.plan_down(files, applied, steps)
        rolled_back: list[MigrationId] = []
        for migration in plan.to_apply:
            start = time.monotonic()
            _run_down_migration(migration, self._db)
            duration_ms = int((time.monotonic() - start) * 1000)
            await self._store.record_applied(make_record(migration.id, migration.checksum, "down", duration_ms))
            rolled_back.append(migration.id)
        return rolled_back

    async def status(self) -> list[MigrationStatus]:
        """Return the status of every known migration."""
        files = loader.load(self._config)
        applied = await self._store.get_applied()
        statuses: list[MigrationStatus] = []
        for f in files:
            record = await self._store.get_record(f.id)
            checksum_ok = record is None or record["checksum"] == f.checksum
            statuses.append(
                MigrationStatus(
                    id=f.id,
                    applied=f.id in applied,
                    applied_at=record["applied_at"] if record else None,
                    checksum_ok=checksum_ok,
                )
            )
        return statuses

    async def validate(self) -> list[ChecksumMismatchError]:
        """Check that applied migration files match their recorded checksums."""
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
