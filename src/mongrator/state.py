"""Migration state storage: Protocol definitions and sync/async implementations.

The tracking collection stores one document per applied migration::

    {
        "_id": "20260408_143022_add_users_email_index",
        "applied_at": ISODate("2026-04-08T14:30:22Z"),
        "checksum": "e3b0c44298fc1c149afb...",
        "direction": "up",
        "duration_ms": 42
    }
"""

from datetime import UTC, datetime
from typing import Literal, Protocol, runtime_checkable

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.collection import Collection

from .migration import MigrationId, MigrationRecord


@runtime_checkable
class StateStore(Protocol):
    """Synchronous migration state store."""

    def get_applied(self) -> set[MigrationId]: ...

    def record_applied(self, record: MigrationRecord) -> None: ...

    def remove_record(self, migration_id: MigrationId) -> None: ...

    def get_record(self, migration_id: MigrationId) -> MigrationRecord | None: ...


@runtime_checkable
class AsyncStateStore(Protocol):
    """Asynchronous migration state store."""

    async def get_applied(self) -> set[MigrationId]: ...

    async def record_applied(self, record: MigrationRecord) -> None: ...

    async def remove_record(self, migration_id: MigrationId) -> None: ...

    async def get_record(self, migration_id: MigrationId) -> MigrationRecord | None: ...


class SyncStateStore:
    """StateStore backed by a synchronous pymongo collection."""

    def __init__(self, collection: "Collection") -> None:  # type: ignore[type-arg]
        self._col = collection

    def get_applied(self) -> set[MigrationId]:
        return {doc["_id"] for doc in self._col.find({"direction": "up"}, {"_id": 1})}

    def record_applied(self, record: MigrationRecord) -> None:
        self._col.replace_one({"_id": record["_id"]}, record, upsert=True)

    def remove_record(self, migration_id: MigrationId) -> None:
        self._col.delete_one({"_id": migration_id})

    def get_record(self, migration_id: MigrationId) -> MigrationRecord | None:
        return self._col.find_one({"_id": migration_id})  # type: ignore[return-value]


class AsyncMongoStateStore:
    """AsyncStateStore backed by a pymongo AsyncCollection."""

    def __init__(self, collection: "AsyncCollection") -> None:  # type: ignore[type-arg]
        self._col = collection

    async def get_applied(self) -> set[MigrationId]:
        cursor = self._col.find({"direction": "up"}, {"_id": 1})
        return {doc["_id"] async for doc in cursor}

    async def record_applied(self, record: MigrationRecord) -> None:
        await self._col.replace_one({"_id": record["_id"]}, record, upsert=True)

    async def remove_record(self, migration_id: MigrationId) -> None:
        await self._col.delete_one({"_id": migration_id})

    async def get_record(self, migration_id: MigrationId) -> MigrationRecord | None:
        return await self._col.find_one({"_id": migration_id})  # type: ignore[return-value]


def make_record(
    migration_id: MigrationId,
    checksum: str,
    direction: Literal["up", "down"],
    duration_ms: int,
) -> MigrationRecord:
    """Construct a MigrationRecord with the current UTC timestamp."""
    return MigrationRecord(
        _id=migration_id,
        applied_at=datetime.now(tz=UTC),
        checksum=checksum,
        direction=direction,
        duration_ms=duration_ms,
    )
