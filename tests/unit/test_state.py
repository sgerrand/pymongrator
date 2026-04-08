"""Unit tests for mongrator.state — sync and async stores, make_record."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from mongrator.state import AsyncMongoStateStore, SyncStateStore, make_record

# ---------------------------------------------------------------------------
# make_record
# ---------------------------------------------------------------------------


def test_make_record_fields() -> None:
    record = make_record("001_a", "deadbeef", "up", 42)
    assert record["_id"] == "001_a"
    assert record["checksum"] == "deadbeef"
    assert record["direction"] == "up"
    assert record["duration_ms"] == 42


def test_make_record_timestamp_is_utc() -> None:
    before = datetime.now(tz=UTC)
    record = make_record("001_a", "abc", "up", 0)
    after = datetime.now(tz=UTC)
    assert before <= record["applied_at"] <= after
    assert record["applied_at"].tzinfo is not None


def test_make_record_down_direction() -> None:
    record = make_record("001_a", "abc", "down", 10)
    assert record["direction"] == "down"


# ---------------------------------------------------------------------------
# SyncStateStore
# ---------------------------------------------------------------------------


def _sync_store() -> tuple[SyncStateStore, MagicMock]:
    col = MagicMock()
    return SyncStateStore(col), col


def test_sync_get_applied_returns_ids() -> None:
    store, col = _sync_store()
    col.find.return_value = [{"_id": "001_a"}, {"_id": "002_b"}]
    result = store.get_applied()
    assert result == {"001_a", "002_b"}
    col.find.assert_called_once_with({}, {"_id": 1})


def test_sync_get_applied_empty() -> None:
    store, col = _sync_store()
    col.find.return_value = []
    assert store.get_applied() == set()


def test_sync_record_applied_upserts() -> None:
    store, col = _sync_store()
    record = make_record("001_a", "abc", "up", 5)
    store.record_applied(record)
    col.replace_one.assert_called_once_with({"_id": "001_a"}, record, upsert=True)


def test_sync_remove_record() -> None:
    store, col = _sync_store()
    store.remove_record("001_a")
    col.delete_one.assert_called_once_with({"_id": "001_a"})


def test_sync_get_record_found() -> None:
    store, col = _sync_store()
    expected = make_record("001_a", "abc", "up", 5)
    col.find_one.return_value = expected
    result = store.get_record("001_a")
    assert result == expected
    col.find_one.assert_called_once_with({"_id": "001_a"})


def test_sync_get_record_not_found() -> None:
    store, col = _sync_store()
    col.find_one.return_value = None
    assert store.get_record("999_missing") is None


# ---------------------------------------------------------------------------
# AsyncMongoStateStore
# ---------------------------------------------------------------------------


def _async_store() -> tuple[AsyncMongoStateStore, MagicMock]:
    col = MagicMock()
    # find() returns an async iterator
    col.find.return_value = _async_iter([{"_id": "001_a"}, {"_id": "002_b"}])
    col.replace_one = AsyncMock()
    col.delete_one = AsyncMock()
    col.find_one = AsyncMock()
    return AsyncMongoStateStore(col), col


def _async_iter(items: list) -> object:
    """Return an object that supports `async for`."""

    class _AsyncIter:
        def __aiter__(self):
            return self._gen()

        async def _gen(self):
            for item in items:
                yield item

    return _AsyncIter()


@pytest.mark.asyncio
async def test_async_get_applied_returns_ids() -> None:
    store, col = _async_store()
    result = await store.get_applied()
    assert result == {"001_a", "002_b"}


@pytest.mark.asyncio
async def test_async_get_applied_empty() -> None:
    col = MagicMock()
    col.find.return_value = _async_iter([])
    store = AsyncMongoStateStore(col)
    assert await store.get_applied() == set()


@pytest.mark.asyncio
async def test_async_record_applied_upserts() -> None:
    store, col = _async_store()
    record = make_record("001_a", "abc", "up", 5)
    await store.record_applied(record)
    col.replace_one.assert_called_once_with({"_id": "001_a"}, record, upsert=True)


@pytest.mark.asyncio
async def test_async_remove_record() -> None:
    store, col = _async_store()
    await store.remove_record("001_a")
    col.delete_one.assert_called_once_with({"_id": "001_a"})


@pytest.mark.asyncio
async def test_async_get_record_found() -> None:
    store, col = _async_store()
    expected = make_record("001_a", "abc", "up", 5)
    col.find_one.return_value = expected
    result = await store.get_record("001_a")
    assert result == expected
    col.find_one.assert_called_once_with({"_id": "001_a"})


@pytest.mark.asyncio
async def test_async_get_record_not_found() -> None:
    store, col = _async_store()
    col.find_one.return_value = None
    assert await store.get_record("999_missing") is None
