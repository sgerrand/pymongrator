"""Unit tests for mongrator.migration datatypes."""

from __future__ import annotations

import types
from datetime import UTC, datetime
from pathlib import Path

from mongrator.migration import MigrationFile, MigrationStatus


def _module_with(*names: str) -> types.ModuleType:
    """Return a module stub with the given names set to no-op callables."""
    mod = types.ModuleType("_test_migration")
    for name in names:
        setattr(mod, name, lambda db: None)  # setattr avoids unresolved-attribute on ModuleType
    return mod


# ---------------------------------------------------------------------------
# MigrationFile
# ---------------------------------------------------------------------------


def test_no_module_has_no_up_or_down() -> None:
    f = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc")
    assert f.up is None
    assert f.down is None
    assert not f.has_up()
    assert not f.has_down()


def test_module_with_up_only() -> None:
    f = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc", module=_module_with("up"))
    assert callable(f.up)
    assert f.down is None
    assert f.has_up()
    assert not f.has_down()


def test_module_with_up_and_down() -> None:
    f = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc", module=_module_with("up", "down"))
    assert callable(f.up)
    assert callable(f.down)
    assert f.has_up()
    assert f.has_down()


def test_module_missing_attribute_returns_none() -> None:
    mod = types.ModuleType("_test")
    # module exists but has neither up nor down
    f = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc", module=mod)
    assert f.up is None
    assert f.down is None


def test_up_callable_is_invocable() -> None:
    calls: list[object] = []
    mod = types.ModuleType("_test")
    setattr(mod, "up", lambda db: calls.append(db))
    f = MigrationFile(id="001_a", path=Path("001_a.py"), checksum="abc", module=mod)
    sentinel = object()
    up_fn = f.up
    assert up_fn is not None
    up_fn(sentinel)
    assert calls == [sentinel]


# ---------------------------------------------------------------------------
# MigrationStatus
# ---------------------------------------------------------------------------


def test_migration_status_defaults() -> None:
    s = MigrationStatus(id="001_a", applied=False)
    assert s.applied_at is None
    assert s.checksum_ok is True


def test_migration_status_applied() -> None:
    now = datetime.now(tz=UTC)
    s = MigrationStatus(id="001_a", applied=True, applied_at=now, checksum_ok=True)
    assert s.applied
    assert s.applied_at == now


def test_migration_status_modified() -> None:
    s = MigrationStatus(id="001_a", applied=True, checksum_ok=False)
    assert not s.checksum_ok
