"""Unit tests for mongrator.exceptions."""

import pytest

from mongrator.exceptions import (
    ChecksumMismatchError,
    ConfigurationError,
    DuplicateMigrationIdError,
    InvalidMigrationFileError,
    MigrationImportError,
    MigrationLockError,
    MigrationNotFoundError,
    MigratorError,
    NoDownMethodError,
)


def test_all_errors_are_migrator_errors() -> None:
    for cls in (
        ConfigurationError,
        ChecksumMismatchError,
        DuplicateMigrationIdError,
        MigrationImportError,
        InvalidMigrationFileError,
        NoDownMethodError,
        MigrationNotFoundError,
        MigrationLockError,
    ):
        assert issubclass(cls, MigratorError)


def test_checksum_mismatch_attributes() -> None:
    err = ChecksumMismatchError("001_a", "deadbeef", "cafebabe")
    assert err.migration_id == "001_a"
    assert err.expected == "deadbeef"
    assert err.actual == "cafebabe"
    assert "001_a" in str(err)
    assert "deadbeef" in str(err)
    assert "cafebabe" in str(err)


def test_duplicate_migration_id_attributes() -> None:
    err = DuplicateMigrationIdError("001_a")
    assert err.migration_id == "001_a"
    assert "001_a" in str(err)


def test_migration_import_error_attributes() -> None:
    cause = RuntimeError("syntax error")
    err = MigrationImportError("/path/to/migration.py", cause)
    assert err.path == "/path/to/migration.py"
    assert err.cause is cause
    assert "/path/to/migration.py" in str(err)


def test_invalid_migration_file_attributes() -> None:
    err = InvalidMigrationFileError("/path/to/bad.py", "missing up()")
    assert err.path == "/path/to/bad.py"
    assert err.reason == "missing up()"
    assert "missing up()" in str(err)


def test_no_down_method_attributes() -> None:
    err = NoDownMethodError("001_a")
    assert err.migration_id == "001_a"
    assert "001_a" in str(err)


def test_migration_not_found_attributes() -> None:
    err = MigrationNotFoundError("999_missing")
    assert err.migration_id == "999_missing"
    assert "999_missing" in str(err)


def test_migration_lock_error() -> None:
    err = MigrationLockError()
    assert "lock" in str(err).lower()
    assert isinstance(err, MigratorError)


def test_errors_are_catchable_as_base() -> None:
    with pytest.raises(MigratorError):
        raise ChecksumMismatchError("x", "a", "b")
