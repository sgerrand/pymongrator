class MigratorError(Exception):
    """Base class for all mongrator errors."""


class ConfigurationError(MigratorError):
    """Invalid or missing configuration."""


class ChecksumMismatchError(MigratorError):
    """Applied migration file has been modified since it was run."""

    def __init__(self, migration_id: str, expected: str, actual: str) -> None:
        self.migration_id = migration_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Checksum mismatch for '{migration_id}': "
            f"expected {expected!r}, got {actual!r}. "
            "The migration file has been modified after being applied."
        )


class DuplicateMigrationIdError(MigratorError):
    """Two migration files share the same ID."""

    def __init__(self, migration_id: str) -> None:
        self.migration_id = migration_id
        super().__init__(f"Duplicate migration ID: '{migration_id}'")


class MigrationImportError(MigratorError):
    """A migration file could not be imported."""

    def __init__(self, path: str, cause: Exception) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"Failed to import migration '{path}': {cause}")


class InvalidMigrationFileError(MigratorError):
    """A migration file is missing required callables or has an invalid structure."""

    def __init__(self, path: str, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"Invalid migration file '{path}': {reason}")


class NoDownMethodError(MigratorError):
    """A migration has no rollback path."""

    def __init__(self, migration_id: str) -> None:
        self.migration_id = migration_id
        super().__init__(
            f"Migration '{migration_id}' has no rollback path. "
            "Define a down() function or use ops.* helpers that support auto-rollback."
        )


class MigrationNotFoundError(MigratorError):
    """A referenced migration ID does not exist."""

    def __init__(self, migration_id: str) -> None:
        self.migration_id = migration_id
        super().__init__(f"Migration not found: '{migration_id}'")


class MigrationLockError(MigratorError):
    """Could not acquire the migration lock."""

    def __init__(self) -> None:
        super().__init__(
            "Could not acquire migration lock. "
            "Another migration may be in progress. "
            "If this is stale, the lock will expire automatically."
        )
