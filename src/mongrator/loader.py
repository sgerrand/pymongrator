import hashlib
import importlib.util
import sys
import types
from pathlib import Path

from .config import MigratorConfig
from .exceptions import (
    DuplicateMigrationIdError,
    InvalidMigrationFileError,
    MigrationImportError,
)
from .migration import Checksum, MigrationFile, MigrationId


def _checksum(path: Path) -> Checksum:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _migration_id(path: Path) -> MigrationId:
    return path.stem


def load(config: MigratorConfig) -> list[MigrationFile]:
    """Scan migrations_dir, import each .py file, and return an ordered list.

    Files are sorted lexicographically by filename, which is chronological
    when the recommended {timestamp}_{slug}.py naming convention is used.

    Raises:
        DuplicateMigrationIdError: if two files share the same stem.
        MigrationImportError: if a file cannot be imported.
        InvalidMigrationFileError: if a file does not define an up() callable.
    """
    migrations_dir = config.migrations_dir
    if not migrations_dir.exists():
        return []

    paths = sorted(migrations_dir.glob("*.py"))
    seen: dict[MigrationId, Path] = {}
    results: list[MigrationFile] = []

    for path in paths:
        migration_id = _migration_id(path)

        if migration_id in seen:
            raise DuplicateMigrationIdError(migration_id)
        seen[migration_id] = path

        checksum = _checksum(path)
        module = _import_file(path, migration_id)

        if not callable(getattr(module, "up", None)):
            raise InvalidMigrationFileError(str(path), "missing a callable up() function")

        results.append(MigrationFile(id=migration_id, path=path, checksum=checksum, module=module))

    return results


def _import_file(path: Path, module_name: str) -> types.ModuleType:
    # Use a namespaced module name to avoid collisions with installed packages.
    qualified_name = f"mongrator._migrations.{module_name}"
    spec = importlib.util.spec_from_file_location(qualified_name, path)
    if spec is None or spec.loader is None:
        raise MigrationImportError(str(path), RuntimeError("could not create module spec"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified_name] = module
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as e:
        del sys.modules[qualified_name]
        raise MigrationImportError(str(path), e) from e
    return module
