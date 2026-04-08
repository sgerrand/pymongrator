from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .exceptions import ConfigurationError

_DEFAULT_COLLECTION = "mongrator_migrations"
_DEFAULT_MIGRATIONS_DIR = Path("migrations")


@dataclass(frozen=True)
class MigratorConfig:
    """Immutable configuration for a migrator instance."""

    uri: str
    database: str
    migrations_dir: Path
    collection: str = _DEFAULT_COLLECTION

    @classmethod
    def from_toml(cls, path: Path) -> MigratorConfig:
        """Load configuration from a TOML file (e.g. mongrator.toml)."""
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except FileNotFoundError:
            raise ConfigurationError(f"Config file not found: {path}")
        except tomllib.TOMLDecodeError as e:
            raise ConfigurationError(f"Invalid TOML in {path}: {e}")

        try:
            uri: str = data["uri"]
            database: str = data["database"]
        except KeyError as e:
            raise ConfigurationError(f"Missing required config key: {e}")

        migrations_dir = Path(data.get("migrations_dir", str(_DEFAULT_MIGRATIONS_DIR)))
        collection: str = data.get("collection", _DEFAULT_COLLECTION)
        return cls(uri=uri, database=database, migrations_dir=migrations_dir, collection=collection)

    @classmethod
    def from_env(cls) -> MigratorConfig:
        """Load configuration from environment variables.

        Variables:
            MONGRATOR_URI         — MongoDB connection URI (required)
            MONGRATOR_DB          — database name (required)
            MONGRATOR_MIGRATIONS_DIR — path to migrations directory (default: migrations)
            MONGRATOR_COLLECTION  — tracking collection name (default: mongrator_migrations)
        """
        uri = os.environ.get("MONGRATOR_URI")
        database = os.environ.get("MONGRATOR_DB")
        if not uri:
            raise ConfigurationError("MONGRATOR_URI environment variable is not set")
        if not database:
            raise ConfigurationError("MONGRATOR_DB environment variable is not set")
        migrations_dir = Path(os.environ.get("MONGRATOR_MIGRATIONS_DIR", str(_DEFAULT_MIGRATIONS_DIR)))
        collection = os.environ.get("MONGRATOR_COLLECTION", _DEFAULT_COLLECTION)
        return cls(uri=uri, database=database, migrations_dir=migrations_dir, collection=collection)
