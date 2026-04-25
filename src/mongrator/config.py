import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from .exceptions import ConfigurationError

_DEFAULT_COLLECTION = "mongrator_migrations"
_DEFAULT_MIGRATIONS_DIR = Path("migrations")


def _load_dotenv(path: Path) -> dict[str, str]:
    """Parse a .env file into a dict of KEY=VALUE pairs.

    Handles blank lines, ``#`` comments, unquoted values, and values
    wrapped in single or double quotes.  Does **not** handle ``export``
    prefixes, multiline values, or variable interpolation.
    """
    env: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return env

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip matching quotes
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        env[key] = value
    return env


@dataclass(frozen=True)
class MigratorConfig:
    """Immutable configuration for a migrator instance."""

    uri: str
    database: str
    migrations_dir: Path
    collection: str = _DEFAULT_COLLECTION

    @classmethod
    def from_toml(cls, path: Path) -> Self:
        """Load configuration from a TOML file (e.g. mongrator.toml)."""
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except FileNotFoundError:
            raise ConfigurationError(f"Config file not found: {path}")
        except tomllib.TOMLDecodeError as e:
            raise ConfigurationError(f"Invalid TOML in {path}: {e}")

        cfg = data.get("mongrator", data)

        try:
            uri: str = cfg["uri"]
            database: str = cfg["database"]
        except KeyError as e:
            raise ConfigurationError(f"Missing required config key: {e}")

        migrations_dir = Path(cfg.get("migrations_dir", str(_DEFAULT_MIGRATIONS_DIR)))
        collection: str = cfg.get("collection", _DEFAULT_COLLECTION)
        return cls(uri=uri, database=database, migrations_dir=migrations_dir, collection=collection)

    @classmethod
    def from_env(cls, dotenv_path: Path | None = None) -> Self:
        """Load configuration from environment variables.

        If *dotenv_path* is given and the file exists, its values are used as
        defaults — real environment variables always take precedence.

        Variables:
            MONGRATOR_URI         — MongoDB connection URI (required)
            MONGRATOR_DB          — database name (required)
            MONGRATOR_MIGRATIONS_DIR — path to migrations directory (default: migrations)
            MONGRATOR_COLLECTION  — tracking collection name (default: mongrator_migrations)
        """
        dotenv: dict[str, str] = {}
        if dotenv_path is not None:
            dotenv = _load_dotenv(dotenv_path)

        def _get(key: str, default: str | None = None) -> str | None:
            return os.environ.get(key) or dotenv.get(key) or default

        uri = _get("MONGRATOR_URI")
        database = _get("MONGRATOR_DB")
        if not uri:
            raise ConfigurationError("MONGRATOR_URI environment variable is not set")
        if not database:
            raise ConfigurationError("MONGRATOR_DB environment variable is not set")
        migrations_dir = Path(_get("MONGRATOR_MIGRATIONS_DIR", str(_DEFAULT_MIGRATIONS_DIR)))
        collection = _get("MONGRATOR_COLLECTION", _DEFAULT_COLLECTION)
        return cls(uri=uri, database=database, migrations_dir=migrations_dir, collection=collection)
