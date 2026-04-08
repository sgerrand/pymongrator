"""Unit tests for mongrator.config."""

from pathlib import Path

import pytest

from mongrator.config import _DEFAULT_COLLECTION, _DEFAULT_MIGRATIONS_DIR, MigratorConfig
from mongrator.exceptions import ConfigurationError

# ---------------------------------------------------------------------------
# from_toml
# ---------------------------------------------------------------------------


def test_from_toml_minimal(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mongrator.toml"
    cfg_file.write_text('uri = "mongodb://localhost:27017"\ndatabase = "mydb"\n')
    config = MigratorConfig.from_toml(cfg_file)
    assert config.uri == "mongodb://localhost:27017"
    assert config.database == "mydb"
    assert config.migrations_dir == _DEFAULT_MIGRATIONS_DIR
    assert config.collection == _DEFAULT_COLLECTION


def test_from_toml_all_keys(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mongrator.toml"
    cfg_file.write_text(
        'uri = "mongodb://host:27017"\n'
        'database = "prod"\n'
        'migrations_dir = "db/migrations"\n'
        'collection = "schema_versions"\n'
    )
    config = MigratorConfig.from_toml(cfg_file)
    assert config.migrations_dir == Path("db/migrations")
    assert config.collection == "schema_versions"


def test_from_toml_mongrator_table(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mongrator.toml"
    cfg_file.write_text('[mongrator]\nuri = "mongodb://localhost:27017"\ndatabase = "mydb"\n')
    config = MigratorConfig.from_toml(cfg_file)
    assert config.uri == "mongodb://localhost:27017"
    assert config.database == "mydb"
    assert config.migrations_dir == _DEFAULT_MIGRATIONS_DIR
    assert config.collection == _DEFAULT_COLLECTION


def test_from_toml_mongrator_table_all_keys(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mongrator.toml"
    cfg_file.write_text(
        "[mongrator]\n"
        'uri = "mongodb://host:27017"\n'
        'database = "prod"\n'
        'migrations_dir = "db/migrations"\n'
        'collection = "schema_versions"\n'
    )
    config = MigratorConfig.from_toml(cfg_file)
    assert config.migrations_dir == Path("db/migrations")
    assert config.collection == "schema_versions"


def test_from_toml_missing_uri(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mongrator.toml"
    cfg_file.write_text('database = "mydb"\n')
    with pytest.raises(ConfigurationError, match="uri"):
        MigratorConfig.from_toml(cfg_file)


def test_from_toml_missing_database(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mongrator.toml"
    cfg_file.write_text('uri = "mongodb://localhost:27017"\n')
    with pytest.raises(ConfigurationError, match="database"):
        MigratorConfig.from_toml(cfg_file)


def test_from_toml_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="not found"):
        MigratorConfig.from_toml(tmp_path / "nonexistent.toml")


def test_from_toml_invalid_toml(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mongrator.toml"
    cfg_file.write_text("this is not valid toml ][")
    with pytest.raises(ConfigurationError, match="Invalid TOML"):
        MigratorConfig.from_toml(cfg_file)


# ---------------------------------------------------------------------------
# from_env
# ---------------------------------------------------------------------------


def test_from_env_minimal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGRATOR_URI", "mongodb://localhost:27017")
    monkeypatch.setenv("MONGRATOR_DB", "testdb")
    monkeypatch.delenv("MONGRATOR_MIGRATIONS_DIR", raising=False)
    monkeypatch.delenv("MONGRATOR_COLLECTION", raising=False)
    config = MigratorConfig.from_env()
    assert config.uri == "mongodb://localhost:27017"
    assert config.database == "testdb"
    assert config.migrations_dir == _DEFAULT_MIGRATIONS_DIR
    assert config.collection == _DEFAULT_COLLECTION


def test_from_env_all_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGRATOR_URI", "mongodb://host:27017")
    monkeypatch.setenv("MONGRATOR_DB", "prod")
    monkeypatch.setenv("MONGRATOR_MIGRATIONS_DIR", "db/migrations")
    monkeypatch.setenv("MONGRATOR_COLLECTION", "schema_versions")
    config = MigratorConfig.from_env()
    assert config.migrations_dir == Path("db/migrations")
    assert config.collection == "schema_versions"


def test_from_env_missing_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MONGRATOR_URI", raising=False)
    monkeypatch.setenv("MONGRATOR_DB", "testdb")
    with pytest.raises(ConfigurationError, match="MONGRATOR_URI"):
        MigratorConfig.from_env()


def test_from_env_missing_db(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGRATOR_URI", "mongodb://localhost:27017")
    monkeypatch.delenv("MONGRATOR_DB", raising=False)
    with pytest.raises(ConfigurationError, match="MONGRATOR_DB"):
        MigratorConfig.from_env()


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_config_is_frozen() -> None:
    config = MigratorConfig(uri="mongodb://localhost", database="db", migrations_dir=Path("m"))
    with pytest.raises(Exception):  # FrozenInstanceError
        config.uri = "other"  # ty: ignore[invalid-assignment]
