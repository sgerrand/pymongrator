"""Unit tests for mongrator.cli — click-based CLI commands."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from mongrator.cli import (
    EXIT_NOTHING_TO_DO,
    _load_config,
    cli,
    main,
)
from mongrator.exceptions import ChecksumMismatchError, MigratorError
from mongrator.migration import MigrationStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

runner = CliRunner()


# ---------------------------------------------------------------------------
# Global options
# ---------------------------------------------------------------------------


def test_default_config() -> None:
    """Init with default config path creates mongrator.toml and migrations/."""
    with runner.isolated_filesystem() as td:
        result = runner.invoke(cli, ["init"], catch_exceptions=False)
        assert result.exit_code == 0
        assert (Path(td) / "mongrator.toml").exists()
        assert (Path(td) / "migrations").is_dir()


def test_custom_config(tmp_path: Path) -> None:
    """Custom --config option is passed through to subcommands."""
    config_file = tmp_path / "custom.toml"
    mdir = (tmp_path / "migrations").as_posix()
    config_file.write_text(f'[mongrator]\nuri = "mongodb://localhost"\ndatabase = "db"\nmigrations_dir = \'{mdir}\'\n')
    result = runner.invoke(cli, ["--config", str(config_file), "create", "test_migration"])
    assert result.exit_code == 0


def test_missing_command_exits_with_usage_error() -> None:
    result = runner.invoke(cli, [])
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


def test_init_command() -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["init"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


def test_create_command_with_name(tmp_path: Path) -> None:
    config_file = tmp_path / "mongrator.toml"
    migrations_dir = tmp_path / "migrations"
    config_file.write_text(
        f'[mongrator]\nuri = "mongodb://localhost"\ndatabase = "db"\nmigrations_dir = \'{migrations_dir.as_posix()}\'\n'
    )
    result = runner.invoke(cli, ["--config", str(config_file), "create", "add_users_email_index"])
    assert result.exit_code == 0
    files = list(migrations_dir.glob("*.py"))
    assert len(files) == 1
    assert "add_users_email_index" in files[0].name


def test_create_missing_name_exits() -> None:
    result = runner.invoke(cli, ["create"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def test_status_command() -> None:
    mock_run = MagicMock()
    mock_run.status.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------


def test_up_defaults() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["up"])
    assert result.exit_code == EXIT_NOTHING_TO_DO
    assert "Nothing to apply" in result.output


def test_up_with_target() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = ["002_b"]
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["up", "--target", "002_b"])
    assert result.exit_code == 0
    mock_run.up.assert_called_once_with(target="002_b", transactional=False)


def test_up_async_flag() -> None:
    """--async flag is accepted on the up subcommand."""
    with (
        patch("mongrator.cli._load_config"),
        patch("mongrator.cli._async_up", new_callable=AsyncMock, return_value=0) as mock_async_up,
    ):
        result = runner.invoke(cli, ["up", "--async"])
    assert result.exit_code == 0
    mock_async_up.assert_awaited_once()


def test_up_dry_run_flag() -> None:
    mock_run = MagicMock()
    plan = MagicMock()
    plan.to_apply = []
    mock_run.plan_up.return_value = plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["up", "--dry-run"])
    mock_run.plan_up.assert_called_once()


def test_up_transactional_flag() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["up", "--transactional"])
    mock_run.up.assert_called_once_with(target=None, transactional=True)


def test_up_transactional_default() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["up"])
    mock_run.up.assert_called_once_with(target=None, transactional=False)


# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------


def test_down_defaults() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["down"])
    assert result.exit_code == EXIT_NOTHING_TO_DO
    mock_run.down.assert_called_once_with(steps=1, transactional=False)


def test_down_steps() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = ["003_c", "002_b", "001_a"]
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["down", "--steps", "3"])
    assert result.exit_code == 0
    mock_run.down.assert_called_once_with(steps=3, transactional=False)


def test_down_async_flag() -> None:
    with (
        patch("mongrator.cli._load_config"),
        patch("mongrator.cli._async_down", new_callable=AsyncMock, return_value=0) as mock_async_down,
    ):
        result = runner.invoke(cli, ["down", "--async"])
    assert result.exit_code == 0
    mock_async_down.assert_awaited_once()


def test_down_dry_run_flag() -> None:
    mock_run = MagicMock()
    plan = MagicMock()
    plan.to_apply = []
    mock_run.plan_down.return_value = plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["down", "--dry-run"])
    mock_run.plan_down.assert_called_once()


def test_down_transactional_flag() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["down", "--transactional"])
    mock_run.down.assert_called_once_with(steps=1, transactional=True)


def test_down_transactional_default() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["down"])
    mock_run.down.assert_called_once_with(steps=1, transactional=False)


def test_down_invalid_steps_type_exits() -> None:
    result = runner.invoke(cli, ["down", "--steps", "not_a_number"])
    assert result.exit_code != 0


def test_down_zero_steps_exits() -> None:
    result = runner.invoke(cli, ["down", "--steps", "0"])
    assert result.exit_code != 0


def test_down_negative_steps_exits() -> None:
    result = runner.invoke(cli, ["down", "--steps", "-1"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


def test_validate_command() -> None:
    mock_run = MagicMock()
    mock_run.validate.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["validate"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# _load_config
# ---------------------------------------------------------------------------


def test_load_config_from_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "mongrator.toml"
    config_file.write_text('[mongrator]\nuri = "mongodb://localhost:27017"\ndatabase = "testdb"\n')
    config = _load_config(str(config_file))
    assert config.database == "testdb"


def test_load_config_from_env_when_file_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGRATOR_URI", "mongodb://localhost:27017")
    monkeypatch.setenv("MONGRATOR_DB", "envdb")
    config = _load_config("nonexistent.toml")
    assert config.database == "envdb"


# ---------------------------------------------------------------------------
# _cmd_init
# ---------------------------------------------------------------------------


def test_cmd_init_creates_config_and_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(cli, ["--config", str(tmp_path / "mongrator.toml"), "init"])
    assert result.exit_code == 0
    assert (tmp_path / "mongrator.toml").exists()
    assert (tmp_path / "migrations").is_dir()


def test_cmd_init_does_not_overwrite_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "mongrator.toml"
    config_file.write_text("existing content")
    runner.invoke(cli, ["--config", str(config_file), "init"])
    assert config_file.read_text() == "existing content"


# ---------------------------------------------------------------------------
# _cmd_create
# ---------------------------------------------------------------------------


def test_cmd_create_generates_migration_file(tmp_path: Path) -> None:
    config_file = tmp_path / "mongrator.toml"
    migrations_dir = tmp_path / "migrations"
    config_file.write_text(
        f'[mongrator]\nuri = "mongodb://localhost"\ndatabase = "db"\nmigrations_dir = \'{migrations_dir.as_posix()}\'\n'
    )
    result = runner.invoke(cli, ["--config", str(config_file), "create", "add_users_index"])
    assert result.exit_code == 0
    created_files = list(migrations_dir.glob("*.py"))
    assert len(created_files) == 1
    assert "add_users_index" in created_files[0].name


# ---------------------------------------------------------------------------
# _cmd_status
# ---------------------------------------------------------------------------


def test_cmd_status_no_migrations() -> None:
    mock_run = MagicMock()
    mock_run.status.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "No migrations found" in result.output


def test_cmd_status_shows_applied_and_pending() -> None:
    statuses = [
        MigrationStatus(id="001_a", applied=True, applied_at=datetime(2025, 1, 1, tzinfo=UTC)),
        MigrationStatus(id="002_b", applied=False),
    ]
    mock_run = MagicMock()
    mock_run.status.return_value = statuses
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "001_a" in result.output
    assert "applied" in result.output
    assert "pending" in result.output


def test_cmd_status_shows_orphaned() -> None:
    statuses = [
        MigrationStatus(id="001_a", applied=True, applied_at=datetime(2025, 1, 1, tzinfo=UTC)),
        MigrationStatus(id="002_deleted", applied=True, applied_at=datetime(2025, 1, 2, tzinfo=UTC), orphaned=True),
    ]
    mock_run = MagicMock()
    mock_run.status.return_value = statuses
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "ORPHANED" in result.output
    assert "002_deleted" in result.output


# ---------------------------------------------------------------------------
# _cmd_up
# ---------------------------------------------------------------------------


def test_cmd_up_applies_migrations() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = ["001_a", "002_b"]
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["up"])
    assert result.exit_code == 0
    assert "001_a" in result.output
    assert "002_b" in result.output


def test_cmd_up_nothing_to_apply() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["up"])
    assert result.exit_code == EXIT_NOTHING_TO_DO
    assert "Nothing to apply" in result.output


def test_cmd_up_dry_run_with_pending() -> None:
    mock_run = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = [MagicMock(id="001_a")]
    mock_run.plan_up.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["up", "--dry-run"])
    assert result.exit_code == 0
    assert "001_a" in result.output


def test_cmd_up_dry_run_nothing_to_apply() -> None:
    mock_run = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = []
    mock_run.plan_up.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["up", "--dry-run"])
    assert result.exit_code == EXIT_NOTHING_TO_DO
    assert "Nothing to apply" in result.output


def test_cmd_up_passes_transactional_false_by_default() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["up"])
    mock_run.up.assert_called_once_with(target=None, transactional=False)


def test_cmd_up_passes_transactional_true() -> None:
    mock_run = MagicMock()
    mock_run.up.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["up", "--transactional"])
    mock_run.up.assert_called_once_with(target=None, transactional=True)


# ---------------------------------------------------------------------------
# _cmd_down
# ---------------------------------------------------------------------------


def test_cmd_down_rolls_back() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = ["002_b"]
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["down"])
    assert result.exit_code == 0
    assert "002_b" in result.output
    assert "rolled back" in result.output


def test_cmd_down_nothing_to_rollback() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["down"])
    assert result.exit_code == EXIT_NOTHING_TO_DO
    assert "Nothing to roll back" in result.output


def test_cmd_down_dry_run_with_pending() -> None:
    mock_run = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = [MagicMock(id="002_b")]
    mock_run.plan_down.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["down", "--dry-run"])
    assert result.exit_code == 0
    assert "002_b" in result.output


def test_cmd_down_dry_run_nothing_to_rollback() -> None:
    mock_run = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = []
    mock_run.plan_down.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["down", "--dry-run"])
    assert result.exit_code == EXIT_NOTHING_TO_DO
    assert "Nothing to roll back" in result.output


def test_cmd_down_passes_transactional_false_by_default() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["down"])
    mock_run.down.assert_called_once_with(steps=1, transactional=False)


def test_cmd_down_passes_transactional_true() -> None:
    mock_run = MagicMock()
    mock_run.down.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        runner.invoke(cli, ["down", "--transactional"])
    mock_run.down.assert_called_once_with(steps=1, transactional=True)


# ---------------------------------------------------------------------------
# _cmd_validate
# ---------------------------------------------------------------------------


def test_cmd_validate_all_ok() -> None:
    mock_run = MagicMock()
    mock_run.validate.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["validate"])
    assert result.exit_code == 0
    assert "valid checksums" in result.output


def test_cmd_validate_reports_mismatches() -> None:
    errors = [ChecksumMismatchError("001_a", "expected", "actual")]
    mock_run = MagicMock()
    mock_run.validate.return_value = errors
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_run),
    ):
        result = runner.invoke(cli, ["validate"])
    assert result.exit_code == 1
    assert "001_a" in result.stderr


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_dispatches_init(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["mongrator", "init"])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0


def test_main_handles_migrator_error(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr("sys.argv", ["mongrator", "status"])
    with (
        patch("mongrator.cli._load_config", side_effect=MigratorError("test error")),
        pytest.raises(SystemExit) as exc_info,
    ):
        main()
    assert exc_info.value.code == 1
    assert "test error" in capsys.readouterr().err
