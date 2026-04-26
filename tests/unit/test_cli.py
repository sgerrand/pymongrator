"""Unit tests for mongrator.cli — argument parsing and command handlers."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mongrator.cli import (
    EXIT_NOTHING_TO_DO,
    _build_parser,
    _cmd_create,
    _cmd_down,
    _cmd_init,
    _cmd_status,
    _cmd_up,
    _cmd_validate,
    _load_config,
    main,
)
from mongrator.exceptions import ChecksumMismatchError, MigratorError
from mongrator.migration import MigrationStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse(*args: str):
    return _build_parser().parse_args(args)


def parse_fails(*args: str) -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(args)


# ---------------------------------------------------------------------------
# Global options
# ---------------------------------------------------------------------------


def test_default_config() -> None:
    ns = parse("init")
    assert ns.config == "mongrator.toml"


def test_custom_config() -> None:
    ns = parse("--config", "custom.toml", "init")
    assert ns.config == "custom.toml"


def test_missing_command_exits() -> None:
    parse_fails()


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


def test_init_command() -> None:
    ns = parse("init")
    assert ns.command == "init"


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


def test_create_command_with_name() -> None:
    ns = parse("create", "add_users_email_index")
    assert ns.command == "create"
    assert ns.name == "add_users_email_index"


def test_create_missing_name_exits() -> None:
    parse_fails("create")


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def test_status_command() -> None:
    ns = parse("status")
    assert ns.command == "status"


# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------


def test_up_defaults() -> None:
    ns = parse("up")
    assert ns.command == "up"
    assert ns.target is None
    assert ns.use_async is False


def test_up_with_target() -> None:
    ns = parse("up", "--target", "002_b")
    assert ns.target == "002_b"


def test_up_async_flag() -> None:
    ns = parse("up", "--async")
    assert ns.use_async is True


def test_up_target_and_async() -> None:
    ns = parse("up", "--target", "002_b", "--async")
    assert ns.target == "002_b"
    assert ns.use_async is True


def test_up_dry_run_flag() -> None:
    ns = parse("up", "--dry-run")
    assert ns.dry_run is True


def test_up_dry_run_default() -> None:
    ns = parse("up")
    assert ns.dry_run is False


# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------


def test_down_defaults() -> None:
    ns = parse("down")
    assert ns.command == "down"
    assert ns.steps == 1
    assert ns.use_async is False


def test_down_steps() -> None:
    ns = parse("down", "--steps", "3")
    assert ns.steps == 3


def test_down_async_flag() -> None:
    ns = parse("down", "--async")
    assert ns.use_async is True


def test_down_dry_run_flag() -> None:
    ns = parse("down", "--dry-run")
    assert ns.dry_run is True


def test_down_dry_run_default() -> None:
    ns = parse("down")
    assert ns.dry_run is False


def test_down_invalid_steps_type_exits() -> None:
    parse_fails("down", "--steps", "not_a_number")


def test_down_zero_steps_exits() -> None:
    parse_fails("down", "--steps", "0")


def test_down_negative_steps_exits() -> None:
    parse_fails("down", "--steps", "-1")


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


def test_validate_command() -> None:
    ns = parse("validate")
    assert ns.command == "validate"


# ---------------------------------------------------------------------------
# _load_config
# ---------------------------------------------------------------------------


def test_load_config_from_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "mongrator.toml"
    config_file.write_text('[mongrator]\nuri = "mongodb://localhost:27017"\ndatabase = "testdb"\n')
    ns = parse("--config", str(config_file), "init")
    config = _load_config(ns)
    assert config.database == "testdb"


def test_load_config_from_env_when_file_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGRATOR_URI", "mongodb://localhost:27017")
    monkeypatch.setenv("MONGRATOR_DB", "envdb")
    ns = parse("--config", "nonexistent.toml", "init")
    config = _load_config(ns)
    assert config.database == "envdb"


# ---------------------------------------------------------------------------
# _cmd_init
# ---------------------------------------------------------------------------


def test_cmd_init_creates_config_and_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    ns = parse("init")
    rc = _cmd_init(ns)
    assert rc == 0
    assert (tmp_path / "mongrator.toml").exists()
    assert (tmp_path / "migrations").is_dir()


def test_cmd_init_does_not_overwrite_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "mongrator.toml"
    config_file.write_text("existing content")
    ns = parse("init")
    _cmd_init(ns)
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
    ns = parse("--config", str(config_file), "create", "add_users_index")
    rc = _cmd_create(ns)
    assert rc == 0
    files = list(migrations_dir.glob("*.py"))
    assert len(files) == 1
    assert "add_users_index" in files[0].name


# ---------------------------------------------------------------------------
# _cmd_status
# ---------------------------------------------------------------------------


def test_cmd_status_no_migrations(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_runner.status.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("status")
        rc = _cmd_status(ns)
    assert rc == 0
    assert "No migrations found" in capsys.readouterr().out


def test_cmd_status_shows_applied_and_pending(capsys: pytest.CaptureFixture[str]) -> None:
    statuses = [
        MigrationStatus(id="001_a", applied=True, applied_at=datetime(2025, 1, 1, tzinfo=UTC)),
        MigrationStatus(id="002_b", applied=False),
    ]
    mock_runner = MagicMock()
    mock_runner.status.return_value = statuses
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("status")
        rc = _cmd_status(ns)
    assert rc == 0
    out = capsys.readouterr().out
    assert "001_a" in out
    assert "applied" in out
    assert "pending" in out


def test_cmd_status_shows_orphaned(capsys: pytest.CaptureFixture[str]) -> None:
    statuses = [
        MigrationStatus(id="001_a", applied=True, applied_at=datetime(2025, 1, 1, tzinfo=UTC)),
        MigrationStatus(id="002_deleted", applied=True, applied_at=datetime(2025, 1, 2, tzinfo=UTC), orphaned=True),
    ]
    mock_runner = MagicMock()
    mock_runner.status.return_value = statuses
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("status")
        rc = _cmd_status(ns)
    assert rc == 0
    out = capsys.readouterr().out
    assert "ORPHANED" in out
    assert "002_deleted" in out


# ---------------------------------------------------------------------------
# _cmd_up
# ---------------------------------------------------------------------------


def test_cmd_up_applies_migrations(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_runner.up.return_value = ["001_a", "002_b"]
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("up")
        rc = _cmd_up(ns)
    assert rc == 0
    out = capsys.readouterr().out
    assert "001_a" in out
    assert "002_b" in out


def test_cmd_up_nothing_to_apply(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_runner.up.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("up")
        rc = _cmd_up(ns)
    assert rc == EXIT_NOTHING_TO_DO
    assert "Nothing to apply" in capsys.readouterr().out


def test_cmd_up_dry_run_with_pending(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = [MagicMock(id="001_a")]
    mock_runner.plan_up.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("up", "--dry-run")
        rc = _cmd_up(ns)
    assert rc == 0
    assert "001_a" in capsys.readouterr().out


def test_cmd_up_dry_run_nothing_to_apply(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = []
    mock_runner.plan_up.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("up", "--dry-run")
        rc = _cmd_up(ns)
    assert rc == EXIT_NOTHING_TO_DO
    assert "Nothing to apply" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# _cmd_down
# ---------------------------------------------------------------------------


def test_cmd_down_rolls_back(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_runner.down.return_value = ["002_b"]
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("down")
        rc = _cmd_down(ns)
    assert rc == 0
    out = capsys.readouterr().out
    assert "002_b" in out
    assert "rolled back" in out


def test_cmd_down_nothing_to_rollback(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_runner.down.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("down")
        rc = _cmd_down(ns)
    assert rc == EXIT_NOTHING_TO_DO
    assert "Nothing to roll back" in capsys.readouterr().out


def test_cmd_down_dry_run_with_pending(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = [MagicMock(id="002_b")]
    mock_runner.plan_down.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("down", "--dry-run")
        rc = _cmd_down(ns)
    assert rc == 0
    assert "002_b" in capsys.readouterr().out


def test_cmd_down_dry_run_nothing_to_rollback(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_plan = MagicMock()
    mock_plan.to_apply = []
    mock_runner.plan_down.return_value = mock_plan
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("down", "--dry-run")
        rc = _cmd_down(ns)
    assert rc == EXIT_NOTHING_TO_DO
    assert "Nothing to roll back" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# _cmd_validate
# ---------------------------------------------------------------------------


def test_cmd_validate_all_ok(capsys: pytest.CaptureFixture[str]) -> None:
    mock_runner = MagicMock()
    mock_runner.validate.return_value = []
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("validate")
        rc = _cmd_validate(ns)
    assert rc == 0
    assert "valid checksums" in capsys.readouterr().out


def test_cmd_validate_reports_mismatches(capsys: pytest.CaptureFixture[str]) -> None:
    errors = [ChecksumMismatchError("001_a", "expected", "actual")]
    mock_runner = MagicMock()
    mock_runner.validate.return_value = errors
    with (
        patch("mongrator.cli._load_config"),
        patch("pymongo.MongoClient", return_value=MagicMock()),
        patch("mongrator.runner.SyncRunner", return_value=mock_runner),
    ):
        ns = parse("validate")
        rc = _cmd_validate(ns)
    assert rc == 1
    assert "001_a" in capsys.readouterr().err


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
