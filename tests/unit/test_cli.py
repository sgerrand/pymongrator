"""Unit tests for mongrator.cli — argument parsing only, no runner invocation."""

from __future__ import annotations

import pytest

from mongrator.cli import _build_parser

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


def test_down_invalid_steps_type_exits() -> None:
    parse_fails("down", "--steps", "not_a_number")


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


def test_validate_command() -> None:
    ns = parse("validate")
    assert ns.command == "validate"
