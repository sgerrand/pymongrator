"""Unit tests for mongrator.planner — pure logic, no database required."""

from __future__ import annotations

from pathlib import Path

import pytest

from mongrator.exceptions import MigrationNotFoundError
from mongrator.migration import MigrationFile
from mongrator.planner import plan_down, plan_up


def _file(migration_id: str) -> MigrationFile:
    return MigrationFile(id=migration_id, path=Path(f"{migration_id}.py"), checksum="abc")


# ---------------------------------------------------------------------------
# plan_up
# ---------------------------------------------------------------------------


def test_plan_up_all_pending() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    plan = plan_up(files, applied=set())
    assert [f.id for f in plan.to_apply] == ["001_a", "002_b", "003_c"]
    assert plan.to_skip == []


def test_plan_up_all_applied() -> None:
    files = [_file("001_a"), _file("002_b")]
    plan = plan_up(files, applied={"001_a", "002_b"})
    assert plan.to_apply == []
    assert [f.id for f in plan.to_skip] == ["001_a", "002_b"]


def test_plan_up_partial() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    plan = plan_up(files, applied={"001_a"})
    assert [f.id for f in plan.to_apply] == ["002_b", "003_c"]
    assert [f.id for f in plan.to_skip] == ["001_a"]


def test_plan_up_empty_files() -> None:
    plan = plan_up([], applied=set())
    assert plan.to_apply == []
    assert plan.to_skip == []


def test_plan_up_with_target() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    plan = plan_up(files, applied=set(), target="002_b")
    assert [f.id for f in plan.to_apply] == ["001_a", "002_b"]
    # 003_c is beyond target so not included at all
    assert all(f.id != "003_c" for f in plan.to_apply + plan.to_skip)


def test_plan_up_target_already_applied() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    plan = plan_up(files, applied={"001_a", "002_b"}, target="002_b")
    assert plan.to_apply == []
    assert [f.id for f in plan.to_skip] == ["001_a", "002_b"]


def test_plan_up_target_not_found() -> None:
    files = [_file("001_a"), _file("002_b")]
    with pytest.raises(MigrationNotFoundError):
        plan_up(files, applied=set(), target="999_missing")


def test_plan_up_target_first_migration() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    plan = plan_up(files, applied=set(), target="001_a")
    assert [f.id for f in plan.to_apply] == ["001_a"]


def test_plan_up_applied_not_in_files() -> None:
    # Applied set contains IDs not present in files (orphaned records) — should be ignored.
    files = [_file("001_a"), _file("002_b")]
    plan = plan_up(files, applied={"999_orphan"})
    assert [f.id for f in plan.to_apply] == ["001_a", "002_b"]


# ---------------------------------------------------------------------------
# plan_down
# ---------------------------------------------------------------------------


def test_plan_down_single_step() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    plan = plan_down(files, applied={"001_a", "002_b", "003_c"}, steps=1)
    assert [f.id for f in plan.to_apply] == ["003_c"]
    assert [f.id for f in plan.to_skip] == ["001_a", "002_b"]


def test_plan_down_multiple_steps() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    plan = plan_down(files, applied={"001_a", "002_b", "003_c"}, steps=2)
    assert [f.id for f in plan.to_apply] == ["003_c", "002_b"]
    assert [f.id for f in plan.to_skip] == ["001_a"]


def test_plan_down_more_steps_than_applied() -> None:
    files = [_file("001_a"), _file("002_b")]
    plan = plan_down(files, applied={"001_a", "002_b"}, steps=10)
    assert [f.id for f in plan.to_apply] == ["002_b", "001_a"]
    assert plan.to_skip == []


def test_plan_down_none_applied() -> None:
    files = [_file("001_a"), _file("002_b")]
    plan = plan_down(files, applied=set(), steps=1)
    assert plan.to_apply == []
    assert plan.to_skip == []


def test_plan_down_invalid_steps() -> None:
    files = [_file("001_a")]
    with pytest.raises(ValueError):
        plan_down(files, applied={"001_a"}, steps=0)


def test_plan_down_rollback_order_is_reversed() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c"), _file("004_d")]
    plan = plan_down(files, applied={"001_a", "002_b", "003_c", "004_d"}, steps=3)
    # Should roll back 004_d, 003_c, 002_b in that order
    assert [f.id for f in plan.to_apply] == ["004_d", "003_c", "002_b"]


def test_plan_down_partial_applied_set() -> None:
    files = [_file("001_a"), _file("002_b"), _file("003_c")]
    # Only 001 and 003 are applied (002 was skipped or unapplied)
    plan = plan_down(files, applied={"001_a", "003_c"}, steps=1)
    # Most recent applied is 003_c
    assert [f.id for f in plan.to_apply] == ["003_c"]
    assert [f.id for f in plan.to_skip] == ["001_a"]
