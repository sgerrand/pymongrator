from __future__ import annotations

from dataclasses import dataclass

from .exceptions import MigrationNotFoundError
from .migration import MigrationFile, MigrationId


@dataclass
class MigrationPlan:
    to_apply: list[MigrationFile]
    to_skip: list[MigrationFile]


def plan_up(
    files: list[MigrationFile],
    applied: set[MigrationId],
    target: MigrationId | None = None,
) -> MigrationPlan:
    """Compute which migrations need to be applied in the forward direction.

    Args:
        files:   All known migration files, in chronological order.
        applied: Set of migration IDs already recorded in the tracking collection.
        target:  If given, apply only up to and including this migration ID.
                 Raises MigrationNotFoundError if target is not in files.

    Returns:
        A MigrationPlan with to_apply (pending) and to_skip (already applied).
    """
    if target is not None:
        ids = {f.id for f in files}
        if target not in ids:
            raise MigrationNotFoundError(target)

    to_apply: list[MigrationFile] = []
    to_skip: list[MigrationFile] = []

    for f in files:
        if f.id in applied:
            to_skip.append(f)
        else:
            to_apply.append(f)
        if target is not None and f.id == target:
            break

    return MigrationPlan(to_apply=to_apply, to_skip=to_skip)


def plan_down(
    files: list[MigrationFile],
    applied: set[MigrationId],
    steps: int = 1,
) -> MigrationPlan:
    """Compute which migrations to roll back.

    Rolls back the most recently applied migrations, up to `steps` of them.
    Migrations are identified by their position in the applied set intersected
    with the ordered file list (file order is the canonical order).

    Args:
        files:   All known migration files, in chronological order.
        applied: Set of migration IDs already recorded in the tracking collection.
        steps:   Number of most-recent applied migrations to roll back.

    Returns:
        A MigrationPlan where to_apply contains the migrations to roll back
        (in reverse order) and to_skip contains those left untouched.
    """
    if steps < 1:
        raise ValueError(f"steps must be >= 1, got {steps}")

    applied_in_order = [f for f in files if f.id in applied]
    to_rollback = list(reversed(applied_in_order[-steps:]))
    rollback_ids = {f.id for f in to_rollback}
    to_skip = [f for f in files if f.id in applied and f.id not in rollback_ids]

    return MigrationPlan(to_apply=to_rollback, to_skip=to_skip)
