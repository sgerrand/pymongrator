"""Command-line interface for mongrator.

Subcommands:
    init     — create the migrations directory and a mongrator.toml stub
    create   — generate a new timestamped migration file
    status   — show applied/pending migration table
    up       — apply pending migrations
    down     — roll back applied migrations
    validate — verify checksums of applied migrations
"""

import argparse
import asyncio
import sys
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

import pymongo
from pymongo import AsyncMongoClient

from .config import MigratorConfig
from .exceptions import MigratorError
from .planner import MigrationPlan


def _positive_int(value: str) -> int:
    """Argparse type that accepts only positive integers (>= 1)."""
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid int value: '{value}'")
    if n < 1:
        raise argparse.ArgumentTypeError(f"steps must be >= 1, got {n}")
    return n


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mongrator",
        description="Lightweight MongoDB schema migration tool",
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default="mongrator.toml",
        help="path to config file (default: mongrator.toml)",
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # init
    sub.add_parser("init", help="create migrations directory and config stub")

    # create
    p_create = sub.add_parser("create", help="generate a new migration file")
    p_create.add_argument("name", help="short description, e.g. add_users_email_index")

    # status
    sub.add_parser("status", help="show applied/pending migration table")

    # up
    p_up = sub.add_parser("up", help="apply pending migrations")
    p_up.add_argument("--target", metavar="ID", help="apply only up to this migration ID")
    p_up.add_argument("--async", dest="use_async", action="store_true", help="use async runner")
    p_up.add_argument("--dry-run", action="store_true", help="show which migrations would be applied without executing")

    # down
    p_down = sub.add_parser("down", help="roll back applied migrations")
    p_down.add_argument(
        "--steps", type=_positive_int, default=1, metavar="N", help="number of migrations to roll back (default: 1)"
    )
    p_down.add_argument("--async", dest="use_async", action="store_true", help="use async runner")
    p_down.add_argument(
        "--dry-run", action="store_true", help="show which migrations would be rolled back without executing"
    )

    # validate
    sub.add_parser("validate", help="verify checksums of applied migration files")

    return parser


def _print_dry_run(plan: MigrationPlan, *, direction: str) -> None:
    """Print a dry-run summary for a MigrationPlan.

    Args:
        plan: The computed migration plan.
        direction: Either ``"up"`` or ``"down"``.
    """
    if direction == "up":
        action, empty_msg = "apply", "Nothing to apply."
        header = "Migrations that would be applied:"
    else:
        action, empty_msg = "rollback", "Nothing to roll back."
        header = "Migrations that would be rolled back:"

    if not plan.to_apply:
        print(empty_msg)
        return

    print(header)
    for m in plan.to_apply:
        print(f"  {action}  {m.id}")


def _load_config(args: argparse.Namespace) -> MigratorConfig:
    config_path = Path(args.config)
    if config_path.exists():
        return MigratorConfig.from_toml(config_path)
    return MigratorConfig.from_env()


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _cmd_init(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    if not config_path.exists():
        config_path.write_text(
            '[mongrator]\nuri = "mongodb://localhost:27017"\ndatabase = "mydb"\n'
            'migrations_dir = "migrations"\ncollection = "mongrator_migrations"\n'
        )
        print(f"Created {config_path}")

    migrations_dir = Path("migrations")
    migrations_dir.mkdir(exist_ok=True)
    print(f"Created {migrations_dir}/")
    return 0


def _cmd_create(args: argparse.Namespace) -> int:
    config = _load_config(args)
    config.migrations_dir.mkdir(parents=True, exist_ok=True)

    slug = args.name.strip().replace(" ", "_").lower()
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{slug}.py"
    dest = config.migrations_dir / filename

    template_text = files("mongrator._templates").joinpath("migration.py.tmpl").read_text(encoding="utf-8")
    content = template_text.format(slug=slug, timestamp=timestamp)
    dest.write_text(content, encoding="utf-8")
    print(f"Created {dest}")
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    from .runner import SyncRunner

    config = _load_config(args)
    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        statuses = runner.status()

    if not statuses:
        print("No migrations found.")
        return 0

    col_width = max(len(s.id) for s in statuses) + 2
    print(f"{'Migration':<{col_width}} {'Status':<10} {'Applied At'}")
    print("-" * (col_width + 30))
    for s in statuses:
        state = "applied" if s.applied else "pending"
        if s.applied and not s.checksum_ok:
            state = "MODIFIED"
        applied_at = s.applied_at.isoformat() if s.applied_at else "-"
        print(f"{s.id:<{col_width}} {state:<10} {applied_at}")
    return 0


def _cmd_up(args: argparse.Namespace) -> int:
    config = _load_config(args)
    if args.use_async:
        return asyncio.run(_async_up(config, args.target, dry_run=args.dry_run))
    from .runner import SyncRunner

    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        if args.dry_run:
            plan = runner.plan_up(target=args.target)
            _print_dry_run(plan, direction="up")
            return 0
        applied = runner.up(target=args.target)
    if applied:
        for mid in applied:
            print(f"  applied  {mid}")
    else:
        print("Nothing to apply.")
    return 0


async def _async_up(config: MigratorConfig, target: str | None, *, dry_run: bool = False) -> int:
    from .runner import AsyncRunner

    with pymongo.MongoClient(config.uri) as sync_client:
        async with AsyncMongoClient(config.uri) as client:
            runner = AsyncRunner(client, config, sync_client=sync_client)
            if dry_run:
                plan = await runner.plan_up(target=target)
                _print_dry_run(plan, direction="up")
                return 0
            applied = await runner.up(target=target)
    if applied:
        for mid in applied:
            print(f"  applied  {mid}")
    else:
        print("Nothing to apply.")
    return 0


def _cmd_down(args: argparse.Namespace) -> int:
    config = _load_config(args)
    if args.use_async:
        return asyncio.run(_async_down(config, args.steps, dry_run=args.dry_run))
    from .runner import SyncRunner

    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        if args.dry_run:
            plan = runner.plan_down(steps=args.steps)
            _print_dry_run(plan, direction="down")
            return 0
        rolled_back = runner.down(steps=args.steps)
    if rolled_back:
        for mid in rolled_back:
            print(f"  rolled back  {mid}")
    else:
        print("Nothing to roll back.")
    return 0


async def _async_down(config: MigratorConfig, steps: int, *, dry_run: bool = False) -> int:
    from .runner import AsyncRunner

    with pymongo.MongoClient(config.uri) as sync_client:
        async with AsyncMongoClient(config.uri) as client:
            runner = AsyncRunner(client, config, sync_client=sync_client)
            if dry_run:
                plan = await runner.plan_down(steps=steps)
                _print_dry_run(plan, direction="down")
                return 0
            rolled_back = await runner.down(steps=steps)
    if rolled_back:
        for mid in rolled_back:
            print(f"  rolled back  {mid}")
    else:
        print("Nothing to roll back.")
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    from .runner import SyncRunner

    config = _load_config(args)
    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        errors = runner.validate()

    if not errors:
        print("All applied migrations have valid checksums.")
        return 0

    eg = ExceptionGroup("Checksum mismatches detected", errors)
    print(f"error: {eg}", file=sys.stderr)
    for e in errors:
        print(f"  {e}", file=sys.stderr)
    return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    dispatch = {
        "init": _cmd_init,
        "create": _cmd_create,
        "status": _cmd_status,
        "up": _cmd_up,
        "down": _cmd_down,
        "validate": _cmd_validate,
    }

    try:
        rc = dispatch[args.command](args)
    except MigratorError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)

    sys.exit(rc)
