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

from .config import MigratorConfig
from .exceptions import MigratorError


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

    # down
    p_down = sub.add_parser("down", help="roll back applied migrations")
    p_down.add_argument(
        "--steps", type=int, default=1, metavar="N", help="number of migrations to roll back (default: 1)"
    )
    p_down.add_argument("--async", dest="use_async", action="store_true", help="use async runner")

    # validate
    sub.add_parser("validate", help="verify checksums of applied migration files")

    return parser


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
    try:
        import pymongo
    except ImportError:
        print("error: pymongo is required. Install with: pip install pymongo", file=sys.stderr)
        return 1

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
        return asyncio.run(_async_up(config, args.target))
    import pymongo

    from .runner import SyncRunner

    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        applied = runner.up(target=args.target)
    if applied:
        for mid in applied:
            print(f"  applied  {mid}")
    else:
        print("Nothing to apply.")
    return 0


async def _async_up(config: MigratorConfig, target: str | None) -> int:
    import pymongo
    from pymongo import AsyncMongoClient

    from .runner import AsyncRunner

    with pymongo.MongoClient(config.uri) as sync_client:
        async with AsyncMongoClient(config.uri) as client:
            runner = AsyncRunner(client, config, sync_client=sync_client)
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
        return asyncio.run(_async_down(config, args.steps))
    import pymongo

    from .runner import SyncRunner

    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        rolled_back = runner.down(steps=args.steps)
    if rolled_back:
        for mid in rolled_back:
            print(f"  rolled back  {mid}")
    else:
        print("Nothing to roll back.")
    return 0


async def _async_down(config: MigratorConfig, steps: int) -> int:
    import pymongo
    from pymongo import AsyncMongoClient

    from .runner import AsyncRunner

    with pymongo.MongoClient(config.uri) as sync_client:
        async with AsyncMongoClient(config.uri) as client:
            runner = AsyncRunner(client, config, sync_client=sync_client)
            rolled_back = await runner.down(steps=steps)
    if rolled_back:
        for mid in rolled_back:
            print(f"  rolled back  {mid}")
    else:
        print("Nothing to roll back.")
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    import pymongo

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
