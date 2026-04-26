"""Command-line interface for mongrator.

Subcommands:
    init     — create the migrations directory and a mongrator.toml stub
    create   — generate a new timestamped migration file
    status   — show applied/pending migration table
    up       — apply pending migrations
    down     — roll back applied migrations
    validate — verify checksums of applied migrations

Exit codes:
    0   — success (changes applied / rolled back, or read-only command succeeded)
    1   — runtime error (MigratorError)
    2   — CLI usage error (invalid arguments, missing subcommand)
    4   — already up-to-date (no migrations to apply or roll back)
    130 — interrupted (KeyboardInterrupt / Ctrl-C)
"""

import asyncio
import sys
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

import click
import pymongo
from pymongo import AsyncMongoClient

from .config import MigratorConfig
from .exceptions import MigratorError
from .planner import MigrationPlan

#: Exit code returned when there are no migrations to apply or roll back.
#: Deliberately avoids ``2``, which click uses for CLI usage errors.
EXIT_NOTHING_TO_DO = 4


class _PositiveInt(click.ParamType):
    """Click parameter type that accepts only positive integers (>= 1)."""

    name = "positive_int"

    def convert(self, value: str, param: click.Parameter | None, ctx: click.Context | None) -> int:
        try:
            n = int(value)
        except (ValueError, TypeError):
            self.fail(f"invalid int value: '{value}'", param, ctx)
        if n < 1:
            self.fail(f"steps must be >= 1, got {n}", param, ctx)
        return n


POSITIVE_INT = _PositiveInt()


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
        click.echo(empty_msg)
        return

    click.echo(header)
    for m in plan.to_apply:
        click.echo(f"  {action}  {m.id}")


def _load_config(config_path: str) -> MigratorConfig:
    path = Path(config_path)
    if path.exists():
        return MigratorConfig.from_toml(path)
    dotenv_path = Path(".env")
    return MigratorConfig.from_env(dotenv_path=dotenv_path if dotenv_path.is_file() else None)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--config", "config_path", default="mongrator.toml", metavar="PATH", help="Path to config file.")
@click.pass_context
def cli(ctx: click.Context, config_path: str) -> None:
    """Lightweight MongoDB schema migration tool."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path
    if ctx.invoked_subcommand is None:
        raise click.UsageError("Missing command.", ctx=ctx)


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Create migrations directory and config stub."""
    config_path = Path(ctx.obj["config_path"])
    if not config_path.exists():
        config_path.write_text(
            '[mongrator]\nuri = "mongodb://localhost:27017"\ndatabase = "mydb"\n'
            'migrations_dir = "migrations"\ncollection = "mongrator_migrations"\n'
        )
        click.echo(f"Created {config_path}")

    migrations_dir = Path("migrations")
    migrations_dir.mkdir(exist_ok=True)
    click.echo(f"Created {migrations_dir}/")


@cli.command()
@click.argument("name")
@click.pass_context
def create(ctx: click.Context, name: str) -> None:
    """Generate a new migration file."""
    config = _load_config(ctx.obj["config_path"])
    config.migrations_dir.mkdir(parents=True, exist_ok=True)

    slug = name.strip().replace(" ", "_").lower()
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{slug}.py"
    dest = config.migrations_dir / filename

    template_text = files("mongrator._templates").joinpath("migration.py.tmpl").read_text(encoding="utf-8")
    content = template_text.format(slug=slug, timestamp=timestamp)
    dest.write_text(content, encoding="utf-8")
    click.echo(f"Created {dest}")


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show applied/pending migration table."""
    from .runner import SyncRunner

    config = _load_config(ctx.obj["config_path"])
    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        statuses = runner.status()

    if not statuses:
        click.echo("No migrations found.")
        return

    col_width = max(len(s.id) for s in statuses) + 2
    click.echo(f"{'Migration':<{col_width}} {'Status':<10} {'Applied At'}")
    click.echo("-" * (col_width + 30))
    for s in statuses:
        if s.orphaned:
            state = "ORPHANED"
        elif s.applied and not s.checksum_ok:
            state = "MODIFIED"
        elif s.applied:
            state = "applied"
        else:
            state = "pending"
        applied_at = s.applied_at.isoformat() if s.applied_at else "-"
        click.echo(f"{s.id:<{col_width}} {state:<10} {applied_at}")


@cli.command()
@click.option("--target", metavar="ID", default=None, help="Apply only up to this migration ID.")
@click.option("--async", "use_async", is_flag=True, default=False, help="Use async runner.")
@click.option("--dry-run", is_flag=True, default=False, help="Show migrations that would be applied.")
@click.option(
    "--transactional",
    is_flag=True,
    default=False,
    help="Wrap each migration in a MongoDB transaction (requires replica set).",
)
@click.pass_context
def up(ctx: click.Context, target: str | None, use_async: bool, dry_run: bool, transactional: bool) -> None:
    """Apply pending migrations."""
    config = _load_config(ctx.obj["config_path"])
    if use_async:
        rc = asyncio.run(_async_up(config, target, dry_run=dry_run, transactional=transactional))
        ctx.exit(rc)
        return
    from .runner import SyncRunner

    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        if dry_run:
            plan = runner.plan_up(target=target)
            _print_dry_run(plan, direction="up")
            ctx.exit(EXIT_NOTHING_TO_DO if not plan.to_apply else 0)
            return
        applied = runner.up(target=target, transactional=transactional)
    if applied:
        for mid in applied:
            click.echo(f"  applied  {mid}")
        return
    click.echo("Nothing to apply.")
    ctx.exit(EXIT_NOTHING_TO_DO)


async def _async_up(
    config: MigratorConfig,
    target: str | None,
    *,
    dry_run: bool = False,
    transactional: bool = False,
) -> int:
    from .runner import AsyncRunner

    with pymongo.MongoClient(config.uri) as sync_client:
        async with AsyncMongoClient(config.uri) as client:
            runner = AsyncRunner(client, config, sync_client=sync_client)
            if dry_run:
                plan = await runner.plan_up(target=target)
                _print_dry_run(plan, direction="up")
                return EXIT_NOTHING_TO_DO if not plan.to_apply else 0
            applied = await runner.up(target=target, transactional=transactional)
    if applied:
        for mid in applied:
            click.echo(f"  applied  {mid}")
        return 0
    click.echo("Nothing to apply.")
    return EXIT_NOTHING_TO_DO


@cli.command()
@click.option("--steps", type=POSITIVE_INT, default=1, metavar="N", help="Number of migrations to roll back.")
@click.option("--async", "use_async", is_flag=True, default=False, help="Use async runner.")
@click.option("--dry-run", is_flag=True, default=False, help="Show migrations that would be rolled back.")
@click.option(
    "--transactional",
    is_flag=True,
    default=False,
    help="Wrap each migration in a MongoDB transaction (requires replica set).",
)
@click.pass_context
def down(ctx: click.Context, steps: int, use_async: bool, dry_run: bool, transactional: bool) -> None:
    """Roll back applied migrations."""
    config = _load_config(ctx.obj["config_path"])
    if use_async:
        rc = asyncio.run(_async_down(config, steps, dry_run=dry_run, transactional=transactional))
        ctx.exit(rc)
        return
    from .runner import SyncRunner

    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        if dry_run:
            plan = runner.plan_down(steps=steps)
            _print_dry_run(plan, direction="down")
            ctx.exit(EXIT_NOTHING_TO_DO if not plan.to_apply else 0)
            return
        rolled_back = runner.down(steps=steps, transactional=transactional)
    if rolled_back:
        for mid in rolled_back:
            click.echo(f"  rolled back  {mid}")
        return
    click.echo("Nothing to roll back.")
    ctx.exit(EXIT_NOTHING_TO_DO)


async def _async_down(
    config: MigratorConfig,
    steps: int,
    *,
    dry_run: bool = False,
    transactional: bool = False,
) -> int:
    from .runner import AsyncRunner

    with pymongo.MongoClient(config.uri) as sync_client:
        async with AsyncMongoClient(config.uri) as client:
            runner = AsyncRunner(client, config, sync_client=sync_client)
            if dry_run:
                plan = await runner.plan_down(steps=steps)
                _print_dry_run(plan, direction="down")
                return EXIT_NOTHING_TO_DO if not plan.to_apply else 0
            rolled_back = await runner.down(steps=steps, transactional=transactional)
    if rolled_back:
        for mid in rolled_back:
            click.echo(f"  rolled back  {mid}")
        return 0
    click.echo("Nothing to roll back.")
    return EXIT_NOTHING_TO_DO


@cli.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """Verify checksums of applied migration files."""
    from .runner import SyncRunner

    config = _load_config(ctx.obj["config_path"])
    with pymongo.MongoClient(config.uri) as client:
        runner = SyncRunner(client, config)
        errors = runner.validate()

    if not errors:
        click.echo("All applied migrations have valid checksums.")
        return

    eg = ExceptionGroup("Checksum mismatches detected", errors)
    click.echo(f"error: {eg}", err=True)
    for e in errors:
        click.echo(f"  {e}", err=True)
    ctx.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    try:
        cli(standalone_mode=False)
    except click.exceptions.Abort:
        sys.exit(130)
    except click.exceptions.Exit as e:
        sys.exit(e.exit_code)
    except click.exceptions.UsageError as e:
        if e.ctx:
            click.echo(e.ctx.command.get_usage(e.ctx), err=True)
        click.echo(f"error: {e.format_message()}", err=True)
        sys.exit(2)
    except MigratorError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
    else:
        sys.exit(0)
