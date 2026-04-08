# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`mongrator` is a lightweight MongoDB schema migration tool. The package entry point is `mongrator.main()`, exposed as the `mongrator` CLI command.

- Python 3.13+
- Build system: `uv_build`
- Runtime dependency: `pymongo>=4.10` (async via `pymongo.AsyncMongoClient`, no Motor)

## Commands

```sh
# Install dependencies and set up environment
uv sync --group dev

# Run the CLI
uv run mongrator

# Run tests
uv run pytest

# Run a single test
uv run pytest path/to/test_file.py::test_name

# Lint, format check, and type check (run all before committing)
uvx ruff check
uvx ruff format --check
uvx ty check

# Build the package
uv build
```

## Code style

- Do not add `from __future__ import annotations` — this project requires Python 3.13+, where `X | Y` union types and postponed evaluation are available natively.

## Structure

Source lives under `src/mongrator/`. Key modules:

- `cli.py` — argparse CLI; no business logic, delegates to runners
- `config.py` — `MigratorConfig` frozen dataclass; `from_toml()` / `from_env()`
- `runner.py` — `SyncRunner` (pymongo `MongoClient`) and `AsyncRunner` (`AsyncMongoClient`)
- `state.py` — `SyncStateStore` / `AsyncMongoStateStore` backed by the tracking collection
- `planner.py` — pure logic (no I/O); `plan_up()` / `plan_down()`
- `loader.py` — scans migrations dir, imports `.py` files, computes SHA-256 checksums
- `ops.py` — declarative helpers (`create_index`, `rename_field`, etc.) with auto-rollback
- `migration.py` — shared datatypes: `MigrationFile`, `MigrationRecord`, `MigrationStatus`
- `exceptions.py` — `MigratorError` hierarchy
