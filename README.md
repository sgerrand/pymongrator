# mongrator

[![PyPI Version](https://img.shields.io/pypi/v/mongrator)](https://pypi.org/project/mongrator)
[![Python Versions](https://img.shields.io/pypi/pyversions/mongrator)](https://pypi.org/project/mongrator)
[![Monthly Downloads](https://static.pepy.tech/badge/mongrator/month)](https://pepy.tech/project/mongrator)
[![CI](https://github.com/sgerrand/pymongrator/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sgerrand/pymongrator/actions/workflows/ci.yml)

Lightweight MongoDB schema migration tool with synchronous and asynchronous PyMongo support.

## Installation

```sh
pip install mongrator
```

## Quick start

```sh
# Create config and migrations directory
mongrator init

# Generate a new migration file
mongrator create add_users_email_index

# Check migration status
mongrator status

# Apply pending migrations
mongrator up

# Roll back the last migration
mongrator down
```

## Configuration

`mongrator init` creates a `mongrator.toml` stub:

```toml
uri = "mongodb://localhost:27017"
database = "mydb"
migrations_dir = "migrations"
collection = "mongrator_migrations"  # optional
```

Alternatively, configure via environment variables:

| Variable | Description | Required |
|----------|-------------|----------|
| `MONGRATOR_URI` | MongoDB connection URI | yes |
| `MONGRATOR_DB` | Database name | yes |
| `MONGRATOR_MIGRATIONS_DIR` | Path to migrations directory | no (default: `migrations`) |
| `MONGRATOR_COLLECTION` | Tracking collection name | no (default: `mongrator_migrations`) |

## Writing migrations

Migration files are plain Python named `{timestamp}_{slug}.py` (e.g. `20260408_143022_add_users_email_index.py`). Each file must define an `up(db)` function. A `down(db)` function is optional but enables rollback.

### Using the ops helpers (recommended)

The `ops` helpers record their own inverses, so `down()` is generated automatically:

```python
from mongrator import ops

def up(db):
    return [
        ops.create_index("users", {"email": 1}, unique=True),
        ops.rename_field("users", "username", "handle"),
        ops.add_field("users", "verified", default_value=False),
    ]
```

### Using plain PyMongo

For complex logic, write directly against the `db` argument and define `down()` manually:

```python
def up(db):
    db["orders"].update_many(
        {"status": {"$exists": False}},
        {"$set": {"status": "pending"}},
    )

def down(db):
    db["orders"].update_many({}, {"$unset": {"status": ""}})
```

### Available ops helpers

| Helper | Reversible | Description |
|--------|-----------|-------------|
| `ops.create_index(collection, keys, **kwargs)` | yes | Create an index |
| `ops.drop_index(collection, index_name)` | no | Drop an index by name |
| `ops.rename_field(collection, old, new, filter=None)` | yes | Rename a field across documents |
| `ops.add_field(collection, field, default_value, filter=None)` | yes | Add a field with a default value |

## CLI reference

```
mongrator init                        create migrations dir and mongrator.toml
mongrator create <name>               generate a new migration file
mongrator status                      show applied/pending migrations
mongrator up [--target ID]            apply pending migrations
mongrator up --async [--target ID]    apply using async runner
mongrator down [--steps N]            roll back N migrations (default: 1)
mongrator down --async [--steps N]    roll back using async runner
mongrator validate                    verify checksums of applied migrations
mongrator --config PATH <command>     use an alternate config file
```

## Async usage

Pass `--async` to `up` or `down` to use the async runner (backed by `pymongo.AsyncMongoClient`):

```sh
mongrator up --async
```

To use the runners programmatically:

```python
# Synchronous
from pathlib import Path
import pymongo
from mongrator.config import MigratorConfig
from mongrator.runner import SyncRunner

config = MigratorConfig(uri="mongodb://localhost:27017", database="mydb", migrations_dir=Path("migrations"))
runner = SyncRunner(pymongo.MongoClient(config.uri), config)
runner.up()

# Asynchronous
from pymongo import AsyncMongoClient
from mongrator.runner import AsyncRunner

runner = AsyncRunner(AsyncMongoClient(config.uri), config)
await runner.up()
```

## Migration tracking

Applied migrations are recorded in the `mongrator_migrations` collection (configurable) within the target database. Each document stores:

- `_id` — migration file stem
- `applied_at` — UTC timestamp
- `checksum` — SHA-256 of the migration file at time of application
- `direction` — `"up"` or `"down"`
- `duration_ms` — execution time in milliseconds

Running `mongrator validate` compares current file checksums against recorded values and reports any modifications.
