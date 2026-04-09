# mongrator

Lightweight MongoDB schema migration tool with synchronous and asynchronous PyMongo support.

## Features

- **Simple CLI** -- `init`, `create`, `status`, `up`, `down`, `validate`
- **Declarative ops helpers** -- reversible operations with auto-rollback
- **Async support** -- backed by `pymongo.AsyncMongoClient`
- **Checksum tracking** -- detect modifications to applied migrations
- **Plain Python migrations** -- no DSL, just `up(db)` and `down(db)`

## Installation

```sh
pip install mongrator
```

Or with [uv](https://docs.astral.sh/uv/):

```sh
uv add mongrator
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

Head over to [Getting started](getting-started.md) for a full walkthrough.
