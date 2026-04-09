# Writing migrations

Migration files are plain Python named `{timestamp}_{slug}.py` (e.g. `20260408_143022_add_users_email_index.py`). Each file must define an `up(db)` function. A `down(db)` function is optional but enables rollback.

## Using the ops helpers

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

### Available ops helpers

| Helper | Reversible | Description |
|--------|-----------|-------------|
| `ops.create_index(collection, keys, **kwargs)` | yes | Create an index |
| `ops.drop_index(collection, index_name)` | no | Drop an index by name |
| `ops.rename_field(collection, old, new, filter=None)` | yes | Rename a field across documents |
| `ops.add_field(collection, field, default_value, filter=None)` | yes | Add a field with a default value |

See the [ops API reference](api/ops.md) for full details.

## Using plain PyMongo

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

config = MigratorConfig(
    uri="mongodb://localhost:27017",
    database="mydb",
    migrations_dir=Path("migrations"),
)
runner = SyncRunner(pymongo.MongoClient(config.uri), config)
runner.up()
```

```python
# Asynchronous
from pymongo import AsyncMongoClient
from mongrator.runner import AsyncRunner

runner = AsyncRunner(AsyncMongoClient(config.uri), config)
await runner.up()
```

## Migration tracking

Applied migrations are recorded in the tracking collection (default: `mongrator_migrations`). Each document stores:

- `_id` -- migration file stem
- `applied_at` -- UTC timestamp
- `checksum` -- SHA-256 of the migration file at time of application
- `direction` -- `"up"` or `"down"`
- `duration_ms` -- execution time in milliseconds

Running `mongrator validate` compares current file checksums against recorded values and reports any modifications.
