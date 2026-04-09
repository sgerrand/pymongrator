# Getting started

## Prerequisites

- Python 3.13+
- A running MongoDB instance

## Install mongrator

```sh
pip install mongrator
```

## Initialise your project

```sh
mongrator init
```

This creates:

- `mongrator.toml` -- configuration file
- `migrations/` -- directory for migration files

Edit `mongrator.toml` to point at your database:

```toml
[mongrator]
uri = "mongodb://localhost:27017"
database = "mydb"
migrations_dir = "migrations"
collection = "mongrator_migrations"
```

See [Configuration](configuration.md) for all options including environment variable support.

## Create a migration

```sh
mongrator create add_users_email_index
```

This generates a timestamped file in the migrations directory, e.g. `migrations/20260408_143022_add_users_email_index.py`.

## Write migration logic

Open the generated file and define the `up(db)` function. Using the [ops helpers](writing-migrations.md#using-the-ops-helpers) is the simplest approach:

```python
from mongrator import ops

def up(db):
    return [
        ops.create_index("users", {"email": 1}, unique=True),
    ]
```

Because ops helpers record their own inverses, rollback works automatically. See [Writing migrations](writing-migrations.md) for more details.

## Apply migrations

```sh
mongrator up
```

## Check status

```sh
mongrator status
```

## Roll back

```sh
mongrator down
```
