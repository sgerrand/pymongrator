# CLI reference

## Global options

| Option          | Description                                      |
|-----------------|--------------------------------------------------|
| `--config PATH` | Path to config file (default: `mongrator.toml`)  |

## Commands

### `mongrator init`

Create the migrations directory and a `mongrator.toml` configuration stub.

```sh
mongrator init
```

### `mongrator create <name>`

Generate a new timestamped migration file.

```sh
mongrator create add_users_email_index
```

### `mongrator status`

Show applied and pending migrations.

```sh
mongrator status
```

### `mongrator up`

Apply pending migrations.

```sh
mongrator up [--target ID] [--async]
```

| Option        | Description                                       |
|---------------|---------------------------------------------------|
| `--target ID` | Apply only up to this migration ID                |
| `--async`     | Use the async runner (`pymongo.AsyncMongoClient`) |

### `mongrator down`

Roll back applied migrations.

```sh
mongrator down [--steps N] [--async]
```

| Option      | Description                                      |
|-------------|--------------------------------------------------|
| `--steps N` | Number of migrations to roll back (default: 1)   |
| `--async`   | Use the async runner                             |

### `mongrator validate`

Verify checksums of applied migration files. Reports any files that have been modified since they were applied.

```sh
mongrator validate
```
