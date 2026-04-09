# Configuration

mongrator can be configured via a TOML file or environment variables.

## TOML file

`mongrator init` creates a `mongrator.toml` stub:

```toml
[mongrator]
uri = "mongodb://localhost:27017"
database = "mydb"
migrations_dir = "migrations"
collection = "mongrator_migrations"
```

Use `--config PATH` to specify an alternate config file location.

## Environment variables

| Variable                   | Description                  | Required                             |
|----------------------------|------------------------------|--------------------------------------|
| `MONGRATOR_URI`            | MongoDB connection URI       | yes                                  |
| `MONGRATOR_DB`             | Database name                | yes                                  |
| `MONGRATOR_MIGRATIONS_DIR` | Path to migrations directory | no (default: `migrations`)           |
| `MONGRATOR_COLLECTION`     | Tracking collection name     | no (default: `mongrator_migrations`) |

Environment variables are used when no config file is found at the expected path.

## Programmatic configuration

::: mongrator.config.MigratorConfig
