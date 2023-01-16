Recap catalogs store metadata and expose read and search APIs. Recap ships with a database catalog and Recap catalog implementation. The database catalog is enabled by default (with SQLite).

## Database Catalog

The database catalog uses [SQLAlchemy](https://www.sqlalchemy.org/) to persists catalog data. By default, a SQLite database is used; the file is located in `~/.recap/catalog/recap.db`. Search is implemented using SQLite's [json_extract syntax](https://www.sqlite.org/json1.html#the_json_extract_function) syntax. See [Commands](commands.md) for an example.

You can configure the SQLite catalog in your `settings.toml` like so:

```toml
[catalog]
url = "sqlite://"
engine.connect_args.check_same_thread = false
```

Anything under the `engine` namespace will be forwarded to the SQLAlchemy engine.

You can use any [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/) with the database catalog. Here's a `settings.toml` that's configured for PostgreSQL:

```toml
[catalog]
url = "postgresql://user:pass@localhost/some_db"
```

## Recap Catalog

The Recap catalog makes HTTP requests to a [Recap server](server.md).

```toml
[catalog]
plugin = "recap"
url = "http://localhost:8000"
```

The Recap catalog enables different systems to share the same metadata when they all talk to the same Recap server.