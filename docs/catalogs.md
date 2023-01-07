Recap catalogs store metadata and expose read and search APIs. Recap ships with a database, filesystem, and Recap catalog implementation. The database catalog with SQLite is enabled by default.

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

## Filesystem Catalog

The filesystem catalog uses [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) to persist metadata in a filesystem (or object store like S3).

Search is implemented using [pyjq](https://pypi.org/project/pyjq/). The filesystem catalog recurses over every metadata file in the catalog and runs a [jq](https://stedolan.github.io/jq/) query on each JSON file.

Configure the filesystem catalog with `type = "fs"`.

```toml
[catalog]
type = "fs"
url = "file:///some/catalog/directory"
```

Anything under the `catalog.fs` namespace will be forwarded to the `fsspec.filesystem` Python call.

## Recap Catalog

The Recap catalog makes HTTP requests to a [Recap server](server.md).

```toml
[catalog]
type = "recap"
url = "http://localhost:8000"
```

The Recap catalog enables different systems to share the same metadata when they all talk to the same Recap server.