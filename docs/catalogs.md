Recap catalogs store metadata and expose read and search APIs. Recap ships with the [DuckDB](https://duckdb.org/), Filesystem, and Recap catalog implementations. The DuckDB catalog is enabled by default.

## DuckDB Catalog

The DuckDB catalog stores data in a local DuckDB file. By default, the file is located in `~/.recap/catalog/recap.duckdb`. Search is implemented using DuckDB's [JSON path](https://duckdb.org/docs/extensions/json) syntax. See [Commands](commands.md) for an example.

You can configure the DuckDB catalog in your `settings.toml` like so:

```toml
[catalog]
type = "duckdb"
url = "file:///some/path/to/duck.db"
duckdb.read_only = true
```

Anything under the `catalog.duckdb` namespace will be forwarded to the DuckDB Python client.

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