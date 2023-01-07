Recap ships with four standard command plugins:

* `recap catalog`
* `recap crawl`
* `recap plugins`
* `recap serve`

!!! note

    You can see all available commands by running `recap --help` or `recap plugins commands`.

## Catalog

Recaps `recap catalog` command reads metadata Recap's data catalog. List the catalog's directory structure with `recap list`, read metadata from a directory with `recap read`, and search with `recap search`.

### Search

Recap's search syntax depends on the [Catalog](catalogs.md) plugin that's used. As mentioned in the [Quickstart](quickstart.md), Recap stores its metadata in [SQLite](https://www.sqlite.org/) by default. You can use SQLite's [json_extract syntax](https://www.sqlite.org/json1.html#the_json_extract_function) to search the catalog:

    recap catalog search "json_extract(metadata, '$.location.table') = 'some_table'"

The database file defaults to `~/.recap/catalog/recap.db`, if you wish to open a SQLite client directly.

## Crawl

Use `recap crawl` to crawl infrastructure and store its metadata in Recap's catalog. The `crawl` command takes an optional `URL` parameter. If specified, the URL will be crawled. If not specified, all `crawlers` defined in your `settings.toml` file will be crawled (see [Configuration](configuration.md) for more information).

### Excludes

You might not want to use all analyzers when crawling infrastrucutre. Some analyzers (particularly the `profile` analyzer) are costly and slow to run. To exclude an analyzer, use `--exclude` with `recap crawl`. For example:

    recap crawl postgresql://username@localhost/some_db --exclude='db.profile'

### Filters

Recap crawls all paths by default, which can be slow. You might wish to only crawl some subset of data. You can use `--filter` to control what data the crawler looks at. Recap uses Unix filename pattern matching as defined in Python's [fnmatch](https://docs.python.org/3/library/fnmatch.html) module.

To crawl only a specific table, you would use this syntax:

    recap crawl postgresql://username@localhost/some_db --filter='/**/tables/some_table'

## Plugins

The `recap plugins` command lists the Recap plugins that you have installed in your environment.

To list the available analyzers, run this command:

```
recap plugins analyzers
[
  "db.access",
  "db.column",
  "db.comment",
  "db.foreign_key",
  "db.index",
  "db.location",
  "db.primary_key",
  "db.profile",
  "db.view_definitions"
]
```

See `recap plugins --help` for more.

## Serve

You might wish to centralize your Recap data catalog in one place, and have all users read from the same location. Recap ships with an HTTP/JSON server for this use case. Start a Recap server on a single host using `recap serve`. Clients can then configure their [Recap catalog](catalogs.md#recap-catalog) in `settings.toml` to point to the server location:

```toml
[catalog]
type = "recap"
url = "http://192.168.0.1:8000"
```

See the [server](server.md) documentation for more information.