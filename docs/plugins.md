You can extend Recap with plugins. In fact, everything in Recap is a plugin except for its crawler (and even that might change eventually).

Plugins are implemented using Pythons `entry-points` package metadata. See Python's [using pacakge metadata](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata) page for more details on this style of plugin architecture.

There are five types of plugins:

* Analyzers
* Browsers
* Catalogs
* Commands
* Routers

## Analyzers

[Analyzer plugins](analyzers.md) must implement the [AbstractAnalyzer](https://github.com/recap-cloud/recap/blob/main/recap/analyzers/abstract.py) class.

Packages can export their analyzers using the `recap.analyzers` entry-point. Here's how Recap's built-in analyzers are defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.analyzers"]
"db.location" = "recap.analyzers.db.location"
"sqlalchemy.access" = "recap.analyzers.sqlalchemy.access"
"sqlalchemy.columns" = "recap.analyzers.sqlalchemy.columns"
"sqlalchemy.comment" = "recap.analyzers.sqlalchemy.comment"
"sqlalchemy.foreign_keys" = "recap.analyzers.sqlalchemy.foreign_keys"
"sqlalchemy.indexes" = "recap.analyzers.sqlalchemy.indexes"
"sqlalchemy.primary_key" = "recap.analyzers.sqlalchemy.primary_key"
"sqlalchemy.profile" = "recap.analyzers.sqlalchemy.profile"
"sqlalchemy.view_definition" = "recap.analyzers.sqlalchemy.view_definition"
```

Every entry-point points to a module with a `create_analyzer(**config)` method.

## Browsers

[Browser plugins](browsers.md) must implement the [AbstractBrowser](https://github.com/recap-cloud/recap/blob/main/recap/browsers/abstract.py) class.

Packages can export their browsers using the `recap.browsers` entry-point. Here's how Recap's built-in browser is defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.browsers"]
db = "recap.browsers.db"
```

Every entry-point points to a module with a `create_browser(**config)` method.

## Catalogs

[Catalog plugins](catalogs.md) must implement the [AbstractCatalog](https://github.com/recap-cloud/recap/blob/main/recap/catalogs/abstract.py) class.

Packages can export their catalogs using the `recap.catalogs` entry-point. Here's how Recap's built-in catalogs are defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.catalogs"]
db = "recap.catalogs.db"
recap = "recap.catalogs.recap"
```

Every entry-point points to a module with a `create_catalog(**config)` method.

## Commands

[Command plugins](commands.md) use [Typer](https://typer.tiangolo.com/). Plugins must expose a `typer.Typer()` object, usually defined as:

```python
app = typer.Typer()
```

Packages can export their commands using the `recap.commands` entry-point. Here's how Recap's built-in commands are defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.commands"]
catalog = "recap.commands.catalog:app"
crawl = "recap.commands.crawl:app"
plugins = "recap.commands.plugins:app"
serve = "recap.commands.serve:app"
```

## Routers

[Server plugins](server.md) use [FastAPI](https://fastapi.tiangolo.com/). Plugins must expose a `fastapi.APIRouter()` object, usually defined as:

```python
router = fastapi.APIRouter()
```

Packages can export their commands using the `recap.routers` entry-point. Here's how Recap's built-in routers are defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.routers"]
"catalog.typed" = "recap.routers.catalog.typed:router"
"catalog.untyped" = "recap.routers.catalog.untyped:router"
```

Routers are added relative to the HTTP server's root path.

!!! tip

    Recap calls `include_router(route)` for each object in the `recap.routers` entry-point. This means that anything that works with `include_router` can be exposed (even [GraphQL APIs](https://fastapi.tiangolo.com/advanced/graphql/)).

!!! warning

    [Order matters](https://fastapi.tiangolo.com/tutorial/path-params/#order-matters) when adding routers to FastAPI. Recap does not currently support router order prioritization; routers are added in an unpredictable order. If multiple routers contain the same path, the first one will handle incoming requests to its path.
