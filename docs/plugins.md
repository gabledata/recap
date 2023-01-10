You can extend Recap with plugins. In fact, everything in Recap is a plugin except for its crawler (and even that might change eventually).

Plugins are implemented using Pythons `entry-points` package metadata. See Python's [using pacakge metadata](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata) page for more details on this style of plugin architecture.

There are four types of plugins:

* Analyzers
* Browsers
* Catalogs
* Commands

## Analyzers

[Analyzer plugins](analyzers.md) must implement the [AbstractAnalyzer](https://github.com/recap-cloud/recap/blob/main/recap/plugins/analyzers/abstract.py) class.

Packages can export their analyzers using the `recap.analyzers` entrypoint. Here's how Recap's built-in analyzers are defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.analyzers"]
"db.access" = "recap.plugins.analyzers.table:TableAccessAnalyzer"
"db.column" = "recap.plugins.analyzers.table:TableColumnAnalyzer"
"db.comment" = "recap.plugins.analyzers.table:TableCommentAnalyzer"
"db.foreign_key" = "recap.plugins.analyzers.table:TableForeignKeyAnalyzer"
"db.index" = "recap.plugins.analyzers.table:TableIndexAnalyzer"
"db.location" = "recap.plugins.analyzers.table:TableLocationAnalyzer"
"db.primary_key" = "recap.plugins.analyzers.table:TablePrimaryKeyAnalyzer"
"db.profile" = "recap.plugins.analyzers.table:TableProfileAnalyzer"
"db.view_definitions" = "recap.plugins.analyzers.table:TableViewDefinitionAnalyzer"
```

## Browsers

[Browser plugins](browsers.md) must implement the [AbstractBrowser](https://github.com/recap-cloud/recap/blob/main/recap/plugins/browsers/abstract.py) class.

Packages can export their browsers using the `recap.browsers` entrypoint. Here's how Recap's built-in browser is defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.browsers"]
db = "recap.plugins.browsers.db:DatabaseBrowser"
```

## Catalogs

[Catalog plugins](catalogs.md) must implement the [AbstractCatalog](https://github.com/recap-cloud/recap/blob/main/recap/plugins/catalogs/abstract.py) class.

Packages can export their catalogs using the `recap.catalogs` entrypoint. Here's how Recap's built-in catalogs are defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.catalogs"]
db = "recap.plugins.catalogs.db:DatabaseCatalog"
recap = "recap.plugins.catalogs.recap:RecapCatalog"
```

## Commands

[Command plugins](commands.md) use [Typer](https://typer.tiangolo.com/). Plugins must expose a `typer.Typer()` object, usually defined as:

```python
app = typer.Typer()
```

Packages can export their commands using the `recap.commands` entrypoint. Here's how Recap's built-in commands are defined in its [pyproject.toml](https://github.com/recap-cloud/recap/blob/main/pyproject.toml):

```toml
[project.entry-points."recap.commands"]
catalog = "recap.plugins.commands.catalog:app"
crawl = "recap.plugins.commands.crawl:app"
plugins = "recap.plugins.commands.plugins:app"
serve = "recap.plugins.commands.serve:app"
```