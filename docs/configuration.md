Though Recap's CLI can run without any configuration, you might want to configure Recap using a config file. Recap uses [Dynaconf](https://www.dynaconf.com/) for its configuration system.

## Config Locations

Configuraton is stored in `~/.recap/settings.toml` by default. You can override the default location by setting the `SETTINGS_FILE_FOR_DYNACONF` environment variable. See Dynaconf's [documentation](https://www.dynaconf.com/configuration/#envvar) for more information.

## Schema

Recap's `settings.toml` has two main sections: `catalog` and `crawlers`.

* The `catalog` section configures the storage layer; it uses SQLite by default. Run `recap plugins catalogs` to see other options.
* The `crawlers` section defines infrastructure to crawl. Only the `url` field is required. You may optionally specify analyzer `excludes` and path `filters` as well.

```toml
[catalog]
plugin = "recap"
url = "http://localhost:8000"

[[crawlers]]
url = "postgresql://username@localhost/some_db"
excludes = [
	"db.profile"
]
filters = [
	"/**/tables/some_table"
]
```

## Secrets

Do not store database credentials in your `settings.toml`; use Dynaconf's secret management instead. See Dynaconf's [documentation](https://www.dynaconf.com/secrets/) for more information.