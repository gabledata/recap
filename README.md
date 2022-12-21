<h1 align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap"></a>
  <br>
</h1>

<p align="center">
<i>Recap crawls and serves metadata</i>
</p>

There are a ton of data catalogs out there, but most are complex, bloated, and built for a wide audience. Recap is the opposite of that; it's tiny and built specifically for engineers.

You can use Recap to build data tools for:

* Observability
* Monitoring
* Debugging
* Security
* Compliance
* Governance
* Cost

## Principles

Recap was designed to be small and flexible.

* **Lightweight**: Recap starts with a single command and doesn't require other infrastructure (not even Docker).
* **CLI-first**: Recap doesn't even have a GUI!
* **RESTful**: Recap comes with a REST API to ease integration with outside tools.
* **Automated**: Recap is not meant for manual taxonomy curation.
* **Modular**: Recap's storage and crawling layers are fully pluggable.
* **Programmable**: Recap is a Python library, so you can invoke it directly from your code.

## Integrations

Recap uses [SQLAlchemy](https://www.sqlalchemy.org/) to access databases, so any SQLAlchemy dialect should work. Recap has been tested with:

* Snowflake
* Bigquery
* PostgreSQL

Stream and filesystem crawling is in the works.

## Usage

### Quickstart

Start by installing Recap. Python 3.9 or above is required.

```
pip install recap-core
```

Now let's crawl a database:

    recap refresh postgresql://username@localhost/some_db

You can use any SQLAlchemy connect string. 

    recap refresh bigquery://some-project-12345
    recap refresh snowflake://some_user:some_pass@some_account_id

For Snowflake and BigQuery, you'll have to `pip install snowflake-sqlalchemy` or `pip install sqlalchemy-bigquery`, respectively.

Crawled metadata is stored in a directory structure. See what's available using:

    recap list /

Recap will respond with a JSON list:

```
[
  "databases"
]
```

Append children to the list path to browse around:

    recap list /databases

After you poke around, try and read some metadata. Every node in the path can have metadata, but only table and view children contain metadata. You can look at metadata using the `recap read` command:

```
recap read /databases/postgresql/instances/localhost/schemas/some_db/tables/some_table
```

Recap will print all of `some_table`'s metadata to the CLI in JSON format:

```
{
  "access": {
    "username": {
      "privileges": [
        "INSERT",
        "SELECT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER"
      ],
      "read": true,
      "write": true
    }
  },
  "columns": {
    "email": {
      "autoincrement": false,
      "default": null,
      "generic_type": "VARCHAR",
      "nullable": false,
      "type": "VARCHAR"
    },
    "id": {
      "autoincrement": true,
      "default": "nextval('\"some_db\".some_table_id_seq'::regclass)",
      "generic_type": "BIGINT",
      "nullable": false,
      "type": "BIGINT"
    }
  },
  "data_profile": {
    "email": {
      "count": 10,
      "distinct": 10,
      "empty_strings": 0,
      "max_length": 32,
      "min_length": 13,
      "nulls": 0
    },
    "id": {
      "average": 5.5,
      "count": 10,
      "max": 10,
      "min": 1,
      "negatives": 0,
      "nulls": 0,
      "sum": 55.0,
      "zeros": 0
    }
  },
  "indexes": {
    "index_some_table_on_email": {
      "columns": [
        "email"
      ],
      "unique": false
    }
  },
  "location": {
    "database": "postgresql",
    "instance": "localhost",
    "schema": "some_db",
    "table": "some_table"
  },
  "primary_key": {
    "constrained_columns": [
      "id"
    ],
    "name": "some_table_pkey"
  }
}
```

You can search for metadata, too. Recap stores its metadata in [DuckDB](https://duckdb.org/) by default, so you can use DuckDB's [JSON path syntax](https://duckdb.org/docs/extensions/json) to search the catalog:

    recap search "metadata->'$.location'->>'$.table' = 'some_table'"

The database file defaults to `~/.recap/catalog/recap.duckdb`, if you wish to open a DuckDB client directly.

### API

#### Server

Recap comes with an API out of the box. You can start it with:

    recap api

A `uvicorn` server will bind to http://localhost:8000 by default. You can look at the API endpoints using http://localhost:8000/docs.

You can pass custom `uvicorn` configuration by creating `~/.recap/settings.toml` and setting parameters under the `api` space like:

```
api.host = "0.0.0.0"
```

#### Client

You can use Recap's CLI to query a remote Recap API server if you wish. Set `catalog.url` in `settings.toml` to point to your Recap API location.

```
catalog.url = "http://localhost:8000"
```

### Configuration

Recap uses [Dynaconf](https://www.dynaconf.com/) to manage configuration. By default, Recap can see anything you put into `~/recap/settings.toml`.

You can customize your `settings.toml` location using the `SETTINGS_FILE_FOR_DYNACONF` environment variable:

    SETTINGS_FILE_FOR_DYNACONF=/tmp/api.toml recap list

You can also configure your catalog and crawlers in `settings.toml`. Here's an example:

```
[api]
host = "0.0.0.0"

[catalog]
url = file:///tmp/recap.duckdb

[[crawlers]]
url = "bigquery://some-project-12345"

[[crawlers]]
url = "postgresql://username@localhost/some_db"

[[crawlers]]
url = "snowflake://some_user:some_pass@some_account_id"
```

## Warning

Recap is still a little baby application. It's going to wake up crying in the middle of the night. It's going to vomit on the floor once in a while. But if you give it some love and care, it'll be worth it. As time goes on, it'll grow up and be more mature. Bear with it.
