## Install

Start by installing Recap. Python 3.10 or above is required.

    pip install recap-core

## Crawl

Now let's crawl a database:


=== "CLI"

        recap crawl postgresql://username@localhost/some_db

=== "Python"

    ```python
    from recap.analyzers.db.column import TableColumnAnalyzer
    from recap.browsers.db import DatabaseBrowser
    from recap.catalogs.db import DatabaseCatalog
    from recap.crawler import Crawler
    from sqlalchemy import create_engine

    some_db_engine = create_engine('postgresql://username@localhost/some_db')
    catalog_engine = create_engine('sqlite://')
    analyzers = [
      TableColumnAnalyzer(some_db_engine),
      # Other analyzers can go here, too.
    ]
    browser = DatabaseBrowser(some_db_engine)
    catalog = DatabaseCatalog(catalog_engine)
    crawler = Crawler(browser, catalog, analyzers)
    crawler.crawl()
    ```

You can use any [SQLAlchemy](https://docs.sqlalchemy.org/en/14/dialects/) connect string.

    recap crawl bigquery://some-project-12345
    recap crawl snowflake://username:password@account_identifier/SOME_DB/SOME_SCHHEMA?warehouse=SOME_COMPUTE

!!! warning

    You must install appropriate drivers and [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/14/dialects/) for the databases you wish to crawl. For PostgreSQL, you'll have to `pip install psycopg2`. For Snowflake and BigQuery, you'll have to `pip install snowflake-sqlalchemy` or `pip install sqlalchemy-bigquery`, respectively.

## List

Crawled metadata is stored in a directory structure. See what's available using:

=== "CLI"

        recap catalog list /

=== "Python"

    ```python
    from recap.catalogs.db import DatabaseCatalog
    from sqlalchemy import create_engine

    engine = create_engine('sqlite://')
    catalog = DatabaseCatalog(engine)
    children = catalog.ls('/')
    ```

Recap will respond with a JSON list in the CLI:

```json
[
  "databases"
]
```

Append children to the path to browse around:

=== "CLI"

        recap catalog list /databases

=== "Python"

    ```python
    from recap.catalogs.db import DatabaseCatalog
    from sqlalchemy import create_engine

    engine = create_engine('sqlite://')
    catalog = DatabaseCatalog(engine)
    results = catalog.ls('/databases')
    ```

## Read

After you poke around, try and read some metadata. Every node in the path can have metadata, but right now only tables and views do. You can look at metadata using the `recap catalog read` command:

=== "CLI"

        recap catalog read /databases/postgresql/instances/localhost/schemas/some_db/tables/some_table

=== "Python"

    ```python
    from recap.catalogs.db import DatabaseCatalog
    from sqlalchemy import create_engine

    engine = create_engine('sqlite://')
    catalog = DatabaseCatalog(engine)
    metadata = catalog.read('/databases/postgresql/instances/localhost/schemas/some_db/tables/some_table')
    ```

Recap will print all of `some_table`'s metadata to the CLI in JSON format:

```json
{
  "sqlalchemy.access": {
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
  "sqlalchemy.columns": {
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
  "sqlalchemy.profile": {
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
  "sqlalchemy.indexes": {
    "index_some_table_on_email": {
      "columns": [
        "email"
      ],
      "unique": false
    }
  },
  "db.location": {
    "database": "postgresql",
    "instance": "localhost",
    "schema": "some_db",
    "table": "some_table"
  },
  "sqlalchemy.primary_key": {
    "constrained_columns": [
      "id"
    ],
    "name": "some_table_pkey"
  }
}
```

## Search

Recap stores its metadata in [SQLite](https://www.sqlite.org/) by default. You can use SQLite's [json_extract syntax](https://www.sqlite.org/json1.html#the_json_extract_function) to search the catalog:

=== "CLI"

        recap catalog search "json_extract(metadata, '$.\"db.location\".table') = 'some_table'"

=== "Python"

    ```python
    from recap.catalogs.db import DatabaseCatalog
    from sqlalchemy import create_engine

    engine = create_engine('sqlite://')
    catalog = DatabaseCatalog(engine)
    results = catalog.search("json_extract(metadata, '$.\"db.location\".table') = 'some_table'")
    ```

The database file defaults to `~/.recap/catalog/recap.db`, if you wish to open a SQLite client directly.
