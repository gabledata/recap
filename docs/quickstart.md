## Install

Start by installing Recap. Python 3.10 is required.

    pip install recap-core

## Crawl

Now let's crawl a database:

=== "CLI"

    ```
    recap crawl postgresql://username@localhost/some_db
    ```

=== "Python"

    ```python
    from recap.repl import *

    crawl("postgresql://username@localhost/some_db")
    ```

You can use any [SQLAlchemy](https://docs.sqlalchemy.org/en/14/dialects/) connect string.

=== "CLI"

    ```
    recap crawl bigquery://some-project-12345
    recap crawl snowflake://username:password@account_identifier/SOME_DB/SOME_SCHEMA?warehouse=SOME_COMPUTE
    ```

=== "Python"

    ```python
    from recap.repl import *

    crawl("bigquery://some-project-12345")
    crawl("snowflake://username:password@account_identifier/SOME_DB/SOME_SCHEMA?warehouse=SOME_COMPUTE")
    ```

!!! note

    You must install appropriate drivers and [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/14/dialects/) for the databases you wish to crawl. For PostgreSQL, you'll have to `pip install psycopg2`. For Snowflake and BigQuery, you'll have to `pip install snowflake-sqlalchemy` and `pip install sqlalchemy-bigquery`, respectively.

You can also crawl filesystems and object stores.

=== "CLI"

    ```
    recap crawl /tmp/data
    recap crawl s3://power-analysis-ready-datastore
    ```

=== "Python"

    ```python
    from recap.repl import *

    crawl("/tmp/data")
    crawl("s3://power-analysis-ready-datastore")
    ```

!!! note

    You must install appropriate [fsspec](https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations) implementation for the filesystem you wish to crawl.

## List

Crawled metadata is stored in a directory structure. See what's available using:

=== "CLI"

        recap ls /tmp/data

=== "Python"

    ```python
    from recap.repl import *

    ls("/tmp/data")
    ```

Recap will respond with a JSON list in the CLI:

```json
[
  "file:///tmp/data/foo",
  "file:///tmp/data/bar.json"
]
```

## Read

After you poke around, try and read some metadata.

=== "CLI"

        recap schema file:///tmp/data/foo.json

=== "Python"

    ```python
    from recap.repl import *

    schema("/tmp/data/foo.json")
    ```

Recap will print `foo.json`'s inferred schema to the CLI in JSON format:

```json
{
  "fields": [
    {
      "name": "test",
      "type": "string",
      "default": null,
      "nullable": null,
      "comment": null
    }
  ]
}
```

## Time

Recap keeps historical data. You can set the `time` parameter to see what data looked like at specific point in time. This is useful for debugging data quality issues.

=== "CLI"

        recap schema file:///tmp/data/foo.json --time 2020-02-22

=== "Python"

    ```python
    from recap.repl import *

    schema("/tmp/data/foo.json", datetime(2022, 2, 22))
    ```


## Search

Recap stores its metadata in [SQLite](https://www.sqlite.org/) by default. You can use SQLite's [json_extract](https://www.sqlite.org/json1.html#the_json_extract_function) syntax to search the catalog:

=== "CLI"

        recap search schema "json_extract(metadata_obj, '$.fields') IS NOT NULL" 

=== "Python"

    ```python
    from recap.repl import *

    search("json_extract(metadata_obj, '$.fields') IS NOT NULL")
    ```

The database file defaults to `~/.recap/recap.db`, if you wish to open a SQLite client directly.

## Integrations

See the [Integrations](api/recap.integrations.md) page to see all of the systems Recap supports, and what data you can crawl.
