## Install

Start by installing Recap. Python 3.10 or above is required.

    pip install recap-core

## Crawl

Now let's crawl a database:

    recap crawl postgresql://username@localhost/some_db

You can use any [SQLAlchemy](https://docs.sqlalchemy.org/en/14/dialects/) connect string.

    recap crawl bigquery://some-project-12345
    recap crawl snowflake://username:password@account_identifier/SOME_DB/SOME_SCHHEMA?warehouse=SOME_COMPUTE

!!! warning

    You must install appropriate drivers and [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/14/dialects/) for the databases you wish to crawl. For PostgreSQL, you'll have to `pip install psycopg2`. For Snowflake and BigQuery, you'll have to `pip install snowflake-sqlalchemy` or `pip install sqlalchemy-bigquery`, respectively.

## List

Crawled metadata is stored in a directory structure. See what's available using:

    recap catalog list /

Recap will respond with a JSON list:

```json
[
  "databases"
]
```

Append children to the path to browse around:

    recap catalog list /databases

## Read

After you poke around, try and read some metadata. Every node in the path can have metadata, but right now only tables and views do. You can look at metadata using the `recap catalog read` command:

    recap catalog read /databases/postgresql/instances/localhost/schemas/some_db/tables/some_table

Recap will print all of `some_table`'s metadata to the CLI in JSON format:

```json
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
  "profile": {
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

## Search

You can search for metadata, too. Recap stores its metadata in [SQLite](https://www.sqlite.org/) by default. You can use SQLite's [json_extract syntax](https://www.sqlite.org/json1.html#the_json_extract_function) to search the catalog:

    recap catalog search "json_extract(metadata, '$.location.table') = 'some_table'"

The database file defaults to `~/.recap/catalog/recap.db`, if you wish to open a SQLite client directly.
