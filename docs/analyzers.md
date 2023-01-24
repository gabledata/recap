Recap analyzers generate metadata; they're given a path and return a [Pydantic](https://pydantic.dev) model with the metadata the analyzer discovered.

## Database Analyzers

Recap ships with the following analyzer plugins:

* `db.location`
* `sqlalchemy.access`
* `sqlalchemy.columns`
* `sqlalchemy.comment`
* `sqlalchemy.foreign_keys`
* `sqlalchemy.indexes`
* `sqlalchemy.primary_key`
* `sqlalchemy.profile`
* `sqlalchemy.view_definition`

Other analyzers will be available if you've installed additional [analyzer plugins](plugins.md#analyzers). Run `recap plugins analyzers` to see a complete list.

!!! note

    Analyzers namespace prefixes differentiate analyzers that return similar data. The SQLAlchemy `sqlalchemy.columns` analyzer returns column schemas in the standard SQLAlchemy schema format. A more specific `snowflake.columns` analyzer could return a more Snowflake-specific schema. Clients decide which analyzer metadata to use.

### Access

Returns user access information for a table or view.

```json
"sqlalchemy.access": {
  "<username>": {
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
}
```

### Column

Returns column schemas.

```json
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
}
```

### Comment

Returns a table's coment, if any.

```json
"sqlalchemy.comment": "The roles that can be applied to the current user."
```

### Foreign Key

Returns foreign key information.

```json
"sqlalchemy.foreign_keys": [
  {
    "constrained_columns": [
      "some_id"
    ],
    "name": "fk_some_id",
    "options": {},
    "referred_columns": [
      "id"
    ],
    "referred_schema": "some_schema",
    "referred_table": "some_table"
  }
]
```

### Index

Returns index information.

```json
"sqlalchemy.indexes": {
  "index_some_table_on_email": {
    "columns": [
      "email"
    ],
    "unique": false
  }
}
```

### Location

Returns the type, instance, schema, and table/view location.

```json
"db.location": {
  "database": "postgresql",
  "instance": "localhost",
  "schema": "some_db",
  "table": "some_table"
}
```

### Primary Key

Returns primary key information

```json
"sqlalchemy.primary_key": {
  "constrained_columns": [
    "id"
  ],
  "name": "some_table_pkey"
}
```

### Profile

Returns basic table statistics (max, min, nulls, count, and so on).

```json
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
}
```

### View Definitions

Returns a view's query, if any.

```json
"sqlalchemy.view_definition": "SELECT * FROM table;"
```
