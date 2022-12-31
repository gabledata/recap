Recap analyzers generate metadata; they're given a path and return a dictionary of metadata.

!!! note

    `recap crawl` will run all available analyzers, if possible. You can exclude analyzers using `--exclude`. See the [commands](commands.md) documentation for more information.

## Database Analyzers

Recap ships with the following analyzer plugins:

* `db.access`
* `db.column`
* `db.comment`
* `db.foreign_key`
* `db.index`
* `db.location`
* `db.primary_key`
* `db.profile`
* `db.view_definitions`

### Access

Returns user access information for a table or view.

```json
"access": {
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
}
```

### Comment

Returns a table's coment, if any.

```json
"comment": "The roles that can be applied to the current user."
```

### Foreign Key

Returns foreign key information.

```json
"foreign_keys": [
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
"indexes": {
  "index_some_table_on_email": {
    "columns": [
      "email"
    ],
    "unique": false
  }
}
```

### Location

Returns the type, instance, schema, and table/ view location.

```json
"location": {
  "database": "postgresql",
  "instance": "localhost",
  "schema": "some_db",
  "table": "some_table"
}
```

### Primary Key

Returns primary key information

```json
"primary_key": {
  "constrained_columns": [
    "id"
  ],
  "name": "some_table_pkey"
}
```

### Profile

Returns basic table statistics (max, min, nulls, count, and so on).

```json
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
}
```

### View Definitions

Returns a view's query, if any.

```json
"view_definition": "SELECT * FROM table;"
```
