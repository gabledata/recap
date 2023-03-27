"""
This module uses INFORMATION_SCHEMA's terminology and hierarchy, which
models database hierarchy as:

    catalog -> schema -> table -> column

If you're using PostgreSQL, "schema" is usually "public". If you're using
MySQL, ["catalog" is always hard-coded to "def"](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-columns-table.html),
and usually excluded from MySQL connect URLs. In BigQuery, "catalog" is the
project and "schema"is the dataset.
"""

from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from recap.registry import registry
from recap.schema.converters.recap import RecapConverter
from recap.schema.converters.sqlalchemy import SQLAlchemyConverter
from recap.schema.models import Type


@registry.metadata(
    "postgresql://{netloc}/{database}/{schema}/{table}",
    include_engine=True,
)
@registry.metadata(
    "snowflake://{netloc}/{database}/{schema}/{table}",
    include_engine=True,
)
def schema(
    engine: Engine,
    schema: str,
    table: str,
    **_,
) -> Type:
    """
    Fetch a Recap schema for a SQL table.

    :param engine: SQLAlchemy Engine to use when inspecting schemas and tables.
    :param schema: A database schema.
    :param table: A table name.
    :returns: A Recap schema.
    """

    columns = inspect(engine).get_columns(
        table,
        schema,
    )
    struct = SQLAlchemyConverter().to_recap_type(columns)
    struct_obj = RecapConverter().from_recap_type(struct)
    return Type.parse_obj(struct_obj)


@registry.relationship(
    "postgresql://{netloc}/{database}",
    "contains",
    include_engine=True,
)
@registry.relationship(
    "postgresql://{netloc}/{database}/{schema}",
    "contains",
    include_engine=True,
)
@registry.relationship(
    "snowflake://{netloc}/{database}",
    "contains",
    include_engine=True,
)
@registry.relationship(
    "snowflake://{netloc}/{database}/{schema}",
    "contains",
    include_engine=True,
)
def ls(
    engine: Engine,
    schema: str | None = None,
    **_,
) -> list[str]:
    """
    Fetch (INFORMATION_SCHEMA) schemas or tables from a database.

    URLs are of the form:

        postgresql://{netloc}/{database}/{schema}/{table}
        snowflake://{netloc}/{database}/{schema}/{table}
        mysql://{netloc}/{schema}/{table}
        bigquery://{project}/{dataset}/{table}

    :param engine: SQLAlchemy Engine to use when inspecting schemas and tables.
    :param schema: A database schema.
    :param table: A table name.
    :returns: A Recap schema.
    """

    if schema:
        tables = inspect(engine).get_table_names(schema)
        views = inspect(engine).get_view_names(schema)
        return [
            # Remove schema prefix if it's there
            f"{engine.url}/{schema}/{table.split('.')[-1]}"
            for table in tables + views
        ]
    else:
        return [
            f"{engine.url}/{schema}" for schema in inspect(engine).get_schema_names()
        ]
