from __future__ import annotations

from sqlglot import transpile

from recap import metadata


def to_ddl(
    schema: metadata.Schema,
    table: str,
    dialect: str,
    primary_key: str | None = "id",
) -> str:
    column_defs = _get_column_defs(schema, True, primary_key)
    sql = f"CREATE TABLE {table} ({column_defs})"
    return "\n".join(
        transpile(
            sql,
            read="duckdb",
            write=dialect,
            identify=True,
            pretty=True,
        )
    )


def _get_column_defs(
    schema: metadata.Schema,
    is_root: bool = False,
    primary_key: str | None = None,
) -> str:
    column_def = ""
    match schema:
        case metadata.Int16Schema():
            column_def = "SMALLINT"
        case metadata.Int32Schema():
            column_def = "INTEGER"
        case metadata.Int64Schema():
            column_def = "BIGINT"
        case metadata.Float32Schema():
            column_def = "REAL"
        case metadata.Float64Schema():
            column_def = "DOUBLE"
        case metadata.BooleanSchema():
            column_def = "BOOLEAN"
        case metadata.StringSchema():
            column_def = "VARCHAR"
        case metadata.BytesSchema():
            column_def = "BLOB"
        case metadata.TimestampSchema():
            column_def = "TIMESTAMP"
        case metadata.DateSchema():
            column_def = "DATE"
        case metadata.TimeSchema():
            column_def = "TIME"
        case metadata.ArraySchema() if schema.value_schema:
            column_def = f"{_get_column_defs(schema.value_schema)}[]"
        case metadata.StructSchema():
            for field in schema.fields or []:
                field_def = _get_column_defs(field.schema_)
                if is_root and field.name == primary_key:
                    field_def += " PRIMARY KEY"
                elif is_root and not field.schema_.optional:
                    field_def += " NOT NULL"
                column_def += f"{field.name} {field_def}, "
            column_def = column_def.rstrip(", ")
            if not is_root:
                column_def = f"STRUCT({column_def})"
        case metadata.MapSchema(
            key_schema=metadata.Schema(),
            value_schema=metadata.Schema(),
        ):
            key_def = _get_column_defs(schema.key_schema)  # type: ignore
            value_def = _get_column_defs(schema.value_schema)  # type: ignore
            column_def = f"MAP({key_def}, {value_def})"
        case _:
            raise ValueError(
                f"Can't convert to DuckDB DDL from Recap type={schema.type_}"
            )
    return column_def
