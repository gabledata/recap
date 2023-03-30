from __future__ import annotations

from sqlglot import transpile

from recap import types
from recap.converters.converter import Converter


class SQLGlotConverter(Converter):
    def from_recap_type(
        self,
        type_: types.Type,
        table: str,
        dialect: str,
        primary_key: str | None = "id",
        **_,
    ) -> str:
        def _get_column_defs(
            schema: types.Type,
            is_root: bool = False,
            primary_key: str | None = None,
        ) -> str:
            column_def = ""
            match schema:
                case types.Int() if types.Int16().subsumes(schema):
                    column_def = "SMALLINT"
                case types.Int() if types.Int32().subsumes(schema):
                    column_def = "INTEGER"
                case types.Int() if types.Int64().subsumes(schema):
                    column_def = "BIGINT"
                case types.Float() if types.Float32().subsumes(schema):
                    column_def = "REAL"
                case types.Float() if types.Float64().subsumes(schema):
                    column_def = "DOUBLE"
                case types.Bool():
                    column_def = "BOOLEAN"
                # TODO Need to figure out what Duck's max varchar truly is.
                # TODO support VARCHAR(n)
                case types.String() if types.String32().subsumes(schema):
                    column_def = "VARCHAR"
                # TODO DuckDB is using uint's for blob length.
                # Bytes32 uses an int for length, so Duck's BLOB can hold 4GB, not 2GB.
                case types.Bytes() if types.Bytes32().subsumes(schema):
                    column_def = "BLOB"
                # TODO Not clear to me whether Duck's TIMESTAMP is actually a DATETIME.
                # TODO Add min/max for timestamp.
                # https://duckdb.org/docs/sql/data_types/timestamp
                case types.Timestamp64(str(unit)) if unit in [
                    types.TimeUnit.YEAR,
                    types.TimeUnit.MONTH,
                    types.TimeUnit.DAY,
                    types.TimeUnit.HOUR,
                    types.TimeUnit.MINUTE,
                    types.TimeUnit.SECOND,
                    types.TimeUnit.MILLISECOND,
                    types.TimeUnit.MICROSECOND,
                ]:
                    column_def = "TIMESTAMP"
                case types.Date64():
                    column_def = "DATE"
                case types.Time64():
                    column_def = "TIME"
                # TODO Not clear what Duck's max list length is.
                # TODO Not celar if ARRAY supports ARRAY(n).
                # (^ If so, should use min/max length.)
                case types.List():
                    column_def = f"{_get_column_defs(schema.values)}[]"
                case types.Struct():
                    for field_type in schema.fields or []:
                        field_def = _get_column_defs(field_type)
                        if field_name := field_type.extra_attrs.get("name"):
                            if is_root and field_name == primary_key:
                                field_def += " PRIMARY KEY"
                            # TODO Support required fields.
                            # elif not field.type_.optional:
                            #    field_def += " NOT NULL"
                            # TODO Support defaults.
                            column_def += f"{field_name} {field_def}, "
                        else:
                            raise ValueError(
                                f"Missing field name for type={field_type}"
                            )
                    column_def = column_def.rstrip(", ")
                    if not is_root:
                        column_def = f"STRUCT({column_def})"
                case types.Map(
                    key_schema=types.Type(),
                    value_schema=types.Type(),
                ):
                    key_def = _get_column_defs(schema.keys)
                    value_def = _get_column_defs(schema.values)
                    column_def = f"MAP({key_def}, {value_def})"
                case _:
                    raise ValueError(
                        f"Can't convert to DuckDB DDL from Recap type={schema}"
                    )
            return column_def

        column_defs = _get_column_defs(type_, True, primary_key)
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
