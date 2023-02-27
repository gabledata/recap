from typing import Any

from sqlalchemy import types

from recap import metadata


def to_recap_schema(columns: list[dict[str, Any]]) -> metadata.StructSchema:
    fields = []
    for column in columns:
        match column["type"]:
            case types.SmallInteger():
                SchemaClass = metadata.Int16Schema
            case types.Integer():
                SchemaClass = metadata.Int32Schema
            case types.BigInteger():
                SchemaClass = metadata.Int64Schema
            case types.Boolean():
                SchemaClass = metadata.BooleanSchema
            case types.Float():
                SchemaClass = metadata.Float32Schema
            case types.LargeBinary() | types._Binary():
                SchemaClass = metadata.BytesSchema
            case types.Numeric():
                SchemaClass = metadata.DecimalSchema
            case types.String() | types.Text() | types.Unicode() | types.UnicodeText() | types.JSON():
                SchemaClass = metadata.StringSchema
            case types.TIMESTAMP() | types.DATETIME():
                SchemaClass = metadata.TimestampSchema
            case types.TIME():
                SchemaClass = metadata.TimeSchema
            case types.DATE():
                SchemaClass = metadata.DateSchema
            case _:
                raise ValueError(
                    "Can't convert to Recap type from frictionless "
                    f"type={type(column['type'])}"
                )
        fields.append(
            metadata.Field(
                name=column["name"],
                schema=SchemaClass(
                    default=column["default"],
                    optional=column["nullable"],
                    doc=column.get("comment"),
                ),
            )
        )
    return metadata.StructSchema(fields=fields, optional=False)
