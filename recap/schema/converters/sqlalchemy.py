from typing import Any

from sqlalchemy import types

from recap.schema import model


def to_recap_schema(columns: list[dict[str, Any]]) -> model.StructSchema:
    fields = []
    for column in columns:
        match column["type"]:
            case types.SmallInteger():
                SchemaClass = model.Int16Schema
            case types.Integer():
                SchemaClass = model.Int32Schema
            case types.BigInteger():
                SchemaClass = model.Int64Schema
            case types.Boolean():
                SchemaClass = model.BooleanSchema
            case types.Float():
                SchemaClass = model.Float32Schema
            case types.LargeBinary() | types._Binary():
                SchemaClass = model.BytesSchema
            case types.Numeric():
                SchemaClass = model.DecimalSchema
            case types.String() | types.Text() | types.Unicode() | types.UnicodeText() | types.JSON():
                SchemaClass = model.StringSchema
            case types.TIMESTAMP() | types.DATETIME():
                SchemaClass = model.TimestampSchema
            case types.TIME():
                SchemaClass = model.TimeSchema
            case types.DATE():
                SchemaClass = model.DateSchema
            case _:
                raise ValueError(
                    "Can't convert to Recap type from frictionless "
                    f"type={type(column['type'])}"
                )
        fields.append(
            model.Field(
                name=column["name"],
                schema=SchemaClass(
                    default=column["default"],
                    optional=column["nullable"],
                    doc=column.get("comment"),
                ),
            )
        )
    return model.StructSchema(fields=fields, optional=False)
