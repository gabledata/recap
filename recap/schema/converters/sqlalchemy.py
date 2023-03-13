from typing import Any

from sqlalchemy.types import (
    DATE,
    DATETIME,
    JSON,
    TIME,
    TIMESTAMP,
    BigInteger,
    Boolean,
    Float,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
    _Binary,
)

from recap.schema import types


# TODO Handle optional (column["nullable"],)
# TODO Handle default
# This is a really lossy implementation. If more accuracy is needed, a DB
# specific convert should be implemented.
def to_recap_schema(columns: list[dict[str, Any]]) -> types.Struct:
    fields = []
    for column in columns:
        schema_args = {}
        if doc := column.get("comment"):
            schema_args["doc"] = doc
        if "default" in column:
            schema_args["default"] = types.DefaultValue(
                value=column["default"],
            )
        match column["type"]:
            case SmallInteger():
                field_type = types.Int16(**schema_args)
            case Integer():
                field_type = types.Int32(**schema_args)
            case BigInteger():
                field_type = types.Int64(**schema_args)
            case Boolean():
                field_type = types.Bool(**schema_args)
            case Float():
                field_type = types.Float32(**schema_args)
            case LargeBinary() | _Binary():
                field_type = types.Bytes32(**schema_args)
            case Numeric():
                field_type = types.Decimal128(
                    precision=column["type"].precision or 38,
                    scale=column["type"].scale or 9,
                    **schema_args,
                )
            case String() | JSON():
                field_type = types.String32(**schema_args)
            case TIMESTAMP():
                field_type = types.Timestamp64(
                    timezone="UTC",
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case DATETIME():
                field_type = types.Timestamp64(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case TIME():
                field_type = types.Time64(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case DATE():
                field_type = types.Date64(**schema_args)
            case _:
                raise ValueError(
                    "Can't convert to Recap type from SQLAlchemy "
                    f"type={type(column['type'])}"
                )
        fields.append(
            types.Field(
                name=column["name"],
                type_=field_type,
            )
        )
    return types.Struct(fields=fields)
