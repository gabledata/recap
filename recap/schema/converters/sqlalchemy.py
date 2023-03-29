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
from recap.schema.converters.converter import Converter


class SQLAlchemyConverter(Converter):
    # TODO Handle optional (column["nullable"],)
    # TODO Handle default
    # This is a really lossy implementation. If more accuracy is needed, a DB
    # specific convert should be implemented.
    def to_recap_type(self, columns: list[dict[str, Any]], **_) -> types.Struct:
        fields = []
        for column in columns:
            schema_args = {}
            if doc := column.get("comment"):
                schema_args["doc"] = doc
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
                        zone="UTC",
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
                    field_type = types.Date64(
                        unit=types.TimeUnit.MICROSECOND,
                        **schema_args,
                    )
                case _:
                    raise ValueError(
                        "Can't convert to Recap type from SQLAlchemy "
                        f"type={type(column['type'])}"
                    )
            if column.get("nullable"):
                field_type = types.Union(types=[types.Null(), field_type])
                # Force `null` default since SQLAlchemy doesn't appear to
                # differentiate between an unset default and a default set to
                # `null`.
                field_type.extra_attrs["default"] = None
            if default := column.get("default"):
                field_type.extra_attrs["default"] = default
            field_type.extra_attrs["name"] = column["name"]
            fields.append(field_type)
        return types.Struct(fields=fields)
