"""
This module contains the core metadata models that Recap understands. All
models extend Pydantic's `BaseModel` class.

Right now, Recap's only metadata model is a Schema. Other entities, such as
accounts and jobs, are represented by URLs, but have no associated metadata.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from pydantic import Field as PydanticField


class Field(BaseModel):
    schema_: Schema = PydanticField(alias="schema")
    """
    A field's schema
    """

    name: str | None = None
    """
    A field's name.
    """


class Schema(BaseModel):
    """
    Recap's representation of a Schema.
    """

    # TODO This should be ClassVar, but Pydantic doesn't seem to want to
    # seralize ClassVars with Python 3.10.
    type_: str = PydanticField(alias="type", const=True)
    """
    The schema's type.
    """

    default: Any = None
    """
    :returns: Default value for the schema.
    """

    name: str | None = None
    """
    The schema's name.
    """

    optional: bool = True
    """
    :returns: True if the schema is optional.
    """

    version: int | None = None
    """
    Optional schema version. Newer versions must be larger than older versions.
    """

    doc: str | None = None
    """
    A schema's documentation.
    """

    parameters: dict[str, str] | None = None
    """
    A map of optional schema parameters.
    """

    def is_primitive(self) -> bool:
        match self.type_:
            case (
                "STRING"
                | "INT8"
                | "INT16"
                | "INT32"
                | "INT64"
                | "FLOAT32"
                | "FLOAT64"
                | "BOOLEAN"
                | "BYTES"
            ):
                return True
        return False

    def __str__(self) -> str:
        return self.json(
            by_alias=True,
            exclude_none=True,
            indent=2,
        )


class Int8Schema(Schema):
    type_: str = PydanticField(default="INT8", alias="type", const=True)


class Int16Schema(Schema):
    type_: str = PydanticField(default="INT16", alias="type", const=True)


class Int32Schema(Schema):
    type_: str = PydanticField(default="INT32", alias="type", const=True)


class Int64Schema(Schema):
    type_: str = PydanticField(default="INT64", alias="type", const=True)


class Float32Schema(Schema):
    type_: str = PydanticField(default="FLOAT32", alias="type", const=True)


class Float64Schema(Schema):
    type_: str = PydanticField(default="FLOAT64", alias="type", const=True)


class BooleanSchema(Schema):
    type_: str = PydanticField(default="BOOLEAN", alias="type", const=True)


class StringSchema(Schema):
    type_: str = PydanticField(default="STRING", alias="type", const=True)


class BytesSchema(Schema):
    type_: str = PydanticField(default="BYTES", alias="type", const=True)


class ArraySchema(Schema):
    type_: str = PydanticField(default="ARRAY", alias="type", const=True)

    value_schema: Schema
    """
    Value schema for this map or array schema. Throws a ValueError if schema is
    not a map or array.
    """


class MapSchema(Schema):
    type_: str = PydanticField(default="MAP", alias="type", const=True)

    key_schema: Schema
    """
    Key schema for this map schema. Throws a ValueError if schema is not a map.
    """

    value_schema: Schema
    """
    Value schema for this map or array schema. Throws a ValueError if schema is
    not a map or array.
    """


class StructSchema(Schema):
    type_: str = PydanticField(default="STRUCT", alias="type", const=True)

    fields: list[Field]
    """
    List of fields for this struct.
    """

    def field(self, name: str) -> Field | None:
        for field in self.fields or []:
            if field.name == name:
                return field


class DateSchema(Schema):
    type_: str = PydanticField(default="DATE", alias="type", const=True)


class DecimalSchema(Schema):
    type_: str = PydanticField(default="DECIMAL", alias="type", const=True)


class TimeSchema(Schema):
    type_: str = PydanticField(default="TIME", alias="type", const=True)


class TimestampSchema(Schema):
    type_: str = PydanticField(default="TIMESTAMP", alias="type", const=True)


class UnionSchema(Schema):
    type_: str = PydanticField(default="UNION", alias="type", const=True)

    schemas: list[Schema]


# Update forward refs since Schema references Field, which references Schema.
Schema.update_forward_refs()
Field.update_forward_refs()
