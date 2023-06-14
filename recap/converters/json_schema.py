import json

from recap.types import (
    BoolType,
    FloatType,
    IntType,
    ListType,
    NullType,
    RecapType,
    StringType,
    StructType,
    make_nullable,
)


class JSONSchemaConverter:
    def convert(self, json_schema_str: str) -> StructType:
        json_schema = json.loads(json_schema_str)
        recap_schema = self._parse(json_schema)
        if not isinstance(recap_schema, StructType):
            raise ValueError("JSON schema must be an object")
        return recap_schema

    def _parse(self, json_schema: dict) -> RecapType:
        extra_attrs = {}
        if "description" in json_schema:
            extra_attrs["doc"] = json_schema["description"]
        if "default" in json_schema:
            extra_attrs["default"] = json_schema["default"]

        match json_schema:
            case {"type": "object", "properties": properties}:
                fields = []
                for name, prop in properties.items():
                    field = self._parse(prop)
                    if name not in json_schema.get("required", []):
                        field = make_nullable(field)
                    field.extra_attrs["name"] = name
                    fields.append(field)
                return StructType(fields, **extra_attrs)
            case {"type": "array", "items": items}:
                values = self._parse(items)
                return ListType(values, **extra_attrs)
            case {"type": "string", "format": "date"}:
                return StringType(
                    bytes_=9223372036854775807,
                    logical="org.iso.8601.Date",
                    **extra_attrs,
                )
            case {"type": "string", "format": "date-time"}:
                return StringType(
                    bytes_=9223372036854775807,
                    logical="org.iso.8601.DateTime",
                    **extra_attrs,
                )
            case {"type": "string", "format": "time"}:
                return StringType(
                    bytes_=9223372036854775807,
                    logical="org.iso.8601.Time",
                    **extra_attrs,
                )
            case {"type": "string"}:
                return StringType(bytes_=9223372036854775807, **extra_attrs)
            case {"type": "number"}:
                return FloatType(bits=64, **extra_attrs)
            case {"type": "integer"}:
                return IntType(bits=32, signed=True, **extra_attrs)
            case {"type": "boolean"}:
                return BoolType(**extra_attrs)
            case {"type": "null"}:
                return NullType(**extra_attrs)
            case _:
                raise ValueError(f"Unsupported JSON schema: {json_schema}")
