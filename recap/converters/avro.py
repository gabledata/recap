import json

from recap.types import (
    BoolType,
    BytesType,
    EnumType,
    FloatType,
    IntType,
    ListType,
    MapType,
    NullType,
    ProxyType,
    RecapType,
    RecapTypeRegistry,
    StringType,
    StructType,
    UnionType,
)


class AvroConverter:
    def __init__(self) -> None:
        self.registry = RecapTypeRegistry()

    def convert(self, avro_schema_str: str) -> StructType:
        avro_schema = json.loads(avro_schema_str)
        recap_schema = self._parse(avro_schema)
        if not isinstance(recap_schema, StructType):
            raise ValueError("Avro schema must be a record")
        return recap_schema

    def _parse(self, avro_schema: dict | str) -> RecapType:
        return_type = None
        if isinstance(avro_schema, str):
            avro_schema = {"type": avro_schema}
        extra_attrs = {}
        if "doc" in avro_schema:
            extra_attrs["doc"] = avro_schema["doc"]
        if "name" in avro_schema:
            if (
                (type_ := avro_schema.get("type"))
                and isinstance(type_, str)
                and type_ in {"record", "enum", "fixed"}
            ):
                extra_attrs["alias"] = avro_schema["name"]
            else:
                extra_attrs["name"] = avro_schema["name"]
        if "default" in avro_schema:
            extra_attrs["default"] = avro_schema["default"]
        if logical_type := avro_schema.get("logicalType"):
            return self._parse_logical(logical_type, avro_schema, extra_attrs)
        match avro_schema:
            case {"type": "null"}:
                return_type = NullType(**extra_attrs)
            case {"type": "boolean"}:
                return_type = BoolType(**extra_attrs)
            case {"type": "int"}:
                return_type = IntType(32, signed=True, **extra_attrs)
            case {"type": "long"}:
                return_type = IntType(64, signed=True, **extra_attrs)
            case {"type": "float"}:
                return_type = FloatType(32, **extra_attrs)
            case {"type": "double"}:
                return_type = FloatType(64, **extra_attrs)
            case {"type": "bytes"}:
                return_type = BytesType(9_223_372_036_854_775_807, **extra_attrs)
            case {"type": "string"}:
                return_type = StringType(9_223_372_036_854_775_807, **extra_attrs)
            case {"type": "record", "fields": fields}:
                fields = [self._parse(field) for field in avro_schema.get("fields", [])]
                return_type = StructType(fields, **extra_attrs)
            case {"type": "enum", "symbols": symbols}:
                return_type = EnumType(symbols, **extra_attrs)
            case {"type": "array", "items": items}:
                values = self._parse(items)
                return_type = ListType(values, **extra_attrs)
            case {"type": "map", "values": values}:
                keys = StringType(9_223_372_036_854_775_807)
                return_type = MapType(keys, self._parse(values), **extra_attrs)
            case {"type": "union", "types": types} | {"type": list(types)}:
                return_type = UnionType([self._parse(t) for t in types], **extra_attrs)
            case {"type": str(alias)}:
                # Short circuit so we don't re-register aliases for proxies
                return ProxyType(alias, self.registry, **extra_attrs)
            case {"type": dict(type_)}:
                return_type = self._parse(type_)

                # Don't pass extra_attrs into _parse since we don't want field
                # "name" to overwrite "alias" for record/enum/fixed.
                return_type.extra_attrs |= extra_attrs

                # Short circuit so we don't re-register aliases for nested types
                return return_type
            case _:
                raise ValueError(f"Unsupported Avro schema: {avro_schema}")

        if return_type.alias is not None:
            self.registry.register_alias(return_type.alias, return_type)

        return return_type

    def _parse_logical(
        self,
        logical_type: str,
        avro_schema: dict,
        extra_attrs: dict,
    ) -> RecapType:
        match logical_type:
            case "decimal":
                return BytesType(
                    logical="build.recap.Decimal",
                    bytes_=avro_schema.get("size", 9_223_372_036_854_775_807),
                    variable=avro_schema["type"] == "fixed",
                    precision=avro_schema["precision"],
                    scale=avro_schema.get("scale", 0),
                    **extra_attrs,
                )
            case "uuid":
                return StringType(
                    logical="build.recap.UUID",
                    bytes_=36,
                    variable=False,
                    **extra_attrs,
                )
            case "date":
                return IntType(
                    logical="build.recap.Date",
                    bits=32,
                    signed=True,
                    unit="day",
                    **extra_attrs,
                )
            case "time-millis":
                return IntType(
                    logical="build.recap.Time",
                    bits=32,
                    signed=True,
                    unit="millisecond",
                    **extra_attrs,
                )
            case "time-micros":
                return IntType(
                    logical="build.recap.Time",
                    bits=64,
                    signed=True,
                    unit="microsecond",
                    **extra_attrs,
                )
            case "timestamp-millis":
                return IntType(
                    logical="build.recap.Timestamp",
                    bits=64,
                    signed=True,
                    unit="millisecond",
                    timezone="UTC",
                    **extra_attrs,
                )
            case "timestamp-micros":
                return IntType(
                    logical="build.recap.Timestamp",
                    bits=64,
                    signed=True,
                    unit="microsecond",
                    timezone="UTC",
                    **extra_attrs,
                )
            case "duration":
                return BytesType(
                    logical="build.recap.Interval",
                    bytes_=12,
                    signed=True,
                    unit="millisecond",
                    **extra_attrs,
                )
            case _:
                raise ValueError(f"Unsupported Avro logical type: {logical_type}")
