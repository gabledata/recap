from __future__ import annotations

from typing import Any

from avro.schema import (
    ArraySchema,
    BytesDecimalSchema,
    EnumSchema,
    FixedDecimalSchema,
    FixedSchema,
    MapSchema,
    NamedSchema,
    PrimitiveSchema,
    RecordSchema,
    Schema,
    TimeMicrosSchema,
    TimeMillisSchema,
    TimestampMicrosSchema,
    TimestampMillisSchema,
    UUIDSchema,
    UnionSchema,
    make_avsc_object,
)

from recap.schema import types


def from_avro(avro_schema: Schema, aliases: list[str] = []) -> types.Type:
    schema_args = {}
    if isinstance(avro_schema, NamedSchema):
        alias = avro_schema.fullname
        if avro_schema.fullname in aliases:
            return types.Type(alias=alias)
        else:
            schema_args["alias"] = alias
            aliases.append(alias)
    match avro_schema:
        case (
            RecordSchema(doc=str(doc))
            | FixedSchema(doc=str(doc))
            | EnumSchema(doc=str(doc))
            | ArraySchema(doc=str(doc))
            | MapSchema(doc=str(doc))
            | UnionSchema(doc=str(doc))
        ):
            schema_args["doc"] = str(doc)
    match avro_schema:
        # TODO Python Avro doesn't support local timestamps or duration types.
        case BytesDecimalSchema():
            return types.Decimal(
                precision=avro_schema.precision,
                scale=avro_schema.scale,
            )
        case FixedDecimalSchema():
            return types.Decimal(
                precision=avro_schema.precision,
                scale=avro_schema.scale,
                bytes=avro_schema.size,
            )
        case UUIDSchema():
            return UUID()
        case TimeMillisSchema():
            return types.Time32(
                unit=types.TimeUnit.MILLISECOND,
            )
        case TimeMicrosSchema():
            return types.Time64(
                unit=types.TimeUnit.MICROSECOND,
            )
        case TimestampMillisSchema():
            return types.Timestamp64(
                unit=types.TimeUnit.MILLISECOND,
            )
        case TimestampMicrosSchema():
            return types.Timestamp64(
                unit=types.TimeUnit.MICROSECOND,
            )
        case PrimitiveSchema(type="string"):
            return types.String64(**schema_args)
        case PrimitiveSchema(type="int"):
            return types.Int32(**schema_args)
        case PrimitiveSchema(type="long"):
            return types.Int64(**schema_args)
        case PrimitiveSchema(type="float"):
            return types.Float32(**schema_args)
        case PrimitiveSchema(type="double"):
            return types.Float64(**schema_args)
        case PrimitiveSchema(type="bytes"):
            return types.Bytes64(**schema_args)
        case PrimitiveSchema(type="boolean"):
            return types.Bool(**schema_args)
        case PrimitiveSchema(type="null"):
            return types.Null(**schema_args)
        case RecordSchema():
            fields = [
                types.Field(
                    name=field.name,
                    type_=from_avro(field.type, aliases),
                    default=(
                        types.DefaultValue(value=field.default)
                        if field.has_default else None
                    )
                )
                for field in avro_schema.fields
            ]
            return types.Struct(fields=fields, **schema_args)
        case ArraySchema():
            item_type = avro_schema.items
            # Avro arrays are unbounded, so no size constraint is needed.
            return types.List(
                values=from_avro(item_type, aliases),
                **schema_args,
            )
        case MapSchema():
            values_type = avro_schema.values
            return types.Map(
                # Avro maps always have string types.
                keys=types.String64(),
                values=from_avro(values_type, aliases),
                **schema_args,
            )
        case UnionSchema():
            return types.Union(
                types=[from_avro(schema_, aliases) for schema_ in avro_schema.schemas],
                **schema_args,
            )
        case EnumSchema():
            return types.Enum(
                symbols=list(avro_schema.symbols),
                **schema_args,
            )
        case FixedSchema():
            return types.Bytes(
                bytes=avro_schema.size,
                variable=False,
                **schema_args,
            )
        case _:
            raise ValueError(
                "Can't convert to Recap type from Avro " f"type={avro_schema}"
            )


def to_avro(type_: types.Type) -> Schema:
    return make_avsc_object(_to_avro_dict(type_))


def _to_avro_dict(
    type_: types.Type, aliases: list[str] = []
) -> dict[str, Any] | list | str:
    schema_args = {}
    if alias := type_.alias:
        aliases.append(alias)
        schema_args["name"] = alias
    if doc := type_.doc:
        schema_args["doc"] = doc
    match type_:
        case types.Decimal(
            min_length=int(min_length),
            max_length=int(max_length),
        ):
            decimal_type = "fixed" if min_length == max_length else "bytes"
            return {
                "type": decimal_type,
                "logicalType": "decimal",
                "precision": type_.precision,
                "scale": type_.scale,
            }
        case UUID():
            return {"type": "string", "logicalType": "uuid"}
        case types.Time32(unit=types.TimeUnit.MILLISECOND):
            return {
                "type": "int",
                "logicalType": "time-millis",
            }
        case types.Time64(unit=types.TimeUnit.MICROSECOND):
            return {
                "type": "long",
                "logicalType": "time-microsecond",
            }
        case types.Time64(unit=types.TimeUnit.MILLISECOND):
            return {
                "type": "long",
                "logicalType": "timestamp-millis",
            }
        case types.Time64(unit=types.TimeUnit.MICROSECOND):
            return {
                "type": "long",
                "logicalType": "timestamp-micros",
            }
        case types.Null():
            return schema_args | {"type": "null"}
        case types.Bool():
            return schema_args | {"type": "bool"}
        case types.String() if types.String64().subsumes(type_):
            return schema_args | {"type": "string"}
        case types.Int() if types.Int32().subsumes(type_):
            return schema_args | {"type": "int"}
        case types.Int() if types.Int64().subsumes(type_):
            return schema_args | {"type": "long"}
        case types.Float() if types.Float32().subsumes(type_):
            return schema_args | {"type": "float"}
        case types.Float() if types.Float64().subsumes(type_):
            return schema_args | {"type": "double"}
        case types.Float():
            return schema_args | {"type": "bytes", "logicalType": "decimal"}
        case types.Bytes(variable=False):
            return schema_args | {"type": "fixed", "size": type_.bytes}
        case types.Bytes(bytes=int(bytes)) if bytes <= types.Bytes64().bytes:
            return "bytes"
        case types.Enum():
            return schema_args | {
                "type": "enum",
                "symbols": type_.symbols,
            }
        case types.Union(types=list(union_types)):
            return [_to_avro_dict(union_type, aliases) for union_type in union_types]
        case types.Struct():
            return schema_args | {
                "type": "record",
                # Fields are not schema.Types, so unwrap them manually.
                "fields": [
                    {
                        "name": field.name,
                        "type": _to_avro_dict(field.type_, aliases),
                    }
                    # Only set default if it exists, since setting default to
                    # null is different from an unset default.
                    | (
                        {"default": field.default.value}
                        if field.default else {}
                    )
                    for field in type_.fields
                ],
            }
        case types.List():
            return schema_args | {
                "type": "array",
                "items": _to_avro_dict(type_.values),
            }
        case types.Map():
            return schema_args | {
                "type": "map",
                "values": _to_avro_dict(type_.values),
            }
        case types.Type(alias=str(alias)) if alias in aliases:
            return alias
        case _:
            raise ValueError("Can't convert to Avro type from Recap " f"type={type_}")


class UUID(types.String):
    """
    A derived type representing a UUID Avro logicalType.

    Annoyingly, Avro appears to represent UUIDs as variable length strings
    rather than either fixed-length strings or byte arrays.
    """

    # len("771450ea-75b0-4270-b79c-2f867f1d48d4")
    bytes: int = 36
