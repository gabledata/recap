# pylint: disable=missing-docstring

import json

from recap.converters.avro import AvroConverter
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
    StringType,
    StructType,
    UnionType,
)


def test_primitives():
    converter = AvroConverter()
    primitives = [
        ("null", NullType()),
        ("boolean", BoolType()),
        ("int", IntType(32, signed=True)),
        ("long", IntType(64, signed=True)),
        ("float", FloatType(32)),
        ("double", FloatType(64)),
        ("bytes", BytesType(9223372036854775807, variable=True)),
        ("string", StringType(9223372036854775807, variable=True)),
    ]

    for avro_type, expected in primitives:
        actual = converter._parse(avro_type)
        assert isinstance(actual, type(expected)), f"For type: {avro_type}"


def test_enum():
    converter = AvroConverter()
    avro_enum = {
        "type": "record",
        "name": "TestEnum",
        "fields": [
            {
                "name": "color",
                "type": {
                    "type": "enum",
                    "name": "Color",
                    "symbols": ["RED", "GREEN", "BLUE"],
                },
            }
        ],
    }
    actual = converter.convert(json.dumps(avro_enum))

    enum_type = EnumType(symbols=["RED", "GREEN", "BLUE"], name="color", alias="Color")
    expected = StructType(fields=[enum_type], alias="TestEnum")

    assert actual == expected


def test_array():
    converter = AvroConverter()
    avro_array = {
        "type": "record",
        "name": "TestArray",
        "fields": [{"name": "items", "type": {"type": "array", "items": "string"}}],
    }
    actual = converter.convert(json.dumps(avro_array))

    array_type = ListType(name="items", values=StringType(9223372036854775807))
    expected = StructType(fields=[array_type], alias="TestArray")

    assert actual == expected


def test_map():
    converter = AvroConverter()
    avro_map = {
        "type": "record",
        "name": "TestMap",
        "fields": [{"name": "values", "type": {"type": "map", "values": "int"}}],
    }
    actual = converter.convert(json.dumps(avro_map))

    # Construct an expected Map RecapType instance
    map_type = MapType(
        name="values",
        keys=StringType(9223372036854775807),
        values=IntType(32, signed=True),
    )
    expected = StructType(fields=[map_type], alias="TestMap")

    assert actual == expected


def test_union():
    converter = AvroConverter()
    avro_union = {
        "type": "record",
        "name": "TestUnion",
        "fields": [
            {
                "name": "unionField",
                "type": {"type": "union", "types": ["string", "null"]},
            }
        ],
    }
    actual = converter.convert(json.dumps(avro_union))

    # Construct an expected Union RecapType instance
    union_type = UnionType(
        name="unionField", types=[StringType(9223372036854775807), NullType()]
    )
    expected = StructType(fields=[union_type], alias="TestUnion")

    assert actual == expected


def test_record():
    converter = AvroConverter()
    avro_record = {
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "string"},
        ],
    }
    actual = converter.convert(json.dumps(avro_record))
    assert isinstance(actual, StructType)
    assert len(actual.fields) == 2
    assert isinstance(actual.fields[0], IntType)
    assert actual.fields[0].bits == 32
    assert actual.fields[0].extra_attrs["name"] == "a"
    assert isinstance(actual.fields[1], StringType)
    assert actual.fields[1].bytes_ == 9223372036854775807
    assert actual.fields[1].extra_attrs["name"] == "b"
    assert actual.extra_attrs == {}
    assert actual.alias == "Test"


def test_self_referencing():
    converter = AvroConverter()

    # Avro schema for a simple linked list
    avro_schema = {
        "type": "record",
        "name": "LinkedList",
        "fields": [
            {"name": "value", "type": "int"},
            {"name": "next", "type": ["null", "LinkedList"]},  # Self reference
        ],
    }

    # The expected schema is a recursive structure, so we can't create it
    # directly as Python would run into an infinite recursion. We can only
    # check that the resulting structure has the expected properties.
    actual = converter.convert(json.dumps(avro_schema))

    assert isinstance(actual, StructType)
    assert len(actual.fields) == 2
    assert isinstance(actual.fields[0], IntType)
    assert isinstance(actual.fields[1], UnionType)
    assert isinstance(actual.fields[1].types[0], NullType)
    assert isinstance(actual.fields[1].types[1], ProxyType)
    assert actual.fields[1].types[1].resolve() == actual


def test_record_with_docs_and_default():
    converter = AvroConverter()

    # Avro schema for a record with docs and default attributes
    avro_schema = {
        "type": "record",
        "name": "Person",
        "doc": "A person record",
        "fields": [
            {
                "name": "name",
                "type": "string",
                "doc": "The person's name",
                "default": "unknown",
            },
            {"name": "age", "type": "int", "doc": "The person's age", "default": 0},
        ],
    }

    actual = converter.convert(json.dumps(avro_schema))

    assert isinstance(actual, StructType)
    assert len(actual.fields) == 2
    assert actual.doc == "A person record"
    assert isinstance(actual.fields[0], StringType)
    assert actual.fields[0].doc == "The person's name"
    assert actual.fields[0].extra_attrs["default"] == "unknown"
    assert isinstance(actual.fields[1], IntType)
    assert actual.fields[1].doc == "The person's age"
    assert actual.fields[1].extra_attrs["default"] == 0


def test_decimal():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_decimal",
        "fields": [
            {
                "name": "decimal",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": 2,
                },
            }
        ],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, BytesType)
    assert field.logical == "build.recap.Decimal"
    assert field.bytes_ == avro_schema["fields"][0]["type"].get(
        "size",
        9223372036854775807,
    )
    assert field.variable == (avro_schema["fields"][0]["type"]["type"] == "fixed")
    assert (
        field.extra_attrs["precision"] == avro_schema["fields"][0]["type"]["precision"]
    )
    assert field.extra_attrs["scale"] == avro_schema["fields"][0]["type"].get(
        "scale",
        0,
    )


def test_uuid():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_uuid",
        "fields": [{"name": "uuid", "type": {"type": "string", "logicalType": "uuid"}}],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, StringType)
    assert field.logical == "build.recap.UUID"
    assert field.bytes_ == 36
    assert field.variable == False


def test_date():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_date",
        "fields": [{"name": "date", "type": {"type": "int", "logicalType": "date"}}],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Date"
    assert field.bits == 32
    assert field.signed is True
    assert field.extra_attrs["unit"] == "day"


def test_time_millis():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_time_millis",
        "fields": [
            {"name": "time", "type": {"type": "int", "logicalType": "time-millis"}}
        ],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Time"
    assert field.bits == 32
    assert field.signed is True
    assert field.extra_attrs["unit"] == "millisecond"


def test_time_micros():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_time_micros",
        "fields": [
            {"name": "time", "type": {"type": "long", "logicalType": "time-micros"}}
        ],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Time"
    assert field.bits == 64
    assert field.signed is True
    assert field.extra_attrs["unit"] == "microsecond"


def test_timestamp_millis():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_timestamp_millis",
        "fields": [
            {
                "name": "timestamp",
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            }
        ],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Timestamp"
    assert field.bits == 64
    assert field.signed is True
    assert field.extra_attrs["unit"] == "millisecond"
    assert field.extra_attrs["timezone"] == "UTC"


def test_timestamp_micros():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_timestamp_micros",
        "fields": [
            {
                "name": "timestamp",
                "type": {"type": "long", "logicalType": "timestamp-micros"},
            }
        ],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Timestamp"
    assert field.bits == 64
    assert field.signed is True
    assert field.extra_attrs["unit"] == "microsecond"
    assert field.extra_attrs["timezone"] == "UTC"


def test_duration():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_duration",
        "fields": [
            {
                "name": "duration",
                "type": {"type": "fixed", "size": 12, "logicalType": "duration"},
            }
        ],
    }
    schema = converter.convert(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, BytesType)
    assert field.logical == "build.recap.Duration"
    assert field.bytes_ == 12
    assert field.extra_attrs["unit"] == "millisecond"
