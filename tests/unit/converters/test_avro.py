# pylint: disable=missing-docstring

import json

import pytest

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
    StringType,
    StructType,
    UnionType,
    from_dict,
)


def test_to_recap_parse_primitives():
    converter = AvroConverter()
    primitives = [
        ("null", NullType()),
        ("boolean", BoolType()),
        ("int", IntType(32, signed=True)),
        ("long", IntType(64, signed=True)),
        ("float", FloatType(32)),
        ("double", FloatType(64)),
        ("bytes", BytesType(9_223_372_036_854_775_807, variable=True)),
        ("string", StringType(9_223_372_036_854_775_807, variable=True)),
    ]
    for avro_type, recap_type in primitives:
        assert converter._parse(avro_type, "_root") == recap_type


def test_to_recap_fixed():
    converter = AvroConverter()
    avro_enum = {
        "type": "record",
        "name": "TestEnum",
        "fields": [
            {
                "name": "fixed_field",
                "type": {
                    "type": "fixed",
                    "name": "MD5",
                    "size": 16,
                },
            }
        ],
    }
    actual = converter.to_recap(json.dumps(avro_enum))

    fixed_type = BytesType(
        bytes_=16,
        name="fixed_field",
        alias="_root.MD5",
    )
    expected = StructType(fields=[fixed_type], name="TestEnum", alias="_root.TestEnum")

    assert actual == expected


def test_to_recap_enum():
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
    actual = converter.to_recap(json.dumps(avro_enum))

    enum_type = EnumType(
        symbols=["RED", "GREEN", "BLUE"],
        name="color",
        alias="_root.Color",
    )
    expected = StructType(fields=[enum_type], name="TestEnum", alias="_root.TestEnum")

    assert actual == expected


def test_to_recap_enum_with_inherited_namespace():
    converter = AvroConverter()
    avro_enum = {
        "type": "record",
        "namespace": "com.example",
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
    actual = converter.to_recap(json.dumps(avro_enum))

    enum_type = EnumType(
        symbols=["RED", "GREEN", "BLUE"],
        name="color",
        alias="com.example.Color",
    )
    expected = StructType(
        fields=[enum_type],
        name="TestEnum",
        alias="com.example.TestEnum",
    )

    assert actual == expected


def test_to_recap_enum_with_namespace_override():
    converter = AvroConverter()
    avro_enum = {
        "type": "record",
        "namespace": "com.example",
        "name": "TestEnum",
        "fields": [
            {
                "name": "color",
                "type": {
                    "type": "enum",
                    "namespace": "foo.bar",
                    "name": "Color",
                    "symbols": ["RED", "GREEN", "BLUE"],
                },
            }
        ],
    }
    actual = converter.to_recap(json.dumps(avro_enum))

    enum_type = EnumType(
        symbols=["RED", "GREEN", "BLUE"],
        name="color",
        alias="foo.bar.Color",
    )
    expected = StructType(
        fields=[enum_type],
        name="TestEnum",
        alias="com.example.TestEnum",
    )

    assert actual == expected


def test_to_recap_enum_with_fully_qualified_name():
    converter = AvroConverter()
    avro_enum = {
        "type": "record",
        "name": "com.example.TestEnum",
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
    actual = converter.to_recap(json.dumps(avro_enum))

    enum_type = EnumType(
        symbols=["RED", "GREEN", "BLUE"],
        name="color",
        alias="com.example.Color",
    )
    expected = StructType(
        fields=[enum_type],
        name="TestEnum",
        alias="com.example.TestEnum",
    )

    assert actual == expected


def test_to_recap_enum_with_fully_qualified_name_and_override():
    converter = AvroConverter()
    avro_enum = {
        "type": "record",
        "name": "com.example.TestEnum",
        "fields": [
            {
                "name": "color",
                "type": {
                    "type": "enum",
                    "name": "foo.bar.Color",
                    "symbols": ["RED", "GREEN", "BLUE"],
                },
            }
        ],
    }
    actual = converter.to_recap(json.dumps(avro_enum))

    enum_type = EnumType(
        symbols=["RED", "GREEN", "BLUE"],
        name="color",
        alias="foo.bar.Color",
    )
    expected = StructType(
        fields=[enum_type],
        name="TestEnum",
        alias="com.example.TestEnum",
    )

    assert actual == expected


def test_to_recap_array():
    converter = AvroConverter()
    avro_array = {
        "type": "record",
        "name": "TestArray",
        "fields": [{"name": "items", "type": {"type": "array", "items": "string"}}],
    }
    actual = converter.to_recap(json.dumps(avro_array))

    array_type = ListType(name="items", values=StringType(9_223_372_036_854_775_807))
    expected = StructType(
        fields=[array_type], name="TestArray", alias="_root.TestArray"
    )

    assert actual == expected


def test_to_recap_map():
    converter = AvroConverter()
    avro_map = {
        "type": "record",
        "name": "TestMap",
        "fields": [{"name": "values", "type": {"type": "map", "values": "int"}}],
    }
    actual = converter.to_recap(json.dumps(avro_map))

    # Construct an expected Map RecapType instance
    map_type = MapType(
        name="values",
        keys=StringType(9_223_372_036_854_775_807),
        values=IntType(32, signed=True),
    )
    expected = StructType(fields=[map_type], name="TestMap", alias="_root.TestMap")

    assert actual == expected


def test_to_recap_union():
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
    actual = converter.to_recap(json.dumps(avro_union))

    # Construct an expected Union RecapType instance
    union_type = UnionType(
        name="unionField",
        types=[StringType(9_223_372_036_854_775_807), NullType()],
    )
    expected = StructType(
        fields=[union_type],
        name="TestUnion",
        alias="_root.TestUnion",
    )

    assert actual == expected


def test_to_recap_record():
    converter = AvroConverter()
    avro_record = {
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "string"},
        ],
    }
    actual = converter.to_recap(json.dumps(avro_record))
    assert isinstance(actual, StructType)
    assert len(actual.fields) == 2
    assert isinstance(actual.fields[0], IntType)
    assert actual.fields[0].bits == 32
    assert actual.fields[0].extra_attrs["name"] == "a"
    assert isinstance(actual.fields[1], StringType)
    assert actual.fields[1].bytes_ == 9_223_372_036_854_775_807
    assert actual.fields[1].extra_attrs["name"] == "b"
    assert actual.extra_attrs == {"name": "Test"}
    assert actual.alias == "_root.Test"


def test_to_recap_self_referencing():
    converter = AvroConverter()

    # Avro schema for a simple linked list
    avro_schema = {
        "type": "record",
        "name": "build.recap.LinkedList",
        "fields": [
            {"name": "value", "type": "int"},
            {
                "name": "next",
                "type": ["null", "build.recap.LinkedList"],
            },
        ],
    }

    # The expected schema is a recursive structure, so we can't create it
    # directly as Python would run into an infinite recursion. We can only
    # check that the resulting structure has the expected properties.
    actual = converter.to_recap(json.dumps(avro_schema))

    assert isinstance(actual, StructType)
    assert actual.extra_attrs["name"] == "LinkedList"
    assert actual.alias == "build.recap.LinkedList"
    assert len(actual.fields) == 2
    assert isinstance(actual.fields[0], IntType)
    assert isinstance(actual.fields[1], UnionType)
    assert actual.fields[1].extra_attrs["name"] == "next"
    assert isinstance(actual.fields[1].types[0], NullType)
    assert isinstance(actual.fields[1].types[1], ProxyType)
    assert actual.fields[1].types[1].alias == "build.recap.LinkedList"

    # Strip the alias since the resolved type won't have an alias (since it's
    # already been defined in the avro_schema struct).
    actual.alias = None

    assert actual.fields[1].types[1].resolve() == actual


def test_to_recap_record_with_docs_and_default():
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

    actual = converter.to_recap(json.dumps(avro_schema))

    assert isinstance(actual, StructType)
    assert len(actual.fields) == 2
    assert actual.doc == "A person record"
    assert isinstance(actual.fields[0], StringType)
    assert actual.fields[0].doc == "The person's name"
    assert actual.fields[0].extra_attrs["default"] == "unknown"
    assert isinstance(actual.fields[1], IntType)
    assert actual.fields[1].doc == "The person's age"
    assert actual.fields[1].extra_attrs["default"] == 0


def test_to_recap_decimal():
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
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, BytesType)
    assert field.logical == "build.recap.Decimal"
    assert field.bytes_ == avro_schema["fields"][0]["type"].get(
        "size",
        9_223_372_036_854_775_807,
    )
    assert field.variable == (avro_schema["fields"][0]["type"]["type"] == "fixed")
    assert (
        field.extra_attrs["precision"] == avro_schema["fields"][0]["type"]["precision"]
    )
    assert field.extra_attrs["scale"] == avro_schema["fields"][0]["type"].get(
        "scale",
        0,
    )


def test_to_recap_uuid():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_uuid",
        "fields": [{"name": "uuid", "type": {"type": "string", "logicalType": "uuid"}}],
    }
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, StringType)
    assert field.logical == "build.recap.UUID"
    assert field.bytes_ == 36
    assert field.variable == False


def test_to_recap_date():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_date",
        "fields": [{"name": "date", "type": {"type": "int", "logicalType": "date"}}],
    }
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Date"
    assert field.bits == 32
    assert field.signed is True
    assert field.extra_attrs["unit"] == "day"


def test_to_recap_time_millis():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_time_millis",
        "fields": [
            {"name": "time", "type": {"type": "int", "logicalType": "time-millis"}}
        ],
    }
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Time"
    assert field.bits == 32
    assert field.signed is True
    assert field.extra_attrs["unit"] == "millisecond"


def test_to_recap_time_micros():
    converter = AvroConverter()
    avro_schema = {
        "type": "record",
        "name": "test_time_micros",
        "fields": [
            {"name": "time", "type": {"type": "long", "logicalType": "time-micros"}}
        ],
    }
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Time"
    assert field.bits == 64
    assert field.signed is True
    assert field.extra_attrs["unit"] == "microsecond"


def test_to_recap_timestamp_millis():
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
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Timestamp"
    assert field.bits == 64
    assert field.signed is True
    assert field.extra_attrs["unit"] == "millisecond"


def test_to_recap_timestamp_micros():
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
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, IntType)
    assert field.logical == "build.recap.Timestamp"
    assert field.bits == 64
    assert field.signed is True
    assert field.extra_attrs["unit"] == "microsecond"


def test_to_recap_duration():
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
    schema = converter.to_recap(json.dumps(avro_schema))
    field = schema.fields[0]
    assert isinstance(field, BytesType)
    assert field.logical == "build.recap.Interval"
    assert field.bytes_ == 12
    assert field.extra_attrs["unit"] == "millisecond"


@pytest.mark.parametrize(
    "avro_type_dict, recap_type",
    [
        ({"type": "null"}, NullType()),
        ({"type": "null"}, NullType(some_attr="some_value")),
        ({"type": "boolean"}, BoolType()),
        ({"type": "int"}, IntType(bits=32, signed=True)),
        ({"type": "long"}, IntType(bits=64, signed=True)),
        ({"type": "float"}, FloatType(bits=32)),
        ({"type": "double"}, FloatType(bits=64)),
        (
            {"type": "bytes"},
            BytesType(
                bytes_=9_223_372_036_854_775_807,
                variable=True,
            ),
        ),
        (
            {"type": "string"},
            StringType(
                bytes_=9_223_372_036_854_775_807,
                variable=True,
            ),
        ),
        (
            {"type": "map", "values": "int"},
            MapType(
                keys=StringType(bytes_=9_223_372_036_854_775_807, variable=True),
                values=IntType(bits=32, signed=True),
            ),
        ),
        (
            {"type": "array", "items": "long"},
            ListType(values=IntType(bits=64, signed=True)),
        ),
        (
            {
                "type": "record",
                "fields": [{"type": "int", "name": "field1"}],
            },
            StructType(fields=[IntType(bits=32, signed=True, name="field1")]),
        ),
        (
            {"type": "enum", "symbols": ["A", "B", "C"]},
            EnumType(symbols=["A", "B", "C"]),
        ),
        (
            {"type": ["null", "int"]},
            UnionType(types=[NullType(), IntType(bits=32, signed=True)]),
        ),
        (
            {"type": "long"},
            IntType(bits=32, signed=False),
        ),
        (
            {"type": "fixed", "size": 4, "name": "fixed"},
            BytesType(bytes_=4, variable=False, name="fixed"),
        ),
        (
            {
                "type": "record",
                "fields": [
                    {
                        "name": "field1",
                        "type": "array",
                        "items": {
                            "type": "map",
                            "values": "int",
                        },
                    },
                ],
            },
            StructType(
                fields=[
                    ListType(
                        name="field1",
                        values=MapType(
                            keys=StringType(),
                            values=IntType(bits=32, signed=True),
                        ),
                    )
                ],
            ),
        ),
        (
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 38,
            },
            IntType(bits=128, signed=True),
        ),
        (
            {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 38,
                "scale": 0,
            },
            BytesType(
                logical="build.recap.Decimal",
                bytes_=9_223_372_036_854_775_807,
                variable=True,
                precision=38,
                scale=0,
            ),
        ),
        (
            {
                "type": "string",
                "logicalType": "uuid",
            },
            StringType(
                logical="build.recap.UUID",
                bytes_=36,
                variable=False,
            ),
        ),
        (
            {
                "type": "int",
                "logicalType": "date",
            },
            IntType(
                logical="build.recap.Date",
                bits=32,
                signed=True,
                unit="day",
            ),
        ),
        (
            {
                "type": "int",
                "logicalType": "time-millis",
            },
            IntType(
                logical="build.recap.Time",
                bits=32,
                signed=True,
                unit="millisecond",
            ),
        ),
        (
            {
                "type": "long",
                "logicalType": "time-micros",
            },
            IntType(
                logical="build.recap.Time",
                bits=64,
                signed=True,
                unit="microsecond",
            ),
        ),
        (
            {
                "type": "long",
                "logicalType": "timestamp-millis",
            },
            IntType(
                logical="build.recap.Timestamp",
                bits=64,
                signed=True,
                unit="millisecond",
            ),
        ),
        (
            {
                "type": "long",
                "logicalType": "timestamp-micros",
            },
            IntType(
                logical="build.recap.Timestamp",
                bits=64,
                signed=True,
                unit="microsecond",
            ),
        ),
        (
            # Not valid Avro, since Fixed requires a name. This is fine since
            # we are assuming fixed, enum, etc are in a StructType with a name.
            {
                "type": "fixed",
                "size": 12,
                "logicalType": "duration",
            },
            BytesType(
                logical="build.recap.Interval",
                bytes_=12,
                variable=False,
                unit="millisecond",
            ),
        ),
        (
            # Test a logical type with extra attributes.
            {
                "type": "long",
                "logicalType": "timestamp-micros",
            },
            IntType(
                logical="build.recap.Timestamp",
                bits=64,
                signed=True,
                unit="microsecond",
                some_attr="foo",
            ),
        ),
        (
            {
                "type": "record",
                "aliases": ["build.recap.TestAlias"],
                "fields": [{"type": "int", "name": "field1"}],
            },
            StructType(
                alias="build.recap.TestAlias",
                fields=[
                    IntType(bits=32, signed=True, name="field1"),
                ],
            ),
        ),
    ],
)
def test_from_recap_primitives(avro_type_dict, recap_type):
    assert AvroConverter().from_recap(recap_type) == avro_type_dict


def test_from_recap_proxy_types_builtin_int():
    recap_type = from_dict(
        {
            "type": "struct",
            "fields": [
                {
                    "name": "field1",
                    "type": "int32",
                },
            ],
        }
    )
    avro_schema = AvroConverter().from_recap(recap_type)
    assert avro_schema == {
        "type": "record",
        "fields": [
            {
                "name": "field1",
                "type": "int",
            },
        ],
    }


def test_from_recap_proxy_types_regular():
    recap_type = from_dict(
        {
            "type": "struct",
            "fields": [
                {
                    "name": "field1",
                    "alias": "build.recap.Field1Alias",
                    "type": "int",
                    "bits": 32,
                },
                {
                    "name": "field2",
                    "type": "build.recap.Field1Alias",
                },
            ],
        }
    )
    avro_schema = AvroConverter().from_recap(recap_type)
    assert avro_schema == {
        "type": "record",
        "fields": [
            {
                "name": "field1",
                "aliases": ["build.recap.Field1Alias"],
                "type": "int",
            },
            {
                "name": "field2",
                "type": "build.recap.Field1Alias",
            },
        ],
    }


def test_from_recap_proxy_types_self_reference():
    recap_type = from_dict(
        {
            "type": "struct",
            "alias": "build.recap.LinkedListNode",
            "fields": [
                {
                    "name": "value",
                    "type": "int32",
                },
                {
                    "name": "next",
                    "type": "build.recap.LinkedListNode",
                },
            ],
        }
    )
    avro_schema = AvroConverter().from_recap(recap_type)
    assert avro_schema == {
        "type": "record",
        "aliases": ["build.recap.LinkedListNode"],
        "fields": [
            {
                "name": "value",
                "type": "int",
            },
            {
                "name": "next",
                "type": "build.recap.LinkedListNode",
            },
        ],
    }
