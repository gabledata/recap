# pylint: disable=missing-docstring

import pytest

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
    from_dict,
    to_dict,
)


def test_null_type():
    test_dict = {"type": "null"}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, NullType)
    assert recap_type.type_ == "null"


def test_bool_type():
    test_dict = {"type": "bool"}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, BoolType)
    assert recap_type.type_ == "bool"


def test_int_type():
    test_dict = {"type": "int", "bits": 32}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, IntType)
    assert recap_type.type_ == "int"
    assert recap_type.bits == 32


def test_float_type():
    test_dict = {"type": "float", "bits": 64}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, FloatType)
    assert recap_type.type_ == "float"
    assert recap_type.bits == 64


def test_string_type():
    test_dict = {"type": "string", "bytes": 32}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, StringType)
    assert recap_type.type_ == "string"
    assert recap_type.bytes_ == 32


def test_bytes_type():
    test_dict = {"type": "bytes", "bytes": 32}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, BytesType)
    assert recap_type.type_ == "bytes"
    assert recap_type.bytes_ == 32


def test_list_type():
    test_dict = {"type": "list", "values": {"type": "int", "bits": 32}}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, ListType)
    assert recap_type.type_ == "list"
    assert isinstance(recap_type.values, IntType)
    assert recap_type.values.type_ == "int"
    assert recap_type.values.bits == 32


def test_map_type():
    test_dict = {
        "type": "map",
        "keys": {"type": "int", "bits": 32},
        "values": {"type": "string", "bytes": 32},
    }
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, MapType)
    assert recap_type.type_ == "map"
    assert isinstance(recap_type.keys, IntType)
    assert recap_type.keys.type_ == "int"
    assert recap_type.keys.bits == 32
    assert isinstance(recap_type.values, StringType)
    assert recap_type.values.type_ == "string"
    assert recap_type.values.bytes_ == 32


def test_struct_type():
    test_dict = {
        "type": "struct",
        "fields": [{"type": "int", "bits": 32}, {"type": "string", "bytes": 32}],
    }
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, StructType)
    assert recap_type.type_ == "struct"
    for field in recap_type.fields:
        if isinstance(field, IntType):
            assert field.type_ == "int"
            assert field.bits == 32
        elif isinstance(field, StringType):
            assert field.type_ == "string"
            assert field.bytes_ == 32


def test_enum_type():
    test_dict = {"type": "enum", "symbols": ["RED", "GREEN", "BLUE"]}
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, EnumType)
    assert recap_type.type_ == "enum"
    assert recap_type.symbols == ["RED", "GREEN", "BLUE"]


@pytest.mark.parametrize(
    "test_dict",
    [
        {
            "type": "union",
            "types": [{"type": "int", "bits": 32}, {"type": "string", "bytes": 32}],
        },
        {
            "type": "union",
            "types": ["int32", "string32"],
        },
        {
            "type": "union",
            "types": ["int32", {"type": "string", "bytes": 32}],
        },
    ],
)
def test_union_type(test_dict):
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, UnionType)
    assert recap_type.type_ == "union"
    for type_ in recap_type.types:
        if isinstance(type_, IntType):
            assert type_.type_ == "int"
            assert type_.bits == 32
        elif isinstance(type_, StringType):
            assert type_.type_ == "string"
            assert type_.bytes_ == 32


aliases = [
    "int8",
    "uint8",
    "int16",
    "uint16",
    "int32",
    "uint32",
    "int64",
    "uint64",
    "float16",
    "float32",
    "float64",
    "string32",
    "string64",
    "bytes32",
    "bytes64",
    "uuid",
    "decimal128",
    "decimal256",
    "duration64",
    "interval128",
    "time32",
    "time64",
    "timestamp64",
    "date32",
    "date64",
]


@pytest.mark.parametrize("alias", aliases)
def test_aliases(alias):
    recap_type_registry = RecapTypeRegistry()
    recap_type = from_dict({"type": alias}, recap_type_registry)
    assert isinstance(recap_type, ProxyType)
    assert recap_type.resolve() == recap_type_registry.from_alias(alias)


def test_from_dict_raises_for_missing_type():
    with pytest.raises(
        ValueError,
        match="'type' is a required field and was not found in the dictionary.",
    ):
        from_dict({"alias": "alias"})


def test_self_referencing_structure():
    # define the test_dict with the self-referencing structure
    test_dict = {
        "type": "struct",
        "fields": [{"type": "int", "bits": 32}, {"type": "self_reference"}],
        "alias": "self_reference",
    }

    # Create a self-referencing RecapType
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, StructType)
    assert recap_type.type_ == "struct"
    for field in recap_type.fields:
        if isinstance(field, IntType):
            assert field.type_ == "int"
            assert field.bits == 32
        elif isinstance(field, ProxyType):
            assert field.type_ == "proxy"
            assert field.alias == "self_reference"
            # Resolve the ProxyType and check it equals the original RecapType
            assert field.resolve() == recap_type


def test_self_referencing_with_attribute_override():
    # Define a dictionary with a struct RecapType that includes a self-reference with an attribute override
    test_dict = {
        "type": "struct",
        "fields": [
            {"type": "int", "bits": 32, "alias": "myint"},
            {"type": "myint", "signed": False},
        ],
        "alias": "self_reference",
    }

    # Create a self-referencing RecapType with attribute overrides
    recap_type = from_dict(test_dict)
    assert isinstance(recap_type, StructType)
    assert recap_type.type_ == "struct"
    for field in recap_type.fields:
        if isinstance(field, IntType):
            assert field.type_ == "int"
            assert field.bits == 32
            assert field.signed is True
        elif isinstance(field, ProxyType):
            assert field.type_ == "proxy"
            assert field.alias == "myint"
            # Resolve the ProxyType and check it equals the original RecapType
            resolved_type = field.resolve()
            assert isinstance(resolved_type, IntType)
            assert resolved_type.type_ == "int"
            assert resolved_type.bits == 32
            assert resolved_type.signed is False


def test_uuid_logical_type():
    logical_type_dict = {
        "type": "string",
        "logical": "build.recap.UUID",
        "bytes": 36,
        "variable": False,
    }
    recap_type = from_dict(logical_type_dict)
    assert isinstance(recap_type, StringType)
    assert recap_type.logical == logical_type_dict["logical"]
    assert recap_type.bytes_ == logical_type_dict["bytes"]
    assert recap_type.variable == logical_type_dict["variable"]


def test_decimal128_logical_type():
    logical_type_dict = {
        "type": "bytes",
        "logical": "build.recap.Decimal",
        "bytes": 16,
        "variable": False,
        "precision": 28,
        "scale": 14,
    }
    recap_type = from_dict(logical_type_dict)
    assert isinstance(recap_type, BytesType)
    assert recap_type.logical == logical_type_dict["logical"]
    assert recap_type.bytes_ == logical_type_dict["bytes"]
    assert recap_type.variable == logical_type_dict["variable"]
    assert recap_type.extra_attrs["precision"] == logical_type_dict["precision"]
    assert recap_type.extra_attrs["scale"] == logical_type_dict["scale"]


def test_duration64_logical_type():
    logical_type_dict = {
        "type": "int",
        "logical": "build.recap.Duration",
        "bits": 64,
        "signed": True,
        "unit": "millisecond",
    }
    recap_type = from_dict(logical_type_dict)
    assert isinstance(recap_type, IntType)
    assert recap_type.logical == logical_type_dict["logical"]
    assert recap_type.bits == logical_type_dict["bits"]
    assert recap_type.signed == logical_type_dict["signed"]
    assert recap_type.extra_attrs["unit"] == logical_type_dict["unit"]


def test_time64_logical_type():
    logical_type_dict = {
        "type": "int",
        "logical": "build.recap.Time",
        "bits": 64,
        "signed": True,
        "unit": "second",
    }
    recap_type = from_dict(logical_type_dict)
    assert isinstance(recap_type, IntType)
    assert recap_type.logical == logical_type_dict["logical"]
    assert recap_type.bits == logical_type_dict["bits"]
    assert recap_type.signed == logical_type_dict["signed"]
    assert recap_type.extra_attrs["unit"] == logical_type_dict["unit"]


def test_timestamp64_logical_type():
    logical_type_dict = {
        "type": "int",
        "logical": "build.recap.Timestamp",
        "bits": 64,
        "signed": True,
        "unit": "millisecond",
    }
    recap_type = from_dict(logical_type_dict)
    assert isinstance(recap_type, IntType)
    assert recap_type.logical == logical_type_dict["logical"]
    assert recap_type.bits == logical_type_dict["bits"]
    assert recap_type.signed == logical_type_dict["signed"]
    assert recap_type.extra_attrs["unit"] == logical_type_dict["unit"]


def test_date64_logical_type():
    logical_type_dict = {
        "type": "int",
        "logical": "build.recap.Date",
        "bits": 64,
        "signed": True,
        "unit": "day",
    }
    recap_type = from_dict(logical_type_dict)
    assert isinstance(recap_type, IntType)
    assert recap_type.logical == logical_type_dict["logical"]
    assert recap_type.bits == logical_type_dict["bits"]
    assert recap_type.signed == logical_type_dict["signed"]
    assert recap_type.extra_attrs["unit"] == logical_type_dict["unit"]


def test_interval_logical_type():
    logical_type_dict = {
        "type": "bytes",
        "logical": "build.recap.Interval",
        "bytes": 12,
        "variable": False,
    }
    recap_type = from_dict(logical_type_dict)
    assert isinstance(recap_type, BytesType)
    assert recap_type.logical == logical_type_dict["logical"]
    assert recap_type.bytes_ == logical_type_dict["bytes"]
    assert recap_type.variable == logical_type_dict["variable"]


def test_complex_struct_type():
    input_dict = {
        "type": "struct",
        "fields": [
            {
                "type": "list",
                "values": {
                    "type": "map",
                    "keys": {"type": "int", "bits": 32},
                    "values": {"type": "string", "bytes": 50},
                },
            }
        ],
    }
    result = from_dict(input_dict)
    assert isinstance(result, StructType)
    assert isinstance(result.fields[0], ListType)
    assert isinstance(result.fields[0].values, MapType)
    assert isinstance(result.fields[0].values.keys, IntType)
    assert result.fields[0].values.keys.bits == 32
    assert isinstance(result.fields[0].values.values, StringType)
    assert result.fields[0].values.values.bytes_ == 50


def test_complex_union_type():
    input_dict = {
        "type": "union",
        "types": [
            {"type": "int", "bits": 32},
            {"type": "struct", "fields": [{"type": "bool"}]},
        ],
    }
    result = from_dict(input_dict)
    assert isinstance(result, UnionType)
    assert len(result.types) == 2
    assert isinstance(result.types[0], IntType)
    assert result.types[0].bits == 32
    assert isinstance(result.types[1], StructType)
    assert isinstance(result.types[1].fields[0], BoolType)


def test_missing_type():
    with pytest.raises(ValueError):
        input_dict = {}
        from_dict(input_dict)


def test_missing_required_field_for_int():
    with pytest.raises(ValueError):
        input_dict = {"type": "int"}
        from_dict(input_dict)


def test_from_dict_with_union():
    # Define a dictionary representing a union of 'null' and a complex type
    type_dict = {
        "type": [
            "null",
            {
                "type": "map",
                "keys": {"type": "int", "bits": 32},
                "values": {"type": "string", "bytes": 4},
            },
        ]
    }

    # Call from_dict and verify the resulting RecapType
    recap_type = from_dict(type_dict)

    assert isinstance(recap_type, UnionType)
    assert len(recap_type.types) == 2
    assert isinstance(recap_type.types[0], NullType)
    assert isinstance(recap_type.types[1], MapType)
    assert isinstance(recap_type.types[1].keys, IntType)
    assert recap_type.types[1].keys.bits == 32
    assert isinstance(recap_type.types[1].values, StringType)
    assert recap_type.types[1].values.bytes_ == 4


def test_from_dict_with_union_alias():
    # Define a dictionary representing a union of 'null' and 'timestamp64'
    type_dict = {
        "type": [
            "null",
            "timestamp64",
        ]
    }

    # Call from_dict and verify the resulting RecapType
    recap_type = from_dict(type_dict)

    assert isinstance(recap_type, UnionType)
    assert len(recap_type.types) == 2
    assert isinstance(recap_type.types[0], NullType)
    assert isinstance(recap_type.types[1], ProxyType)
    assert recap_type.types[1].alias == "timestamp64"
    resolved_type = recap_type.types[1].resolve()
    assert isinstance(resolved_type, IntType)
    assert resolved_type.logical == "build.recap.Timestamp"
    assert resolved_type.bits == 64
    assert resolved_type.signed is True
    assert resolved_type.extra_attrs["unit"] == "millisecond"


def test_struct_with_string_field_no_bytes_set():
    input_dict = {
        "type": "struct",
        "fields": [
            {
                "type": "string",
            }
        ],
    }
    result = from_dict(input_dict)
    assert isinstance(result, StructType)
    assert isinstance(result.fields[0], StringType)
    assert result.fields[0].bytes_ == 65_536


def test_struct_with_bytes_field_no_bytes_set():
    input_dict = {
        "type": "struct",
        "fields": [
            {
                "type": "bytes",
            }
        ],
    }
    result = from_dict(input_dict)
    assert isinstance(result, StructType)
    assert isinstance(result.fields[0], BytesType)
    assert result.fields[0].bytes_ == 65_536


@pytest.mark.parametrize(
    "input_value, expected_dict, clean, alias",
    [
        (
            NullType(),
            {
                "type": "null",
                "alias": None,
                "doc": None,
                "logical": None,
            },
            False,
            False,
        ),
        (
            NullType(),
            "null",
            True,
            False,
        ),
        (
            BoolType(),
            "bool",
            True,
            False,
        ),
        (
            IntType(bits=32),
            {
                "type": "int",
                "bits": 32,
                "signed": True,
                "alias": None,
                "doc": None,
                "logical": None,
            },
            False,
            False,
        ),
        (
            IntType(bits=32),
            {"type": "int", "bits": 32},
            True,
            False,
        ),
        (
            IntType(bits=32, signed=False),
            {"type": "int", "bits": 32, "signed": False},
            True,
            False,
        ),
        (
            FloatType(bits=32),
            {"type": "float", "bits": 32},
            True,
            False,
        ),
        (
            StringType(),
            "string",
            True,
            False,
        ),
        (
            StringType(bytes_=50),
            {"type": "string", "bytes": 50},
            True,
            False,
        ),
        (
            BytesType(),
            "bytes",
            True,
            False,
        ),
        (
            BytesType(bytes_=50),
            {"type": "bytes", "bytes": 50},
            True,
            False,
        ),
        (
            EnumType(symbols=["foo", "bar"]),
            {"type": "enum", "symbols": ["foo", "bar"]},
            True,
            False,
        ),
        (
            ListType(values=IntType(bits=32)),
            {"type": "list", "values": {"type": "int", "bits": 32}},
            True,
            False,
        ),
        (
            ListType(
                values=IntType(bits=32),
                length=10,
            ),
            {
                "type": "list",
                "values": {"type": "int", "bits": 32},
                "length": 10,
            },
            True,
            False,
        ),
        (
            MapType(
                keys=IntType(bits=32),
                values=StringType(bytes_=50),
            ),
            {
                "type": "map",
                "keys": {"type": "int", "bits": 32},
                "values": {"type": "string", "bytes": 50},
            },
            True,
            False,
        ),
        (
            UnionType(
                types=[
                    IntType(bits=32),
                    StringType(bytes_=50),
                ]
            ),
            [
                {"type": "int", "bits": 32},
                {"type": "string", "bytes": 50},
            ],
            True,
            False,
        ),
        (
            StructType(
                fields=[
                    IntType(bits=32),
                    StringType(bytes_=50),
                ]
            ),
            {
                "type": "struct",
                "fields": [
                    {"type": "int", "bits": 32},
                    {"type": "string", "bytes": 50},
                ],
            },
            True,
            False,
        ),
        (
            StructType(
                fields=[
                    UnionType(
                        default=None,
                        types=[
                            NullType(),
                            IntType(bits=32),
                            StringType(bytes_=50),
                        ],
                    )
                ]
            ),
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "union",
                        "default": None,
                        "types": [
                            "null",
                            {"type": "int", "bits": 32},
                            {"type": "string", "bytes": 50},
                        ],
                    }
                ],
            },
            True,
            False,
        ),
        (
            StructType(
                fields=[
                    ListType(
                        values=MapType(
                            keys=IntType(bits=32), values=StringType(bytes_=50)
                        )
                    )
                ]
            ),
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "list",
                        "values": {
                            "type": "map",
                            "keys": {"type": "int", "bits": 32},
                            "values": {"type": "string", "bytes": 50},
                        },
                    }
                ],
            },
            True,
            False,
        ),
        (
            ProxyType("test_proxy", RecapTypeRegistry()),
            "test_proxy",
            True,
            False,
        ),
        (
            IntType(bits=32),
            "int32",
            True,
            True,
        ),
        (
            BytesType(
                logical="build.recap.Decimal",
                bytes_=32,
                variable=False,
                precision=56,
                scale=28,
            ),
            "decimal256",
            True,
            True,
        ),
        (
            StructType(
                fields=[
                    StringType(
                        name="foo",
                        bytes_=10,
                        variable=False,
                        default="bar",
                    ),
                ],
            ),
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "string",
                        "bytes": 10,
                        "variable": False,
                        "default": "bar",
                        "name": "foo",
                    },
                ],
            },
            True,
            True,
        ),
    ],
)
def test_to_dict(
    input_value: RecapType,
    expected_dict: dict,
    clean: bool,
    alias: bool,
) -> None:
    assert to_dict(input_value, clean, alias) == expected_dict


@pytest.mark.parametrize(
    "input_value, clean, alias",
    [
        (
            NullType(),
            False,
            False,
        ),
        (
            NullType(),
            True,
            False,
        ),
        (
            BoolType(),
            True,
            False,
        ),
        (
            IntType(bits=32),
            False,
            False,
        ),
        (
            IntType(bits=32),
            True,
            False,
        ),
        (
            IntType(bits=32, signed=False),
            True,
            False,
        ),
        (
            FloatType(bits=32),
            True,
            False,
        ),
        (
            StringType(),
            True,
            False,
        ),
        (
            StringType(bytes_=50),
            True,
            False,
        ),
        (
            BytesType(),
            True,
            False,
        ),
        (
            BytesType(bytes_=50),
            True,
            False,
        ),
        (
            EnumType(symbols=["foo", "bar"]),
            True,
            False,
        ),
        (
            ListType(values=IntType(bits=32)),
            True,
            False,
        ),
        (
            ListType(
                values=IntType(bits=32),
                length=10,
            ),
            True,
            False,
        ),
        (
            MapType(
                keys=IntType(bits=32),
                values=StringType(bytes_=50),
            ),
            True,
            False,
        ),
        (
            UnionType(
                types=[
                    IntType(bits=32),
                    StringType(bytes_=50),
                ]
            ),
            True,
            False,
        ),
        (
            StructType(
                fields=[
                    IntType(bits=32),
                    StringType(bytes_=50),
                ]
            ),
            True,
            False,
        ),
        (
            StructType(
                fields=[
                    UnionType(
                        default=None,
                        types=[
                            NullType(),
                            IntType(bits=32),
                            StringType(bytes_=50),
                        ],
                    )
                ]
            ),
            True,
            False,
        ),
        (
            StructType(
                fields=[
                    ListType(
                        values=MapType(
                            keys=IntType(bits=32), values=StringType(bytes_=50)
                        )
                    )
                ]
            ),
            True,
            False,
        ),
        (
            ProxyType("test_proxy", RecapTypeRegistry()),
            True,
            False,
        ),
        (
            ProxyType("test_proxy", RecapTypeRegistry()),
            True,
            True,
        ),
        (
            IntType(bits=32),
            True,
            False,
        ),
        (
            BytesType(
                logical="build.recap.Decimal",
                bytes_=32,
                variable=False,
                precision=56,
                scale=28,
            ),
            True,
            False,
        ),
        (
            StructType(
                fields=[
                    StringType(
                        name="foo",
                        bytes_=10,
                        variable=False,
                        default="bar",
                    ),
                ],
            ),
            True,
            False,
        ),
    ],
)
def test_to_from_dict(
    input_value: RecapType,
    clean: bool,
    alias: bool,
):
    assert from_dict(to_dict(input_value, clean, alias)) == input_value


@pytest.mark.parametrize(
    "input_value, clean, alias",
    [
        (
            {
                "type": "null",
                "alias": None,
                "doc": None,
                "logical": None,
            },
            False,
            False,
        ),
        ("null", True, False),
        ("bool", True, False),
        (
            {
                "type": "int",
                "bits": 32,
                "signed": True,
                "alias": None,
                "doc": None,
                "logical": None,
            },
            False,
            False,
        ),
        ({"type": "int", "bits": 32}, True, False),
        ({"type": "int", "bits": 32, "signed": False}, True, False),
        ({"type": "float", "bits": 32}, True, False),
        ("string", True, False),
        ({"type": "string", "bytes": 50}, True, False),
        ("bytes", True, False),
        ({"type": "bytes", "bytes": 50}, True, False),
        ({"type": "enum", "symbols": ["foo", "bar"]}, True, False),
        ({"type": "list", "values": {"type": "int", "bits": 32}}, True, False),
        (
            {
                "type": "list",
                "values": {"type": "int", "bits": 32},
                "length": 10,
            },
            True,
            False,
        ),
        (
            {
                "type": "map",
                "keys": {"type": "int", "bits": 32},
                "values": {"type": "string", "bytes": 50},
            },
            True,
            False,
        ),
        (
            [
                {"type": "int", "bits": 32},
                {"type": "string", "bytes": 50},
            ],
            True,
            False,
        ),
        (
            {
                "type": "struct",
                "fields": [
                    {"type": "int", "bits": 32},
                    {"type": "string", "bytes": 50},
                ],
            },
            True,
            False,
        ),
        (
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "union",
                        "default": None,
                        "types": [
                            "null",
                            {"type": "int", "bits": 32},
                            {"type": "string", "bytes": 50},
                        ],
                    }
                ],
            },
            True,
            False,
        ),
        (
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "list",
                        "values": {
                            "type": "map",
                            "keys": {"type": "int", "bits": 32},
                            "values": {"type": "string", "bytes": 50},
                        },
                    }
                ],
            },
            True,
            False,
        ),
        ("test_proxy", True, True),
        ("test_proxy", True, False),
        ("int32", True, True),
        ("int32", True, False),
        ("decimal256", True, True),
        ("decimal256", True, False),
        (
            {
                "type": "struct",
                "fields": [
                    {
                        "type": "string",
                        "bytes": 10,
                        "variable": False,
                        "default": "bar",
                        "name": "foo",
                    },
                ],
            },
            True,
            True,
        ),
    ],
)
def test_from_to_dict(
    input_value: dict,
    clean: bool,
    alias: bool,
) -> None:
    assert to_dict(from_dict(input_value), clean, alias) == input_value


def test_to_dict_compact_union():
    union_type = UnionType(
        types=[
            IntType(bits=30),
            StringType(bytes_=50),
        ]
    )
    assert to_dict(union_type) == [
        {"type": "int", "bits": 30},
        {"type": "string", "bytes": 50},
    ]


def test_to_dict_compact_type():
    assert to_dict(IntType(bits=32)) == "int32"