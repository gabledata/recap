import pytest
from proto_schema_parser import ast

from recap.converters.protobuf import ProtobufConverter
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
)


def test_to_recap_converter():
    protobuf_schema = """
    syntax = "proto3";
    message Test {
        string name = 1;
        bool is_valid = 2;
        bytes data = 3;
        int32 id = 4;
        int64 large_id = 5;
        uint32 uid = 6;
        uint64 large_uid = 7;
        sint32 sid = 8;
        sint64 large_sid = 9;
        float score = 10;
        double large_score = 11;
        fixed32 fixed_id = 12;
        fixed64 large_fixed_id = 13;
        sfixed32 sfixed_id = 14;
        sfixed64 large_sfixed_id = 15;
    }
    """
    recap_schema = ProtobufConverter().to_recap(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    assert recap_schema.alias == "_root.Test"
    fields = recap_schema.fields
    assert len(fields) == 15

    # Field 1: string name = 1;
    assert isinstance(fields[0], UnionType)
    assert fields[0].extra_attrs["name"] == "name"
    assert fields[0].extra_attrs["default"] == None
    assert isinstance(fields[0].types[1], StringType)
    assert fields[0].types[1].bytes_ == 2_147_483_648
    assert fields[0].types[1].variable == True

    # Field 2: bool is_valid = 2;
    assert isinstance(fields[1], UnionType)
    assert fields[1].extra_attrs["name"] == "is_valid"
    assert fields[1].extra_attrs["default"] == None
    assert isinstance(fields[1].types[1], BoolType)

    # Field 3: bytes data = 3;
    assert isinstance(fields[2], UnionType)
    assert fields[2].extra_attrs["name"] == "data"
    assert fields[2].extra_attrs["default"] == None
    assert isinstance(fields[2].types[1], BytesType)
    assert fields[2].types[1].bytes_ == 2_147_483_648
    assert fields[2].types[1].variable == True

    # Field 4: int32 id = 4;
    assert isinstance(fields[3], UnionType)
    assert fields[3].extra_attrs["name"] == "id"
    assert fields[3].extra_attrs["default"] == None
    assert isinstance(fields[3].types[1], IntType)
    assert fields[3].types[1].bits == 32
    assert fields[3].types[1].signed == True

    # Field 5: int64 large_id = 5;
    assert isinstance(fields[4], UnionType)
    assert fields[4].extra_attrs["name"] == "large_id"
    assert fields[4].extra_attrs["default"] == None
    assert isinstance(fields[4].types[1], IntType)
    assert fields[4].types[1].bits == 64
    assert fields[4].types[1].signed == True

    # Field 6: uint32 uid = 6;
    assert isinstance(fields[5], UnionType)
    assert fields[5].extra_attrs["name"] == "uid"
    assert fields[5].extra_attrs["default"] == None
    assert isinstance(fields[5].types[1], IntType)
    assert fields[5].types[1].bits == 32
    assert fields[5].types[1].signed == False

    # Field 7: uint64 large_uid = 7;
    assert isinstance(fields[6], UnionType)
    assert fields[6].extra_attrs["name"] == "large_uid"
    assert fields[6].extra_attrs["default"] == None
    assert isinstance(fields[6].types[1], IntType)
    assert fields[6].types[1].bits == 64
    assert fields[6].types[1].signed == False

    # Field 8: sint32 sid = 8;
    assert isinstance(fields[7], UnionType)
    assert fields[7].extra_attrs["name"] == "sid"
    assert fields[7].extra_attrs["default"] == None
    assert isinstance(fields[7].types[1], IntType)
    assert fields[7].types[1].bits == 32
    assert fields[7].types[1].signed == True

    # Field 9: sint64 large_sid = 9;
    assert isinstance(fields[8], UnionType)
    assert fields[8].extra_attrs["name"] == "large_sid"
    assert fields[8].extra_attrs["default"] == None
    assert isinstance(fields[8].types[1], IntType)
    assert fields[8].types[1].bits == 64
    assert fields[8].types[1].signed == True

    # Field 10: float score = 10;
    assert isinstance(fields[9], UnionType)
    assert fields[9].extra_attrs["name"] == "score"
    assert fields[9].extra_attrs["default"] == None
    assert isinstance(fields[9].types[1], FloatType)
    assert fields[9].types[1].bits == 32

    # Field 11: double large_score = 11;
    assert isinstance(fields[10], UnionType)
    assert fields[10].extra_attrs["name"] == "large_score"
    assert fields[10].extra_attrs["default"] == None
    assert isinstance(fields[10].types[1], FloatType)
    assert fields[10].types[1].bits == 64

    # Field 12: fixed32 fixed_id = 12;
    assert isinstance(fields[11], UnionType)
    assert fields[11].extra_attrs["name"] == "fixed_id"
    assert fields[11].extra_attrs["default"] == None
    assert isinstance(fields[11].types[1], IntType)
    assert fields[11].types[1].bits == 32
    assert fields[11].types[1].signed == False

    # Field 13: fixed64 large_fixed_id = 13;
    assert isinstance(fields[12], UnionType)
    assert fields[12].extra_attrs["name"] == "large_fixed_id"
    assert fields[12].extra_attrs["default"] == None
    assert isinstance(fields[12].types[1], IntType)
    assert fields[12].types[1].bits == 64
    assert fields[12].types[1].signed == False

    # Field 14: sfixed32 sfixed_id = 14;
    assert isinstance(fields[13], UnionType)
    assert fields[13].extra_attrs["name"] == "sfixed_id"
    assert fields[13].extra_attrs["default"] == None
    assert isinstance(fields[13].types[1], IntType)
    assert fields[13].types[1].bits == 32
    assert fields[13].types[1].signed == True

    # Field 15: sfixed64 large_sfixed_id = 15;
    assert isinstance(fields[14], UnionType)
    assert fields[14].extra_attrs["name"] == "large_sfixed_id"
    assert fields[14].extra_attrs["default"] == None
    assert isinstance(fields[14].types[1], IntType)
    assert fields[14].types[1].bits == 64
    assert fields[14].types[1].signed == True


def test_to_recap_converter_repeated():
    protobuf_schema = """
        syntax = "proto3";
        message Test {
            repeated int32 values = 1;
        }
    """

    recap_schema = ProtobufConverter().to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    ListType(IntType(bits=32, signed=True)),
                ],
                name="values",
                default=None,
            )
        ],
        alias="_root.Test",
    )


def test_to_recap_converter_map():
    protobuf_schema = """
        syntax = "proto3";
        message Test {
            map<string, int32> value = 1;
        }
    """

    recap_schema = ProtobufConverter().to_recap(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    MapType(
                        StringType(variable=True, bytes_=2_147_483_648),
                        IntType(bits=32, signed=True),
                    ),
                ],
                name="value",
                default=None,
            )
        ],
        alias="_root.Test",
    )


def test_to_recap_converter_forward_reference():
    protobuf_schema = """
        syntax = "proto3";
        message Outer {
            Inner outerValue = 1;
        }

        message Inner {
            int32 innerValue = 1;
        }
    """

    converter = ProtobufConverter()
    recap_schema = converter.to_recap(protobuf_schema)

    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    StructType(
                        [
                            UnionType(
                                [
                                    NullType(),
                                    IntType(bits=32, signed=True),
                                ],
                                name="innerValue",
                                default=None,
                            )
                        ],
                        alias="_root.Inner",
                    ),
                ],
                name="outerValue",
                default=None,
            )
        ],
        alias="_root.Outer",
    )


def test_to_recap_converter_map_forward_reference():
    protobuf_schema = """
        syntax = "proto3";
        message Test {
            map<string, Inner> testValue = 1;
        }

        message Inner {
            int32 innerValue = 1;
        }
    """

    converter = ProtobufConverter()
    recap_schema = converter.to_recap(protobuf_schema)

    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    MapType(
                        StringType(variable=True, bytes_=2_147_483_648),
                        StructType(
                            [
                                UnionType(
                                    [
                                        NullType(),
                                        IntType(bits=32, signed=True),
                                    ],
                                    name="innerValue",
                                    default=None,
                                )
                            ],
                            alias="_root.Inner",
                        ),
                    ),
                ],
                name="testValue",
                default=None,
            )
        ],
        alias="_root.Test",
    )


def test_to_recap_converter_nested_message():
    protobuf_schema = """
        syntax = "proto3";
        message Outer {
            message Inner {
                int32 innerValue = 1;
            }
            Inner outerValue = 2;
        }
    """

    converter = ProtobufConverter()
    recap_schema = converter.to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    StructType(
                        [
                            UnionType(
                                [
                                    NullType(),
                                    IntType(bits=32, signed=True),
                                ],
                                name="innerValue",
                                default=None,
                            )
                        ],
                        alias="_root.Inner",
                    ),
                ],
                name="outerValue",
                default=None,
            )
        ],
        alias="_root.Outer",
    )


def test_to_recap_converter_enum():
    protobuf_schema = """
        syntax = "proto3";
        message WeatherReport {
            Season season = 1;
        }
        enum Season {
            SPRING = 0;
            SUMMER = 1;
            AUTUMN = 2;
            WINTER = 3;
        }
    """

    converter = ProtobufConverter()
    recap_schema = converter.to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    EnumType(
                        ["SPRING", "SUMMER", "AUTUMN", "WINTER"],
                        alias="_root.Season",
                    ),
                ],
                name="season",
                default=None,
            )
        ],
        alias="_root.WeatherReport",
    )


def test_to_recap_converter_oneof():
    protobuf_schema = """
        syntax = "proto3";
        message Contact {
            oneof contact_type {
                string email = 1;
                int64 phone_number = 2;
            }
        }
    """

    recap_schema = ProtobufConverter().to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    StringType(variable=True, bytes_=2_147_483_648, name="email"),
                    IntType(bits=64, signed=True, name="phone_number"),
                ],
                name="contact_type",
                default=None,
            )
        ],
        alias="_root.Contact",
    )


def test_to_recap_converter_doubly_nested_message():
    protobuf_schema = """
        syntax = "proto3";
        message Outer {
            message Middle {
                message Inner {
                    int32 innerValue = 1;
                }
                Inner middleValue = 2;
            }
            Middle outerValue = 3;
        }
    """

    converter = ProtobufConverter()
    recap_schema = converter.to_recap(protobuf_schema)

    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    StructType(
                        [
                            UnionType(
                                [
                                    NullType(),
                                    StructType(
                                        [
                                            UnionType(
                                                [
                                                    NullType(),
                                                    IntType(bits=32, signed=True),
                                                ],
                                                name="innerValue",
                                                default=None,
                                            )
                                        ],
                                        alias="_root.Inner",
                                    ),
                                ],
                                name="middleValue",
                                default=None,
                            )
                        ],
                        alias="_root.Middle",
                    ),
                ],
                name="outerValue",
                default=None,
            )
        ],
        alias="_root.Outer",
    )


def test_to_recap_converter_timestamp():
    protobuf_schema = """
    syntax = "proto3";
    import "google/protobuf/timestamp.proto";
    message Test {
        google.protobuf.Timestamp timestamp = 1;
    }
    """
    recap_schema = ProtobufConverter().to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    IntType(
                        bits=64,
                        signed=True,
                        logical="build.recap.Timestamp",
                        unit="nanosecond",
                        timezone="UTC",
                    ),
                ],
                name="timestamp",
                default=None,
            )
        ],
        alias="_root.Test",
    )


def test_to_recap_converter_duration():
    protobuf_schema = """
    syntax = "proto3";
    import "google/protobuf/duration.proto";
    message Test {
        google.protobuf.Duration duration = 1;
    }
    """
    recap_schema = ProtobufConverter().to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    IntType(
                        bits=64,
                        signed=True,
                        logical="build.recap.Duration",
                        unit="nanosecond",
                    ),
                ],
                name="duration",
                default=None,
            )
        ],
        alias="_root.Test",
    )


def test_to_recap_converter_nullvalue():
    protobuf_schema = """
    syntax = "proto3";
    import "google/protobuf/struct.proto";
    message Test {
        google.protobuf.NullValue null_value = 1;
    }
    """
    recap_schema = ProtobufConverter().to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            NullType(
                name="null_value",
                default=None,
            )
        ],
        alias="_root.Test",
    )


def test_to_recap_converter_self_reference():
    protobuf_schema = """
        syntax = "proto3";
        message Outer {
            int32 value = 1;
            Outer next = 2;
        }
    """

    converter = ProtobufConverter()
    recap_schema = converter.to_recap(protobuf_schema)

    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    IntType(bits=32, signed=True),
                ],
                name="value",
                default=None,
            ),
            UnionType(
                [
                    NullType(),
                    ProxyType(
                        alias="_root.Outer",
                        registry=converter.registry,
                    ),
                ],
                name="next",
                default=None,
            ),
        ],
        alias="_root.Outer",
    )

    # Strip the alias since the resolved type won't have an alias.
    recap_schema.alias = None

    assert isinstance(recap_schema.fields[1], UnionType)
    assert isinstance(recap_schema.fields[1].types[1], ProxyType)
    assert recap_schema.fields[1].types[1].resolve() == recap_schema


def test_to_recap_converter_custom_namespace():
    protobuf_schema = """
        syntax = "proto3";
        message Outer {
            message Inner {
                int32 innerValue = 1;
            }
            Inner outerValue = 2;
        }
    """

    converter = ProtobufConverter("foo.bar")
    recap_schema = converter.to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    StructType(
                        [
                            UnionType(
                                [
                                    NullType(),
                                    IntType(bits=32, signed=True),
                                ],
                                name="innerValue",
                                default=None,
                            )
                        ],
                        alias="foo.bar.Inner",
                    ),
                ],
                name="outerValue",
                default=None,
            )
        ],
        alias="foo.bar.Outer",
    )


def test_to_recap_converter_package():
    protobuf_schema = """
        syntax = "proto3";
        package foo.bar.baz;
        message Outer {
            message Inner {
                int32 innerValue = 1;
            }
            Inner outerValue = 2;
        }
    """

    converter = ProtobufConverter()
    recap_schema = converter.to_recap(protobuf_schema)
    assert recap_schema == StructType(
        [
            UnionType(
                [
                    NullType(),
                    StructType(
                        [
                            UnionType(
                                [
                                    NullType(),
                                    IntType(bits=32, signed=True),
                                ],
                                name="innerValue",
                                default=None,
                            )
                        ],
                        alias="foo.bar.baz.Inner",
                    ),
                ],
                name="outerValue",
                default=None,
            )
        ],
        alias="foo.bar.baz.Outer",
    )


def test_to_recap_converter_separate_files():
    protobuf_schema_1 = """
        syntax = "proto3";
        package foo.blah;
        message Inner {
            int32 innerValue = 1;
        }
    """
    protobuf_schema_2 = """
        syntax = "proto3";
        message Outer {
            foo.blah.Inner outerValue = 1;
        }
    """

    converter = ProtobufConverter()

    # Ignore return result. Just calling to load Inner into registry.
    converter.to_recap(protobuf_schema_1)

    recap_schema_2 = converter.to_recap(protobuf_schema_2)

    assert recap_schema_2 == StructType(
        [
            UnionType(
                [
                    NullType(),
                    StructType(
                        [
                            UnionType(
                                [
                                    NullType(),
                                    IntType(bits=32, signed=True),
                                ],
                                name="innerValue",
                                default=None,
                            )
                        ],
                        alias="foo.blah.Inner",
                    ),
                ],
                name="outerValue",
                default=None,
            )
        ],
        alias="_root.Outer",
    )


@pytest.mark.parametrize(
    "recap_type, expected_proto_type",
    [
        (NullType(name="some_field"), "google.protobuf.NullValue"),
        (BoolType(name="some_field"), "bool"),
        (IntType(signed=True, bits=32, name="some_field"), "int32"),
        (IntType(signed=True, bits=64, name="some_field"), "int64"),
        (IntType(signed=False, bits=32, name="some_field"), "uint32"),
        (IntType(signed=False, bits=64, name="some_field"), "uint64"),
        (FloatType(bits=32, name="some_field"), "float"),
        (FloatType(bits=64, name="some_field"), "double"),
        (StringType(bytes_=100, name="some_field"), "string"),
        (BytesType(bytes_=100, name="some_field"), "bytes"),
    ],
)
def test_from_recap(recap_type, expected_proto_type):
    converter = ProtobufConverter()
    struct_type = StructType(fields=[recap_type], alias="build.recap.MyStruct")
    result = converter.from_recap(struct_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 2

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    message = result.file_elements[1]
    assert isinstance(message, ast.Message)
    assert message.name == "MyStruct"
    assert len(message.elements) == 1

    field = message.elements[0]
    assert isinstance(field, ast.Field)
    assert field.name == "some_field"
    assert field.type == expected_proto_type
    assert field.number == 1


def test_from_recap_list_type():
    converter = ProtobufConverter()
    recap_type = ListType(
        values=IntType(signed=True, bits=32, name="some_int"),
        name="some_list",
    )
    struct_type = StructType(fields=[recap_type], alias="build.recap.MyStruct")

    result = converter.from_recap(struct_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 2

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    message = result.file_elements[1]
    assert isinstance(message, ast.Message)
    assert message.name == "MyStruct"
    assert len(message.elements) == 1

    field = message.elements[0]
    assert isinstance(field, ast.Field)
    assert field.name == "some_int"
    assert field.type == "int32"
    assert field.cardinality == ast.FieldCardinality.REPEATED
    assert field.number == 1


def test_from_recap_map_type():
    converter = ProtobufConverter()
    recap_type = MapType(
        keys=StringType(bytes_=100, name="map_key"),
        values=IntType(signed=True, bits=32, name="map_value"),
        name="some_map",
    )
    struct_type = StructType(fields=[recap_type], alias="build.recap.MyStruct")
    result = converter.from_recap(struct_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 2

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    message = result.file_elements[1]
    assert isinstance(message, ast.Message)
    assert message.name == "MyStruct"
    assert len(message.elements) == 1

    field = message.elements[0]
    assert isinstance(field, ast.MapField)
    assert field.name == "some_map"
    assert field.key_type == "string"
    assert field.value_type == "int32"
    assert field.number == 1


def test_from_recap_nullable():
    converter = ProtobufConverter()
    recap_type = UnionType(
        types=[NullType(), IntType(signed=True, bits=32, name="some_int")],
        name="some_union",
    )
    struct_type = StructType(fields=[recap_type], alias="build.recap.MyStruct")
    result = converter.from_recap(struct_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 2

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    message = result.file_elements[1]
    assert isinstance(message, ast.Message)
    assert message.name == "MyStruct"
    assert len(message.elements) == 1

    field = message.elements[0]
    assert isinstance(field, ast.Field)
    assert field.name == "some_int"
    assert field.type == "int32"
    assert field.number == 1


def test_from_recap_union():
    converter = ProtobufConverter()
    recap_type = UnionType(
        types=[
            IntType(signed=True, bits=32, name="some_int"),
            StringType(bytes_=100, name="some_string"),
        ],
        name="some_union",
    )
    struct_type = StructType(fields=[recap_type], alias="build.recap.MyStruct")
    result = converter.from_recap(struct_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 2

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    message = result.file_elements[1]
    assert isinstance(message, ast.Message)
    assert message.name == "MyStruct"
    assert len(message.elements) == 1

    oneof_field = message.elements[0]
    assert isinstance(oneof_field, ast.OneOf)
    assert oneof_field.name == "some_union"
    assert len(oneof_field.elements) == 2

    int_field = oneof_field.elements[0]
    assert isinstance(int_field, ast.Field)
    assert int_field.name == "some_int"
    assert int_field.type == "int32"
    assert int_field.number == 1

    string_field = oneof_field.elements[1]
    assert isinstance(string_field, ast.Field)
    assert string_field.name == "some_string"
    assert string_field.type == "string"
    assert string_field.number == 2


def test_from_recap_enum_type():
    converter = ProtobufConverter()
    recap_type = EnumType(
        name="some_enum",
        alias="build.recap.SomeEnum",
        symbols=[
            "ENUM_VALUE1",
            "ENUM_VALUE2",
        ],
    )
    struct_type = StructType(fields=[recap_type], alias="build.recap.MyStruct")
    result = converter.from_recap(struct_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 3

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    enum_type = result.file_elements[1]
    assert isinstance(enum_type, ast.Enum)
    assert enum_type.name == "SomeEnum"
    assert len(enum_type.elements) == 2
    assert isinstance(enum_type.elements[0], ast.EnumValue)
    assert enum_type.elements[0].name == "ENUM_VALUE1"
    assert isinstance(enum_type.elements[1], ast.EnumValue)
    assert enum_type.elements[1].name == "ENUM_VALUE2"

    message = result.file_elements[2]
    assert isinstance(message, ast.Message)
    assert message.name == "MyStruct"
    assert len(message.elements) == 1

    field = message.elements[0]
    assert isinstance(field, ast.Field)
    assert field.name == "some_enum"
    assert field.type == ".build.recap.SomeEnum"
    assert field.number == 1


def test_from_recap_complex_message():
    converter = ProtobufConverter()
    message_value_type = StructType(
        fields=[
            IntType(signed=True, bits=32, name="some_int"),
        ],
        # Nested type inside MyStruct
        alias="build.recap.MyStruct.ValueType",
    )
    recap_type = MapType(
        keys=StringType(bytes_=100),
        values=message_value_type,
        name="some_map",
    )
    struct_type = StructType(fields=[recap_type], alias="build.recap.MyStruct")
    result = converter.from_recap(struct_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 2

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    message = result.file_elements[1]
    assert isinstance(message, ast.Message)
    assert message.name == "MyStruct"
    assert len(message.elements) == 2

    map_field = message.elements[0]
    assert isinstance(map_field, ast.MapField)
    assert map_field.name == "some_map"
    assert map_field.key_type == "string"
    assert map_field.value_type == ".build.recap.MyStruct.ValueType"
    assert map_field.number == 1

    value_type = message.elements[1]
    assert isinstance(value_type, ast.Message)
    assert value_type.name == "ValueType"
    assert len(value_type.elements) == 1

    int_field = value_type.elements[0]
    assert isinstance(int_field, ast.Field)
    assert int_field.name == "some_int"
    assert int_field.type == "int32"
    assert int_field.number == 1


def test_from_recap_with_cyclic_reference():
    converter = ProtobufConverter()
    linked_list_node_type = StructType(
        fields=[
            IntType(bits=32, name="value"),
            ProxyType(
                alias="build.recap.LinkedListNode",
                registry=converter.registry,
                name="next",
            ),
        ],
        alias="build.recap.LinkedListNode",
    )
    result = converter.from_recap(linked_list_node_type)

    assert isinstance(result, ast.File)
    assert len(result.file_elements) == 2

    package = result.file_elements[0]
    assert isinstance(package, ast.Package)
    assert package.name == "build.recap"

    message = result.file_elements[1]
    assert isinstance(message, ast.Message)
    assert message.name == "LinkedListNode"
    assert len(message.elements) == 2

    value_field = message.elements[0]
    assert isinstance(value_field, ast.Field)
    assert value_field.name == "value"
    assert value_field.type == "int32"
    assert value_field.number == 1

    next_field = message.elements[1]
    assert isinstance(next_field, ast.Field)
    assert next_field.name == "next"
    assert next_field.type == ".build.recap.LinkedListNode"
    assert next_field.number == 2
