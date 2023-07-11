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


def test_protobuf_converter():
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


def test_protobuf_converter_repeated():
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


def test_protobuf_converter_map():
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


def test_protobuf_converter_forward_reference():
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


def test_protobuf_converter_map_forward_reference():
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


def test_protobuf_converter_nested_message():
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


def test_protobuf_converter_enum():
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


def test_protobuf_converter_oneof():
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


def test_protobuf_converter_doubly_nested_message():
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


def test_protobuf_converter_timestamp():
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


def test_protobuf_converter_duration():
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


def test_protobuf_converter_nullvalue():
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


def test_protobuf_converter_self_reference():
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


def test_protobuf_converter_custom_namespace():
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


def test_protobuf_converter_package():
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


def test_protobuf_converter_separate_files():
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
