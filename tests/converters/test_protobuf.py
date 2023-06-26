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
    StringType,
    StructType,
    UnionType,
)


def test_protobuf_converter():
    protobuf_schema = """
    syntax = "proto3";
    message Test2 {
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
    recap_schema = ProtobufConverter().convert(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    fields = recap_schema.fields
    assert len(fields) == 15

    # Field 1: string name = 1;
    assert isinstance(fields[0], UnionType)
    assert isinstance(fields[0].types[1], StringType)
    assert fields[0].types[1].bytes_ == 2_147_483_648
    assert fields[0].types[1].variable == True

    # Field 2: bool is_valid = 2;
    assert isinstance(fields[1], UnionType)
    assert isinstance(fields[1].types[1], BoolType)

    # Field 3: bytes data = 3;
    assert isinstance(fields[2], UnionType)
    assert isinstance(fields[2].types[1], BytesType)
    assert fields[2].types[1].bytes_ == 2_147_483_648
    assert fields[2].types[1].variable == True

    # Field 4: int32 id = 4;
    assert isinstance(fields[3], UnionType)
    assert isinstance(fields[3].types[1], IntType)
    assert fields[3].types[1].bits == 32
    assert fields[3].types[1].signed == True

    # Field 5: int64 large_id = 5;
    assert isinstance(fields[4], UnionType)
    assert isinstance(fields[4].types[1], IntType)
    assert fields[4].types[1].bits == 64
    assert fields[4].types[1].signed == True

    # Field 6: uint32 uid = 6;
    assert isinstance(fields[5], UnionType)
    assert isinstance(fields[5].types[1], IntType)
    assert fields[5].types[1].bits == 32
    assert fields[5].types[1].signed == False

    # Field 7: uint64 large_uid = 7;
    assert isinstance(fields[6], UnionType)
    assert isinstance(fields[6].types[1], IntType)
    assert fields[6].types[1].bits == 64
    assert fields[6].types[1].signed == False

    # Field 8: sint32 sid = 8;
    assert isinstance(fields[7], UnionType)
    assert isinstance(fields[7].types[1], IntType)
    assert fields[7].types[1].bits == 32
    assert fields[7].types[1].signed == True

    # Field 9: sint64 large_sid = 9;
    assert isinstance(fields[8], UnionType)
    assert isinstance(fields[8].types[1], IntType)
    assert fields[8].types[1].bits == 64
    assert fields[8].types[1].signed == True

    # Field 10: float score = 10;
    assert isinstance(fields[9], UnionType)
    assert isinstance(fields[9].types[1], FloatType)
    assert fields[9].types[1].bits == 32

    # Field 11: double large_score = 11;
    assert isinstance(fields[10], UnionType)
    assert isinstance(fields[10].types[1], FloatType)
    assert fields[10].types[1].bits == 64

    # Field 12: fixed32 fixed_id = 12;
    assert isinstance(fields[11], UnionType)
    assert isinstance(fields[11].types[1], IntType)
    assert fields[11].types[1].bits == 32
    assert fields[11].types[1].signed == False

    # Field 13: fixed64 large_fixed_id = 13;
    assert isinstance(fields[12], UnionType)
    assert isinstance(fields[12].types[1], IntType)
    assert fields[12].types[1].bits == 64
    assert fields[12].types[1].signed == False

    # Field 14: sfixed32 sfixed_id = 14;
    assert isinstance(fields[13], UnionType)
    assert isinstance(fields[13].types[1], IntType)
    assert fields[13].types[1].bits == 32
    assert fields[13].types[1].signed == True

    # Field 15: sfixed64 large_sfixed_id = 15;
    assert isinstance(fields[14], UnionType)
    assert isinstance(fields[14].types[1], IntType)
    assert fields[14].types[1].bits == 64
    assert fields[14].types[1].signed == True


def test_protobuf_converter_repeated():
    protobuf_schema = """
        syntax = "proto3";
        message Test3 {
            repeated int32 values = 1;
        }
    """

    recap_schema = ProtobufConverter().convert(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    fields = recap_schema.fields
    assert len(fields) == 1

    # Field 1: repeated int32 values = 1;
    assert isinstance(fields[0], UnionType)
    assert isinstance(fields[0].types[1], ListType)
    assert isinstance(fields[0].types[1].values, IntType)
    assert fields[0].types[1].values.bits == 32
    assert fields[0].types[1].values.signed == True


def test_protobuf_converter_map():
    protobuf_schema = """
        syntax = "proto3";
        message Test4 {
            map<string, int32> value = 1;
        }
    """

    recap_schema = ProtobufConverter().convert(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    fields = recap_schema.fields
    assert len(fields) == 1

    # Field 1: map<string, int32> value = 1;
    assert isinstance(fields[0], MapType)

    assert isinstance(fields[0].keys, StringType)
    assert fields[0].keys.bytes_ == 2_147_483_648
    assert fields[0].keys.variable == True

    assert isinstance(fields[0].values, IntType)
    assert fields[0].values.bits == 32
    assert fields[0].values.signed == True


def test_protobuf_converter_forward_reference():
    protobuf_schema = """
        syntax = "proto3";
        message Outer1 {
            Inner1 value = 1;
        }

        message Inner1 {
            int32 value = 1;
        }
    """

    recap_schema = ProtobufConverter().convert(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    fields = recap_schema.fields
    assert len(fields) == 1

    # Outer
    assert isinstance(fields[0], UnionType)
    assert isinstance(fields[0].types[1], StructType)
    inner_fields = fields[0].types[1].fields
    assert len(inner_fields) == 1

    # Inner
    assert isinstance(inner_fields[0], UnionType)
    assert isinstance(inner_fields[0].types[1], IntType)
    assert inner_fields[0].types[1].bits == 32
    assert inner_fields[0].types[1].signed == True


def test_protobuf_converter_nested_message():
    protobuf_schema = """
        syntax = "proto3";
        message Outer2 {
            message Inner2 {
                int32 value = 1;
            }
            Inner2 value = 2;
        }
    """

    recap_schema = ProtobufConverter().convert(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    fields = recap_schema.fields
    assert len(fields) == 1

    # Outer
    assert isinstance(fields[0], UnionType)
    assert isinstance(fields[0].types[1], StructType)
    inner_fields = fields[0].types[1].fields
    assert len(inner_fields) == 1

    # Inner
    assert isinstance(inner_fields[0], UnionType)
    assert isinstance(inner_fields[0].types[1], IntType)
    assert inner_fields[0].types[1].bits == 32
    assert inner_fields[0].types[1].signed == True


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

    recap_schema = ProtobufConverter().convert(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    fields = recap_schema.fields
    assert len(fields) == 1
    assert isinstance(fields[0], UnionType)
    assert isinstance(fields[0].types[1], EnumType)
    assert set(fields[0].types[1].symbols) == {"SPRING", "SUMMER", "AUTUMN", "WINTER"}


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

    recap_schema = ProtobufConverter().convert(protobuf_schema)
    assert isinstance(recap_schema, StructType)
    fields = recap_schema.fields
    assert len(fields) == 1

    # contact_type
    assert isinstance(fields[0], UnionType)
    assert len(fields[0].types) == 3
    oneof_types = fields[0].types

    # Null
    assert isinstance(oneof_types[0], NullType)

    # Email
    assert isinstance(oneof_types[1], StringType)
    assert oneof_types[1].bytes_ == 2_147_483_648
    assert oneof_types[1].variable == True

    # Phone Number
    assert isinstance(oneof_types[2], IntType)
    assert oneof_types[2].bits == 64
    assert oneof_types[2].signed == True
