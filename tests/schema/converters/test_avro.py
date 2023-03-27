from avro.schema import parse

from recap.schema import types
from recap.schema.converters.avro import AvroConverter


# TODO Test Decimal and Time and Timestamp logical types
class TestAvro:
    def test_basic_avro_to_recap(self):
        avsc = parse(
            """
            {
                "type": "record",
                "namespace": "SomeNamespace",
                "name": "Users",
                "fields": [
                    {"name": "Id", "type": "int"},
                    {"name": "Name", "type": "string"}
                ]
            }
            """
        )
        struct = AvroConverter().to_recap_type(avsc)
        expected = types.Struct(
            alias="SomeNamespace.Users",
            fields=[
                types.Field(name="Id", type_=types.Int32()),
                types.Field(name="Name", type_=types.String64()),
            ],
        )
        assert struct == expected

    def test_basic_recap_to_avro(self):
        struct = types.Struct(
            alias="Users",
            fields=[
                types.Field(name="Id", type_=types.Int32()),
                types.Field(name="Name", type_=types.String64()),
            ],
        )
        avsc = AvroConverter().from_recap_type(struct)
        expected = parse(
            """
            {
                "type": "record",
                "name": "Users",
                "fields": [
                    {"name": "Id", "type": "int"},
                    {"name": "Name", "type": "string"}
                ]
            }
            """
        )
        assert avsc == expected

    def test_cycle_avro_to_recap(self):
        avsc = parse(
            """
            {
                "type": "record",
                "name": "recap.test.LongList",
                "fields" : [
                    {"name": "value", "type": "long"},
                    {"name": "next", "type": ["null", "recap.test.LongList"]}
                ]
            }
            """
        )
        struct = AvroConverter().to_recap_type(avsc)
        expected = types.Struct(
            alias="recap.test.LongList",
            fields=[
                types.Field(name="value", type_=types.Int64()),
                types.Field(
                    name="next",
                    type_=types.Union(
                        types=[
                            types.Null(),
                            types.Type(alias="recap.test.LongList"),
                        ]
                    ),
                ),
            ],
        )
        assert struct == expected

    def test_cyle_recap_to_avro(self):
        struct = types.Struct(
            alias="recap.test.LongList",
            fields=[
                types.Field(name="value", type_=types.Int64()),
                types.Field(
                    name="next",
                    type_=types.Union(
                        types=[
                            types.Null(),
                            types.Type(alias="recap.test.LongList"),
                        ]
                    ),
                ),
            ],
        )
        avsc = AvroConverter().from_recap_type(struct)
        expected = parse(
            """
            {
                "type": "record",
                "name": "recap.test.LongList",
                "fields" : [
                    {"name": "value", "type": "long"},
                    {"name": "next", "type": ["null", "recap.test.LongList"]}
                ]
            }
            """
        )
        assert avsc == expected

    def test_enum_avro_to_recap(self):
        avsc = parse(
            """
            {
                "type": "enum",
                "name": "Suit",
                "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }
            """
        )
        enum = AvroConverter().to_recap_type(avsc)
        expected = types.Enum(
            alias="Suit",
            symbols=["SPADES", "HEARTS", "DIAMONDS", "CLUBS"],
        )
        assert enum == expected

    def test_enum_recap_to_avro(self):
        enum = types.Enum(
            alias="Suit",
            symbols=["SPADES", "HEARTS", "DIAMONDS", "CLUBS"],
        )
        avsc = AvroConverter().from_recap_type(enum)
        expected = parse(
            """
            {
                "type": "enum",
                "name": "Suit",
                "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }
            """
        )
        assert avsc == expected

    def test_array_avro_to_recap(self):
        avsc = parse(
            """
            {
                "type": "array",
                "items" : "string"
            }
            """
        )
        array = AvroConverter().to_recap_type(avsc)
        expected = types.List(values=types.String64())
        assert array == expected

    def test_array_recap_to_avro(self):
        array = types.List(values=types.String64())
        avsc = AvroConverter().from_recap_type(array)
        expected = parse(
            """
            {
                "type": "array",
                "items" : "string"
            }
            """
        )
        assert avsc == expected

    def test_map_avro_to_recap(self):
        avsc = parse(
            """
            {
                "type": "map",
                "values" : "long"
            }
            """
        )
        map = AvroConverter().to_recap_type(avsc)
        expected = types.Map(
            keys=types.String64(),
            values=types.Int64(),
        )
        assert map == expected

    def test_map_recap_to_avro(self):
        map = types.Map(
            keys=types.String64(),
            values=types.Int64(),
        )
        avsc = AvroConverter().from_recap_type(map)
        expected = parse(
            """
            {
                "type": "map",
                "values" : "long"
            }
            """
        )
        assert avsc == expected

    def test_union_avro_to_recap(self):
        avsc = parse(
            """
            [
                {
                    "type": "map",
                    "values" : "long"
                },
                "null"
            ]
            """
        )
        union = AvroConverter().to_recap_type(avsc)
        expected = types.Union(
            types=[
                types.Map(
                    keys=types.String64(),
                    values=types.Int64(),
                ),
                types.Null(),
            ]
        )
        assert union == expected

    def test_union_recap_to_avro(self):
        union = types.Union(
            types=[
                types.Map(
                    keys=types.String64(),
                    values=types.Int64(),
                ),
                types.Null(),
            ]
        )
        avsc = AvroConverter().from_recap_type(union)
        expected = parse(
            """
            [
                {
                    "type": "map",
                    "values" : "long"
                },
                "null"
            ]
            """
        )
        assert avsc == expected

    def test_fixed_avro_to_recap(self):
        avsc = parse(
            """
            {
                "type": "fixed",
                "size": 16,
                "name": "md5"
            }
            """
        )
        fixed = AvroConverter().to_recap_type(avsc)
        expected = types.Bytes(
            alias="md5",
            bytes=16,
            variable=False,
        )
        assert fixed == expected

    def test_fixed_recap_to_avro(self):
        fixed = types.Bytes(
            alias="md5",
            bytes=16,
            variable=False,
        )
        avsc = AvroConverter().from_recap_type(fixed)
        expected = parse(
            """
            {
                "type": "fixed",
                "size": 16,
                "name": "md5"
            }
            """
        )
        assert avsc == expected
