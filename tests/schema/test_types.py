from recap.schema import types
from recap.schema.types import Parser


class TestTypes:
    def test_basic_sdl_to_recap(self):
        obj = {
            "type": "struct",
            "fields": [
                {
                    "type": "int32",
                },
            ],
        }
        parsed = Parser().parse_obj(obj)
        expected = types.Struct(
            fields=[
                types.Field(type_=types.Int32()),
            ],
        )
        assert parsed == expected

    def test_list_sdl_to_recap(self):
        obj = {
            "type": "struct",
            "fields": [
                {
                    "type": "list",
                    "values": "int32",
                },
            ],
        }
        parsed = Parser().parse_obj(obj)
        expected = types.Struct(
            fields=[
                types.Field(
                    type_=types.List(
                        values=types.Int32(),
                    )
                ),
            ],
        )
        assert parsed == expected

    def test_cyclic_alias_sdl_to_recap(self):
        obj = {
            "type": "struct",
            "alias": "com.mycorp.models.LinkedListUint32",
            "fields": [
                {
                    "name": "value",
                    "type": "int32",
                },
                {
                    "name": "next",
                    "type": "com.mycorp.models.LinkedListUint32",
                },
            ],
        }
        parser = Parser()
        parsed = parser.parse_obj(obj)
        expected = types.Struct(
            alias="com.mycorp.models.LinkedListUint32",
            fields=[
                types.Field(
                    name="value",
                    type_=types.Int32(),
                ),
                types.Field(
                    name="next",
                    type_=types.ProxyType(
                        obj=obj,
                        parser=parser,
                    ),
                ),
            ],
        )
        assert parsed == expected

    def test_alias_of_alias_sdl_to_recap(self):
        obj = {
            "type": "struct",
            "fields": [
                {
                    "alias": "com.mycorp.models.Int32",
                    "name": "field1",
                    "type": "int32",
                },
                {
                    "name": "field2",
                    "type": "com.mycorp.models.Int32",
                    "alias": "com.mycorp.models.OtherInt32",
                    # Additional attributes are ignored.
                    "bits": 24,
                },
                {
                    "name": "field3",
                    "type": "com.mycorp.models.OtherInt32",
                },
            ],
        }
        parsed = Parser().parse_obj(obj)
        expected = types.Struct(
            fields=[
                types.Field(
                    name="field1",
                    type_=types.Int32(alias="com.mycorp.models.Int32"),
                ),
                types.Field(
                    name="field2",
                    type_=types.Int32(alias="com.mycorp.models.OtherInt32"),
                ),
                types.Field(
                    name="field3",
                    type_=types.Int32(alias="com.mycorp.models.OtherInt32"),
                ),
            ],
        )
        assert parsed == expected

    def test_alias_of_struct_of_alias_sdl_to_recap(self):
        obj = {
            "type": "struct",
            "alias": "com.mycorp.models.LinkedListUint32",
            "fields": [
                {"name": "value", "type": ["null", "int32"], "default": None},
                {
                    "name": "next",
                    "type": "com.mycorp.models.LinkedListUint32",
                    "alias": "com.mycorp.models.Nested",
                    # Additional attributes are ignored.
                    "bits": 24,
                },
                {
                    "name": "extra_field",
                    "type": "struct",
                    "fields": [
                        {
                            "name": "extra_struct",
                            "type": "com.mycorp.models.Nested",
                            "alias": "com.mycorp.models.Unused",
                        }
                    ],
                },
            ],
        }
        parser = Parser()
        parsed = parser.parse_obj(obj)
        expected = types.Struct(
            alias="com.mycorp.models.LinkedListUint32",
            fields=[
                types.Field(
                    name="value",
                    type_=types.Union(
                        types=[
                            types.Null(),
                            types.Int32(),
                        ]
                    ),
                    default=types.Literal(None),
                ),
                types.Field(
                    name="next",
                    type_=types.ProxyType(
                        alias="com.mycorp.models.Nested",
                        obj=obj,
                        parser=parser,
                    ),
                ),
                types.Field(
                    name="extra_field",
                    type_=types.Struct(
                        fields=[
                            types.Field(
                                name="extra_struct",
                                type_=types.ProxyType(
                                    alias="com.mycorp.models.Unused",
                                    obj=obj,
                                    parser=parser,
                                ),
                            )
                        ],
                    ),
                ),
            ],
        )
        assert parsed == expected
