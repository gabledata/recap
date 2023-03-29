from recap.schema import types
from recap.schema.converters.recap import ProxyType, RecapConverter


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
        parsed = RecapConverter().to_recap_type(obj)
        expected = types.Struct(
            fields=[
                types.Int32(),
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
        parsed = RecapConverter().to_recap_type(obj)
        expected = types.Struct(
            fields=[
                types.List(
                    values=types.Int32(),
                )
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
        converter = RecapConverter()
        parsed = converter.to_recap_type(obj)
        expected = types.Struct(
            alias="com.mycorp.models.LinkedListUint32",
            fields=[
                types.Int32(extra_attrs={"name": "value"}),
                ProxyType(
                    obj=obj,
                    converter=converter,
                    extra_attrs={
                        "name": "next",
                        "type": "com.mycorp.models.LinkedListUint32",
                    },
                ),
            ],
        )
        assert parsed == expected

    def test_multiple_aliases_sdl_to_recap(self):
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
                    # Overwriting alias attributes is allowed
                    "bits": 24,
                    # Extra attriutes go into extra_attrs
                    "an_extra": True,
                },
                {
                    "name": "field3",
                    "type": "com.mycorp.models.Int32",
                },
            ],
        }
        parsed = RecapConverter().to_recap_type(obj)
        expected = types.Struct(
            fields=[
                types.Int32(
                    alias="com.mycorp.models.Int32", extra_attrs={"name": "field1"}
                ),
                types.Int32(
                    alias="com.mycorp.models.Int32",
                    bits=24,
                    extra_attrs={"an_extra": True, "name": "field2"},
                ),
                types.Int32(
                    alias="com.mycorp.models.Int32",
                    extra_attrs={"name": "field3"},
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
                },
                {
                    "name": "extra_field",
                    "type": "struct",
                    "fields": [
                        {
                            "name": "extra_struct",
                            "type": "com.mycorp.models.LinkedListUint32",
                        }
                    ],
                },
            ],
        }
        converter = RecapConverter()
        parsed = converter.to_recap_type(obj)
        expected = types.Struct(
            alias="com.mycorp.models.LinkedListUint32",
            fields=[
                types.Union(
                    types=[
                        types.Null(),
                        types.Int32(),
                    ],
                    extra_attrs={"name": "value", "default": None},
                ),
                ProxyType(
                    obj=obj,
                    converter=converter,
                    extra_attrs={
                        "type": "com.mycorp.models.LinkedListUint32",
                        "name": "next",
                    },
                ),
                types.Struct(
                    fields=[
                        ProxyType(
                            obj=obj,
                            converter=converter,
                            extra_attrs={
                                "type": "com.mycorp.models.LinkedListUint32",
                                "name": "extra_struct",
                            },
                        ),
                    ],
                    extra_attrs={"name": "extra_field"},
                ),
            ],
        )
        assert parsed == expected
