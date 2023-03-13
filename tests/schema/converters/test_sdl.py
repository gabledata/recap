from recap.schema import types
from recap.schema.converters.sdl import to_recap_schema


class TestSdl:
    def test_basic_sdl_to_recap(self):
        sdl = {
            "type": "struct",
            "fields": [
                {
                    "type": "int32",
                },
            ],
        }
        struct = to_recap_schema(sdl)
        expected = types.Struct(
            fields=[
                types.Field(type_=types.Int32()),
            ],
        )
        assert struct == expected

    def test_list_sdl_to_recap(self):
        sdl = {
            "type": "struct",
            "fields": [
                {
                    "type": "list",
                    "values": "int32",
                },
            ],
        }
        struct = to_recap_schema(sdl)
        expected = types.Struct(
            fields=[
                types.Field(
                    type_=types.List(
                        values=types.Int32(),
                    )
                ),
            ],
        )
        assert struct == expected
