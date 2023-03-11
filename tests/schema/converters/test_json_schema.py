from recap.schema import types
from recap.schema.converters.json_schema import from_json_schema


# TODO Way more tests.
class TestJsonSchema:
    def test_basic_json_schema_to_recap(self):
        jsc = {
            "type": "object",
            "properties": {
                "number": {"type": "number"},
                "street_name": {"type": "string"},
            },
            "required": ["number", "street_name"],
        }

        struct = from_json_schema(jsc)
        expected = types.Struct(
            fields=[
                types.Field(name="number", type_=types.Float64()),
                types.Field(name="street_name", type_=types.String32()),
            ],
        )
        assert struct == expected
