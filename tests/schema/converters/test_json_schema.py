from recap.schema import types
from recap.schema.converters.json_schema import JsonSchemaConverter


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

        struct = JsonSchemaConverter().to_recap_type(jsc)
        expected = types.Struct(
            fields=[
                types.Float64(extra_attrs={"name": "number"}),
                types.String32(extra_attrs={"name": "street_name"}),
            ],
        )
        assert struct == expected
