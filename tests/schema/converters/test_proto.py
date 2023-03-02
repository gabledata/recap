import pytest
from google.protobuf import descriptor_pool
from google.protobuf.descriptor import Descriptor

from recap.schema import model
from recap.schema.converters.proto import from_proto

DESCRIPTOR = descriptor_pool.Default().AddSerializedFile(
    b'\n\x11test_models.proto"*\n\x0eSearchResponse\x12\x18\n\x07results\x18\x01 \x03(\x0b\x32\x07.Result"6\n\x06Result\x12\x0b\n\x03url\x18\x01 \x01(\t\x12\r\n\x05title\x18\x02 \x01(\t\x12\x10\n\x08snippets\x18\x03 \x03(\tb\x06proto3'
)


class TestProtoConverter:
    @pytest.fixture
    def search_response_descriptor(self) -> Descriptor:
        DESCRIPTOR = descriptor_pool.Default().AddSerializedFile(
            b'\n\x11test_models.proto"*\n\x0eSearchResponse\x12\x18\n\x07results\x18\x01 \x03(\x0b\x32\x07.Result"6\n\x06Result\x12\x0b\n\x03url\x18\x01 \x01(\t\x12\r\n\x05title\x18\x02 \x01(\t\x12\x10\n\x08snippets\x18\x03 \x03(\tb\x06proto3'
        )
        """
        syntax = "proto3";

        message SearchResponse {
            repeated Result results = 1;
        }

        message Result {
            string url = 1;
            string title = 2;
            repeated string snippets = 3;
        }
        """

        return DESCRIPTOR.message_types_by_name["SearchResponse"]

    def test_search_response(self, search_response_descriptor: Descriptor):
        recap_schema = from_proto(search_response_descriptor)
        assert isinstance(recap_schema, model.StructSchema)
        assert not recap_schema.optional
        assert recap_schema.name == "SearchResponse"
        assert len(recap_schema.fields) == 1
        field = recap_schema.fields[0]
        assert field.name == "results"
        assert isinstance(field, model.Field)
        schema = field.schema_
        assert isinstance(schema, model.ArraySchema)
        result_schema = schema.value_schema
        assert isinstance(result_schema, model.StructSchema)
        assert len(result_schema.fields) == 3
        assert result_schema.fields[0].name == "url"
        assert result_schema.fields[0].schema_ == model.StringSchema(default="")
        assert result_schema.fields[1].name == "title"
        assert result_schema.fields[1].schema_ == model.StringSchema(default="")
        assert result_schema.fields[2].name == "snippets"
        result_snippets_schema = result_schema.fields[2].schema_
        assert isinstance(result_snippets_schema, model.ArraySchema)
        snippet_value_schema = result_snippets_schema.value_schema
        assert isinstance(snippet_value_schema, model.StringSchema)
