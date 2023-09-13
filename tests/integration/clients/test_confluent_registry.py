from confluent_kafka import schema_registry

from recap.clients import create_client
from recap.clients.confluent_registry import ConfluentRegistryClient
from recap.types import IntType, StringType, StructType, UnionType


class TestConfluentRegistryClient:
    @classmethod
    def setup_class(cls):
        cls.schema_registry_client = schema_registry.SchemaRegistryClient(
            {"url": "http://localhost:8081"}
        )

        # Define and register the Avro schema for the "dummy_topic"
        avro_schema_str = """
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age",  "type": "int"}
            ]
        }
        """
        avro_schema = schema_registry.Schema(avro_schema_str, "AVRO")
        cls.schema_registry_client.register_schema("dummy_topic-value", avro_schema)

        # Define and register the Protobuf schema for the "dummy_topic_protobuf"
        protobuf_schema_str = """
        syntax = "proto3";

        message Person {
            string name = 1;
            int32 age = 2;
        }
        """
        protobuf_schema = schema_registry.Schema(protobuf_schema_str, "PROTOBUF")
        cls.schema_registry_client.register_schema(
            "dummy_topic_protobuf-value",
            protobuf_schema,
        )

        # Define and register the JSON schema for the "dummy_topic_json"
        json_schema_str = """
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "User",
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                },
                "age": {
                    "type": "integer"
                }
            },
            "required": ["name", "age"]
        }
        """
        json_schema = schema_registry.Schema(json_schema_str, "JSON")
        cls.schema_registry_client.register_schema(
            "dummy_topic_json-value",
            json_schema,
        )

    def test_struct_avro(self):
        client = ConfluentRegistryClient(self.schema_registry_client)
        result = client.schema("dummy_topic")

        assert isinstance(result, StructType)
        assert len(result.fields) == 2
        assert result.fields[0].extra_attrs["name"] == "name"
        assert isinstance(result.fields[0], StringType)
        assert result.fields[1].extra_attrs["name"] == "age"
        assert isinstance(result.fields[1], IntType)

    def test_struct_proto(self):
        client = ConfluentRegistryClient(self.schema_registry_client)
        result = client.schema("dummy_topic_protobuf")

        assert isinstance(result, StructType)
        assert len(result.fields) == 2
        assert result.fields[0].extra_attrs["name"] == "name"
        assert isinstance(result.fields[0], UnionType)
        assert isinstance(result.fields[0].types[1], StringType)
        assert result.fields[1].extra_attrs["name"] == "age"
        assert isinstance(result.fields[1], UnionType)
        assert isinstance(result.fields[1].types[1], IntType)

    def test_struct_json(self):
        client = ConfluentRegistryClient(self.schema_registry_client)
        result = client.schema("dummy_topic_json")

        assert isinstance(result, StructType)
        assert len(result.fields) == 2
        assert result.fields[0].extra_attrs["name"] == "name"
        assert isinstance(result.fields[0], StringType)
        assert result.fields[1].extra_attrs["name"] == "age"
        assert isinstance(result.fields[1], IntType)

    def test_create_client(self):
        csr_url = "http+csr://localhost:8081"

        with create_client(csr_url) as client:
            assert client.ls() == [
                "dummy_topic-value",
                "dummy_topic_json-value",
                "dummy_topic_protobuf-value",
            ]
