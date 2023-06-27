from confluent_kafka import schema_registry

from recap.readers.confluent_registry import ConfluentRegistryReader
from recap.types import IntType, StringType, StructType


class TestConfluentRegistryReader:
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

    def test_struct_avro(self):
        reader = ConfluentRegistryReader(self.schema_registry_client)
        result = reader.struct("dummy_topic")

        assert isinstance(result, StructType)
        assert len(result.fields) == 2
        assert result.fields[0].extra_attrs["name"] == "name"
        assert isinstance(result.fields[0], StringType)
        assert result.fields[1].extra_attrs["name"] == "age"
        assert isinstance(result.fields[1], IntType)
