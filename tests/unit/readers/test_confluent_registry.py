from unittest.mock import MagicMock

from confluent_kafka import schema_registry

from recap.readers.confluent_registry import ConfluentRegistryReader
from recap.types import IntType, StringType, StructType, UnionType


def test_struct_avro():
    mock_schema_registry_client = MagicMock(spec=schema_registry.SchemaRegistryClient)

    class MockSchema:
        schema_type = "AVRO"
        schema_str = """
        {
          "type": "record",
          "name": "User",
          "fields": [
            {"name": "name", "type": "string"},
            {"name": "age",  "type": "int"}
          ]
        }
        """

    class MockRegisteredSchema:
        schema = MockSchema()

    mock_schema_registry_client.get_latest_version.return_value = MockRegisteredSchema()

    reader = ConfluentRegistryReader(mock_schema_registry_client)
    result = reader.to_recap("dummy_topic")

    # Check that the schema was converted correctly.
    assert isinstance(result, StructType)
    assert len(result.fields) == 2
    assert result.fields[0].extra_attrs["name"] == "name"
    assert isinstance(result.fields[0], StringType)
    assert result.fields[1].extra_attrs["name"] == "age"
    assert isinstance(result.fields[1], IntType)

    # Check that the get_latest_version method was called with the correct subject.
    mock_schema_registry_client.get_latest_version.assert_called_with(
        "dummy_topic-value"
    )


def test_struct_protobuf():
    mock_schema_registry_client = MagicMock(spec=schema_registry.SchemaRegistryClient)

    class MockSchema:
        schema_type = "PROTOBUF"
        schema_str = """
        syntax = "proto3";

        message Person {
          string name = 1;
          int32 age = 2;
        }
        """

    class MockRegisteredSchema:
        schema = MockSchema()

    mock_schema_registry_client.get_latest_version.return_value = MockRegisteredSchema()

    reader = ConfluentRegistryReader(mock_schema_registry_client)
    result = reader.to_recap("dummy_topic")

    # Check that the schema was converted correctly.
    assert isinstance(result, StructType)
    assert len(result.fields) == 2
    assert isinstance(result.fields[0], UnionType)
    assert isinstance(result.fields[0].types[1], StringType)
    assert isinstance(result.fields[1], UnionType)
    assert isinstance(result.fields[1].types[1], IntType)

    # Check that the get_latest_version method was called with the correct subject.
    mock_schema_registry_client.get_latest_version.assert_called_with(
        "dummy_topic-value"
    )


def test_struct_json_schema():
    mock_schema_registry_client = MagicMock(spec=schema_registry.SchemaRegistryClient)

    class MockSchema:
        schema_type = "JSON"
        schema_str = """
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

    class MockRegisteredSchema:
        schema = MockSchema()

    mock_schema_registry_client.get_latest_version.return_value = MockRegisteredSchema()

    reader = ConfluentRegistryReader(mock_schema_registry_client)
    result = reader.to_recap("dummy_topic")

    # Check that the schema was converted correctly.
    assert isinstance(result, StructType)
    assert len(result.fields) == 2
    assert result.fields[0].extra_attrs["name"] == "name"
    assert isinstance(result.fields[0], StringType)
    assert result.fields[1].extra_attrs["name"] == "age"
    assert isinstance(result.fields[1], IntType)

    # Check that the get_latest_version method was called with the correct subject.
    mock_schema_registry_client.get_latest_version.assert_called_with(
        "dummy_topic-value"
    )
