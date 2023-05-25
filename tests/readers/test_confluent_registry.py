from unittest.mock import MagicMock

import pytest
from confluent_kafka import schema_registry

from recap.readers.confluent_registry import ConfluentRegistryReader
from recap.types import IntType, StringType, StructType


def test_struct_avro():
    mock_schema_registry_client = MagicMock(spec=schema_registry.SchemaRegistryClient)

    class MockSchema:
        schema_type = "AVRO"
        # an example simple Avro schema
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
    result = reader.struct("dummy_topic")

    # Check that the schema was converted correctly.
    # You'll need to replace 'your_expected_result' with the expected `StructType` instance.
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


def test_struct_unsupported_schema_type():
    mock_schema_registry_client = MagicMock(spec=schema_registry.SchemaRegistryClient)

    class MockSchema:
        schema_type = "PROTOBUF"
        # an example simple Protobuf schema
        schema_str = """
        message Person {
          string name = 1;
          int32 age = 2;
        }
        """

    class MockRegisteredSchema:
        schema = MockSchema()

    mock_schema_registry_client.get_latest_version.return_value = MockRegisteredSchema()

    reader = ConfluentRegistryReader(mock_schema_registry_client)

    with pytest.raises(ValueError) as e:
        reader.struct("dummy_topic")

    assert str(e.value) == "Unsupported schema type PROTOBUF"
    mock_schema_registry_client.get_latest_version.assert_called_with(
        "dummy_topic-value"
    )
