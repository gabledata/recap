from unittest.mock import MagicMock

import pytest
from confluent_kafka import schema_registry

from recap.clients.confluent_registry import ConfluentRegistryClient
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
            {"name": "age", "type": "int"}
          ]
        }
        """

    class MockRegisteredSchema:
        schema = MockSchema()

    mock_schema_registry_client.get_latest_version.return_value = MockRegisteredSchema()

    client = ConfluentRegistryClient(mock_schema_registry_client)
    result = client.schema("dummy_topic-value")

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

    client = ConfluentRegistryClient(mock_schema_registry_client)
    result = client.schema("dummy_topic-value")

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

    client = ConfluentRegistryClient(mock_schema_registry_client)
    result = client.schema("dummy_topic-value")

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


@pytest.mark.parametrize(
    "method, url_args, expected_result",
    [
        # Test ls method
        ("ls", {"url": "http+csr://registry-url"}, ("http://registry-url", [])),
        (
            "ls",
            {
                "url": "http+csr://registry-url/path1/path2?ssl.ca.location=/foo/bar.crt#foo",
                "paths": ["path1", "path2"],
                "scheme": "http+csr",
                "dialect": "http",
                "netloc": "registry-url",
                "query": "ssl.ca.location=/foo/bar.crt",
                "fragment": "foo",
            },
            ("http://registry-url/path1/path2?ssl.ca.location=/foo/bar.crt#foo", []),
        ),
        # Test schema method
        (
            "schema",
            {
                "url": "http+csr://registry-url/subject",
                "paths": ["subject"],
                "scheme": "http+csr",
                "dialect": "http",
                "netloc": "registry-url",
            },
            ("http://registry-url", ["subject"]),
        ),
        (
            "schema",
            {
                "url": "http+csr://registry-url/path1/path2/subject",
                "paths": ["path1", "path2", "subject"],
                "scheme": "http+csr",
                "dialect": "http",
                "netloc": "registry-url",
                "query": "ssl.ca.location=/foo/bar.crt",
                "fragment": "foo",
            },
            (
                "http://registry-url/path1/path2?ssl.ca.location=/foo/bar.crt#foo",
                ["subject"],
            ),
        ),
        # Test invalid method
        (
            "invalid_method",
            {"url": "http+csr://registry-url"},
            pytest.raises(ValueError, match="Invalid method"),
        ),
    ],
)
def test_parse_method(method, url_args, expected_result):
    if isinstance(expected_result, tuple):
        result = ConfluentRegistryClient.parse(method, **url_args)
        assert result == expected_result
    else:
        with expected_result:
            ConfluentRegistryClient.parse(method, **url_args)
