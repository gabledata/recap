from unittest.mock import patch

import pytest

from recap.clients.fs import FilesystemClient
from recap.types import IntType, StringType, StructType


def test_avro_schema():
    # Mock the `_read_file` method to return a sample Avro schema string.
    mock_avro_schema_str = """
    {
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ]
    }
    """

    # Create an instance of FilesystemClient with scheme "file"
    client = FilesystemClient(scheme="file")

    # Mock the `_read_file` method
    with patch.object(client, "_read_file", return_value=mock_avro_schema_str):
        result = client.schema("dummy_user.avsc")

    # Check that the schema was converted correctly
    assert isinstance(result, StructType)
    assert len(result.fields) == 2
    assert result.fields[0].extra_attrs["name"] == "name"
    assert isinstance(result.fields[0], StringType)
    assert result.fields[1].extra_attrs["name"] == "age"
    assert isinstance(result.fields[1], IntType)


def test_json_schema():
    # Mock the `_read_file` method to return a sample JSON schema string.
    mock_json_schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "title": "User",
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

    # Create an instance of FilesystemClient with scheme "file"
    client = FilesystemClient(scheme="file")

    # Mock the `_read_file` method
    with patch.object(client, "_read_file", return_value=mock_json_schema_str):
        result = client.schema("dummy_user.json")

    # Check that the schema was converted correctly
    assert isinstance(result, StructType)
    assert len(result.fields) == 2
    assert result.fields[0].extra_attrs["name"] == "name"
    assert isinstance(result.fields[0], StringType)
    assert result.fields[1].extra_attrs["name"] == "age"
    assert isinstance(result.fields[1], IntType)


def test_proto3_schema():
    # Mock the `_read_file` method to return a sample proto3 schema string.
    mock_proto3_schema_str = """
    syntax = "proto3";

    message User {
        required string name = 1;
        required int32 age = 2;
    }
    """

    # Create an instance of FilesystemClient with scheme "file"
    client = FilesystemClient(scheme="file")

    # Mock the `_read_file` method
    with patch.object(client, "_read_file", return_value=mock_proto3_schema_str):
        result = client.schema("dummy_user.proto")

    # Check that the schema was converted correctly
    assert isinstance(result, StructType)
    assert len(result.fields) == 2
    assert result.fields[0].extra_attrs["name"] == "name"
    assert isinstance(result.fields[0], StringType)
    assert result.fields[1].extra_attrs["name"] == "age"
    assert isinstance(result.fields[1], IntType)


@pytest.mark.parametrize(
    "url_input,expected_output",
    [
        # Test with a standard http URL
        (
            "http://example.com/path/to/resource",
            ("http://", ["/example.com/path/to/resource/"]),
        ),
        # Test with an https URL and a deeper path
        (
            "https://example.com/path/to/resource/file.ext",
            ("https://", ["/example.com/path/to/resource/file.ext/"]),
        ),
        # Test with a file scheme
        ("file:///path/to/local/file", ("file://", ["/path/to/local/file/"])),
        # Test with no scheme
        ("/path/to/local/file", ("file://", ["/path/to/local/file/"])),
        # Add other test cases as needed
    ],
)
def test_filesystemclient_parse(url_input, expected_output):
    # Given a URL, parse it using the static method
    result = FilesystemClient.parse("", url=url_input)

    # Check that the result matches the expected output
    assert result == expected_output
