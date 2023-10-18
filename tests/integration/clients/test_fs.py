from recap.clients.fs import FilesystemClient
from recap.types import IntType, StringType, StructType


def test_integration_filesystemclient_avro(tmp_path):
    # Sample Avro schema
    avro_schema_str = """
    {
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ]
    }
    """

    # Create a .avsc file in the temporary directory
    avsc_file = tmp_path / "sample.avsc"
    avsc_file.write_text(avro_schema_str)

    # Create an instance of FilesystemClient with scheme "file"
    client = FilesystemClient(scheme="file")

    # Validate the schema using the client
    schema = client.schema(avsc_file.as_posix())

    # Ensure the schema is parsed and converted correctly
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 2
    assert schema.fields[0].extra_attrs["name"] == "name"
    assert isinstance(schema.fields[0], StringType)
    assert schema.fields[1].extra_attrs["name"] == "age"
    assert isinstance(schema.fields[1], IntType)


def test_integration_filesystemclient_json_schema(tmp_path):
    # Sample JSON schema
    json_schema_str = """
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

    # Create a .json file in the temporary directory
    json_file = tmp_path / "sample.json"
    json_file.write_text(json_schema_str)

    # Create an instance of FilesystemClient with scheme "file"
    client = FilesystemClient(scheme="file")

    # Validate the schema using the client
    schema = client.schema(json_file.as_posix())

    # Ensure the schema is parsed and converted correctly
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 2
    assert schema.fields[0].extra_attrs["name"] == "name"
    assert isinstance(schema.fields[0], StringType)
    assert schema.fields[1].extra_attrs["name"] == "age"
    assert isinstance(schema.fields[1], IntType)


def test_integration_filesystemclient_proto(tmp_path):
    # Sample protobuf schema (proto3)
    proto_str = """
    syntax = "proto3";

    message User {
        required string name = 1;
        required int32 age = 2;
    }
    """

    # Create a .proto file in the temporary directory
    proto_file = tmp_path / "sample.proto"
    proto_file.write_text(proto_str)

    # Create an instance of FilesystemClient with scheme "file"
    client = FilesystemClient(scheme="file")

    # Validate the schema using the client
    schema = client.schema(proto_file.as_posix())

    # Ensure the schema is parsed and converted correctly
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 2
    assert schema.fields[0].extra_attrs["name"] == "name"
    assert isinstance(schema.fields[0], StringType)
    assert schema.fields[1].extra_attrs["name"] == "age"
    assert isinstance(schema.fields[1], IntType)
