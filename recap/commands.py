from enum import Enum

from recap.clients import create_client, parse_url
from recap.settings import RecapSettings

settings = RecapSettings()


class SchemaFormat(str, Enum):
    """
    Schema formats Recap can convert to. Used in the `schema` method.
    """

    avro = "avro"
    json = "json"
    protobuf = "protobuf"
    recap = "recap"


FORMAT_MAP = {
    "application/schema+json": SchemaFormat.json,
    "application/avro+json": SchemaFormat.avro,
    "application/x-protobuf": SchemaFormat.protobuf,
    "application/x-recap+json": SchemaFormat.recap,
}


def ls(url: str | None = None, strict: bool = True) -> list[str] | None:
    """
    List a URL's children.

    :param url: URL where children are located. If `url` is None, list root URLs.
    :param strict: If True, raise an error if the URL is not configured in settings.
    :return: List of children. Values are relative to `url`.
    """

    if not url:
        return settings.safe_urls
    connection_url, method_args = parse_url("ls", url, strict)
    with create_client(connection_url) as client:
        return client.ls(*method_args)


def schema(
    url: str,
    format: SchemaFormat = SchemaFormat.recap,
    strict: bool = True,
) -> dict | str:
    """
    Get a URL's schema.

    :param url: URL where schema is located.
    :param format: Schema format to convert to.
    :param strict: If True, raise an error if the URL is not configured in settings.
    :return: Schema in the requested format (encoded as a dict or string).
    """

    connection_url, method_args = parse_url("schema", url, strict)
    with create_client(connection_url) as client:
        recap_struct = client.schema(*method_args)
        output_obj: dict | str
        match format:
            case SchemaFormat.avro:
                from recap.converters.avro import AvroConverter

                output_obj = AvroConverter().from_recap(recap_struct)
            case SchemaFormat.json:
                from recap.converters.json_schema import JSONSchemaConverter

                output_obj = JSONSchemaConverter().from_recap(recap_struct)
            case SchemaFormat.protobuf:
                from proto_schema_parser.generator import Generator

                from recap.converters.protobuf import ProtobufConverter

                proto_file = ProtobufConverter().from_recap(recap_struct)
                proto_str = Generator().generate(proto_file)

                output_obj = proto_str
            case SchemaFormat.recap:
                from recap.types import to_dict

                struct_dict = to_dict(recap_struct)
                if not isinstance(struct_dict, dict):
                    raise ValueError(
                        f"Expected a schema dict, but got {type(struct_dict)}"
                    )
                output_obj = struct_dict
            case _:
                raise ValueError(f"Unknown schema format: {format}")
        return output_obj
