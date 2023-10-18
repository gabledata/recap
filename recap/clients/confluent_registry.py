from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

from confluent_kafka.schema_registry import SchemaRegistryClient

from recap.types import StructType

ALLOWED_ATTRS = {
    "url",
    "ssl.ca.location",
    "ssl.key.location",
    "ssl.certificate.location",
    "basic.auth.user.info",
}


class ConfluentRegistryClient:
    def __init__(self, registry: SchemaRegistryClient):
        self.registry = registry

    @staticmethod
    @contextmanager
    def create(**url_args) -> Generator[ConfluentRegistryClient, None, None]:
        # Filter out kwargs that are not allowed by the SchemaRegistryClient
        url_args = {k: v for k, v in url_args.items() if k in ALLOWED_ATTRS}

        with SchemaRegistryClient(url_args) as registry:
            yield ConfluentRegistryClient(registry)

    @staticmethod
    def parse(method: str, **url_args) -> tuple[str, list[Any]]:
        from urllib.parse import urlunparse

        match method:
            case "ls":
                url_with_clean_scheme = (
                    url_args["url"]
                    .replace("http+csr://", "http://")
                    .replace("https+csr://", "https://")
                )
                return (url_with_clean_scheme, [])
            case "schema":
                subject = url_args["paths"].pop(-1)
                connection_url = urlunparse(
                    [
                        url_args.get("dialect") or url_args.get("scheme"),
                        url_args.get("netloc"),
                        "/".join(url_args.get("paths", [])),
                        url_args.get("params"),
                        url_args.get("query"),
                        url_args.get("fragment"),
                    ]
                )
                return (connection_url, [subject])
            case _:
                raise ValueError("Invalid method")

    def ls(self) -> list[str]:
        return self.registry.get_subjects()

    def schema(self, subject: str) -> StructType:
        has_kv = subject.endswith("-key") or subject.endswith("-value")
        subject = subject if has_kv else f"{subject}-value"
        registered_schema = self.registry.get_latest_version(subject)
        schema_str = registered_schema.schema.schema_str
        match registered_schema.schema.schema_type:
            case "AVRO":
                from recap.converters.avro import AvroConverter

                return AvroConverter().to_recap(schema_str)
            case "JSON":
                from recap.converters.json_schema import JSONSchemaConverter

                return JSONSchemaConverter().to_recap(schema_str)
            case "PROTOBUF":
                from recap.converters.protobuf import ProtobufConverter

                return ProtobufConverter().to_recap(schema_str)
            case _:
                raise ValueError(
                    f"Unsupported schema type {registered_schema.schema.schema_type}"
                )
