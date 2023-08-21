from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from confluent_kafka.schema_registry import SchemaRegistryClient

from recap.converters.avro import AvroConverter
from recap.converters.json_schema import JSONSchemaConverter
from recap.converters.protobuf import ProtobufConverter
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
    def create(**kwargs) -> Generator[ConfluentRegistryClient, None, None]:
        # Filter out kwargs that are not allowed by the SchemaRegistryClient
        kwargs = {k: v for k, v in kwargs.items() if k in ALLOWED_ATTRS}

        with SchemaRegistryClient(kwargs) as registry:
            yield ConfluentRegistryClient(registry)

    def ls(self) -> list[str]:
        return self.registry.get_subjects()

    def get_schema(self, subject: str) -> StructType:
        has_kv = subject.endswith("-key") or subject.endswith("-value")
        subject = subject if has_kv else f"{subject}-value"
        registered_schema = self.registry.get_latest_version(subject)
        schema_str = registered_schema.schema.schema_str
        match registered_schema.schema.schema_type:
            case "AVRO":
                return AvroConverter().to_recap(schema_str)
            case "JSON":
                return JSONSchemaConverter().to_recap(schema_str)
            case "PROTOBUF":
                return ProtobufConverter().to_recap(schema_str)
            case _:
                raise ValueError(
                    f"Unsupported schema type {registered_schema.schema.schema_type}"
                )
