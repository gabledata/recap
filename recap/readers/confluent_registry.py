from confluent_kafka.schema_registry import SchemaRegistryClient

from recap.converters.avro import AvroConverter
from recap.converters.json_schema import JSONSchemaConverter
from recap.converters.protobuf import ProtobufConverter
from recap.types import StructType


class ConfluentRegistryReader:
    def __init__(self, registry: str | SchemaRegistryClient):
        self.registry = (
            SchemaRegistryClient({"url": registry})
            if isinstance(registry, str)
            else registry
        )

    def struct(self, topic: str) -> StructType:
        subject = f"{topic}-value"
        registered_schema = self.registry.get_latest_version(subject)
        schema_str = registered_schema.schema.schema_str
        match registered_schema.schema.schema_type:
            case "AVRO":
                return AvroConverter().convert(schema_str)
            case "JSON":
                return JSONSchemaConverter().convert(schema_str)
            case "PROTOBUF":
                return ProtobufConverter().convert(schema_str)
            case _:
                raise ValueError(
                    f"Unsupported schema type {registered_schema.schema.schema_type}"
                )
