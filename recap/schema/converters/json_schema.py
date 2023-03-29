from typing import Any

from recap.schema import types
from recap.schema.converters.converter import Converter

DEFAULT_SCHEMA_VERSION = "https://json-schema.org/draft/2020-12/schema"


class JsonSchemaConverter(Converter):
    def __init__(self, json_schema_ver: str | None = None):
        self.json_schema_ver = json_schema_ver or DEFAULT_SCHEMA_VERSION

    # TODO Support $refs.
    # TODO Handle anyOf.
    # TODO Handle validators.
    # TODO This whole implementation is woefully incomplete. Gotta start somewhere.
    def to_recap_type(self, json_schema: dict[str, Any], **_) -> types.Type:
        schema_args = {}
        if doc := json_schema.get("description"):
            schema_args["doc"] = doc
        match json_schema.get("type"):
            # TODO This translation is lossy.
            # Read https://json-schema.org/draft/2020-12/json-schema-validation.html#name-dates-times-and-duration
            case "string" if json_schema.get("format") == "date-time":
                return types.Timestamp64(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "string" if json_schema.get("format") == "date":
                return types.Date64(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "string" if json_schema.get("format") == "time":
                return types.Time64(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "string" if json_schema.get("format") == "duration":
                return types.Duration64(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "string":
                return types.String32(**schema_args)
            case "number":
                return types.Float64(**schema_args)
            case "boolean":
                return types.Bool(**schema_args)
            case "integer":
                return types.Int64(**schema_args)
            case "null":
                return types.Null(**schema_args)
            case "object":
                fields = []
                properties = json_schema.get("properties", {})
                required = set(json_schema.get("required", []))
                for name, field_schema in properties.items():
                    schema = self.to_recap_type(field_schema)
                    if name not in required:
                        schema = types.Union(
                            types=[
                                types.Null(),
                                schema,
                            ],
                        )
                    schema.extra_attrs["name"] = name
                    if default := field_schema.get("default"):
                        schema.extra_attrs["default"] = default
                    fields.append(schema)
                return types.Struct(
                    doc=json_schema.get("description"),
                    fields=fields,
                    **schema_args,
                )
            case "array":
                schema = self.to_recap_type(json_schema.get("items", {}))
                return types.List(
                    values=schema,
                    **schema_args,
                )
            case _:
                raise ValueError(
                    "Can't convert to Recap type from JSON schema "
                    f"type={json_schema.get('type')}"
                )

    def from_recap_type(
        self,
        type_: types.Type,
        **_,
    ) -> dict[str, Any]:
        json_schema = {}
        if type_.doc:
            json_schema["description"] = type_.doc
        match type_:
            case types.Int() if types.Int64().subsumes(type_):
                json_schema["type"] = "integer"
            case types.String() if types.String32().subsumes(type_):
                json_schema["type"] = "string"
            case types.Float() if types.Float64().subsumes(type_):
                json_schema["type"] = "number"
            case types.Bool():
                json_schema["type"] = "boolean"
            case types.Timestamp64():
                json_schema |= {
                    "type": "string",
                    "format": "date-time",
                }
            case types.Date64():
                json_schema |= {
                    "type": "string",
                    "format": "date",
                }
            case types.Time64():
                json_schema |= {
                    "type": "string",
                    "format": "time",
                }
            case types.Null():
                json_schema["type"] = "null"
            case types.List():
                json_schema |= {
                    "type": "array",
                    "items": self.from_recap_type(type_.values),
                }
            case types.Struct():
                properties = {}
                required = []
                json_schema["type"] = "object"
                for field_type in type_.fields or []:
                    if name := field_type.extra_attrs.get("name"):
                        properties[name] = self.from_recap_type(field_type)
                    else:
                        raise ValueError(f"Missing name for field={field_type}")
                    # TODO Handle required
                    # if not field.type_.optional:
                    #    required.append(field.name)
                if self.json_schema_ver:
                    json_schema["$schema"] = self.json_schema_ver
                if properties:
                    json_schema["properties"] = properties
                if required:
                    json_schema["required"] = required
            case types.Map(key=types.String()):
                json_schema |= {
                    "type": "object",
                    "additionalProperties": self.from_recap_type(type_.values),
                }
            case types.Union():
                json_schema["anyOf"] = [
                    self.from_recap_type(union_subschema)
                    for union_subschema in type_.types
                ]
            case _:
                raise ValueError(
                    f"Can't convert from Recap type to JSON schema type={type_}"
                )
        return json_schema
