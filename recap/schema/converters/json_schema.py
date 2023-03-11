from typing import Any

from recap.schema import types

DEFAULT_SCHEMA_VERSION = "https://json-schema.org/draft/2020-12/schema"


# TODO Support $refs.
# TODO Handle anyOf.
# TODO Handle validators.
# TODO This whole implementation is woefully incomplete. Gotta start somewhere.
def from_json_schema(json_schema: dict[str, Any]) -> types.Type:
    schema_args = {}
    if doc := json_schema.get("description"):
        schema_args["doc"] = doc
    if "default" in json_schema:
        schema_args["default"] = json_schema["default"]
    match json_schema.get("type"):
        # TODO This translation is lossy.
        # Read https://json-schema.org/draft/2020-12/json-schema-validation.html#name-dates-times-and-duration
        case "string" if json_schema.get("format") == "date-time":
            return types.Timestamp(**schema_args)
        case "string" if json_schema.get("format") == "date":
            return types.Date(**schema_args)
        case "string" if json_schema.get("format") == "time":
            return types.Time(**schema_args)
        case "string" if json_schema.get("format") == "duration":
            return types.Duration(**schema_args)
        case "string":
            return types.String32(**schema_args)
        case "number":
            return types.Float64(**schema_args)
        case "boolean":
            return types.Bool(**schema_args)
        case "object":
            fields = []
            properties = json_schema.get("properties", {})
            required = set(json_schema.get("required", []))
            for name, field_schema in properties.items():
                schema = from_json_schema(field_schema)
                if name not in required:
                    schema = types.Union(
                        types=[
                            types.Null(),
                            schema,
                        ],
                        # Move default up to the union.
                        default=schema.default,
                    )
                fields.append(
                    types.Field(
                        name=name,
                        type_=schema,
                    )
                )
            return types.Struct(
                name=json_schema.get("title"),
                default=json_schema.get("default"),
                doc=json_schema.get("description"),
                fields=fields,
            )
        case "array":
            schema = from_json_schema(json_schema.get("items", {}))
            return types.List(
                values=schema,
                **schema_args,
            )
        case _:
            raise ValueError(
                "Can't convert to Recap type from JSON schema "
                f"type={json_schema.get('type')}"
            )


def to_json_schema(
    schema: types.Type,
    json_schema_ver: str | None = DEFAULT_SCHEMA_VERSION,
) -> dict[str, Any]:
    json_schema = {}
    if isinstance(schema, types.Struct) or isinstance(schema, types.Field):
        json_schema["title"] = schema.name
    if schema.doc:
        json_schema["description"] = schema.doc
    match schema:
        case types.Int() if types.Int64().subsumes(schema):
            json_schema["type"] = "integer"
        case types.String() if types.String32().subsumes(schema):
            json_schema["type"] = "string"
        case types.Float() if types.Float64().subsumes(schema):
            json_schema["type"] = "number"
        case types.Bool():
            json_schema["type"] = "boolean"
        case types.Timestamp():
            json_schema |= {
                "type": "string",
                "format": "date-time",
            }
        case types.Date():
            json_schema |= {
                "type": "string",
                "format": "date",
            }
        case types.Time():
            json_schema |= {
                "type": "string",
                "format": "time",
            }
        case types.List():
            json_schema |= {
                "type": "array",
                "items": to_json_schema(schema.values, None),
            }
        case types.Struct():
            properties = {}
            required = []
            json_schema["type"] = "object"
            for field in schema.fields or []:
                properties[field.name] = to_json_schema(field.type_, None)
                # TODO Handle required .
                # if not field.type_.optional:
                #    required.append(field.name)
            if json_schema_ver:
                json_schema["$schema"] = json_schema_ver
            if properties:
                json_schema["properties"] = properties
            if required:
                json_schema["required"] = required
        case types.Map(key=types.String()):
            json_schema |= {
                "type": "object",
                "additionalProperties": to_json_schema(schema.values),
            }
        case types.Union():
            json_schema["anyOf"] = [
                to_json_schema(union_subschema) for union_subschema in schema.types
            ]
        case _:
            raise ValueError(
                f"Can't convert from Recap type to JSON schema schema={schema}"
            )
    return json_schema
