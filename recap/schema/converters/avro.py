from typing import Any

from recap.schema import model


# TODO support referencing named complex types
# TODO support Enums
def from_avro(avro_schema: dict[str, Any]) -> model.Schema:
    schema_args = {
        # Optional is modeled by a union type of ["null", "other_type"].
        "optional": False,
    }
    if name := avro_schema.get("name"):
        schema_args["name"] = name
    if doc := avro_schema.get("doc"):
        schema_args["doc"] = doc
    if default := avro_schema.get("default"):
        schema_args["default"] = default
    match avro_schema.get("type"):
        case "string":
            return model.StringSchema(**schema_args)
        case "boolean":
            return model.BooleanSchema(**schema_args)
        case "int":
            return model.Int32Schema(**schema_args)
        case "long":
            return model.Int64Schema(**schema_args)
        case "float":
            return model.Float32Schema(**schema_args)
        case "double":
            return model.Float64Schema(**schema_args)
        case "bytes":
            return model.BytesSchema(**schema_args)
        case dict() as type:
            return from_avro(type)
        case list():
            types = avro_schema.get("type", [])
            is_optional = "null" in types
            if len(types) == 2 and is_optional:
                # This is just a basic optional type, so grab non-null type and
                # use it as the schema.
                other_type = [type_ for type_ in types if type_ != "null"][0]
                if not isinstance(other_type, dict):
                    other_type = {"type": other_type}
                schema = from_avro(other_type)
                schema.optional = True
                return schema
            else:
                return model.UnionSchema(
                    schemas=[
                        from_avro({"type": avro_schema})
                        if not isinstance(avro_schema, dict)
                        else from_avro(avro_schema)
                        for avro_schema in types
                        if avro_schema != "null"
                    ],
                    optional=is_optional,
                )
        case "record":
            fields = []
            for field in avro_schema.get("fields", []):
                field_type = field["type"]
                if not isinstance(field_type, dict):
                    field_type = {"type": field_type}
                fields.append(
                    model.Field(
                        name=field["name"],
                        schema=from_avro(field_type),
                    )
                )
            return model.StructSchema(
                fields=fields,
                **schema_args,
            )
        case "array":
            items = avro_schema["items"]
            # Primitives are goofy in Avro arrays.
            if not isinstance(items, dict):
                items = {"type": items}
            schema = from_avro(items)
            return model.ArraySchema(
                value_schema=schema,
                **schema_args,
            )
        case "map":
            values = avro_schema["values"]
            # Primitives are goofy in Avro maps.
            if not isinstance(values, dict):
                values = {"type": values}
            schema = from_avro(values)
            return model.MapSchema(
                key_schema=model.StringSchema(),
                value_schema=schema,
                **schema_args,
            )
        case _:
            raise ValueError(
                "Can't convert to Recap type from Avro "
                f"type={avro_schema.get('type')}"
            )
