from frictionless.schema import Schema as FrictionlessSchema

from recap.schema import types


# Frictionless spec doesn't define min/max float, int, or string length.
# This makes it hard to convert them to anything without coercion.
# This implementation is deliberately very incomplete.
# Only using it for CSV/TSV schema inferrence.
def to_recap_schema(
    frictionless_schema: FrictionlessSchema,
) -> types.Struct:
    fields = []
    for frictionless_field in frictionless_schema.fields:
        schema_args = {}
        if doc := frictionless_field.description:
            schema_args["doc"] = doc
        match frictionless_field.type:
            case "string":
                field_type = types.String(**schema_args)
            case "number":
                field_type = types.Float(**schema_args)
            case "integer":
                field_type = types.Int(**schema_args)
            case "boolean":
                field_type = types.Bool(**schema_args)
            # TODO Should handle types (object, array) here.
            case _:
                raise ValueError(
                    "Can't convert to Recap type from frictionless "
                    f"type={frictionless_field.type}"
                )
        fields.append(
            types.Field(
                name=frictionless_field.name,
                type_=field_type,
            )
        )
    return types.Struct(fields=fields)
