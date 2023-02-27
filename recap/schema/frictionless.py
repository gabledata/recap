from frictionless.schema import Schema as FrictionlessSchema

from recap import metadata


def to_recap_schema(
    frictionless_schema: FrictionlessSchema,
) -> metadata.StructSchema:
    fields = []
    for frictionless_field in frictionless_schema.fields:
        match frictionless_field.type:
            case "string":
                SchemaClass = metadata.StringSchema
            case "number":
                SchemaClass = metadata.Float64Schema
            case "integer":
                SchemaClass = metadata.Int64Schema
            case "boolean":
                SchemaClass = metadata.BooleanSchema
            case "datetime":
                SchemaClass = metadata.TimestampSchema
            case "yearmonth":
                SchemaClass = metadata.StringSchema
            # TODO Should handle types (object, array) here.
            case _:
                raise ValueError(
                    "Can't convert to Recap type from frictionless "
                    f"type={frictionless_field.type}"
                )
        fields.append(
            metadata.Field(
                name=frictionless_field.name,
                schema=SchemaClass(
                    doc=frictionless_field.description,
                    optional=frictionless_field.required,
                ),
            )
        )
    return metadata.StructSchema(fields=fields, optional=False)
