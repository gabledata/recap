from frictionless.schema import Schema as FrictionlessSchema

from recap.schema import model


def to_recap_schema(
    frictionless_schema: FrictionlessSchema,
) -> model.StructSchema:
    fields = []
    for frictionless_field in frictionless_schema.fields:
        match frictionless_field.type:
            case "string":
                SchemaClass = model.StringSchema
            case "number":
                SchemaClass = model.Float64Schema
            case "integer":
                SchemaClass = model.Int64Schema
            case "boolean":
                SchemaClass = model.BooleanSchema
            case "datetime":
                SchemaClass = model.TimestampSchema
            case "yearmonth":
                SchemaClass = model.StringSchema
            # TODO Should handle types (object, array) here.
            case _:
                raise ValueError(
                    "Can't convert to Recap type from frictionless "
                    f"type={frictionless_field.type}"
                )
        fields.append(
            model.Field(
                name=frictionless_field.name,
                schema=SchemaClass(
                    doc=frictionless_field.description,
                    optional=frictionless_field.required,
                ),
            )
        )
    return model.StructSchema(fields=fields, optional=False)
