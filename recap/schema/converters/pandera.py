import pandera as pa
from pandera.typing import Index, DataFrame, Series


from recap.schema import model


def to_recap_schema(
    pandera_schema: DataFrame,
) -> model.StructSchema:
    fields = []

    for column in pandera_schema.columns:
        match pandera_schema.columns[column].dtype:
            case "int" | "int64":
                SchemaClass = model.Int64Schema
            case "int8":
                SchemaClass = model.Int8Schema
            case "int16":
                SchemaClass = model.Int16Schema
            case "int32":
                SchemaClass = model.Int32Schema
            case "float" | "float64":
                SchemaClass = model.Float64Schema
            case "float16":
                SchemaClass = model.Float16Schema
            case "float32":
                SchemaClass = model.Float32Schema
            # case "float128":
            #     SchemaClass = model.Float64Schema
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
