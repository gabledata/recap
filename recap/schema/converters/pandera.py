import pandera as pa
from pandera.typing import Index, DataFrame, Series


from recap.schema import model


def from_pandera(
    pandera_schema: pa.DataFrameSchema,
) -> model.StructSchema:
    fields = []

    for column in pandera_schema.columns:
        match str.lower(pandera_schema.columns[column].dtype):
            case "int64":
                SchemaClass = model.Int64Schema
            case "float64":
                SchemaClass = model.Float64Schema
            case "bool":
                SchemaClass = model.BooleanSchema
            case "str":
                SchemaClass = model.StringSchema
            case _:
                raise ValueError(
                    "Can't convert to Recap type from Pandera "
                    f"type={pandera_schema.columns[column].dtype}"
                )
        fields.append(
            model.Field(
                name=column,
                schema=SchemaClass(
                    doc=pandera_schema.columns[column].description,
                    optional=pandera_schema.columns[column].required
                ),
            )
        )
    return model.StructSchema(fields=fields, optional=False)
