from google.cloud.bigquery import SchemaField

from recap.schema import model


def to_recap_schema(columns: list[SchemaField]) -> model.StructSchema:
    fields = []
    for column in columns:
        match column.field_type:
            case "STRING":
                SchemaClass = model.StringSchema
            case "BYTES":
                SchemaClass = model.BytesSchema
            case "INTEGER" | "INT64":
                SchemaClass = model.Int64Schema
            case "FLOAT" | "FLOAT64":
                SchemaClass = model.Float64Schema
            case "BOOLEAN" | "BOOL":
                SchemaClass = model.BooleanSchema
            case "TIMESTAMP":
                SchemaClass = model.TimestampSchema
            case "TIME":
                SchemaClass = model.TimeSchema
            case "DATE":
                SchemaClass = model.DateSchema
            case "NUMERIC" | "BIGNUMERIC":
                SchemaClass = model.DecimalSchema
            case _:
                raise ValueError(
                    "Can't convert to Recap type from bigquery "
                    f"type={column.field_type}"
                )
        fields.append(
            model.Field(
                name=column.name,
                schema=SchemaClass(
                    default=column.default_value_expression,
                    optional=column.is_nullable,
                    doc=column.description,
                ),
            )
        )
    return model.StructSchema(fields=fields, optional=False)
