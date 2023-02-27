from google.cloud.bigquery import SchemaField

from recap import metadata


def to_recap_schema(columns: list[SchemaField]) -> metadata.StructSchema:
    fields = []
    for column in columns:
        match column.field_type:
            case "STRING":
                SchemaClass = metadata.StringSchema
            case "BYTES":
                SchemaClass = metadata.BytesSchema
            case "INTEGER" | "INT64":
                SchemaClass = metadata.Int64Schema
            case "FLOAT" | "FLOAT64":
                SchemaClass = metadata.Float64Schema
            case "BOOLEAN" | "BOOL":
                SchemaClass = metadata.BooleanSchema
            case "TIMESTAMP":
                SchemaClass = metadata.TimestampSchema
            case "TIME":
                SchemaClass = metadata.TimeSchema
            case "DATE":
                SchemaClass = metadata.DateSchema
            case "NUMERIC" | "BIGNUMERIC":
                SchemaClass = metadata.DecimalSchema
            case _:
                raise ValueError(
                    "Can't convert to Recap type from bigquery "
                    f"type={column.field_type}"
                )
        fields.append(
            metadata.Field(
                name=column.name,
                schema=SchemaClass(
                    default=column.default_value_expression,
                    optional=column.is_nullable,
                    doc=column.description,
                ),
            )
        )
    return metadata.StructSchema(fields=fields, optional=False)
