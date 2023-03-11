from google.cloud.bigquery import SchemaField

from recap.schema import types


# TODO Support column.is_nullable and column.default_value_expression
# TODO ARRAY, GEOGRAPHY, JSON, STRUCT
def to_recap_schema(columns: list[SchemaField]) -> types.Struct:
    fields = []
    for column in columns:
        schema_args = {}
        if doc := column.description:
            schema_args["doc"] = doc
        match column.field_type:
            case "STRING":
                field_type = types.String64(**schema_args)
            case "BYTES":
                field_type = types.Bytes64(**schema_args)
            case "INTEGER" | "INT64":
                field_type = types.Int64(**schema_args)
            case "FLOAT" | "FLOAT64":
                field_type = types.Float64(**schema_args)
            case "BOOLEAN" | "BOOL":
                # TODO Handle BOOL(L)
                field_type = types.Bool(**schema_args)
            case "DATETIME":
                field_type = types.Timestamp(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "TIMESTAMP":
                field_type = types.Timestamp(
                    timezone="UTC",
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "TIME":
                field_type = types.Time(
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "DATE":
                field_type = types.Date(**schema_args)
            case "INTERVAL":
                field_type = types.Interval(
                    # 10000 years in months
                    months_min=-120_000,
                    months_max=120_000,
                    days_min=-3660000,
                    days_max=3660000,
                    # 87840000:0:0.0 H:M:S.Mi hours in microseconds
                    remainder_min=-316224000000000000,
                    remainder_max=316224000000000000,
                    unit=types.TimeUnit.MICROSECOND,
                    **schema_args,
                )
            case "NUMERIC" | "DECIMAL":
                # TODO Support parameterized NUMERIC(P, S)
                field_type = types.Decimal128(
                    precision=38,
                    scale=9,
                    **schema_args,
                )
            case "BIGNUMERIC":
                # TODO Support parameterized BIGNUMERIC(P, S)
                # TODO figure out how to handle partial precision (76.76)
                field_type = types.Decimal256(
                    precision=76,
                    scale=38,
                    **schema_args,
                )
            case _:
                raise ValueError(
                    "Can't convert to Recap type from bigquery "
                    f"type={column.field_type}"
                )
        fields.append(
            types.Field(
                name=column.name,
                type_=field_type,
            )
        )
    return types.Struct(fields=fields)
