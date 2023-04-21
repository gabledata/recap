from google.cloud.bigquery import SchemaField

from recap import types
from recap.converters.converter import Converter


class BigQueryConverter(Converter):
    # TODO Support column.is_nullable and column.default_value_expression
    # TODO ARRAY, GEOGRAPHY, JSON, STRUCT
    def to_recap_type(self, columns: list[SchemaField], **_) -> types.Struct:
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
                    field_type = types.Timestamp64(
                        unit=types.TimeUnit.MICROSECOND,
                        **schema_args,
                    )
                case "TIMESTAMP":
                    field_type = types.Timestamp64(
                        zone="UTC",
                        unit=types.TimeUnit.MICROSECOND,
                        **schema_args,
                    )
                case "TIME":
                    field_type = types.Time64(
                        unit=types.TimeUnit.MICROSECOND,
                        **schema_args,
                    )
                case "DATE":
                    field_type = types.Date64(
                        unit=types.TimeUnit.MICROSECOND,
                        **schema_args,
                    )
                case "INTERVAL":
                    field_type = types.Interval128(
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
            if column.is_nullable:
                field_type = types.Union(types=[types.Null(), field_type])
                # Default to `null` for `is_nullable=True`
                field_type.extra_attrs["default"] = None
            if default := column.default_value_expression:
                field_type.extra_attrs["default"] = default
            field_type.extra_attrs["name"] = column.name
            fields.append(field_type)
        return types.Struct(fields=fields)
