from frictionless.schema import Schema as FrictionlessSchema

from recap.schema import types
from recap.schema.converters.converter import Converter


class FrictionlessConverter(Converter):
    # Frictionless spec doesn't define min/max float, int, or string length.
    # This makes it hard to convert them to anything without coercion.
    # This implementation is deliberately very incomplete.
    # Only using it for CSV/TSV schema inferrence.
    def to_recap_type(
        self,
        frictionless_schema: FrictionlessSchema,
        **_,
    ) -> types.Struct:
        fields = []
        for frictionless_field in frictionless_schema.fields:
            schema_args = {}
            if doc := frictionless_field.description:
                schema_args["doc"] = doc
            match frictionless_field.type:
                case "string":
                    field_type = types.String32(**schema_args)
                case "number":
                    field_type = types.Float64(**schema_args)
                case "integer":
                    field_type = types.Int64(**schema_args)
                case "boolean":
                    field_type = types.Bool(**schema_args)
                case "datetime" | "yearmonth":
                    # DATETIME is an ISO8601 format string.
                    # YEARMONTH is a YYYY-MM string.
                    field_type = types.String32(**schema_args)
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
