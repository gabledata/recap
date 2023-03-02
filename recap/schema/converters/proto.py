from google.protobuf.descriptor import Descriptor, FieldDescriptor

from recap.schema import model


def from_proto(descriptor: Descriptor) -> model.Schema:
    fields = []
    for field in descriptor.fields:
        fields.append(model.Field(name=field.name, schema=_from_proto_field(field)))
    return model.StructSchema(
        name=descriptor.name,
        optional=False,
        fields=fields,
    )


def _from_proto_field(field: FieldDescriptor) -> model.Schema:
    match field.type, field.message_type, field.label:
        case (FieldDescriptor.TYPE_STRING, _, _):
            schema = model.StringSchema(
                optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
                default=field.default_value,
            )
        case (FieldDescriptor.TYPE_BOOL, _, _):
            schema = model.BooleanSchema(
                optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
                default=field.default_value,
            )
        case (FieldDescriptor.TYPE_BYTES, _, _):
            schema = model.BytesSchema(
                optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
                default=field.default_value,
            )
        case (
            FieldDescriptor.TYPE_DOUBLE | FieldDescriptor.TYPE_FIXED64,
            _,
            _,
        ):
            schema = model.Float64Schema(
                optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
                default=field.default_value,
            )
        case (FieldDescriptor.TYPE_FIXED32 | FieldDescriptor.TYPE_FLOAT, _, _):
            schema = model.Float32Schema(
                optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
                default=field.default_value,
            )
        case (FieldDescriptor.TYPE_INT32, _, _):
            schema = model.Int32Schema(
                optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
                default=field.default_value,
            )
        case (FieldDescriptor.TYPE_INT64 | FieldDescriptor.TYPE_UINT32, _, _):
            schema = model.Int64Schema(
                optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
                default=field.default_value,
            )
        case (FieldDescriptor.TYPE_MESSAGE, Descriptor(), _):
            schema = from_proto(field.message_type)
        case _:
            raise ValueError(
                "Can't convert to Recap type from JSON schema "
                f"type={field.type}, label={field.label}"
            )
    if field.label == FieldDescriptor.LABEL_REPEATED:
        schema.optional = False
        schema.default = None
        schema = model.ArraySchema(
            value_schema=schema,
            optional=(field.label == FieldDescriptor.LABEL_OPTIONAL),
            default=field.default_value,
        )
    return schema
