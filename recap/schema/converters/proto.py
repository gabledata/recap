from google.protobuf.descriptor import Descriptor, FieldDescriptor

from recap.schema import types
from recap.schema.converters.converter import Converter


class ProtobufConverter(Converter):
    def to_recap_type(self, descriptor: Descriptor, **_) -> types.Struct:
        # TODO Support sint32/sint64
        # TODO Add enum support
        # TODO Handle Oneof
        # TODO Handle maps
        # TODO Add "doc" support
        def _from_proto_field(field: FieldDescriptor) -> types.Type:
            match field.type, field.message_type, field.label:
                # TODO Looks like Protobuf is using unsigned ints for string length.
                case (FieldDescriptor.TYPE_STRING, _, _):
                    field_type = types.String32()
                case (FieldDescriptor.TYPE_BOOL, _, _):
                    field_type = types.Bool()
                # TODO Looks like Protobuf is using unsigned ints for byte length.
                case (FieldDescriptor.TYPE_BYTES, _, _):
                    field_type = types.Bytes32()
                # TODO Should set min/max length for fixed.
                case (
                    FieldDescriptor.TYPE_DOUBLE | FieldDescriptor.TYPE_FIXED64,
                    _,
                    _,
                ):
                    field_type = types.Float64()
                # TODO Should set min/max length for fixed.
                case (FieldDescriptor.TYPE_FIXED32 | FieldDescriptor.TYPE_FLOAT, _, _):
                    field_type = types.Float32()
                case (FieldDescriptor.TYPE_INT32, _, _):
                    field_type = types.Int32()
                case (FieldDescriptor.TYPE_UINT32, _, _):
                    field_type = types.Uint32()
                case (FieldDescriptor.TYPE_INT64, _, _):
                    field_type = types.Int64()
                case (FieldDescriptor.TYPE_UINT64, _, _):
                    field_type = types.Uint64()
                case (FieldDescriptor.TYPE_MESSAGE, Descriptor(), _):
                    field_type = self.to_recap_type(field.message_type)
                case _:
                    raise ValueError(
                        "Can't convert to Recap type from JSON schema "
                        f"type={field.type}, label={field.label}"
                    )
            if field.label == FieldDescriptor.LABEL_REPEATED:
                field_type = types.List(values=field_type)
            # TODO Use type-specific default if optional's default is unspecified.
            # See https://protobuf.dev/programming-guides/proto/#optional for more.
            if field.label == FieldDescriptor.LABEL_OPTIONAL:
                field_type = types.Union(
                    types=[
                        types.Null(),
                        field_type,
                    ]
                )
            return field_type

        struct_fields = []
        for field in descriptor.fields:
            struct_field = types.Field(name=field.name, type_=_from_proto_field(field))
            if field.has_default_value:
                struct_field.default = types.Literal(value=field.default_value)
            struct_fields.append(
                struct_field,
            )

        return types.Struct(fields=struct_fields)
