from proto_schema_parser.ast import (
    Enum,
    EnumValue,
    Field,
    FieldCardinality,
    File,
    MapField,
    Message,
    OneOf,
    Package,
)
from proto_schema_parser.parser import Parser

from recap.types import (
    BoolType,
    BytesType,
    EnumType,
    FloatType,
    IntType,
    ListType,
    MapType,
    NullType,
    ProxyType,
    RecapType,
    RecapTypeRegistry,
    StringType,
    StructType,
    UnionType,
)

DEFAULT_NAMESPACE = "_root"
"""
Namespace to use when no namespace is specified in the schema.
"""


class ProtobufConverter:
    def __init__(self, namespace: str = DEFAULT_NAMESPACE) -> None:
        self.namespaces = [namespace]
        self.registry = RecapTypeRegistry()

    def to_recap(
        self,
        protobuf_schema_str: str,
    ) -> StructType:
        file = Parser().parse(protobuf_schema_str)
        root_message = self._parse(file)
        root_message = self._resolve_proxies(root_message)

        if not isinstance(root_message, StructType):
            raise ValueError("Protobuf schema must be a Message")

        return root_message

    def _resolve_proxies(
        self,
        recap_type: RecapType,
        resolved: set[str] | None = None,
        seen: set[str] | None = None,
    ) -> RecapType:
        resolved = resolved or set()
        seen = seen or set()

        # Detect circular references.
        if not isinstance(recap_type, ProxyType) and recap_type.alias is not None:
            seen.add(recap_type.alias)

        if (
            isinstance(recap_type, ProxyType)
            # Skip if already resolved.
            and recap_type.alias not in resolved
            # Skip if circular reference.
            and recap_type.alias not in seen
        ):
            extra_attrs = recap_type.extra_attrs
            alias = recap_type.alias
            recap_type = recap_type.resolve()
            recap_type.alias = alias
            recap_type.extra_attrs |= extra_attrs
            resolved.add(recap_type.alias)  # pyright: ignore[reportGeneralTypeIssues]

        if isinstance(recap_type, ListType):
            recap_type.values = self._resolve_proxies(recap_type.values, resolved, seen)
        elif isinstance(recap_type, MapType):
            recap_type.keys = self._resolve_proxies(recap_type.keys, resolved, seen)
            recap_type.values = self._resolve_proxies(recap_type.values, resolved, seen)
        elif isinstance(recap_type, UnionType):
            recap_type.types = [
                self._resolve_proxies(t, resolved, seen) for t in recap_type.types
            ]
        elif isinstance(recap_type, StructType):
            recap_type.fields = [
                self._resolve_proxies(f, resolved, seen) for f in recap_type.fields
            ]

        return recap_type

    def _parse(self, file: File) -> StructType:
        root_message = None

        # Find the namespace first, since we need it for message and enum aliases.
        packages = [p for p in file.file_elements if isinstance(p, Package)]

        if packages:
            self.namespaces.append(packages[0].name)

        # Now parse messages and enums.
        for file_element in file.file_elements:
            if isinstance(file_element, Message):
                struct_type = self._parse_message(file_element)
                root_message = root_message or struct_type
            elif isinstance(file_element, Enum):
                self._parse_enum(file_element)

        if root_message is None:
            raise ValueError("Protobuf schema must contain at least one Message")

        self.namespaces.pop()

        return root_message

    def _parse_message(self, message: Message) -> StructType:
        fields = []

        for element in message.elements:
            if isinstance(element, Field):
                recap_type = self._parse_field(element)
                fields.append(recap_type)
            elif isinstance(element, MapField):
                recap_type = self._parse_map_field(element)
                fields.append(recap_type)
            elif isinstance(element, OneOf):
                recap_type = self._parse_oneof(element)
                fields.append(recap_type)
            elif isinstance(element, Message):
                self._parse_message(element)
            elif isinstance(element, Enum):
                self._parse_enum(element)

        struct_type = StructType(
            fields=fields,
            alias=f"{self.namespaces[-1]}.{message.name}",
        )

        self.registry.register_alias(struct_type)

        return struct_type

    def _parse_field(self, field: Field) -> RecapType:
        recap_type = self._protobuf_type_to_recap_type(field.type)

        if field.cardinality == FieldCardinality.REPEATED:
            recap_type = ListType(values=recap_type)

        if field.cardinality != FieldCardinality.REQUIRED:
            if not isinstance(recap_type, NullType):
                recap_type = UnionType(types=[NullType(), recap_type], default=None)
            recap_type.extra_attrs["default"] = None

        recap_type.extra_attrs["name"] = field.name

        return recap_type

    def _parse_map_field(self, field: MapField) -> RecapType:
        key_type = self._protobuf_type_to_recap_type(field.key_type)

        # Proto map keys are always strings or integers.
        assert isinstance(key_type, IntType) or isinstance(key_type, StringType)

        value_type = self._protobuf_type_to_recap_type(field.value_type)
        map_type = UnionType(
            types=[
                NullType(),
                MapType(keys=key_type, values=value_type),
            ],
            default=None,
        )

        if field.name is not None:
            map_type.extra_attrs["name"] = field.name

        return map_type

    def _protobuf_type_to_recap_type(self, protobuf_type: str) -> RecapType:
        # NOTE: Protobuf doesn't support type aliases, so we don't need to
        # register aliases here (or in oneof, map, etc).
        match protobuf_type:
            case "string":
                return StringType(bytes_=2_147_483_648, variable=True)
            case "bool":
                return BoolType()
            case "bytes":
                return BytesType(bytes_=2_147_483_648, variable=True)
            case "int32":
                return IntType(bits=32)
            case "int64":
                return IntType(bits=64)
            case "uint32":
                return IntType(bits=32, signed=False)
            case "uint64":
                return IntType(bits=64, signed=False)
            case "sint32":
                return IntType(bits=32)
            case "sint64":
                return IntType(bits=64)
            case "float":
                return FloatType(bits=32)
            case "double":
                return FloatType(bits=64)
            case "fixed32":
                return IntType(bits=32, signed=False)
            case "fixed64":
                return IntType(bits=64, signed=False)
            case "sfixed32":
                return IntType(bits=32)
            case "sfixed64":
                return IntType(bits=64)
            # Handle some of Google's well-known types.
            # Technically, we should honor `import google/protobuf/*.proto` and
            # make sure the protos match what's expected, but assuming they match
            # is fine for now.
            case "google.protobuf.Timestamp":
                # https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp
                # Note: Some protobuf values might overrun this since they support
                # from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z.
                return IntType(
                    logical="build.recap.Timestamp",
                    bits=64,
                    signed=True,
                    unit="nanosecond",
                    timezone="UTC",
                )
            case "google.protobuf.Duration":
                # https://protobuf.dev/reference/protobuf/google.protobuf/#duration
                # Note: Some protobuf values might overrun this since they support
                # int64 seconds and int32 nanoseconds.
                return IntType(
                    bits=64,
                    logical="build.recap.Duration",
                    unit="nanosecond",
                )
            case "google.protobuf.NullValue":
                return NullType()
            case _:
                # Create a ProxyType and hope we have a type alias for this type.
                alias = (
                    protobuf_type
                    if "." in protobuf_type
                    else f"{self.namespaces[-1]}.{protobuf_type}"
                )
                return ProxyType(alias, self.registry)

    def _parse_oneof(self, oneof: OneOf) -> RecapType:
        types: list[RecapType] = [NullType()]
        for element in oneof.elements:
            if isinstance(element, Field):
                # !! HACK !!
                # Force oneof subfield to be required since we're unioning with null above.
                # Otherwise we end up with [NullType, [NullType, RecapType], ...].
                # This is safe to force because oneof subfields can't be repeated.
                original_cardinality = element.cardinality
                element.cardinality = FieldCardinality.REQUIRED
                recap_type = self._parse_field(element)
                element.cardinality = original_cardinality
                types.append(recap_type)
        return UnionType(types=types, name=oneof.name, default=None)

    def _parse_enum(self, enum: Enum) -> RecapType:
        symbols = []
        for element in enum.elements:
            if isinstance(element, EnumValue):
                symbols.append(element.name)
        enum_type = EnumType(
            symbols=symbols,
            alias=f"{self.namespaces[-1]}.{enum.name}",
        )
        self.registry.register_alias(enum_type)
        return enum_type
