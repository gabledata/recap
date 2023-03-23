from __future__ import annotations

from copy import copy
from dataclasses import dataclass, field, fields
from enum import Enum as PyEnum
from typing import Any, ClassVar

FIELD_METADATA_NAMESPACE = "cloud.recap"
FIELD_METADATA_TYPE = f"{FIELD_METADATA_NAMESPACE}.type"


@dataclass(kw_only=True)
class Type:
    default_alias: ClassVar[str]
    extra_attrs: dict[str, Any] = field(default_factory=dict)
    alias: str | None = None
    doc: str | None = None


@dataclass(kw_only=True)
class Null(Type):
    default_alias: ClassVar[str] = "null"


@dataclass(kw_only=True)
class Bool(Type):
    default_alias: ClassVar[str] = "bool"


@dataclass(kw_only=True)
class Int(Type):
    default_alias: ClassVar[str] = "int"
    bits: int
    signed: bool = True

    def subsumes(self, other: Type) -> bool:
        # TODO This is buggy. Should handle signed properly.
        return (
            isinstance(other, Int)
            and other.bits <= self.bits
            and other.signed == self.signed
        )


@dataclass(kw_only=True)
class Float(Type):
    default_alias: ClassVar[str] = "float"
    bits: int

    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Float) and other.bits <= self.bits


@dataclass(kw_only=True)
class String(Type):
    default_alias: ClassVar[str] = "string"
    bytes: int
    variable: bool = True

    def subsumes(self, other: Type) -> bool:
        return isinstance(other, String) and other.bytes <= self.bytes


@dataclass(kw_only=True)
class Bytes(Type):
    default_alias: ClassVar[str] = "bytes"
    bytes: int
    variable: bool = True

    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Bytes) and other.bytes <= self.bytes


@dataclass(kw_only=True)
class List(Type):
    default_alias: ClassVar[str] = "list"
    values: Type = field(metadata={FIELD_METADATA_TYPE: "type"})
    length: int | None = None
    variable: bool = True


@dataclass(kw_only=True)
class Map(Type):
    default_alias: ClassVar[str] = "map"
    keys: Type = field(metadata={FIELD_METADATA_TYPE: "type"})
    values: Type = field(metadata={FIELD_METADATA_TYPE: "type"})


@dataclass(kw_only=True)
class Struct(Type):
    default_alias: ClassVar[str] = "struct"
    fields: list[Field] = field(metadata={FIELD_METADATA_TYPE: "list[field]"})


@dataclass
class Field:
    type_: Type
    name: str | None = None
    default: Literal | None = None


@dataclass
class Literal:
    value: Any


@dataclass(kw_only=True)
class Enum(Type):
    default_alias: ClassVar[str] = "enum"
    symbols: list[str]


@dataclass(kw_only=True)
class Union(Type):
    default_alias: ClassVar[str] = "union"
    types: list[Type] = field(metadata={FIELD_METADATA_TYPE: "list[type]"})


@dataclass(kw_only=True)
class ProxyType(Type):
    obj: dict[str, Any]
    parser: Parser


@dataclass(kw_only=True)
class Int8(Int):
    default_alias: ClassVar[str] = "int8"
    bits: int = 8


@dataclass(kw_only=True)
class Uint8(Int):
    default_alias: ClassVar[str] = "uint8"
    bits: int = 8


@dataclass(kw_only=True)
class Int16(Int):
    default_alias: ClassVar[str] = "int16"
    bits: int = 16


@dataclass(kw_only=True)
class Uint16(Int):
    default_alias: ClassVar[str] = "uint16"
    bits: int = 16


@dataclass(kw_only=True)
class Int32(Int):
    default_alias: ClassVar[str] = "int32"
    bits: int = 32


@dataclass(kw_only=True)
class Uint32(Int):
    default_alias: ClassVar[str] = "uint8"
    bits: int = 32


@dataclass(kw_only=True)
class Int64(Int):
    default_alias: ClassVar[str] = "int64"
    bits: int = 64


@dataclass(kw_only=True)
class Uint64(Int):
    default_alias: ClassVar[str] = "uint64"
    bits: int = 64


@dataclass(kw_only=True)
class Float16(Float):
    default_alias: ClassVar[str] = "float16"
    bits: int = 16


@dataclass(kw_only=True)
class Float32(Float):
    default_alias: ClassVar[str] = "float32"
    bits: int = 32


@dataclass(kw_only=True)
class Float64(Float):
    default_alias: ClassVar[str] = "float64"
    bits: int = 64


@dataclass(kw_only=True)
class String32(String):
    default_alias: ClassVar[str] = "string32"
    bytes: int = 2_147_483_647


@dataclass(kw_only=True)
class String64(String):
    default_alias: ClassVar[str] = "string64"
    bytes: int = 9_223_372_036_854_775_807


@dataclass(kw_only=True)
class Bytes32(Bytes):
    default_alias: ClassVar[str] = "bytes32"
    bytes: int = 2_147_483_647


@dataclass(kw_only=True)
class Bytes64(Bytes):
    default_alias: ClassVar[str] = "bytes64"
    bytes: int = 9_223_372_036_854_775_807


@dataclass(kw_only=True)
class UUID(String):
    default_alias: ClassVar[str] = "uuid"
    # len("771450ea-75b0-4270-b79c-2f867f1d48d4")
    bytes: int = 36
    variable: bool = False


@dataclass(kw_only=True)
class Decimal(Bytes):
    default_alias: ClassVar[str] = "decimal"
    precision: int
    scale: int
    bytes: int = 2_147_483_647


@dataclass(kw_only=True)
class Decimal128(Decimal):
    default_alias: ClassVar[str] = "decimal128"


@dataclass(kw_only=True)
class Decimal256(Decimal):
    default_alias: ClassVar[str] = "decimal256"
    bytes: int = 16
    variable: bool = False


class TimeUnit(str, PyEnum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    MILLISECOND = "MILLISECOND"
    MICROSECOND = "MICROSECOND"
    NANOSECOND = "NANOSECOND"
    PICOSECOND = "PICOSECOND"


@dataclass(kw_only=True)
class Duration64(Int64):
    default_alias: ClassVar[str] = "duration64"
    unit: str


@dataclass(kw_only=True)
class Interval128(Bytes):
    default_alias: ClassVar[str] = "interval128"
    unit: str
    bytes: int = 16
    variable: bool = False


@dataclass(kw_only=True)
class Time32(Int32):
    default_alias: ClassVar[str] = "time32"
    unit: str


@dataclass(kw_only=True)
class Time64(Int64):
    default_alias: ClassVar[str] = "time64"
    unit: str


@dataclass(kw_only=True)
class Timestamp64(Int):
    default_alias: ClassVar[str] = "timestamp64"
    unit: str
    zone: str | None = None
    bits: int = 64


@dataclass(kw_only=True)
class Date32(Int32):
    default_alias: ClassVar[str] = "date32"
    unit: str


@dataclass(kw_only=True)
class Date64(Int64):
    default_alias: ClassVar[str] = "date64"
    unit: str


class Parser:
    """
    Parser parses Recap schema objects, which can be `dict`, `str`, or `list`
    types, and returns a `Type`.

    The parser includes the default built-in derived types. Other types may be
    included using the `register_type` or `register_alias` methods.

    WARN: This parser is not thread-safe.
    """

    def __init__(self):
        self.types: dict[str, type[Type]] = {}
        """
        This is the parser's type table. It maps types to `Type`s:

        ```
        'bool': <class 'recap.schema.types2.Bool'>,
        'bytes': <class 'recap.schema.types2.Bytes'>,
        'decimal': <class 'recap.schema.types2.Decimal'>,
        'enum': <class 'recap.schema.types2.Enum'>,
        'float': <class 'recap.schema.types2.Float'>,
        'int': <class 'recap.schema.types2.Int'>,
        'list': <class 'recap.schema.types2.List'>,
        'map': <class 'recap.schema.types2.Map'>,
        'null': <class 'recap.schema.types2.Null'>,
        'string': <class 'recap.schema.types2.String'>,
        'struct': <class 'recap.schema.types2.Struct'>,
        'timestamp64': <class 'recap.schema.types2.Timestamp64'>,
        'union': <class 'recap.schema.types2.Union'>
        ```
        """

        self.aliases: dict[str, Type] = {}
        """
        A map from aliases to resolved types.

        ```
        'com.mycorp.models.Int32': Int32(extra_attrs={},
                                        alias='com.mycorp.models.Int32',
                                        doc=None,
                                        bits=32,
                                        signed=True),
        'com.mycorp.models.Int24': Int(extra_attrs={},
                                       alias='com.mycorp.models.Int24',
                                       doc=None,
                                       bits=24,
                                       signed=True)
        """

        # Built-in primitives
        self.register_type(Null)
        self.register_type(Bool)
        self.register_type(Int)
        self.register_type(Float)
        self.register_type(String)
        self.register_type(Bytes)

        # Built-in complex types
        self.register_type(List)
        self.register_type(Map)
        self.register_type(Struct)
        self.register_type(Enum)
        self.register_type(Union)

        # Built-in derived types
        self.register_type(Int8)
        self.register_type(Uint8)
        self.register_type(Int16)
        self.register_type(Uint16)
        self.register_type(Int32)
        self.register_type(Uint32)
        self.register_type(Int64)
        self.register_type(Uint64)
        self.register_type(Float16)
        self.register_type(Float32)
        self.register_type(Float64)
        self.register_type(String32)
        self.register_type(String64)
        self.register_type(Bytes32)
        self.register_type(Bytes64)
        self.register_type(UUID)
        self.register_type(Decimal)
        self.register_type(Decimal128)
        self.register_type(Decimal256)
        self.register_type(Duration64)
        self.register_type(Interval128)
        self.register_type(Time32)
        self.register_type(Time64)
        self.register_type(Timestamp64)
        self.register_type(Date32)
        self.register_type(Date64)
        self.register_type(Decimal)

    def register_type(self, type_cls: type[Type]):
        """
        Register a new type with the parser.

        :param type_cls: The type class to register.
        """

        self.types[type_cls.default_alias] = type_cls

    def parse_obj(self, obj: dict[str, Any] | str | list) -> Type:
        """
        Parse a Recap type object and returns a `Type`.

        :param obj: A type object. Can be a dictionary like
            `{"type": "int32"}`, a string like `"int32"`, or a union list like
            `["null", "int32"]`.
        :returns: A Recap `Type` that's equivalent to the supplied `obj`.
        """

        alias_stack = []
        """
        A stack that tracks the aliases a parser has seen as it traverses
        nested structures. Aliases must be tracked in order to determine if the
        parser has hit a cyclic alias reference like this:

        ```
        {
            "type": "struct",
            "alias": "com.mycorp.models.LinkedListUint32",
            "fields": [
                {
                    "name": "value",
                    "type": "int32",
                },
                {
                    "name": "next",
                    "type": "com.mycorp.models.LinkedListUint32",
                },
            ],
        }
        ```

        In such a case, the parser will detect that the `LinkedListUint32`
        alias is still being parsed when it hits the `next` field, and will
        return a ProxyType for `next` instead of calling `parse_obj` on a
        Struct type.
        """

        def _parse_obj(obj: dict[str, Any] | str | list) -> Type:
            """
            Create a nested function to take advantage of `alias_stack` without
            making it an instance variable.

            This method checks three things:

            1. Is the object's type a resolved alias? If so, just return it.
            2. Is the object's type a known type? If so, parse it.
            3. Is the object's type an unresolved alias? If so, return a proxy.

            The parsing in (2) can be recursive if the type contains types
            (List, Struct, and so on).

            Type checking is done using `metadata` type hints in each `Type`
            field. We can't directly check `field.type` because Python has type
            erasure, so a dataclass attribute of `list[Type]` is just `list` at
            runtime. In lieu of this, `field(metadata={})` is set with a
            Recap-specific constant (FIELD_METADATA_TYPE) and a value of
            `type`, `list[type]`, or `list[field]`. Based on this metadata type
            hint, the parser determins how to parse an attribute. If no
            metadata type hint is specified, the attribute is passed through to
            the dataclass's constructor (or extra_attrs if it's not part of the
            dataclass's attributes).
            """

            obj = self._normalize_type(obj)

            if "alias" in obj:
                alias_stack.append(obj)

            if type_ := self.aliases.get(obj["type"]):
                # We've hit an alias.
                parsed_obj = type_
            elif type_cls := self.types.get(obj["type"]):
                # We've hit a registered type.
                attrs = {}
                extra_attrs = {}
                type_fields = {field.name: field for field in fields(type_cls)}

                for obj_attr_key, obj_attr_value in obj.items():
                    if field := type_fields.get(obj_attr_key):
                        # If object's attribute is in the dataclass, parse it.
                        match field.metadata.get(FIELD_METADATA_TYPE):
                            case "type":
                                # If the field is a Type, parse it.
                                attrs[obj_attr_key] = _parse_obj(obj_attr_value)
                            case "list[type]":
                                # If the field is list[Type], parse each Type.
                                attrs[obj_attr_key] = [
                                    _parse_obj(list_obj) for list_obj in obj_attr_value
                                ]
                            case "list[field]":
                                # If the field is list[Field] parse each Field.
                                struct_fields = []
                                for field_obj in obj_attr_value:
                                    # Strip name and default so they don't
                                    # appear in Field.type_'s extra_attrs.
                                    type_fields = {
                                        fk: field_obj[fk]
                                        for fk in field_obj
                                        if fk not in ["name", "default"]
                                    }
                                    # Convert each field {} obj to a Field.
                                    struct_fields.append(
                                        Field(
                                            name=field_obj.get("name"),
                                            default=(
                                                Literal(field_obj["default"])
                                                if "default" in field_obj
                                                else None
                                            ),
                                            type_=_parse_obj(type_fields),
                                        )
                                    )
                                attrs[obj_attr_key] = struct_fields
                            case _:
                                # This is some other type, so pass it through.
                                attrs[obj_attr_key] = obj_attr_value
                    elif obj_attr_key != "type":
                        # This is an attribute that's not part of the Type, so
                        # pass it through as an extra atribute. Exclude "type"
                        # from extra_attrs because the type is `type_cls`,
                        # itself.
                        extra_attrs[obj_attr_key] = obj_attr_value

                # Create the actual object with all of the parsed attributes.
                parsed_obj = type_cls(**attrs, extra_attrs=extra_attrs)
            elif proxy_obj := next(
                filter(lambda o: o["alias"] == obj["type"], alias_stack),
                None,
            ):
                # We've hit a cyclic reference.
                parsed_obj = ProxyType(
                    obj=proxy_obj,
                    parser=self,
                )
            else:
                raise ValueError(f"Unable to parse object={obj}")

            if alias := obj.get("alias"):
                alias_stack.pop()
                if alias not in self.aliases:
                    # Shallow copy `parsed_obj` because it could be an object
                    # from `self.aliases` dictionary. Aliases of aliases are
                    # allowed, and can have their own docstring. Copying the
                    # object allows us to set these attributes without
                    # clobbering the parent object.
                    parsed_obj = copy(parsed_obj)
                    parsed_obj.alias = alias
                    parsed_obj.doc = obj.get("doc")
                    self.aliases[alias] = parsed_obj

            return parsed_obj

        return _parse_obj(obj)

    def to_obj(self, type_: Type) -> dict[str, Any] | list | str:
        """
        Converts a Recap type to a standard Python object.

        :param type_: A Recap `Type` to convert to a standard Python object.
        :returns: A type object. Can be a dictionary like `{"type": "int32"}`,
            a string like `"int32"`, or a union list like `["null", "int32"]`.
        """

        if isinstance(type_, ProxyType):
            # If the type_ is a ProxyType, short-circuit and return a basic
            # type dict with "type" set (and "alias" and "doc" if they're not
            # None). The object's type is always the alias of the object it
            # wraps.
            proxy_obj = {"type": type_.obj["alias"]}
            if alias := type_.alias:
                proxy_obj["alias"] = alias
            if doc := type_.doc:
                proxy_obj["doc"] = doc
            return proxy_obj

        obj: dict[str, Any] = {"type": type_.default_alias}

        for field in fields(type_):
            # Put fields into `obj` if they aren't type's default value.
            if (obj_attr := getattr(type_, field.name)) and obj_attr != field.default:
                match field.metadata.get(FIELD_METADATA_TYPE):
                    case "type":
                        obj_attr = self.to_obj(obj_attr)
                    case "list[type]":
                        obj_attr = [self.to_obj(list_obj) for list_obj in obj_attr]
                    case "list[field]":
                        field_objs = []
                        for struct_field in obj_attr:
                            field_obj = {"name": struct_field.name}
                            if default := struct_field.default:
                                field_obj["default"] = default.value
                            field_type_obj = self.to_obj(struct_field.type_)
                            if isinstance(field_type_obj, dict):
                                # If the nested object is a dictionary, merge
                                # it into the main level.
                                field_obj |= field_type_obj
                            else:
                                # Otherwise, it's a list/str, so set "type".
                                field_obj["type"] = field_type_obj
                            field_objs.append(field_obj)
                        obj_attr = field_objs
                obj[field.name] = obj_attr
        if len(obj) == 1:
            # If obj only has "type" then use the string (or list) form.
            return obj["type"]
        if obj["type"] == "union" and len(obj) == 2:
            # If obj is a union with only "symbols" set, use list form.
            return obj["types"]
        return obj

    def _normalize_type(
        self,
        obj: dict[str, Any] | str | list,
    ) -> dict[str, Any]:
        """
        Converts types with syntatic sugar to a standard dictionary object with
        a `"type"` field.

        :param obj: A type object. Can be a dictionary like
            `{"type": "int32"}`, a string like `"int32"`, or a union list like
            `[None, "int32"]`.
        :returns: A standard `{"type": ...}` dictionary.
        """

        match obj:
            case str(type_str):
                return {"type": type_str}
            case list(types):
                return {"type": "union", "types": types}
            case {"type": list(types), **rest}:
                return rest | {"type": "union", "types": types}
            case _:
                return obj
