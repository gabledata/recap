from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import Any, ClassVar


@dataclass(init=False)
class Type(ABC):
    alias: ClassVar[str]
    extra_attrs: dict[str, Any] = field(default_factory=dict)

    def __init__(self, **extra_attrs: dict[str, Any]):
        self.extra_attrs = extra_attrs

    @classmethod
    def parse_obj(cls, obj: dict[str, Any], parser: Parser) -> Type:
        match obj:
            case {"type": type_str, **extra_attrs} if type_str == cls.alias:
                return cls(**extra_attrs)
            case _:
                raise ValueError(f"Unable to parse `{cls.alias}` for object={obj}")


@dataclass
class Null(Type):
    alias: ClassVar[str] = "null"


@dataclass
class Bool(Type):
    alias: ClassVar[str] = "bool"


@dataclass(init=False)
class Int(Type):
    alias: ClassVar[str] = "int"
    bits: int
    signed: bool = True

    def __init__(self, bits: int, signed: bool = True, **extra_attrs):
        super().__init__(**extra_attrs)
        self.bits = bits
        self.signed = signed


@dataclass(init=False)
class Float(Type):
    alias: ClassVar[str] = "float"
    bits: int

    def __init__(self, bits: int, **extra_attrs):
        super().__init__(**extra_attrs)
        self.bits = bits


@dataclass(init=False)
class String(Type):
    alias: ClassVar[str] = "string"
    bytes: int
    variable: bool = True

    def __init__(self, bytes: int, variable: bool = True, **extra_attrs):
        super().__init__(**extra_attrs)
        self.bytes = bytes
        self.variable = variable


@dataclass(init=False)
class Bytes(Type):
    alias: ClassVar[str] = "bytes"
    bytes: int
    variable: bool = True

    def __init__(self, bytes: int, variable: bool = True, **extra_attrs):
        super().__init__(**extra_attrs)
        self.bytes = bytes
        self.variable = variable


@dataclass(init=False)
class List(Type):
    alias: ClassVar[str] = "list"
    values: Type
    bits: int | None = None

    def __init__(self, values: Type, bits: int | None = None, **extra_attrs):
        super().__init__(**extra_attrs)
        self.values = values
        self.bits = bits

    @classmethod
    def parse_obj(cls, obj: dict[str, Any], parser: Parser) -> Type:
        match obj:
            case {
                "type": type_str,
                "values": str(values_obj) | dict(values_obj),
                **extra_attrs,
            } if type_str == cls.alias:
                values_type = parser.parse_obj(values_obj)
                return List(values_type, **extra_attrs)
            case _:
                raise ValueError(f"Unable to parse `{cls.alias}` for object={obj}")


@dataclass(init=False)
class Map(Type):
    alias: ClassVar[str] = "map"
    keys: Type
    values: Type

    def __init__(self, keys: Type, values: Type, **extra_attrs):
        super().__init__(**extra_attrs)
        self.keys = keys
        self.values = values

    @classmethod
    def parse_obj(cls, obj: dict[str, Any], parser: Parser) -> Type:
        match obj:
            case {
                "type": type_str,
                "keys": str(keys_obj) | dict(keys_obj),
                "values": str(values_obj) | dict(values_obj),
                **extra_attrs,
            } if type_str == cls.alias:
                keys_type = parser.parse_obj(keys_obj)
                values_type = parser.parse_obj(values_obj)
                return Map(keys_type, values_type, **extra_attrs)
            case _:
                raise ValueError(f"Unable to parse `{cls.alias}` for object={obj}")


@dataclass(init=False)
class Struct(Type):
    alias: ClassVar[str] = "struct"
    fields: list[Field]

    def __init__(self, fields: list[Field], **extra_attrs):
        super().__init__(**extra_attrs)
        self.fields = fields

    @classmethod
    def parse_obj(cls, obj: dict[str, Any], parser: Parser) -> Type:
        match obj:
            case {
                "type": type_str,
                "fields": list(field_objs),
                **extra_attrs,
            } if type_str == cls.alias:
                fields = []
                for field_obj in field_objs:
                    fields.append(
                        Field(
                            name=field_obj.get("name"),
                            type_=parser.parse_obj(field_obj),
                            default=(
                                Literal(field_obj["default"])
                                if "default" in field_obj
                                else None
                            ),
                        )
                    )
                return Struct(fields, **extra_attrs)
            case _:
                raise ValueError(f"Unable to parse `{cls.alias}` for object={obj}")


@dataclass
class Field:
    type_: Type
    name: str | None = None
    default: Literal | None = None


@dataclass
class Literal:
    value: Any


@dataclass(init=False)
class Enum(Type):
    alias: ClassVar[str] = "enum"
    symbols: list[str]

    def __init__(self, symbols: list[str], **extra_attrs):
        super().__init__(**extra_attrs)
        self.symbols = symbols


@dataclass(init=False)
class Union(Type):
    alias: ClassVar[str] = "union"
    types: list[Type]

    def __init__(self, types: list[Type], **extra_attrs):
        super().__init__(**extra_attrs)
        self.types = types

    @classmethod
    def parse_obj(cls, obj: dict[str, Any], parser: Parser) -> Type:
        match obj:
            case {
                "type": type_str,
                "types": list(types_obj),
                **extra_attrs,
            } if type_str == cls.alias:
                types = []
                for type_obj in types_obj:
                    types.append(parser.parse_obj(type_obj))
                return Union(types, **extra_attrs)
            case _:
                raise ValueError(f"Unable to parse `{cls.alias}` for object={obj}")


@dataclass(init=False)
class ProxyType(Type):
    obj: dict[str, Any]
    parser: Parser

    def __init__(self, obj: dict[str, Any], parser: Parser, **extra_attrs):
        super().__init__(**extra_attrs)
        self.obj = obj
        self.parser = parser

    @classmethod
    def parse_obj(cls, obj: dict[str, Any], parser: Parser) -> Type:
        return ProxyType(obj, parser)


@dataclass(init=False)
class Decimal(Bytes):
    alias: ClassVar[str] = "decimal"
    precision: int
    scale: int

    def __init__(
        self,
        precision: int,
        scale: int,
        bytes: int = 2_147_483_647,
        **extra_attrs,
    ):
        super().__init__(bytes=bytes, **extra_attrs)
        self.precision = precision
        self.scale = scale


@dataclass(init=False)
class Timestamp64(Int):
    alias: ClassVar[str] = "timestamp64"
    zone: str

    def __init__(self, zone: str, bits: int = 64, **extra_attrs):
        super().__init__(bits=bits, **extra_attrs)
        self.zone = zone


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

        self.aliases: dict[str, dict[str, Any]] = {}
        """
        A map from aliases to bound attributes for the alias.

        ```
        'bytes32': {'bytes': 2147483647, 'type': 'bytes'},
        'bytes64': {'bytes': 9223372036854775807, 'type': 'bytes'},
        'float16': {'bits': 16, 'type': 'float'},
        'float32': {'bits': 32, 'type': 'float'},
        'float64': {'bits': 64, 'type': 'float'},
        'int16': {'bits': 16, 'type': 'int'},
        'int32': {'bits': 32, 'type': 'int'},
        'int64': {'bits': 64, 'type': 'int'},
        'int8': {'bits': 8, 'type': 'int'},
        'string32': {'bytes': 2147483647, 'type': 'string'},
        'string64': {'bytes': 9223372036854775807, 'type': 'string'},
        'uint16': {'bits': 16, 'signed': False, 'type': 'int'},
        'uint32': {'bits': 32, 'signed': False, 'type': 'int'},
        'uint64': {'bits': 64, 'signed': False, 'type': 'int'},
        'uint8': {'bits': 8, 'signed': False, 'type': 'int'},
        ```

        Alias attributes must be kept so they can be underlayed to create a
        concrete type. For example, a type like this:

        ```
        {
            "type": "struct",
            "fields": [
                {
                    "type": "int32",
                    "bits": 24,
                    "alias": "int24",
                },
                {
                    "type": "int24",
                    "signed": False,
                    "alias": "uint24",
                },
            ]
        }
        ```

        Will resolve `uint24` to an `int` with `signed=False` and `bits=24`.
        Since these aliases are declared at runtime, the bound attributes must
        be kept in this `self.aliases` dictionary so we can resolve them
        just-in-time.
        """

        self.alias_stack = []
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

        WARN: This stack is not thread-safe.
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
        self.register_alias(
            "int8",
            {"type": "int", "bits": 8},
        )
        self.register_alias(
            "uint8",
            {"type": "int", "bits": 8, "signed": False},
        )
        self.register_alias(
            "int16",
            {"type": "int", "bits": 16},
        )
        self.register_alias(
            "uint16",
            {"type": "int", "bits": 16, "signed": False},
        )
        self.register_alias(
            "int32",
            {"type": "int", "bits": 32},
        )
        self.register_alias(
            "uint32",
            {"type": "int", "bits": 32, "signed": False},
        )
        self.register_alias(
            "int64",
            {"type": "int", "bits": 64},
        )
        self.register_alias(
            "uint64",
            {"type": "int", "bits": 64, "signed": False},
        )
        self.register_alias(
            "float16",
            {"type": "float", "bits": 16},
        )
        self.register_alias(
            "float32",
            {"type": "float", "bits": 32},
        )
        self.register_alias(
            "float64",
            {"type": "float", "bits": 64},
        )
        self.register_alias(
            "string32",
            {"type": "string", "bytes": 2_147_483_647},
        )
        self.register_alias(
            "string64",
            {"type": "string", "bytes": 9_223_372_036_854_775_807},
        )
        self.register_alias(
            "bytes32",
            {"type": "bytes", "bytes": 2_147_483_647},
        )
        self.register_alias(
            "bytes64",
            {"type": "bytes", "bytes": 9_223_372_036_854_775_807},
        )
        self.register_type(Decimal)
        self.register_type(Timestamp64)

    def register_type(self, type_cls: type[Type]):
        """
        Register a new type with the parser.

        :param type_cls: The type class to register.
        """

        self.types[type_cls.alias] = type_cls

    def register_alias(self, alias: str, attrs: dict[str, Any] = {}):
        """
        Register a new alias with the parser.

        :param alias: The name of the alias.
        :param attrs: A dictionary of bound attributes for the alias. The
            `attrs` may be a partial; that is, they need not be a complete set
            of attributes required when instantiating the alias's base class.
        """

        self.aliases[alias] = attrs

    def parse_obj(self, obj: dict[str, Any] | str | list) -> Type:
        """
        Parse a Recap type object and return a `Type` object.

        :param obj: A type object. Can be a dictionary like
            `{"type": "int32"}`, a string like `"int32"`, or a union list like
            `[None, "int32"]`.
        :returns: A Recap `Type` that's equivalent to the supplied `obj`.
        """

        obj = self._normalize_type(obj)
        type_str = obj["type"]
        inheritance = [type_str]
        # Exclude alias so types that derive this object won't accidentally
        # inherit it.
        obj_attrs = {k: obj[k] for k in obj if k not in "alias"}

        # Walk the alias inheritance tree and underlay inherited attributes.
        while alias_attrs := self.aliases.get(type_str):
            # Pop the type so we inherit the alias's type on each iteration.
            # This is important because we want type_str and type_cls to both
            # point to a conceret root type, not a type alias.
            type_str = obj_attrs.pop("type")
            obj_attrs = alias_attrs | obj_attrs
            inheritance.append(type_str)

        # Once we hit a type_str not in aliases, assume it's a root type. Get
        # the class.
        type_cls = self.types[type_str]

        # If this object inherits from an alias that we haven't yet fully
        # resolved, use a proxy.
        if inherited_cls := set(inheritance) & set(self.alias_stack):
            type_cls = ProxyType
            # Re-add the alias since ProxyType needs the original object when
            # it tries to parse in the future.
            obj_attrs["alias"] = inherited_cls.pop()

        if alias := obj.get("alias"):
            self.alias_stack.append(alias)
            self.aliases[alias] = obj

        parsed_obj = type_cls.parse_obj(obj_attrs, self)

        if alias := obj.get("alias"):
            self.alias_stack.pop()

        return parsed_obj

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
