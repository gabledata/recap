from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, fields
from typing import Any

from recap import types
from recap.converters.converter import Converter


@dataclass(kw_only=True)
class ProxyType(types.Type):
    obj: dict[str, Any]
    converter: RecapConverter


class RecapConverter(Converter):
    """
    Parser parses Recap schema objects, which can be `dict`, `str`, or `list`
    types, and returns a `Type`.

    The parser includes the default built-in derived types. Other types may be
    included using the `register_type` or `register_alias` methods.

    WARN: This parser is not thread-safe.
    """

    def __init__(self):
        self.types: dict[str, type[types.Type]] = {}
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
        self.register_type(types.Null)
        self.register_type(types.Bool)
        self.register_type(types.Int)
        self.register_type(types.Float)
        self.register_type(types.String)
        self.register_type(types.Bytes)

        # Built-in complex types
        self.register_type(types.List)
        self.register_type(types.Map)
        self.register_type(types.Struct)
        self.register_type(types.Enum)
        self.register_type(types.Union)

        # Built-in derived types
        self.register_type(types.Int8)
        self.register_type(types.Uint8)
        self.register_type(types.Int16)
        self.register_type(types.Uint16)
        self.register_type(types.Int32)
        self.register_type(types.Uint32)
        self.register_type(types.Int64)
        self.register_type(types.Uint64)
        self.register_type(types.Float16)
        self.register_type(types.Float32)
        self.register_type(types.Float64)
        self.register_type(types.String32)
        self.register_type(types.String64)
        self.register_type(types.Bytes32)
        self.register_type(types.Bytes64)
        self.register_type(types.UUID)
        self.register_type(types.Decimal)
        self.register_type(types.Decimal128)
        self.register_type(types.Decimal256)
        self.register_type(types.Duration64)
        self.register_type(types.Interval128)
        self.register_type(types.Time32)
        self.register_type(types.Time64)
        self.register_type(types.Timestamp64)
        self.register_type(types.Date32)
        self.register_type(types.Date64)
        self.register_type(types.Decimal)

    def register_type(self, type_cls: type[types.Type]):
        """
        Register a new type with the parser.

        :param type_cls: The type class to register.
        """

        self.types[type_cls.default_alias] = type_cls

    def to_recap_type(self, obj: dict[str, Any] | str | list, **_) -> types.Type:
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

        def _parse_obj(obj: dict[str, Any] | str | list) -> types.Type:
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
            resolve_obj = self._resolve_obj(obj)

            if obj.get("alias"):
                alias_stack.append(obj)

            if TypeClass := self.types.get(resolve_obj["type"]):
                # We've hit a registered type.
                attrs = {}
                extra_attrs = {}
                type_fields = {field.name: field for field in fields(TypeClass)}

                for obj_attr_key, obj_attr_value in resolve_obj.items():
                    if field := type_fields.get(obj_attr_key):
                        # If object's attribute is in the dataclass, parse it.
                        match field.metadata.get(types.FIELD_METADATA_TYPE):
                            case "type":
                                # If the field is a Type, parse it.
                                attrs[obj_attr_key] = _parse_obj(obj_attr_value)
                            case "list[type]":
                                # If the field is list[Type], parse each Type.
                                attrs[obj_attr_key] = [
                                    _parse_obj(list_obj) for list_obj in obj_attr_value
                                ]
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
                parsed_obj = TypeClass(**attrs, extra_attrs=extra_attrs)
            elif proxy_obj := next(
                filter(lambda o: o["alias"] == obj["type"], alias_stack),
                None,
            ):
                # We've hit a cyclic reference.
                parsed_obj = ProxyType(
                    obj=proxy_obj,
                    converter=self,
                    extra_attrs=obj,
                )
            else:
                raise ValueError(f"Unable to parse object={obj}")

            if obj.get("alias"):
                alias_stack.pop()
                self._register_alias(obj)

            return parsed_obj

        return _parse_obj(obj)

    def from_recap_type(self, type_: types.Type) -> dict[str, Any] | list | str:
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
            if extra_attrs := type_.extra_attrs:
                proxy_obj |= extra_attrs
            return proxy_obj

        obj: dict[str, Any] = {"type": type_.default_alias}

        for field in fields(type_):
            # Put fields into `obj` if they aren't type's default value.
            if (obj_attr := getattr(type_, field.name)) and obj_attr != field.default:
                match field.metadata.get(types.FIELD_METADATA_TYPE):
                    case "type":
                        obj_attr = self.from_recap_type(obj_attr)
                    case "list[type]":
                        obj_attr = [
                            self.from_recap_type(list_obj) for list_obj in obj_attr
                        ]
                if field.name == "extra_attrs" and isinstance(obj_attr, dict):
                    obj |= obj_attr
                else:
                    obj[field.name] = obj_attr
        if len(obj) == 1:
            # If obj only has "type" then use the string (or list) form.
            return obj["type"]
        if obj["type"] == "union" and len(obj) == 2:
            # If obj is a union with only "symbols" set, use list form.
            return obj["types"]
        return obj

    def _resolve_obj(self, obj: dict[str, Any]) -> dict[str, Any]:
        resolved_obj = deepcopy(obj)

        if alias_attrs := self.aliases.get(obj["type"]):
            # Inherit parent attributes that don't exist in the obj.
            resolved_obj = alias_attrs | resolved_obj
            # But force type to be the parent type (not the alias).
            resolved_obj["type"] = alias_attrs["type"]

            if "alias" not in resolved_obj:
                # Get rid of the alias since we don't allow alias inheritance.
                resolved_obj.pop("alias", None)

        return resolved_obj

    def _register_alias(self, obj):
        if alias := obj.get("alias"):
            if alias in self.aliases:
                raise ValueError(f"Alias={alias} already exists.")

            if obj["type"] not in self.types:
                raise ValueError(
                    f"Alias={alias} of alias={obj['type']} is not allowed."
                )

            self.aliases[alias] = deepcopy(obj)
        else:
            raise ValueError(f"Unable to find alias for for obj={obj}")

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


# Global Recap converter. Extra types can be registered on this instance.
CONVERTER = RecapConverter()
