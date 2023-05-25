from __future__ import annotations

from importlib import import_module
from typing import Any, ClassVar

from recap.types import Type


class Converter:
    """
    Converts to and from Recap types. Neither method is abstract because
    converters can implement either one, or both.
    """

    _registry: ClassVar[dict[str, str | Converter]] = {}
    """
    An internal registry of converters. The key is the name of the converter
    ("avro", "protobuf", etc). The value is a Converter instance or a string of
    the form `package.module.klass`.
    """

    def to_recap_type(self, *args, **kwargs) -> Type:
        """
        Convert a schema object (Avro, Proto, table DDL, Parquet, and so on)
        to a RecapType.
        """

        raise NotImplementedError

    def from_recap_type(self, type_: Type, **kwargs) -> Any:
        """
        Convert a Recap type to a schema object Avro, Proto, table DDL,
        Parquet, and so on).
        """

        raise NotImplementedError

    @classmethod
    def register_converter(cls, name: str, converter: str | Converter):
        """
        :param converter: A conveter instance or a string of the form
            `package.module.klass`.
        """

        cls._registry[name] = converter

    @classmethod
    def get_converter(cls, name: str) -> Converter:
        if converter := cls._registry[name]:
            if isinstance(converter, str):
                mod, converter_class_name = converter.rsplit(".", 1)
                mod = import_module(mod)
                converter = getattr(mod, converter_class_name)()
                cls._registry[name] = converter
            return converter
        raise ValueError(f"Unable to find converter for name={name}")
