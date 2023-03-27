from typing import Any

from recap.schema.types import Type


class Converter:
    """
    Converts to and from Recap types. Neither method is abstract because
    converters can implement either one, or both.
    """

    def to_recap_type(self, **_) -> Type:
        """
        Convert a schema object (Avro, Proto, table DDL, Parquet, and so on)
        to a RecapType.
        """

        raise NotImplementedError

    def from_recap_type(self, type_: Type) -> Any:
        """
        Convert a Recap type to a schema object Avro, Proto, table DDL,
        Parquet, and so on).
        """

        raise NotImplementedError
