from __future__ import annotations

from enum import Enum as PyEnum
from enum import auto
from typing import Any

from pydantic import BaseModel

#
# Base types
#


class Type(BaseModel):
    alias: str | None = None
    doc: str | None = None

    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Type)


class Null(Type):
    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Null)


class Bool(Type):
    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Bool)


class Int(Type):
    bits: int
    signed: bool

    def subsumes(self, other: Type) -> bool:
        return (
            isinstance(other, Int)
            and self.bits >= other.bits
            and self.signed == other.signed
        )


class Float(Type):
    bits: int

    def subsumes(self, other: Type) -> bool:
        return (
            isinstance(other, Float)
            and self.bits >= other.bits
        )


class String(Type):
    bytes: int
    variable: bool = True

    def subsumes(self, other: Type) -> bool:
        return (
            isinstance(other, String)
            and self.bytes >= other.bytes
        )


class Bytes(Type):
    bytes: int
    variable: bool = True

    def subsumes(self, other: Type) -> bool:
        return (
            isinstance(other, Bytes)
            and self.bytes >= other.bytes
        )


class List(Type):
    values: Type
    bits: int | None = None


class Map(Type):
    keys: Type
    values: Type


class Struct(Type):
    fields: list[Field] = []
    name: str | None = None

    # TODO This is certainly wrong.
    # Needs tests.
    # Do we check struct name as part of equality?
    # Do we check field defaults as part of equality?
    # We probably care about field ordering here, too.
    def subsumes(self, other: Type) -> bool:
        if isinstance(other, Struct):
            for field in other.fields:
                if field not in self.fields:
                    return False
            return True
        return False


class Field(BaseModel):
    type_: Type
    name: str | None = None
    default: DefaultValue | None = None
    """
    The default value for a reader if the field is not set in the struct.

    Defaults are tricky because you need to differentiate between an unset
    default and a null default. Here, we treat None as unset, and a
    DefaultValue(value=None) as a default value of null.
    """


class DefaultValue(BaseModel):
    value: Any = None


class Enum(Type):
    symbols: list[str]

    # TODO What if this enum is a superset?
    # This needs thought.
    def subsumes(self, other: Type) -> bool:
        if isinstance(other, Enum):
            return other.symbols == self.symbols
        return False


class Union(Type):
    types: list[Type]

    def subsumes(self, other: Type) -> bool:
        if isinstance(other, Union):
            for other_type in other.types:
                if other_type not in self.types:
                    return False
        return False


#
# Units
#


class TimeUnit(str, PyEnum):
    YEAR = auto()
    MONTH = auto()
    DAY = auto()
    HOUR = auto()
    MINUTE = auto()
    SECOND = auto()
    MILLISECOND = auto()
    MICROSECOND = auto()
    NANOSECOND = auto()
    PICOSECOND = auto()


#
# Derived types
#


class Int8(Int):
    bits: int = 8
    signed: int = True


class Uint8(Int):
    bits: int = 8
    signed: int = False


class Int16(Int):
    bits: int = 16
    signed: int = True


class Uint16(Int):
    bits: int = 16
    signed: int = False


class Int32(Int):
    bits: int = 32
    signed: int = True


class Uint32(Int):
    bits: int = 32
    signed: int = False


class Int64(Int):
    bits: int = 64
    signed: int = True


class Uint64(Int):
    bits: int = 64
    signed: int = False


class Float16(Float):
    bits: int = 16


class Float32(Float):
    bits: int = 32


class Float64(Float):
    bits: int = 64


class String32(String):
    bytes: int = 2_147_483_647


class String64(String):
    bytes: int = 9_223_372_036_854_775_807


class Bytes32(Bytes):
    bytes: int = 2_147_483_647


class Bytes64(Bytes):
    bytes: int = 9_223_372_036_854_775_807


class Decimal(Bytes32):
    """
    A decimal is a variable-length byte-array in the style of Avro. See:

    https://avro.apache.org/docs/1.10.2/spec.html#Decimal

    Since Recap doesn't care about serialization, Decimal doens't care about
    endianness byte-order.

    Theoretically, Decimal could extend Float rather than byte. This became
    unwieldy because max/min are very, very big. I think the byte size is more
    important, and Bytes measures that better.

    Decimal is (technically) not unbounded. It has a max precision of:

    ```
    floor(log10(2 ** (8 * INT_MAX - 1) - 1))
    ```
    """

    precision: int
    """
    Total number of digits (as an Int32). 123.456 has a precision of 6.
    """

    scale: int
    """
    Digits to the right of the decimal point (as an int32). 123.456 has a scale
    of 3.
    """


class Decimal128(Decimal):
    """
    A 128-bit decimal. This is not an IEEE-754 quad, which supports
    approximately 34 digits of precision:

    https://en.wikipedia.org/wiki/Quadruple-precision_floating-point_format

    It's modeling the same encoding as Decimal, which supports:

    ```
    floor(log10(2 ** (8 * byte_length - 1) - 1))
    ```

    Base-10 digits of precision (according to Avro's Decimal docs). Thus:

    ```
    floor(log10(2 ** (8 * 16 - 1) - 1))
    ```

    Is 38 digits of precision (the 16 comes from 128 bits / 8 bits per-byte).
    """

    pass


class Decimal256(Decimal):
    """
    Models the same style of decimal as Decimal128, but in a 256-bit array.

    Using the same formula as Decimal128, the max precision is 76 digits.
    """

    pass


class Duration64(Int64):
    """
    A length of time without timezones and leap seconds.
    """

    unit: TimeUnit


class Interval128(Bytes):
    """
    An interval of time on a calendar. This measurement allows you to measure
    time without worrying about leap seconds, leap years, and time changes.
    Years, quarters, hours, and minutes can be expressed using this type.

    The interval is measured in months, days, and an intra-day time
    measurement. Months and days are each 32-bit signed integers. The remainder
    is a 64-bit signed integer. Leap seconds are ignored.
    """

    bits: int = 128
    unit: TimeUnit


class Timestamp64(Int64):
    """
    Time since the UNIX epoch without timezones and leap seconds.
    """

    unit: TimeUnit

    # TODO Use Olson timezone database strings?
    timezone: str | None = None


class Time32(Int32):
    """
    Time since midnight without timezones and leap seconds.
    """

    unit: TimeUnit


class Time64(Int64):
    """
    Time since midnight without timezones and leap seconds.
    """

    unit: TimeUnit


class Date32(Int32):
    """
    Days since the UNIX epoch without timezones and leap seconds.
    """

    unit: TimeUnit


class Date64(Int64):
    """
    Days since the UNIX epoch without timezones and leap seconds.
    """

    unit: TimeUnit


# Struct references Field, so it needs to update its refs.
Struct.update_forward_refs()

# Field references DefaultValue, so it needs to update its refs.
Field.update_forward_refs()
