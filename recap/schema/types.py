from __future__ import annotations

import decimal
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
    default: DefaultValue | None = None
    """
    Defaults are tricky because you need to differentiate between an unset
    default and a null default. Here, we treat None as unset, and a
    DefaultValue(value=None) as a default value of null.
    """

    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Type)


class DefaultValue(BaseModel):
    value: Any = None


class Null(Type):
    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Null)


class Bool(Type):
    def subsumes(self, other: Type) -> bool:
        return isinstance(other, Bool)


class Int(Type):
    min_: int | None = None
    max_: int | None = None

    def subsumes(self, other: Type) -> bool:
        match other:
            case Int() | Float():
                if self.max_ and (not other.max_ or other.max_ > self.max_):
                    return False
                if self.min_ and (not other.min_ or other.min_ < self.min_):
                    return False
                return True
            case _:
                return False


class Float(Type):
    min_: decimal.Decimal | None = None
    max_: decimal.Decimal | None = None

    def subsumes(self, other: Type) -> bool:
        match other:
            case Int() | Float():
                if self.max_ and (not other.max_ or other.max_ > self.max_):
                    return False
                if self.min_ and (not other.min_ or other.min_ < self.min_):
                    return False
                return True
            case _:
                return False


class String(Type):
    min_length: int = 0
    max_length: int | None = None

    def subsumes(self, other: Type) -> bool:
        if isinstance(other, String):
            if other.min_length < self.min_length:
                return False
            if self.max_length and (
                not other.max_length or other.max_length > self.max_length
            ):
                return False
            return True
        return False


class Bytes(Type):
    min_length: int = 0
    max_length: int | None = None

    def subsumes(self, other: Type) -> bool:
        if isinstance(other, Bytes):
            if other.min_length < self.min_length:
                return False
            if self.max_length and (
                not other.max_length or other.max_length > self.max_length
            ):
                return False
            return True
        return False


class List(Type):
    values: Type
    min_length: int = 0
    max_length: int | None = None

    def subsumes(self, other: Type) -> bool:
        if isinstance(other, List):
            if not self.values.subsumes(other.values):
                return False
            if other.min_length < self.min_length:
                return False
            if self.max_length and (
                not other.max_length or other.max_length > self.max_length
            ):
                return False
            return True
        return False


class Map(Type):
    keys: Type
    values: Type


class Struct(Type):
    fields: list[Field] = []
    name: str | None = None


class Field(BaseModel):
    type_: Type
    name: str | None = None


class Enum(Type):
    symbols: list[Any]


class Union(Type):
    types: list[Type]


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
    min_: int = -128
    max_: int = 127


class Uint8(Int):
    min_: int = 0
    max_: int = 255


class Int16(Int):
    min_: int = -32_768
    max_: int = 32_767


class Uint16(Int):
    min_: int = 0
    max_: int = 65536


class Int32(Int):
    min_: int = -2_147_483_648
    max_: int = 2_147_483_647


class Uint32(Int):
    min_: int = 0
    max_: int = 4_294_967_296


class Int64(Int):
    min_: int = -9_223_372_036_854_775_808
    max_: int = 9_223_372_036_854_775_807


class Uint64(Int):
    min_: int = 0
    max_: int = 18_446_744_073_709_551_615


class Float16(Float):
    min_: decimal.Decimal = decimal.Decimal("-6.550400e04")
    max_: decimal.Decimal = decimal.Decimal("6.550400e04")


class Float32(Float):
    min_: decimal.Decimal = decimal.Decimal(
        "-3.40282346638528859811704183484516925440e+38"
    )
    max_: decimal.Decimal = decimal.Decimal(
        "3.40282346638528859811704183484516925440e+38"
    )


class Float64(Float):
    min_: decimal.Decimal = decimal.Decimal(
        "-1.797693134862315708145274237317043567981e+308"
    )
    max_: decimal.Decimal = decimal.Decimal(
        "1.797693134862315708145274237317043567981e+308"
    )


class String32(String):
    max_length: int = 2_147_483_647


class String64(String):
    max_length: int = 9_223_372_036_854_775_807


class Bytes32(Bytes):
    max_length: int = 2_147_483_647


class Bytes64(Bytes):
    max_length: int = 9_223_372_036_854_775_807


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

    min_length: int = 128
    max_length: int = 128


class Decimal256(Decimal):
    """
    Models the same style of decimal as Decimal128, but in a 256-bit array.

    Using the same formula as Decimal128, the max precision is 76 digits.
    """

    min_length: int = 256
    max_length: int = 256


class Duration(Type):
    """
    A length of time without timezones and leap seconds.
    """

    min_: int | None = None
    max_: int | None = None
    unit: TimeUnit


class Interval(Type):
    """
    An interval of time on a calendar. This measurement allows you to measure
    time without worrying about leap seconds, leap years, and time changes.
    Years, quarters, hours, and minutes can be expressed using this type.
    """

    months_min: int
    months_max: int
    days_min: int
    days_max: int
    remainder_min: int
    remainder_max: int
    unit: TimeUnit


class Timestamp(Duration):
    """
    Time since the UNIX epoch without timezones and leap seconds.
    """

    # TODO Use Olson timezone database strings?
    timezone: str | None = None


class Time(Duration):
    """
    Time since midnight without timezones and leap seconds.
    """

    pass


class Date(Duration):
    """
    Days since the UNIX epoch without timezones and leap seconds.
    """

    unit: TimeUnit = TimeUnit.DAY


# Struct references Field, so it needs to update its refs.
Struct.update_forward_refs()

# Type references DefaultValue, so it needs to update its refs.
Type.update_forward_refs()
