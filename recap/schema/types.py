from __future__ import annotations

from dataclasses import dataclass, field
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
    fields: list[Type] = field(metadata={FIELD_METADATA_TYPE: "list[type]"})


@dataclass(kw_only=True)
class Enum(Type):
    default_alias: ClassVar[str] = "enum"
    symbols: list[str]


@dataclass(kw_only=True)
class Union(Type):
    default_alias: ClassVar[str] = "union"
    types: list[Type] = field(metadata={FIELD_METADATA_TYPE: "list[type]"})


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
